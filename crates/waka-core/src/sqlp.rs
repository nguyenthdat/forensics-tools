use std::{
    borrow::Cow,
    collections::HashMap,
    env,
    fs::File,
    io,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::Instant,
};

use anyhow::anyhow;
use polars::{
    datatypes::PlSmallStr,
    io::avro::{AvroWriter, Compression as AvroCompression},
    prelude::{
        Arc, CsvWriter, DataFrame, GzipLevel, IpcCompression, IpcWriter, JsonFormat, JsonWriter,
        LazyCsvReader, LazyFileListReader, NullValues, OptFlags, ParquetCompression, ParquetWriter,
        Schema, SerWriter, StatisticsOptions, ZstdLevel,
    },
    sql::SQLContext,
};
use polars_utils::plpath::PlPath;
use regex::Regex;
use serde::Deserialize;

use crate::{
    config::{Config, DEFAULT_WTR_BUFFER_CAPACITY, Delimiter},
    joinp::tsvssv_delim,
    util,
    util::process_input,
};

static DEFAULT_GZIP_COMPRESSION_LEVEL: u8 = 6;
static DEFAULT_ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Deserialize, Clone)]
pub struct Args {
    pub arg_input:                  Vec<PathBuf>,
    pub arg_sql:                    String,
    pub flag_format:                String,
    pub flag_try_parsedates:        bool,
    pub flag_infer_len:             usize,
    pub flag_cache_schema:          bool,
    pub flag_streaming:             bool,
    pub flag_low_memory:            bool,
    pub flag_no_optimizations:      bool,
    pub flag_ignore_errors:         bool,
    pub flag_truncate_ragged_lines: bool,
    pub flag_decimal_comma:         bool,
    pub flag_datetime_format:       Option<String>,
    pub flag_date_format:           Option<String>,
    pub flag_time_format:           Option<String>,
    pub flag_float_precision:       Option<usize>,
    pub flag_rnull_values:          String,
    pub flag_wnull_value:           String,
    pub flag_compression:           String,
    pub flag_compress_level:        Option<i32>,
    pub flag_statistics:            bool,
    pub flag_output:                Option<String>,
    pub flag_delimiter:             Option<Delimiter>,
    pub flag_quiet:                 bool,
}

#[derive(Default, Clone, PartialEq)]
pub enum OutputMode {
    #[default]
    Csv,
    Json,
    Jsonl,
    Parquet,
    Arrow,
    Avro,
    None,
}

// shamelessly copied from
// https://github.com/pola-rs/polars-cli/blob/main/src/main.rs
impl OutputMode {
    pub fn execute_query(
        &self,
        query: &str,
        ctx: &mut SQLContext,
        mut delim: u8,
        args: Args,
    ) -> anyhow::Result<(usize, usize)> {
        let mut df = DataFrame::default();
        let execute_inner = || {
            df = ctx
                .execute(query)
                .and_then(polars::prelude::LazyFrame::collect)?;

            // we don't want to write anything if the output mode is None
            if matches!(self, OutputMode::None) {
                return Ok(());
            }

            let float_precision = std::env::var("QSV_POLARS_FLOAT_PRECISION")
                .ok()
                .and_then(|s| s.parse().ok())
                .or(args.flag_float_precision);

            let w = match args.flag_output {
                Some(path) => {
                    delim = tsvssv_delim(path.clone(), delim);
                    Box::new(File::create(path)?) as Box<dyn Write>
                },
                None => Box::new(io::stdout()) as Box<dyn Write>,
            };
            let mut w = io::BufWriter::with_capacity(256_000, w);

            let out_result = match self {
                OutputMode::Csv => CsvWriter::new(&mut w)
                    .with_separator(delim)
                    .with_datetime_format(args.flag_datetime_format)
                    .with_date_format(args.flag_date_format)
                    .with_time_format(args.flag_time_format)
                    .with_float_precision(float_precision)
                    .with_null_value(args.flag_wnull_value)
                    .with_decimal_comma(args.flag_decimal_comma)
                    .include_bom(util::get_envvar_flag("QSV_OUTPUT_BOM"))
                    .finish(&mut df),
                OutputMode::Json => JsonWriter::new(&mut w)
                    .with_json_format(JsonFormat::Json)
                    .finish(&mut df),
                OutputMode::Jsonl => JsonWriter::new(&mut w)
                    .with_json_format(JsonFormat::JsonLines)
                    .finish(&mut df),
                OutputMode::Parquet => {
                    let compression: PqtCompression = args
                        .flag_compression
                        .parse()
                        .unwrap_or(PqtCompression::Uncompressed);

                    let parquet_compression = match compression {
                        PqtCompression::Uncompressed => ParquetCompression::Uncompressed,
                        PqtCompression::Snappy => ParquetCompression::Snappy,
                        PqtCompression::Lz4Raw => ParquetCompression::Lz4Raw,
                        PqtCompression::Gzip => {
                            let gzip_level = args
                                .flag_compress_level
                                .unwrap_or_else(|| DEFAULT_GZIP_COMPRESSION_LEVEL.into())
                                as u8;
                            ParquetCompression::Gzip(Some(GzipLevel::try_new(gzip_level)?))
                        },
                        PqtCompression::Zstd => {
                            let zstd_level = args
                                .flag_compress_level
                                .unwrap_or(DEFAULT_ZSTD_COMPRESSION_LEVEL);
                            ParquetCompression::Zstd(Some(ZstdLevel::try_new(zstd_level)?))
                        },
                    };

                    let statistics_options = if args.flag_statistics {
                        StatisticsOptions {
                            min_value:      true,
                            max_value:      true,
                            distinct_count: true,
                            null_count:     true,
                        }
                    } else {
                        StatisticsOptions {
                            min_value:      false,
                            max_value:      false,
                            distinct_count: false,
                            null_count:     false,
                        }
                    };

                    ParquetWriter::new(&mut w)
                        .with_row_group_size(Some(768 ^ 2))
                        .with_statistics(statistics_options)
                        .with_compression(parquet_compression)
                        .finish(&mut df)
                        .map(|_| ())
                },
                OutputMode::Arrow => {
                    let compression: ArrowCompression = args
                        .flag_compression
                        .parse()
                        .unwrap_or(ArrowCompression::Uncompressed);

                    let ipc_compression: Option<IpcCompression> = match compression {
                        ArrowCompression::Uncompressed => None,
                        ArrowCompression::Lz4 => Some(IpcCompression::LZ4),
                        ArrowCompression::Zstd => Some(IpcCompression::ZSTD),
                    };

                    IpcWriter::new(&mut w)
                        .with_compression(ipc_compression)
                        .finish(&mut df)
                },
                OutputMode::Avro => {
                    let compression: QsvAvroCompression = args
                        .flag_compression
                        .parse()
                        .unwrap_or(QsvAvroCompression::Uncompressed);

                    let avro_compression = match compression {
                        QsvAvroCompression::Uncompressed => None,
                        QsvAvroCompression::Deflate => Some(AvroCompression::Deflate),
                        QsvAvroCompression::Snappy => Some(AvroCompression::Snappy),
                    };

                    AvroWriter::new(&mut w)
                        .with_compression(avro_compression)
                        .finish(&mut df)
                },
                OutputMode::None => Ok(()),
            };

            w.flush()?;
            out_result
        };

        match execute_inner() {
            Ok(()) => Ok(df.shape()),
            Err(e) => Err(anyhow!("Failed to execute query: {query}: {e}")),
        }
    }
}

impl FromStr for OutputMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "csv" => Ok(OutputMode::Csv),
            "json" => Ok(OutputMode::Json),
            "jsonl" => Ok(OutputMode::Jsonl),
            "parquet" => Ok(OutputMode::Parquet),
            "arrow" => Ok(OutputMode::Arrow),
            "avro" => Ok(OutputMode::Avro),
            _ => Err(format!("Invalid output mode: {s}")),
        }
    }
}

#[derive(Default, Copy, Clone)]
pub enum PqtCompression {
    Uncompressed,
    Gzip,
    Snappy,
    #[default]
    Zstd,
    Lz4Raw,
}
#[derive(Default, Copy, Clone)]
pub enum ArrowCompression {
    #[default]
    Uncompressed,
    Lz4,
    Zstd,
}

#[derive(Default, Copy, Clone)]
pub enum QsvAvroCompression {
    #[default]
    Uncompressed,
    Deflate,
    Snappy,
}

impl FromStr for PqtCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "uncompressed" => Ok(PqtCompression::Uncompressed),
            "gzip" => Ok(PqtCompression::Gzip),
            "snappy" => Ok(PqtCompression::Snappy),
            "lz4raw" => Ok(PqtCompression::Lz4Raw),
            "zstd" => Ok(PqtCompression::Zstd),
            _ => Err(format!("Invalid Parquet compression format: {s}")),
        }
    }
}

impl FromStr for ArrowCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "uncompressed" => Ok(ArrowCompression::Uncompressed),
            "lz4" => Ok(ArrowCompression::Lz4),
            "zstd" => Ok(ArrowCompression::Zstd),
            _ => Err(format!("Invalid Arrow compression format: {s}")),
        }
    }
}

impl FromStr for QsvAvroCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "uncompressed" => Ok(QsvAvroCompression::Uncompressed),
            "deflate" => Ok(QsvAvroCompression::Deflate),
            "snappy" => Ok(QsvAvroCompression::Snappy),
            _ => Err(format!("Invalid Avro compression format: {s}")),
        }
    }
}

pub fn run(argv: &[&str]) -> anyhow::Result<()> {
    let mut args: Args = util::get_args("", argv)?;

    let tmpdir = tempfile::tempdir()?;

    let mut skip_input = false;
    args.arg_input = if args.arg_input == [PathBuf::from_str("SKIP_INPUT").unwrap()] {
        skip_input = true;
        Vec::new()
    } else {
        process_input(args.arg_input, &tmpdir, "")?
    };

    let rnull_values = if args.flag_rnull_values == "<empty string>" {
        vec![PlSmallStr::EMPTY]
    } else {
        args.flag_rnull_values
            .split(',')
            .map(|value| {
                if value == "<empty string>" {
                    PlSmallStr::EMPTY
                } else {
                    PlSmallStr::from_str(value)
                }
            })
            .collect()
    };

    if args.flag_wnull_value == "<empty string>" {
        args.flag_wnull_value.clear();
    }

    let output_mode: OutputMode = args.flag_format.parse().unwrap_or(OutputMode::Csv);
    let no_output: OutputMode = OutputMode::None;

    let delim = if let Some(delimiter) = args.flag_delimiter {
        delimiter.as_byte()
    } else if let Ok(delim) = env::var("QSV_DEFAULT_DELIMITER") {
        Delimiter::decode_delimiter(&delim)?.as_byte()
    } else {
        b','
    };

    let comment_char = if let Ok(comment_char) = env::var("QSV_COMMENT_CHAR") {
        Some(PlSmallStr::from_string(comment_char))
    } else {
        None
    };

    let mut optflags = OptFlags::from_bits_truncate(0);
    if args.flag_no_optimizations {
        optflags |= OptFlags::TYPE_COERCION;
    } else {
        optflags |= OptFlags::PROJECTION_PUSHDOWN
            | OptFlags::PREDICATE_PUSHDOWN
            | OptFlags::CLUSTER_WITH_COLUMNS
            | OptFlags::TYPE_COERCION
            | OptFlags::SIMPLIFY_EXPR
            | OptFlags::SLICE_PUSHDOWN
            | OptFlags::COMM_SUBPLAN_ELIM
            | OptFlags::COMM_SUBEXPR_ELIM
            | OptFlags::ROW_ESTIMATE
            | OptFlags::FAST_PROJECTION
            | OptFlags::COLLAPSE_JOINS;
    }

    optflags.set(OptFlags::NEW_STREAMING, args.flag_streaming);

    // check if the input is a SQL script (ends with .sql)
    let is_sql_script = std::path::Path::new(&args.arg_sql)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"));

    // if infer_len is 0, its not a SQL script, and there is only one input CSV, we can infer the
    // schema of the CSV more intelligently by counting the number of rows in the file instead of
    // scanning the entire file with a 0 infer_len which triggers a full table scan.
    args.flag_infer_len =
        if args.flag_infer_len == 0 && !is_sql_script && !skip_input && args.arg_input.len() == 1 {
            let rconfig = Config::builder()
                .path(args.arg_input[0].to_string_lossy())
                .build()
                .delimiter(args.flag_delimiter)
                .no_headers(false);
            util::count_rows(&rconfig).unwrap_or(0) as usize
        } else {
            args.flag_infer_len
        };

    // gated by tracing::log_enabled!(tracing::Level::Debug) to avoid the
    // relatively expensive overhead of generating the debug string
    // for the optimization flags struct
    let debuglog_flag = tracing::enabled!(tracing::Level::DEBUG);
    if debuglog_flag {
        tracing::debug!("Optimization flags: {optflags:?}");
        tracing::debug!(
            "Delimiter: {delim} Infer_schema_len: {infer_len} try_parse_dates: {parse_dates} \
             ignore_errors: {ignore_errors}, low_memory: {low_memory}, float_precision: \
             {float_precision:?}, skip_input: {skip_input}, is_sql_script: {is_sql_script}",
            infer_len = args.flag_infer_len,
            parse_dates = args.flag_try_parsedates,
            ignore_errors = args.flag_ignore_errors,
            low_memory = args.flag_low_memory,
            float_precision = args.flag_float_precision,
        );
    }

    // if there is only one input file, check if the pschema.json file exists and is newer or
    // created at the same time as the table file, if so, we can enable the cache schema flag
    if args.arg_input.len() == 1 {
        let schema_file = args.arg_input[0]
            .canonicalize()?
            .with_extension("pschema.json");
        if schema_file.exists()
            && schema_file.metadata()?.modified()? >= args.arg_input[0].metadata()?.modified()?
        {
            args.flag_cache_schema = true;
        }
    }

    let mut ctx = SQLContext::new();
    let mut table_aliases = HashMap::with_capacity(args.arg_input.len());
    let mut lossy_table_name = Cow::default();
    let mut table_name;

    // <SKIP_INPUT> is a sentinel value that tells sqlp to skip all input processing,
    // Use it when you want to use Polars SQL's table functions directly in the SQL query
    // e.g. SELECT read_csv('<input_file>')...; read_parquet(); read_ipc(); read_json()
    if skip_input {
        // we don't need to do anything here, as we are skipping input
        if debuglog_flag {
            tracing::debug!("Skipping input processing...");
        }
    } else {
        // parse the CSV first, and register the input files as tables in the SQL context
        if debuglog_flag {
            tracing::debug!("Parsing input files and registering tables in the SQL context...");
        }

        let cache_schemas = args.flag_cache_schema;

        for (idx, table) in args.arg_input.iter().enumerate() {
            // as we are using the table name as alias, we need to make sure that the table name is
            // a valid identifier. if its not utf8, we use the lossy version
            table_name = Path::new(table)
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or_else(|| {
                    lossy_table_name = table.to_string_lossy();
                    &lossy_table_name
                });

            table_aliases.insert(table_name.to_string(), format!("_t_{}", idx + 1));

            if debuglog_flag {
                tracing::debug!(
                    "Registering table: {table_name} as {alias}",
                    alias = table_aliases.get(table_name).unwrap(),
                );
            }

            // we build the lazyframe, accounting for the --cache-schema flag
            let mut create_schema = cache_schemas;

            let schema_file = table.canonicalize()?.with_extension("pschema.json");

            // check if the pschema.json file exists and is newer or created at the same time
            // as the table file
            let mut valid_schema_exists = schema_file.exists()
                && schema_file.metadata()?.modified()? >= table.metadata()?.modified()?;

            let separator = tsvssv_delim(table, delim);
            if separator == b',' && args.flag_decimal_comma {
                return Err(anyhow!(
                    "Using --decimal-comma with a comma separator is invalid, use --delimiter to \
                     set a different separator."
                ));
            }

            let table_plpath = PlPath::new(&table.to_string_lossy());

            let mut lf = if cache_schemas || valid_schema_exists {
                let mut work_lf = LazyCsvReader::new(table_plpath)
                    .with_has_header(true)
                    .with_missing_is_null(true)
                    .with_comment_prefix(comment_char.clone())
                    .with_null_values(Some(NullValues::AllColumns(rnull_values.clone())))
                    .with_separator(tsvssv_delim(table, delim))
                    .with_try_parse_dates(args.flag_try_parsedates)
                    .with_ignore_errors(args.flag_ignore_errors)
                    .with_truncate_ragged_lines(args.flag_truncate_ragged_lines)
                    .with_decimal_comma(args.flag_decimal_comma)
                    .with_low_memory(args.flag_low_memory);

                if !valid_schema_exists {
                    // we don't have a valid pschema.json file,
                    // check if we have stats, as we can derive pschema.json file from it
                    valid_schema_exists = util::infer_polars_schema(
                        args.flag_delimiter,
                        debuglog_flag,
                        table,
                        &schema_file,
                    )?;
                }

                if valid_schema_exists {
                    // We have a valid pschema.json file!
                    // load the schema and deserialize it and use it with the lazy frame
                    let file = File::open(&schema_file)?;
                    let mut buf_reader = BufReader::new(file);
                    let mut schema_json = String::with_capacity(100);
                    buf_reader.read_to_string(&mut schema_json)?;
                    let schema: Schema = serde_json::from_str(&schema_json)?;
                    if debuglog_flag {
                        tracing::debug!("Loaded schema from file: {}", schema_file.display());
                    }
                    work_lf = work_lf.with_schema(Some(Arc::new(schema)));
                    create_schema = false;
                } else {
                    // there is no valid pschema.json file, infer the schema using --infer-len
                    work_lf = work_lf.with_infer_schema_length(Some(args.flag_infer_len));
                    create_schema = true;
                }
                work_lf.finish()?
            } else {
                // Read input file robustly
                // First try, as --cache-schema is not enabled, try using the --infer-len length
                let reader = LazyCsvReader::new(table_plpath.clone())
                    .with_has_header(true)
                    .with_missing_is_null(true)
                    .with_comment_prefix(comment_char.clone())
                    .with_null_values(Some(NullValues::AllColumns(rnull_values.clone())))
                    .with_separator(tsvssv_delim(table, delim))
                    .with_infer_schema_length(Some(args.flag_infer_len))
                    .with_try_parse_dates(args.flag_try_parsedates)
                    .with_ignore_errors(args.flag_ignore_errors)
                    .with_truncate_ragged_lines(args.flag_truncate_ragged_lines)
                    .with_decimal_comma(args.flag_decimal_comma)
                    .with_low_memory(args.flag_low_memory);

                if let Ok(lf) = reader.finish() {
                    lf
                } else {
                    // First try didn't work.
                    // Second try, infer a schema and try again
                    valid_schema_exists = util::infer_polars_schema(
                        args.flag_delimiter,
                        debuglog_flag,
                        table,
                        &schema_file,
                    )?;

                    if valid_schema_exists {
                        let file = File::open(&schema_file)?;
                        let mut buf_reader = BufReader::new(file);
                        let mut schema_json = String::with_capacity(100);
                        buf_reader.read_to_string(&mut schema_json)?;
                        let schema: Schema = serde_json::from_str(&schema_json)?;

                        // Second try, using the inferred schema
                        let reader_2ndtry = LazyCsvReader::new(table_plpath.clone())
                            .with_schema(Some(Arc::new(schema)))
                            .with_try_parse_dates(args.flag_try_parsedates)
                            .with_ignore_errors(args.flag_ignore_errors)
                            .with_truncate_ragged_lines(args.flag_truncate_ragged_lines)
                            .with_decimal_comma(args.flag_decimal_comma)
                            .with_low_memory(args.flag_low_memory);

                        if let Ok(lf) = reader_2ndtry.finish() {
                            lf
                        } else {
                            // Second try didn't work.
                            // Try one last time without an infer schema length, scanning the whole
                            // file
                            LazyCsvReader::new(table_plpath)
                                .with_infer_schema_length(None)
                                .with_try_parse_dates(args.flag_try_parsedates)
                                .with_ignore_errors(args.flag_ignore_errors)
                                .with_truncate_ragged_lines(args.flag_truncate_ragged_lines)
                                .with_decimal_comma(args.flag_decimal_comma)
                                .with_low_memory(args.flag_low_memory)
                                .finish()?
                        }
                    } else {
                        // Ok, we failed to infer a schema, try without an infer schema length
                        // and scan the whole file to get the schema
                        LazyCsvReader::new(table_plpath)
                            .with_infer_schema_length(None)
                            .with_try_parse_dates(args.flag_try_parsedates)
                            .with_ignore_errors(args.flag_ignore_errors)
                            .with_truncate_ragged_lines(args.flag_truncate_ragged_lines)
                            .with_decimal_comma(args.flag_decimal_comma)
                            .with_low_memory(args.flag_low_memory)
                            .finish()?
                    }
                }
            };
            ctx.register(table_name, lf.clone().with_optimizations(optflags));

            // the lazy frame's schema has been updated and --cache-schema is enabled
            // update the pschema.json file, if necessary
            if create_schema {
                let schema = lf.collect_schema()?;
                let schema_json = serde_json::to_string_pretty(&schema)?;

                let schema_file = table.canonicalize()?.with_extension("pschema.json");
                let mut file = BufWriter::new(File::create(&schema_file)?);
                file.write_all(schema_json.as_bytes())?;
                file.flush()?;
                if debuglog_flag {
                    tracing::debug!("Saved schema to file: {}", schema_file.display());
                }
            }
        }
    }

    if debuglog_flag && !skip_input {
        let tables_in_context = ctx.get_tables();
        tracing::debug!("Table(s) registered in SQL Context: {tables_in_context:?}");
    }

    // check if the query is a SQL script
    let queries = if is_sql_script {
        let mut file = File::open(&args.arg_sql)?;
        let mut sql_script = String::new();
        file.read_to_string(&mut sql_script)?;

        // remove comments from the SQL script
        // we only support single-line comments in SQL scripts
        // i.e. comments that start with "--" and end at the end of the line
        // so the regex is performant and simple
        let comment_regex = Regex::new(r"^--.*$")?;
        let sql_script = comment_regex.replace_all(&sql_script, "");
        sql_script
            .split(';')
            .map(std::string::ToString::to_string)
            .filter(|s| !s.trim().is_empty())
            .collect()
    } else {
        // its not a sql script, just a single query
        vec![args.arg_sql.clone()]
    };

    if debuglog_flag {
        tracing::debug!("SQL query/ies({}): {queries:?}", queries.len());
    }

    let num_queries = queries.len();
    let last_query: usize = num_queries.saturating_sub(1);
    let mut is_last_query;
    let mut current_query = String::new();
    let mut query_result_shape = (0_usize, 0_usize);
    let mut now = Instant::now();

    for (idx, query) in queries.iter().enumerate() {
        // check if this is the last query in the script
        is_last_query = idx == last_query;

        // replace aliases in query
        current_query.clone_from(query);
        for (table_name, table_alias) in &table_aliases {
            // we quote the table name to avoid issues with reserved keywords and
            // other characters that are not allowed in identifiers
            current_query = current_query.replace(table_alias, &(format!(r#""{table_name}""#)));
        }

        if debuglog_flag {
            tracing::debug!("Executing query {idx}: {current_query}");
            now = Instant::now();
        }
        query_result_shape = if is_last_query {
            // if this is the last query, we use the output mode specified by the user
            output_mode.execute_query(&current_query, &mut ctx, delim, args.clone())?
        } else {
            // this is not the last query, we only execute the query, but don't write the output
            no_output.execute_query(&current_query, &mut ctx, delim, args.clone())?
        };
        if debuglog_flag {
            tracing::debug!(
                "Query {idx} successfully executed in {elapsed:?} seconds: {query_result_shape:?}",
                elapsed = now.elapsed().as_secs_f32()
            );
        }
    }

    compress_output_if_needed(args.flag_output)?;

    if !args.flag_quiet {
        eprintln!("{query_result_shape:?}");
    }

    Ok(())
}

/// if the output ends with ".sz", we snappy compress the output
/// and replace the original output with the compressed output
pub fn compress_output_if_needed(output_file: Option<String>) -> anyhow::Result<()> {
    use crate::snappy::compress;

    if let Some(output) = output_file
        && std::path::Path::new(&output)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sz"))
    {
        tracing::info!("Compressing output with Snappy");

        // we need to copy the output to a tempfile first, and then
        // compress the tempfile to the original output sz file
        let mut tempfile = tempfile::NamedTempFile::new()?;
        io::copy(&mut File::open(output.clone())?, tempfile.as_file_mut())?;
        tempfile.flush()?;

        // safety: we just created the tempfile, so we know that the path is valid utf8
        // https://github.com/Stebalien/tempfile/issues/192
        let input_fname = tempfile.path().to_str().unwrap();
        let input = File::open(input_fname)?;
        let output_sz_writer = std::fs::File::create(output)?;
        compress(
            input,
            output_sz_writer,
            util::max_jobs(),
            DEFAULT_WTR_BUFFER_CAPACITY,
        )?;
    }
    Ok(())
}
