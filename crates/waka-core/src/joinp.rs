use std::{
    env,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    mem::swap,
    path::{Path, PathBuf},
    str,
};

use anyhow::anyhow;
use polars::prelude::*;
use polars_utils::plpath::PlPath;
use serde::Deserialize;
use tempfile::tempdir;

use crate::{config::Delimiter, sqlp::compress_output_if_needed, util, util::get_stats_records};

#[derive(Deserialize)]
struct Args {
    arg_columns1:              String,
    arg_input1:                String,
    arg_columns2:              String,
    arg_input2:                String,
    flag_left:                 bool,
    flag_left_anti:            bool,
    flag_left_semi:            bool,
    flag_right:                bool,
    flag_right_anti:           bool,
    flag_right_semi:           bool,
    flag_full:                 bool,
    flag_cross:                bool,
    flag_non_equi:             Option<String>,
    flag_coalesce:             bool,
    flag_filter_left:          Option<String>,
    flag_filter_right:         Option<String>,
    flag_validate:             Option<String>,
    flag_maintain_order:       Option<String>,
    flag_nulls:                bool,
    flag_streaming:            bool,
    flag_try_parsedates:       bool,
    flag_decimal_comma:        bool,
    flag_infer_len:            usize,
    flag_cache_schema:         i8,
    flag_low_memory:           bool,
    flag_no_optimizations:     bool,
    flag_ignore_errors:        bool,
    flag_asof:                 bool,
    flag_no_sort:              bool,
    flag_left_by:              Option<String>,
    flag_right_by:             Option<String>,
    flag_strategy:             Option<String>,
    flag_tolerance:            Option<String>,
    flag_allow_exact_matches:  bool,
    flag_sql_filter:           Option<String>,
    flag_datetime_format:      Option<String>,
    flag_date_format:          Option<String>,
    flag_time_format:          Option<String>,
    flag_float_precision:      Option<usize>,
    flag_null_value:           String,
    flag_output:               Option<String>,
    flag_delimiter:            Option<Delimiter>,
    flag_quiet:                bool,
    flag_ignore_case:          bool,
    flag_ignore_leading_zeros: bool,
    flag_norm_unicode:         Option<String>,
}

// IMPORTANT: This must be kept in sync with the default value
// of the --infer-len option in the USAGE string above.
const DEFAULT_INFER_LEN: usize = 10000;

#[derive(PartialEq, Eq)]
enum SpecialJoin {
    NonEqui(String),
    AsOfAutoSort,
    AsOfNoSort,
    None,
}

pub fn run(argv: &[&str]) -> anyhow::Result<()> {
    let mut args: Args = util::get_args("", argv)?;

    // always try to parse dates when its an asof join
    // just in case the user doesn't specify it
    // and they're using date/time/datetime columns
    if args.flag_asof {
        args.flag_try_parsedates = true;
    }

    let tmpdir = tempdir()?;
    let join = args.new_join(&tmpdir)?;

    let flag_validate = args
        .flag_validate
        .unwrap_or_else(|| "none".to_string())
        .to_lowercase();
    let validation = match flag_validate.as_str() {
        // no unique checks
        "manytomany" | "none" => JoinValidation::ManyToMany,
        // join keys are unique in the left data set
        "onetomany" => JoinValidation::OneToMany,
        // join keys are unique in the right data set
        "manytoone" => JoinValidation::ManyToOne,
        // join keys are unique in both left & right data sets
        "onetoone" => JoinValidation::OneToOne,
        s => return Err(anyhow!("Invalid join validation: {s}")),
    };

    let flag_maintain_order = args
        .flag_maintain_order
        .unwrap_or_else(|| "none".to_string())
        .to_lowercase();
    let maintain_order = match flag_maintain_order.as_str() {
        "none" => MaintainOrderJoin::None,
        "left" => MaintainOrderJoin::Left,
        "right" => MaintainOrderJoin::Right,
        "left_right" => MaintainOrderJoin::LeftRight,
        "right_left" => MaintainOrderJoin::RightLeft,
        s => return Err(anyhow!("Invalid maintain order option: {s}")),
    };

    let flag_norm_unicode = args
        .flag_norm_unicode
        .unwrap_or_else(|| "none".to_string())
        .to_lowercase();
    let normalization_form = match flag_norm_unicode.as_str() {
        "nfc" => Some(UnicodeForm::NFC),
        "nfd" => Some(UnicodeForm::NFD),
        "nfkc" => Some(UnicodeForm::NFKC),
        "nfkd" => Some(UnicodeForm::NFKD),
        "none" => None,
        s => return Err(anyhow!("Invalid normalization form: {s}")),
    };

    let join_shape: (usize, usize) = match (
        args.flag_left,
        args.flag_left_anti,
        args.flag_left_semi,
        args.flag_right,
        args.flag_right_anti,
        args.flag_right_semi,
        args.flag_full,
        args.flag_cross,
        args.flag_asof,
        args.flag_non_equi.is_some(),
    ) {
        // default inner join
        (false, false, false, false, false, false, false, false, false, false) => join.run(
            JoinType::Inner,
            validation,
            maintain_order,
            SpecialJoin::None,
            normalization_form.as_ref(),
        ),
        // left join
        (true, false, false, false, false, false, false, false, false, false) => join.run(
            JoinType::Left,
            validation,
            maintain_order,
            SpecialJoin::None,
            normalization_form.as_ref(),
        ),
        // left anti join
        (false, true, false, false, false, false, false, false, false, false) => join.run(
            JoinType::Anti,
            validation,
            maintain_order,
            SpecialJoin::None,
            normalization_form.as_ref(),
        ),
        // left semi join
        (false, false, true, false, false, false, false, false, false, false) => join.run(
            JoinType::Semi,
            validation,
            maintain_order,
            SpecialJoin::None,
            normalization_form.as_ref(),
        ),
        // right join
        (false, false, false, true, false, false, false, false, false, false) => join.run(
            JoinType::Right,
            validation,
            maintain_order,
            SpecialJoin::None,
            normalization_form.as_ref(),
        ),
        // right anti join
        // swap left and right data sets and run left anti join
        (false, false, false, false, true, false, false, false, false, false) => {
            let mut swapped_join = join;
            swap(&mut swapped_join.left_lf, &mut swapped_join.right_lf);
            swap(&mut swapped_join.left_sel, &mut swapped_join.right_sel);
            swapped_join.run(
                JoinType::Anti,
                validation,
                maintain_order,
                SpecialJoin::None,
                normalization_form.as_ref(),
            )
        },
        // right semi join
        // swap left and right data sets and run left semi join
        (false, false, false, false, false, true, false, false, false, false) => {
            let mut swapped_join = join;
            swap(&mut swapped_join.left_lf, &mut swapped_join.right_lf);
            swap(&mut swapped_join.left_sel, &mut swapped_join.right_sel);
            swapped_join.run(
                JoinType::Semi,
                validation,
                maintain_order,
                SpecialJoin::None,
                normalization_form.as_ref(),
            )
        },
        // full join
        (false, false, false, false, false, false, true, false, false, false) => join.run(
            JoinType::Full,
            validation,
            maintain_order,
            SpecialJoin::None,
            normalization_form.as_ref(),
        ),
        // cross join
        (false, false, false, false, false, false, false, true, false, false) => join.run(
            JoinType::Cross,
            validation,
            MaintainOrderJoin::None,
            SpecialJoin::None,
            normalization_form.as_ref(),
        ),

        // as of join
        (false, false, false, false, false, false, false, false, true, false) => {
            // safety: flag_strategy is always is_some() as it has a default value
            args.flag_strategy = Some(args.flag_strategy.unwrap().to_lowercase());
            let strategy = match args.flag_strategy.as_deref() {
                Some("backward") | None => AsofStrategy::Backward,
                Some("forward") => AsofStrategy::Forward,
                Some("nearest") => AsofStrategy::Nearest,
                Some(s) => return Err(anyhow!("Invalid asof strategy: {}", s)),
            };

            let mut asof_options = AsOfOptions {
                strategy,
                allow_eq: args.flag_allow_exact_matches,
                ..Default::default()
            };

            if strategy == AsofStrategy::Nearest
                && let Some(ref tolerance) = args.flag_tolerance
            {
                // If the tolerance is a positive integer, it is tolerance number of rows.
                // Otherwise, it is a tolerance date language spec.
                if let Ok(numeric_tolerance) = atoi_simd::parse_pos::<u64>(tolerance.as_bytes()) {
                    asof_options.tolerance = Some(numeric_tolerance.into());
                } else {
                    asof_options.tolerance_str = Some(tolerance.into());
                }
            }
            if args.flag_left_by.is_some() {
                asof_options.left_by = Some(
                    args.flag_left_by
                        .unwrap()
                        .split(',')
                        .map(PlSmallStr::from_str)
                        .collect(),
                );
            }
            if args.flag_right_by.is_some() {
                asof_options.right_by = Some(
                    args.flag_right_by
                        .unwrap()
                        .split(',')
                        .map(PlSmallStr::from_str)
                        .collect(),
                );
            }
            join.run(
                JoinType::AsOf(Box::new(asof_options)),
                validation,
                MaintainOrderJoin::None,
                if args.flag_no_sort {
                    SpecialJoin::AsOfNoSort
                } else {
                    SpecialJoin::AsOfAutoSort
                },
                normalization_form.as_ref(),
            )
        },

        // non-equi join
        (false, false, false, false, false, false, false, false, false, true) => {
            // JoinType::Inner is just a placeholder value to satisfy the compiler
            // as this is a non-equi join
            join.run(
                JoinType::Inner,
                validation,
                maintain_order,
                SpecialJoin::NonEqui(args.flag_non_equi.unwrap()),
                normalization_form.as_ref(),
            )
        },
        _ => Err(anyhow!("Please pick exactly one join operation.")),
    }?;

    if !args.flag_quiet {
        eprintln!("{join_shape:?}");
    }

    Ok(())
}

struct JoinStruct {
    left_lf:              LazyFrame,
    left_sel:             String,
    right_lf:             LazyFrame,
    right_sel:            String,
    output:               Option<String>,
    delim:                u8,
    coalesce:             bool,
    streaming:            bool,
    no_optimizations:     bool,
    sql_filter:           Option<String>,
    datetime_format:      Option<String>,
    date_format:          Option<String>,
    time_format:          Option<String>,
    float_precision:      Option<usize>,
    null_value:           String,
    decimal_comma:        bool,
    ignore_case:          bool,
    ignore_leading_zeros: bool,
}

impl JoinStruct {
    fn run(
        mut self,
        jointype: JoinType,
        validation: JoinValidation,
        maintain_order: MaintainOrderJoin,
        special_join: SpecialJoin,
        normalization_form: Option<&UnicodeForm>,
    ) -> anyhow::Result<(usize, usize)> {
        let mut left_selcols: Vec<_> = self
            .left_sel
            .split(',')
            .map(polars::lazy::dsl::col)
            .collect();
        let mut right_selcols: Vec<_> = self
            .right_sel
            .split(',')
            .map(polars::lazy::dsl::col)
            .collect();

        // Handle ignore_case, ignore_leading_zeros, and unicode normalization transformations
        let keys_transformed =
            if self.ignore_case || self.ignore_leading_zeros || normalization_form.is_some() {
                // Create transformation function that applies all enabled transformations
                let transform_col = |col: Expr| {
                    let mut transformed = col.cast(DataType::String);
                    if self.ignore_leading_zeros {
                        transformed = transformed.str().replace_all(lit(r"^0+"), lit(""), false);
                    }
                    if self.ignore_case {
                        transformed = transformed.str().to_lowercase();
                    }
                    if let Some(form) = normalization_form {
                        transformed = transformed.str().normalize(form.clone());
                    }
                    transformed
                };

                // Helper to get clean column name without col("") wrapper
                let clean_col_name = |col: &Expr| {
                    col.to_string()
                        .trim_start_matches(r#"col(""#)
                        .trim_end_matches(r#"")"#)
                        .to_string()
                };

                // Transform left dataframe columns
                for col in &left_selcols {
                    let col_name = clean_col_name(col);
                    let temp_col_name = format!("_qsv-{col_name}-transformed");
                    self.left_lf = self
                        .left_lf
                        .with_column(transform_col(col.clone()).alias(&temp_col_name));
                }

                // Transform right dataframe columns
                for col in &right_selcols {
                    let col_name = clean_col_name(col);
                    let temp_col_name = format!("_qsv-{col_name}-transformed");
                    self.right_lf = self
                        .right_lf
                        .with_column(transform_col(col.clone()).alias(&temp_col_name));
                }

                // Update selcols to use transformed column names
                left_selcols = left_selcols
                    .iter()
                    .map(|col| {
                        polars::lazy::dsl::col(format!("_qsv-{}-transformed", clean_col_name(col)))
                    })
                    .collect();

                right_selcols = right_selcols
                    .iter()
                    .map(|col| {
                        polars::lazy::dsl::col(format!("_qsv-{}-transformed", clean_col_name(col)))
                    })
                    .collect();

                true
            } else {
                false
            };

        let left_selcols_len = left_selcols.len();
        let right_selcols_len = right_selcols.len();

        if left_selcols_len != right_selcols_len {
            return Err(anyhow!(
                "Both columns1 ({left_selcols:?}) and columns2 ({right_selcols:?}) must specify \
                 the same number of columns ({left_selcols_len } != {right_selcols_len})."
            ));
        }

        let coalesce_flag = if self.coalesce {
            JoinCoalesce::CoalesceColumns
        } else {
            JoinCoalesce::JoinSpecific
        };

        let mut out_delim = self.delim;
        let mut out_writer = match self.output {
            Some(ref output_file) => {
                out_delim = tsvssv_delim(output_file, self.delim);

                // no need to use buffered writer here, as CsvWriter already does that
                let path = Path::new(&output_file);
                Box::new(File::create(path).unwrap()) as Box<dyn Write>
            },
            None => Box::new(io::stdout()) as Box<dyn Write>,
        };

        if out_delim == b',' && self.decimal_comma {
            out_writer.flush()?;
            return Err(anyhow!(
                "Using --decimal-comma with a comma separator is invalid, use --delimiter to set \
                 a different separator."
            ));
        }

        let mut optflags = OptFlags::from_bits_truncate(0);
        if self.no_optimizations {
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

        optflags.set(OptFlags::NEW_STREAMING, self.streaming);

        // log::debug!("Optimization flags: {optimization_flags:?}");

        let join_results = if jointype == JoinType::Cross {
            // cross join doesn't need join columns
            self.left_lf
                .with_optimizations(optflags)
                .join_builder()
                .with(self.right_lf.with_optimizations(optflags))
                .how(JoinType::Cross)
                .coalesce(coalesce_flag)
                .allow_parallel(true)
                .validate(validation)
                .finish()
                .collect()?
        } else {
            if special_join == SpecialJoin::AsOfAutoSort {
                // it's an asof join and --no-sort is not set
                // sort by the asof columns, as asof joins require sorted join column data
                let left_selcols_vec: Vec<PlSmallStr> =
                    self.left_sel.split(',').map(PlSmallStr::from_str).collect();

                self.left_lf = self
                    .left_lf
                    .sort(left_selcols_vec, SortMultipleOptions::default());

                let right_selcols_vec: Vec<PlSmallStr> = self
                    .right_sel
                    .split(',')
                    .map(PlSmallStr::from_str)
                    .collect();

                self.right_lf = self
                    .right_lf
                    .sort(right_selcols_vec, SortMultipleOptions::default());
            }

            if let SpecialJoin::NonEqui(expr) = special_join {
                // it's a non-equi join
                let expr = polars::sql::sql_expr(expr)?;

                // Add "_left" & "_right" suffixes to all columns before doing the non-equi join.
                // This is necessary as the NonEqui expression is a SQL where clause and the
                // column names for the left and right data sets are used in the expression.
                let left_cols = self
                    .left_lf
                    .collect_schema()?
                    .iter_names()
                    .cloned()
                    .collect::<Vec<_>>();
                let right_cols = self
                    .right_lf
                    .collect_schema()?
                    .iter_names()
                    .cloned()
                    .collect::<Vec<_>>();

                let left_renamed_cols: Vec<Expr> = left_cols
                    .iter()
                    .map(|col| polars::lazy::dsl::col(col.as_str()).alias(format!("{col}_left")))
                    .collect();
                let right_renamed_cols: Vec<Expr> = right_cols
                    .iter()
                    .map(|col| polars::lazy::dsl::col(col.as_str()).alias(format!("{col}_right")))
                    .collect();

                self.left_lf = self.left_lf.select(left_renamed_cols);
                self.right_lf = self.right_lf.select(right_renamed_cols);

                self.left_lf
                    .with_optimizations(optflags)
                    .join_builder()
                    .with(self.right_lf.with_optimizations(optflags))
                    .join_where(vec![expr])
                    .collect()?
            } else {
                // it's one of the "standard" joins as indicated by jointype
                self.left_lf
                    .with_optimizations(optflags)
                    .join_builder()
                    .with(self.right_lf.with_optimizations(optflags))
                    .left_on(left_selcols)
                    .right_on(right_selcols)
                    .how(jointype)
                    .maintain_order(maintain_order)
                    .coalesce(coalesce_flag)
                    .allow_parallel(true)
                    .validate(validation)
                    .finish()
                    .collect()?
            }
        };

        let mut results_df = match &self.sql_filter {
            Some(sql_filter) => {
                let mut ctx = polars::sql::SQLContext::new();
                ctx.register("join_result", join_results.lazy());
                ctx.execute(sql_filter)
                    .and_then(polars::prelude::LazyFrame::collect)?
            },
            _ => join_results,
        };

        if keys_transformed {
            // Remove temporary transformed columns and
            // duplicate right-side join columns if coalesce is true
            let cols = results_df.get_column_names();
            let mut keep_cols: Vec<String> = Vec::new();

            let left_join_cols: Vec<String> = self
                .left_sel
                .split(',')
                .map(std::string::ToString::to_string)
                .collect();

            for col in cols {
                if col.contains("-transformed") {
                    continue;
                }

                // For join columns, only keep the left version if coalesce is true
                if self.coalesce && col.ends_with("_right") {
                    let base_col = col.trim_end_matches("_right");
                    if left_join_cols.contains(&base_col.to_string()) {
                        continue;
                    }
                }

                keep_cols.push(col.to_string());
            }

            results_df = results_df.select(keep_cols)?;
        }

        // shape is the number of rows and columns
        let join_shape = results_df.shape();

        CsvWriter::new(&mut out_writer)
            .include_header(true)
            .with_separator(out_delim)
            .with_datetime_format(self.datetime_format)
            .with_date_format(self.date_format)
            .with_time_format(self.time_format)
            .with_float_precision(self.float_precision)
            .with_null_value(self.null_value)
            .with_decimal_comma(self.decimal_comma)
            .include_bom(util::get_envvar_flag("QSV_OUTPUT_BOM"))
            .finish(&mut results_df)?;

        compress_output_if_needed(self.output)?;

        Ok(join_shape)
    }
}

impl Args {
    fn new_join(&mut self, tmpdir: &tempfile::TempDir) -> anyhow::Result<JoinStruct> {
        // Helper function to create a LazyFrameReader with common settings
        fn create_lazy_reader(
            file_path: &str,
            comment_char: Option<&PlSmallStr>,
            args: &Args,
            delim: u8,
        ) -> LazyCsvReader {
            LazyCsvReader::new(PlPath::new(file_path))
                .with_has_header(true)
                .with_missing_is_null(args.flag_nulls)
                .with_comment_prefix(comment_char.cloned())
                .with_separator(tsvssv_delim(file_path, delim))
                .with_try_parse_dates(args.flag_try_parsedates)
                .with_decimal_comma(args.flag_decimal_comma)
                .with_low_memory(args.flag_low_memory)
                .with_ignore_errors(args.flag_ignore_errors)
        }

        // Helper function to handle schema creation from stats
        fn create_schema_from_stats(input_path: &Path, args: &Args) -> anyhow::Result<Schema> {
            let schema_args = util::SchemaArgs {
                flag_enum_threshold:  0,
                flag_ignore_case:     false,
                flag_strict_dates:    false,
                flag_pattern_columns: crate::select::SelectColumns::parse("").unwrap(),
                flag_dates_whitelist: String::new(),
                flag_prefer_dmy:      false,
                flag_force:           false,
                flag_stdout:          false,
                flag_jobs:            Some(util::njobs(None)),
                flag_polars:          false,
                flag_no_headers:      false,
                flag_delimiter:       args.flag_delimiter,
                arg_input:            Some(input_path.to_string_lossy().into_owned()),
                flag_memcheck:        false,
            };

            let (csv_fields, csv_stats, _) =
                get_stats_records(&schema_args, util::StatsMode::PolarsSchema)?;

            let mut schema = Schema::with_capacity(csv_stats.len());
            for (idx, stat) in csv_stats.iter().enumerate() {
                schema.insert(
                    PlSmallStr::from_str(
                        simdutf8::basic::from_utf8(csv_fields.get(idx).unwrap()).unwrap(),
                    ),
                    {
                        let datatype = &stat.r#type;
                        #[allow(clippy::match_same_arms)]
                        match datatype.as_str() {
                            "String" => polars::datatypes::DataType::String,
                            "Integer" => {
                                let min = stat.min.as_ref().unwrap();
                                let max = stat.max.as_ref().unwrap();
                                if min.parse::<i32>().is_ok() && max.parse::<i32>().is_ok() {
                                    polars::datatypes::DataType::Int32
                                } else {
                                    polars::datatypes::DataType::Int64
                                }
                            },
                            "Float" => {
                                let min = stat.min.as_ref().unwrap();
                                let max = stat.max.as_ref().unwrap();
                                if min.parse::<f32>().is_ok() && max.parse::<f32>().is_ok() {
                                    polars::datatypes::DataType::Float32
                                } else {
                                    polars::datatypes::DataType::Float64
                                }
                            },
                            "Boolean" => polars::datatypes::DataType::Boolean,
                            "Date" => polars::datatypes::DataType::Date,
                            _ => polars::datatypes::DataType::String,
                        }
                    },
                );
            }
            Ok(schema)
        }

        /// Helper function to setup a LazyFrame with schema handling based on cache_schema flag.
        ///
        /// # Arguments
        /// * `input_path` - Path to the input CSV file
        /// * `comment_char` - Optional comment character to ignore lines starting with it
        /// * `args` - Command line arguments containing schema caching and other options
        /// * `delim` - Delimiter character for CSV parsing
        /// * `debuglog_flag` - Whether debug logging is enabled
        ///
        /// # Returns
        /// Returns a tuple containing:
        /// * The configured LazyFrame for reading the CSV
        /// * A boolean indicating if a new schema needs to be created and cached
        ///
        /// # Schema Caching Modes
        /// * `0` - No schema caching, infer schema from data sample using Polars
        /// * `1` - Cache inferred schema from stats in .pschema.json file
        /// * `-1` - Use string schema for all columns without caching
        /// * `-2` - Use string schema for all columns and cache it
        ///
        /// # Notes
        /// * If the pschema.json file exists and is newer or created at the same time as the table
        ///   file, we enable the cache schema flag even if --cache-schema is 0 (not set)
        /// * Unless --infer-len is explicitly set to a non-default value (10000) and --cache-schema
        ///   is not set to -1 or -2, we enable the cache schema flag even if --cache-schema is 0
        ///   (not set)
        ///
        /// # Errors
        /// Returns error if:
        /// * File operations fail
        /// * Schema parsing fails
        /// * Invalid cache_schema value provided
        fn setup_lazy_frame(
            input_path: &Path,
            comment_char: Option<&PlSmallStr>,
            args: &Args,
            delim: u8,
            debuglog_flag: bool,
        ) -> anyhow::Result<(LazyFrame, bool)> {
            let mut create_schema = false;

            // First, check if the pschema.json file exists and is newer or created at the same time
            // as the table file
            let schema_file = input_path.canonicalize()?.with_extension("pschema.json");
            let mut valid_schema_exists = schema_file.exists()
                && schema_file.metadata()?.modified()? >= input_path.metadata()?.modified()?;

            let cache_schema: i8 = if args.flag_infer_len == 0 {
                0
            } else {
                // if the pschema.json file exists and is newer or created at the same time as the
                // table file, we enable the cache schema flag, so long as
                // --cache-schema is not set to -1 or -2, and
                // --infer-len is not explicitly set to a non-default value
                if valid_schema_exists
                    && args.flag_infer_len != DEFAULT_INFER_LEN
                    && args.flag_cache_schema >= 0
                {
                    1
                } else {
                    args.flag_cache_schema
                }
            };

            let mut reader =
                create_lazy_reader(input_path.to_str().unwrap(), comment_char, args, delim);

            match cache_schema {
                0 => {
                    reader = reader.with_infer_schema_length(if args.flag_infer_len == 0 {
                        None
                    } else {
                        Some(args.flag_infer_len)
                    });
                },
                1 => {
                    if !valid_schema_exists {
                        let schema = create_schema_from_stats(input_path, args)?;
                        let stats_schema = Arc::new(schema);
                        let stats_schema_json = serde_json::to_string_pretty(&stats_schema)?;

                        let mut file = BufWriter::new(File::create(&schema_file)?);
                        file.write_all(stats_schema_json.as_bytes())?;
                        file.flush()?;
                        if debuglog_flag {
                            tracing::debug!("Saved schema to file: {}", schema_file.display());
                        }
                        valid_schema_exists = true;
                    }

                    if valid_schema_exists {
                        let file = File::open(&schema_file)?;
                        let mut buf_reader = BufReader::new(file);
                        let mut schema_json = String::with_capacity(100);
                        buf_reader.read_to_string(&mut schema_json)?;
                        let schema: Schema = serde_json::from_str(&schema_json)?;
                        reader = reader.with_schema(Some(Arc::new(schema)));
                        create_schema = false;
                    } else {
                        reader = reader.with_infer_schema_length(Some(args.flag_infer_len));
                        create_schema = true;
                    }
                },
                -1 | -2 => {
                    // get the headers from the input file
                    let mut rdr = csv::Reader::from_path(input_path)?;
                    let csv_fields = rdr.byte_headers()?.clone();
                    drop(rdr);

                    let mut schema = Schema::with_capacity(csv_fields.len());
                    for field in &csv_fields {
                        schema.insert(
                            PlSmallStr::from_str(simdutf8::basic::from_utf8(field).unwrap()),
                            polars::datatypes::DataType::String,
                        );
                    }
                    let allstring_schema = Arc::new(schema);

                    reader = reader.with_schema(Some(allstring_schema.clone()));
                    create_schema = false;

                    // create and cache allstring schema
                    if cache_schema == -2 {
                        let allstring_schema_json =
                            serde_json::to_string_pretty(&allstring_schema)?;

                        let mut file = BufWriter::new(File::create(&schema_file)?);
                        file.write_all(allstring_schema_json.as_bytes())?;
                        file.flush()?;
                        if debuglog_flag {
                            tracing::debug!(
                                "Saved allstring_schema to file: {}",
                                schema_file.display()
                            );
                        }
                    }
                },
                _ => {
                    return Err(anyhow!(
                        "Invalid --cache-schema value: {cache_schema}. Valid values are 0, 1, -1 \
                         and -2"
                    ));
                },
            }

            Ok((reader.finish()?, create_schema))
        }

        // ============ START OF NEW_JOIN MAIN CODE ==============
        let debuglog_flag = tracing::enabled!(tracing::Level::DEBUG);

        let delim = if let Some(delimiter) = self.flag_delimiter {
            delimiter.as_byte()
        } else {
            b','
        };

        let comment_char = if let Ok(comment_char) = env::var("QSV_COMMENT_CHAR") {
            Some(PlSmallStr::from_string(comment_char))
        } else {
            None
        };

        // Check if input files exist
        let mut input1_path = PathBuf::from(&self.arg_input1);
        if !input1_path.exists() {
            return Err(anyhow!("Input file {} does not exist.", self.arg_input1));
        }
        let mut input2_path = PathBuf::from(&self.arg_input2);
        if !input2_path.exists() {
            return Err(anyhow!("Input file {} does not exist.", self.arg_input2));
        }

        // Handle snappy compression for left input
        if input1_path.extension().and_then(std::ffi::OsStr::to_str) == Some("sz") {
            let decompressed_path = util::decompress_snappy_file(&input1_path, tmpdir)?;
            self.arg_input1.clone_from(&decompressed_path);
            input1_path = PathBuf::from(decompressed_path);
        }

        // Setup left LazyFrame
        let (mut left_lf, create_left_schema) = setup_lazy_frame(
            &input1_path,
            comment_char.as_ref(),
            self,
            delim,
            debuglog_flag,
        )?;

        if create_left_schema {
            let schema = left_lf.collect_schema()?;
            let schema_json = serde_json::to_string_pretty(&schema)?;
            let schema_file = input1_path.canonicalize()?.with_extension("pschema.json");
            let mut file = BufWriter::new(File::create(&schema_file)?);
            file.write_all(schema_json.as_bytes())?;
            file.flush()?;
            if debuglog_flag {
                tracing::debug!("Saved left schema to file: {}", schema_file.display());
            }
        }

        // Apply left filter if needed
        if let Some(filter_left) = &self.flag_filter_left {
            let filter_left_expr = polars::sql::sql_expr(filter_left)?;
            left_lf = left_lf.filter(filter_left_expr);
        }

        // Handle snappy compression for right input
        if input2_path.extension().and_then(std::ffi::OsStr::to_str) == Some("sz") {
            let decompressed_path = util::decompress_snappy_file(&input2_path, tmpdir)?;
            self.arg_input2.clone_from(&decompressed_path);
            input2_path = PathBuf::from(decompressed_path);
        }

        // Setup right LazyFrame
        let (mut right_lf, create_right_schema) = setup_lazy_frame(
            &input2_path,
            comment_char.as_ref(),
            self,
            delim,
            debuglog_flag,
        )?;

        if create_right_schema {
            let schema = right_lf.collect_schema()?;
            let schema_json = serde_json::to_string_pretty(&schema)?;
            let schema_file = input2_path.canonicalize()?.with_extension("pschema.json");
            let mut file = BufWriter::new(File::create(&schema_file)?);
            file.write_all(schema_json.as_bytes())?;
            file.flush()?;
            if debuglog_flag {
                tracing::debug!("Saved right schema to file: {}", schema_file.display());
            }
        }

        // Apply right filter if needed
        if let Some(filter_right) = &self.flag_filter_right {
            let filter_right_expr = polars::sql::sql_expr(filter_right)?;
            right_lf = right_lf.filter(filter_right_expr);
        }

        Ok(JoinStruct {
            left_lf,
            left_sel: self.arg_columns1.clone(),
            right_lf,
            right_sel: self.arg_columns2.clone(),
            output: self.flag_output.clone(),
            delim,
            coalesce: self.flag_coalesce,
            streaming: self.flag_streaming,
            no_optimizations: self.flag_no_optimizations,
            sql_filter: self.flag_sql_filter.clone(),
            datetime_format: self.flag_datetime_format.clone(),
            date_format: self.flag_date_format.clone(),
            time_format: self.flag_time_format.clone(),
            float_precision: self.flag_float_precision,
            null_value: if self.flag_null_value == "<empty string>" {
                String::new()
            } else {
                self.flag_null_value.clone()
            },
            decimal_comma: self.flag_decimal_comma,
            ignore_case: self.flag_ignore_case,
            ignore_leading_zeros: self.flag_ignore_leading_zeros,
        })
    }
}

/// if the file has a TSV/TAB or SSV extension, we automatically use
/// tab or semicolon as the delimiter
/// otherwise, we use the delimiter specified by the user
/// if the file has a .sz extension, we check the original file extension
/// to determine the delimiter
pub fn tsvssv_delim<P: AsRef<Path>>(file: P, orig_delim: u8) -> u8 {
    let inputfile_extension = file
        .as_ref()
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    match inputfile_extension.as_str() {
        "tsv" | "tab" => b'\t',
        "ssv" => b';',
        "sz" => {
            // its a snappy compressed file
            // check what the original file extension is
            let orig_filestem = file
                .as_ref()
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let orig_extension = Path::new(&orig_filestem).extension().unwrap_or_default();
            if orig_extension == "tsv" || orig_extension == "tab" {
                b'\t'
            } else if orig_extension == "ssv" {
                b';'
            } else {
                orig_delim
            }
        },
        _ => orig_delim,
    }
}

#[test]
fn test_tsvssv_delim() {
    assert_eq!(tsvssv_delim("test.tsv", b','), b'\t');
    assert_eq!(tsvssv_delim("test.tab", b','), b'\t');
    assert_eq!(tsvssv_delim("test.ssv", b','), b';');
    assert_eq!(tsvssv_delim("test.sz", b','), b',');
    assert_eq!(tsvssv_delim("test.csv", b','), b',');
    assert_eq!(tsvssv_delim("test.TSV", b','), b'\t');
    assert_eq!(tsvssv_delim("test.Tab", b','), b'\t');
    assert_eq!(tsvssv_delim("test.SSV", b','), b';');
    assert_eq!(tsvssv_delim("test.sZ", b','), b',');
    assert_eq!(tsvssv_delim("test.CsV", b','), b',');
    assert_eq!(tsvssv_delim("test", b','), b',');
    assert_eq!(tsvssv_delim("test.csv.sz", b','), b',');
    assert_eq!(tsvssv_delim("test.tsv.sz", b','), b'\t');
    assert_eq!(tsvssv_delim("test.tab.sz", b','), b'\t');
    assert_eq!(tsvssv_delim("test.ssv.sz", b','), b';');
    assert_eq!(tsvssv_delim("test.csV.Sz", b','), b',');
    assert_eq!(tsvssv_delim("test.TSV.SZ", b','), b'\t');
    assert_eq!(tsvssv_delim("test.Tab.sZ", b','), b'\t');
    assert_eq!(tsvssv_delim("test.SSV.sz", b','), b';');
}
