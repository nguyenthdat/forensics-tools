#![allow(clippy::cast_precision_loss)] // we're not worried about precision loss here

use anyhow::anyhow;
use serde::Deserialize;

use crate::{
    config::{Config, Delimiter},
    util,
};

#[allow(dead_code)]
#[derive(Deserialize)]
struct Args {
    arg_input:            Option<String>,
    flag_human_readable:  bool,
    flag_width:           bool,
    flag_width_no_delims: bool,
    flag_json:            bool,
    flag_no_polars:       bool,
    flag_low_memory:      bool,
    flag_flexible:        bool,
    flag_no_headers:      bool,
    flag_delimiter:       Option<Delimiter>,
}

#[derive(Copy, Clone, PartialEq)]
enum CountDelimsMode {
    IncludeDelims,
    ExcludeDelims,
    NotRequired,
}

#[derive(Default)]
struct WidthStats {
    max:      usize,
    avg:      f64,
    median:   usize,
    min:      usize,
    variance: f64,
    stddev:   f64,
    mad:      f64,
}

pub fn run(argv: &[&str]) -> anyhow::Result<()> {
    let args: Args = util::get_args("", argv)?;
    let conf = Config::new(args.arg_input.as_ref())
        .no_headers(args.flag_no_headers)
        // we also want to count the quotes when computing width
        .quoting(!args.flag_width || !args.flag_width_no_delims)
        // and ignore differing column counts as well
        .flexible(args.flag_flexible)
        .delimiter(args.flag_delimiter);

    let count_delims_mode = if args.flag_width_no_delims {
        CountDelimsMode::ExcludeDelims
    } else if args.flag_width {
        CountDelimsMode::IncludeDelims
    } else {
        CountDelimsMode::NotRequired
    };

    let empty_record_stats = WidthStats::default();

    // if doing width or --flexible is set, we need to use the regular CSV reader
    let (count, record_stats) =
        if count_delims_mode != CountDelimsMode::NotRequired || args.flag_flexible {
            count_input(&conf, count_delims_mode)?
        } else {
            let index_status = conf.indexed().unwrap_or_else(|_| {
                tracing::info!("index is stale");
                None
            });
            match index_status {
                // there's a valid index, use it
                Some(idx) => {
                    tracing::info!("index used");
                    (idx.count(), empty_record_stats)
                },
                None => {
                    // if --no-polars or its a snappy compressed file, use the regular CSV reader
                    if args.flag_no_polars || conf.is_snappy() {
                        count_input(&conf, count_delims_mode)?
                    } else {
                        let count = polars_count_input(&conf, args.flag_low_memory)?;
                        // if polars count returns a zero, do a regular CSV reader count
                        // to be doubly sure as it will be cheap to do so with the regular count
                        if count == 0 {
                            count_input(&conf, count_delims_mode)?
                        } else {
                            (count, empty_record_stats)
                        }
                    }
                },
            }
        };

    if args.flag_json {
        tracing::info!(
            r#"{{"count":{},"max":{},"avg":{},"median":{},"min":{},"variance":{},"stddev":{},"mad":{}}}"#,
            count,
            record_stats.max,
            util::round_num(record_stats.avg, 4),
            record_stats.median,
            record_stats.min,
            util::round_num(record_stats.variance, 4),
            util::round_num(record_stats.stddev, 4),
            util::round_num(record_stats.mad, 4),
        );
    } else if args.flag_human_readable {
        if count_delims_mode == CountDelimsMode::NotRequired {
            tracing::info!("{count}");
        } else {
            tracing::info!(
                "{count};max:{} avg:{} median:{} min:{} variance:{} stddev:{} mad:{}",
                record_stats.max as u64,
                record_stats.avg,
                record_stats.median as u64,
                record_stats.min as u64,
                record_stats.variance,
                record_stats.stddev,
                record_stats.mad,
            );
        }
    } else if count_delims_mode == CountDelimsMode::NotRequired {
        tracing::info!("{count}");
    } else {
        tracing::info!(
            "{count};{max}-{avg}-{median}-{min}-{variance}-{stddev}-{mad}",
            max = record_stats.max,
            avg = util::round_num(record_stats.avg, 4),
            median = record_stats.median,
            min = record_stats.min,
            variance = util::round_num(record_stats.variance, 4),
            stddev = util::round_num(record_stats.stddev, 4),
            mad = util::round_num(record_stats.mad, 4),
        );
    }
    Ok(())
}

/// Counts the number of records in a CSV file and optionally calculates statistics about record
/// widths.
///
/// # Arguments
/// * `conf` - Configuration for reading the CSV file
/// * `count_delims_mode` - Specifies whether to include delimiters in width calculations
///
/// # Returns
/// A tuple containing:
/// * The total number of records in the file
/// * Statistics about record widths including:
///   - Maximum width
///   - Average width
///   - Median width
///   - Minimum width
///   - Variance of widths
///   - Standard deviation of widths
///   - Median absolute deviation (MAD) of widths
///
/// # Details
/// - If an index exists for the file, uses that for the record count
/// - If only counting records (CountDelimsMode::NotRequired), just returns count
/// - For width statistics:
///   - Reads through file calculating width of each record
///   - Width can optionally include delimiters based on CountDelimsMode
///   - Uses parallel sorting for performance on large files
///   - Handles potential numeric overflow in calculations
///
/// # Errors
/// Returns error if:
/// - Unable to read from the CSV file
/// - Out of memory when allocating vectors for statistics
fn count_input(
    conf: &Config,
    count_delims_mode: CountDelimsMode,
) -> anyhow::Result<(u64, WidthStats)> {
    use rayon::{
        iter::{IntoParallelRefIterator, ParallelIterator},
        prelude::ParallelSliceMut,
    };

    // if conf is indexed, we still get the count from the index
    let mut use_index_count = false;
    let mut count = match conf.indexed()? {
        Some(idx) => {
            use_index_count = true;
            tracing::info!("index used");
            idx.count()
        },
        _ => 0_u64,
    };

    let mut rdr = conf.reader()?;
    let mut record = csv::ByteRecord::new();
    let empty_record_stats = WidthStats::default();

    if count_delims_mode == CountDelimsMode::NotRequired {
        if !use_index_count {
            // if we're not using the index, we need to read the file
            // to get the count
            while rdr.read_byte_record(&mut record)? {
                count += 1;
            }
        }
        Ok((count, empty_record_stats))
    } else {
        // read the first record to get the number of delimiters
        // and the width of the first record
        if !rdr.read_byte_record(&mut record)? {
            return Ok((0, empty_record_stats));
        }

        let mut curr_width = record.as_slice().len();

        let mut max = curr_width;
        let mut min = curr_width;
        let mut total_width = curr_width;
        let mut widths = Vec::new();
        widths.try_reserve(if use_index_count {
            count as usize
        } else {
            1_000 // reasonable default to minimize reallocations
        })?;

        widths.push(curr_width);
        let mut manual_count = 1_u64;

        // number of delimiters is number of fields minus 1
        // we subtract 1 because the last field doesn't have a delimiter
        let record_numdelims = if count_delims_mode == CountDelimsMode::IncludeDelims {
            record.len().saturating_sub(1)
        } else {
            0
        };

        while rdr.read_byte_record(&mut record)? {
            manual_count += 1;

            curr_width = record.as_slice().len() + record_numdelims;

            // we don't want to overflow total_width, so we do saturating_add
            total_width = total_width.saturating_add(curr_width);
            widths.push(curr_width);

            if curr_width > max {
                max = curr_width;
            } else if curr_width < min {
                min = curr_width;
            }
        }

        if !use_index_count {
            count = manual_count;
        }

        // Calculate average width
        // if total_width is saturated (== usize::MAX), then avg will be 0.0
        let avg = if total_width == usize::MAX {
            0.0_f64
        } else {
            total_width as f64 / count as f64
        };

        // Calculate median width
        widths.par_sort_unstable();
        let median = if count.is_multiple_of(2) {
            usize::midpoint(
                widths[(count / 2) as usize - 1],
                widths[(count / 2) as usize],
            )
        } else {
            widths[(count / 2) as usize]
        };

        // Calculate standard deviation & variance
        // if avg_width is 0 (because total_width > usize::MAX),
        // then variance & stddev will be 0
        let (variance, stddev) = if avg > 0.0 {
            let variance = widths
                .par_iter()
                .map(|&width| {
                    let diff = width as f64 - avg;
                    diff * diff
                })
                .sum::<f64>()
                / count as f64;
            (variance, variance.sqrt())
        } else {
            (0.0_f64, 0.0_f64)
        };

        // Calculate median absolute deviation (MAD)
        let mad = {
            let mut abs_devs: Vec<f64> = widths
                .iter()
                .map(|&width| (width as f64 - median as f64).abs())
                .collect();
            abs_devs.par_sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
            if count.is_multiple_of(2) {
                f64::midpoint(
                    abs_devs[(count / 2) as usize - 1],
                    abs_devs[(count / 2) as usize],
                )
            } else {
                abs_devs[(count / 2) as usize]
            }
        };

        Ok((
            count,
            WidthStats {
                max,
                avg,
                median,
                min,
                variance,
                stddev,
                mad,
            },
        ))
    }
}

/// Counts the number of records in a CSV file using Polars' optimized CSV reader
///
/// # Arguments
/// * `conf` - Configuration for reading the CSV file
/// * `low_memory` - Whether to use low memory mode when reading the file
///
/// # Returns
/// * Total number of records in the file
///
/// # Details
/// - For stdin input, creates a temporary file to allow Polars to read it
/// - Uses Polars' SQL functionality with lazy evaluation for optimal performance
/// - Handles comment characters and different delimiters
/// - Falls back to regular CSV reader if Polars encounters errors
/// - Adjusts count for no-headers mode since Polars always assumes headers
///
/// # Performance
/// - Uses memory-mapped reading and multithreading for fast processing
/// - For standard CSV files (comma-delimited, no comments), uses optimized read_csv() function
/// - Otherwise uses LazyCsvReader with optimized settings
///
/// # Errors
/// Returns error if:
/// - Unable to create/write temporary file for stdin
/// - Cannot read the CSV file
/// - SQL query execution fails
pub fn polars_count_input(conf: &Config, low_memory: bool) -> anyhow::Result<u64> {
    use polars::{
        lazy::frame::{LazyFrame, OptFlags},
        prelude::*,
        sql::SQLContext,
    };
    use polars_utils::plpath::PlPath;

    // info!("using polars");

    let is_stdin = conf.is_stdin();

    let filepath = if is_stdin {
        let mut temp_file = tempfile::Builder::new().suffix(".csv").tempfile()?;
        let stdin = std::io::stdin();
        let mut stdin_handle = stdin.lock();
        std::io::copy(&mut stdin_handle, &mut temp_file)?;
        drop(stdin_handle);

        let (_, tempfile_pb) = temp_file
            .keep()
            .or(Err(anyhow!("Cannot keep temporary file created for stdin")))?;

        tempfile_pb
    } else {
        conf.path.as_ref().unwrap().clone()
    };

    let mut comment_char = String::new();
    let comment_prefix = if let Some(c) = conf.comment {
        comment_char.push(c as char);
        Some(PlSmallStr::from_str(comment_char.as_str()))
    } else {
        None
    };

    let mut ctx = SQLContext::new();
    let lazy_df: LazyFrame;
    let delimiter = conf.get_delimiter();

    {
        // First, try to read the first row to check if the file is empty
        // do it in a block so schema_df is dropped early
        let schema_df = match LazyCsvReader::new(PlPath::new(&filepath.to_string_lossy()))
            .with_separator(delimiter)
            .with_comment_prefix(comment_prefix.clone())
            .with_n_rows(Some(1))
            .finish()
        {
            Ok(df) => df.collect(),
            Err(e) => {
                tracing::warn!("polars error loading CSV: {e}");
                let (count_regular, _) = count_input(conf, CountDelimsMode::NotRequired)?;
                return Ok(count_regular);
            },
        };

        // If we can't read the schema or the DataFrame is empty, return 0
        if schema_df.is_err() || schema_df.unwrap().height() == 0 {
            return Ok(0);
        }
    }

    // if its a "regular" CSV, use polars' read_csv() SQL table function
    // which is much faster than the LazyCsvReader
    let count_query = if comment_prefix.is_none() && delimiter == b',' && !low_memory {
        format!(
            "SELECT COUNT(*) FROM read_csv('{}')",
            filepath.to_string_lossy(),
        )
    } else {
        // otherwise, read the file into a Polars LazyFrame
        // using the LazyCsvReader builder to set CSV read options
        lazy_df = match LazyCsvReader::new(PlPath::new(&filepath.to_string_lossy()))
            .with_separator(delimiter)
            .with_comment_prefix(comment_prefix)
            .with_low_memory(low_memory)
            .finish()
        {
            Ok(lazy_df) => lazy_df,
            Err(e) => {
                tracing::warn!("polars error loading CSV: {e}");
                let (count_regular, _) = count_input(conf, CountDelimsMode::NotRequired)?;
                return Ok(count_regular);
            },
        };
        let optflags = OptFlags::from_bits_truncate(0)
            | OptFlags::PROJECTION_PUSHDOWN
            | OptFlags::PREDICATE_PUSHDOWN
            | OptFlags::CLUSTER_WITH_COLUMNS
            | OptFlags::TYPE_COERCION
            | OptFlags::SIMPLIFY_EXPR
            | OptFlags::SLICE_PUSHDOWN
            | OptFlags::COMM_SUBPLAN_ELIM
            | OptFlags::COMM_SUBEXPR_ELIM
            | OptFlags::FAST_PROJECTION
            | OptFlags::NEW_STREAMING;
        ctx.register("sql_lf", lazy_df.with_optimizations(optflags));
        "SELECT COUNT(*) FROM sql_lf".to_string()
    };

    // now leverage the magic of Polars SQL with its lazy evaluation, to count the records
    // in an optimized manner with its blazing fast multithreaded, mem-mapped CSV reader!
    let sqlresult_lf = match ctx.execute(&count_query) {
        Ok(sqlresult_lf) => sqlresult_lf,
        Err(e) => {
            // there was a Polars error, so we fall back to the regular CSV reader
            tracing::warn!("polars error executing count query: {e}");
            let (count_regular, _) = count_input(conf, CountDelimsMode::NotRequired)?;
            return Ok(count_regular);
        },
    };

    let mut count = match sqlresult_lf.collect()?["len"].u32() {
        Ok(cnt) => {
            if let Some(count) = cnt.get(0) {
                count as u64
            } else {
                // Empty result, fall back to regular CSV reader
                tracing::warn!("empty polars result, falling back to regular reader");
                let (count_regular, _) = count_input(conf, CountDelimsMode::NotRequired)?;
                count_regular
            }
        },
        Err(e) => {
            // Polars error, fall back to regular CSV reader
            tracing::warn!("polars error, falling back to regular reader: {e}");
            let (count_regular, _) = count_input(conf, CountDelimsMode::NotRequired)?;
            count_regular
        },
    };

    // remove the temporary file we created to read from stdin
    // we use the keep() method to prevent the file from being deleted
    // when the tempfile went out of scope, so we need to manually delete it
    if is_stdin {
        std::fs::remove_file(filepath)?;
    }

    // Polars SQL requires headers, so it made the first row the header row
    // regardless of the --no-headers flag. That's why we need to add 1 to the count
    if conf.no_headers {
        count += 1;
    }

    Ok(count)
}
