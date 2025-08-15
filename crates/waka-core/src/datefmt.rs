use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
#[cfg(any(feature = "feature_capable", feature = "lite"))]
use indicatif::{ProgressBar, ProgressDrawTarget};
use qsv_dateparser::parse_with_preference_and_timezone;
use rayon::{
    iter::{IndexedParallelIterator, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use serde::Deserialize;

use crate::{
    CliResult,
    config::{Config, Delimiter},
    select::SelectColumns,
    util,
    util::replace_column_value,
};

#[allow(dead_code)]
#[derive(Deserialize)]
struct Args {
    arg_column:          SelectColumns,
    arg_input:           Option<String>,
    flag_rename:         Option<String>,
    flag_prefer_dmy:     bool,
    flag_keep_zero_time: bool,
    flag_ts_resolution:  String,
    flag_formatstr:      String,
    flag_input_tz:       String,
    flag_output_tz:      String,
    flag_default_tz:     Option<String>,
    flag_utc:            bool,
    flag_zulu:           bool,
    flag_batch:          usize,
    flag_jobs:           Option<usize>,
    flag_new_column:     Option<String>,
    flag_output:         Option<String>,
    flag_no_headers:     bool,
    flag_delimiter:      Option<Delimiter>,
    flag_progressbar:    bool,
}

#[derive(Default, Clone, Copy)]
enum TimestampResolution {
    #[default]
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl FromStr for TimestampResolution {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sec" => Ok(TimestampResolution::Second),
            "milli" => Ok(TimestampResolution::Millisecond),
            "micro" => Ok(TimestampResolution::Microsecond),
            "nano" => Ok(TimestampResolution::Nanosecond),
            _ => Err(format!("Invalid timestamp resolution: {s}")),
        }
    }
}

#[inline]
fn unix_timestamp(input: &str, resolution: TimestampResolution) -> Option<DateTime<Utc>> {
    let Ok(ts_input_val) = atoi_simd::parse::<i64>(input.as_bytes()) else {
        return None;
    };

    match resolution {
        TimestampResolution::Second => Utc
            .timestamp_opt(ts_input_val, 0)
            .single()
            .map(|result| result.with_timezone(&Utc)),
        TimestampResolution::Millisecond => Utc
            .timestamp_millis_opt(ts_input_val)
            .single()
            .map(|result| result.with_timezone(&Utc)),
        TimestampResolution::Microsecond => Utc
            .timestamp_micros(ts_input_val)
            .single()
            .map(|result| result.with_timezone(&Utc)),
        TimestampResolution::Nanosecond => {
            let result = Utc.timestamp_nanos(ts_input_val).with_timezone(&Utc);
            Some(result)
        },
    }
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_column);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.flag_output.as_ref()).writer()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    let tsres = args.flag_ts_resolution.parse::<TimestampResolution>()?;

    let mut headers = rdr.headers()?.clone();

    if let Some(new_name) = args.flag_rename {
        let new_col_names = util::ColumnNameParser::new(&new_name).parse()?;
        if new_col_names.len() != sel.len() {
            return fail_incorrectusage_clierror!(
                "Number of new columns does not match input column selection."
            );
        }
        for (i, col_index) in sel.iter().enumerate() {
            headers = replace_column_value(&headers, *col_index, &new_col_names[i]);
        }
    }

    if !rconfig.no_headers {
        if let Some(new_column) = &args.flag_new_column {
            headers.push_field(new_column);
        }
        wtr.write_record(&headers)?;
    }

    let mut flag_formatstr = args.flag_formatstr;
    let flag_new_column = args.flag_new_column;

    // prep progress bar
    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    let show_progress =
        (args.flag_progressbar || util::get_envvar_flag("QSV_PROGRESSBAR")) && !rconfig.is_stdin();

    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    let progress = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr_with_hz(5));

    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    if show_progress {
        util::prep_progress(&progress, util::count_rows(&rconfig)?);
    } else {
        progress.set_draw_target(ProgressDrawTarget::hidden());
    }

    let prefer_dmy = args.flag_prefer_dmy || rconfig.get_dmy_preference();
    let keep_zero_time = args.flag_keep_zero_time;

    // amortize memory allocation by reusing record
    #[allow(unused_assignments)]
    let mut batch_record = csv::StringRecord::new();

    let num_jobs = util::njobs(args.flag_jobs);

    // reuse batch buffers
    let batchsize = util::optimal_batch_size(&rconfig, args.flag_batch, num_jobs);
    let mut batch = Vec::with_capacity(batchsize);
    let mut batch_results = Vec::with_capacity(batchsize);

    // set timezone variables
    let default_tz = match args.flag_default_tz.as_deref() {
        Some(tz) => {
            if tz.eq_ignore_ascii_case("local") {
                if let Some(tz) = localzone::get_local_zone() {
                    log::info!("default-tz local timezone: {tz}");
                    tz.parse::<Tz>()?
                } else {
                    log::warn!("default-tz local timezone {tz} not found. Defaulting to UTC.");
                    chrono_tz::UTC
                }
            } else {
                tz.parse::<Tz>()?
            }
        },
        None => chrono_tz::UTC,
    };

    let mut input_tz = match args.flag_input_tz.parse::<Tz>() {
        Ok(tz) => tz,
        _ => {
            if args.flag_input_tz.eq_ignore_ascii_case("local") {
                if let Some(tz) = localzone::get_local_zone() {
                    log::info!("input-tz local timezone: {tz}");
                    tz.parse::<Tz>()?
                } else {
                    default_tz
                }
            } else {
                default_tz
            }
        },
    };
    #[allow(clippy::useless_let_if_seq)] // more readable this way
    let mut output_tz = match args.flag_output_tz.parse::<Tz>() {
        Ok(tz) => tz,
        _ => {
            if args.flag_output_tz.eq_ignore_ascii_case("local") {
                if let Some(tz) = localzone::get_local_zone() {
                    log::info!("output-tz local timezone: {tz}");
                    tz.parse::<Tz>()?
                } else {
                    default_tz
                }
            } else {
                default_tz
            }
        },
    };

    if args.flag_utc {
        input_tz = chrono_tz::UTC;
        output_tz = chrono_tz::UTC;
    }
    if args.flag_zulu {
        output_tz = chrono_tz::UTC;
        flag_formatstr = "%Y-%m-%dT%H:%M:%SZ".to_string();
    }

    let is_output_utc = output_tz == chrono_tz::UTC;

    // main loop to read CSV and construct batches for parallel processing.
    // each batch is processed via Rayon parallel iterator.
    // loop exits when batch is empty.
    'batch_loop: loop {
        for _ in 0..batchsize {
            match rdr.read_record(&mut batch_record) {
                Ok(true) => batch.push(std::mem::take(&mut batch_record)),
                Ok(false) => break, // nothing else to add to batch
                Err(e) => {
                    return fail_clierror!("Error reading file: {e}");
                },
            }
        }

        if batch.is_empty() {
            // break out of infinite loop when at EOF
            break 'batch_loop;
        }

        // do actual datefmt via Rayon parallel iterator
        batch
            .par_iter()
            .map(|record_item| {
                let mut record = record_item.clone();

                let mut cell = String::new();
                #[allow(unused_assignments)]
                let mut formatted_date = String::new();
                let mut format_date_with_tz: DateTime<Tz>;
                let mut parsed_date;
                let new_column = flag_new_column.is_some();
                for col_index in &*sel {
                    record[*col_index].clone_into(&mut cell);
                    if !cell.is_empty() {
                        parsed_date = if let Some(ts) = unix_timestamp(&cell, tsres) {
                            Ok(ts)
                        } else {
                            parse_with_preference_and_timezone(&cell, prefer_dmy, &input_tz)
                        };
                        if let Ok(format_date) = parsed_date {
                            // don't need to call with_timezone() if output_tz is UTC
                            // as format_date is already in UTC
                            formatted_date = if is_output_utc {
                                format_date.format(&flag_formatstr).to_string()
                            } else {
                                format_date_with_tz = format_date.with_timezone(&output_tz);
                                format_date_with_tz.format(&flag_formatstr).to_string()
                            };
                            if !keep_zero_time && formatted_date.ends_with("T00:00:00+00:00") {
                                formatted_date[..10].clone_into(&mut cell);
                            } else {
                                formatted_date.clone_into(&mut cell);
                            }
                        }
                    }
                    if new_column {
                        record.push_field(&cell);
                    } else {
                        record = replace_column_value(&record, *col_index, &cell);
                    }
                }
                record
            })
            .collect_into_vec(&mut batch_results);

        // rayon collect() guarantees original order, so we can just append results each batch
        for result_record in &batch_results {
            wtr.write_record(result_record)?;
        }

        #[cfg(any(feature = "feature_capable", feature = "lite"))]
        if show_progress {
            progress.inc(batch.len() as u64);
        }

        batch.clear();
    } // end batch loop

    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    if show_progress {
        util::finish_progress(&progress);
    }
    Ok(wtr.flush()?)
}
