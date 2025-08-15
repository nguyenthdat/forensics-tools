use std::cmp;

use csv::ByteRecord;
#[cfg(any(feature = "feature_capable", feature = "lite"))]
use indicatif::{HumanCount, ProgressBar, ProgressDrawTarget};
use serde::{Deserialize, Serialize};

use crate::{
    CliResult,
    cmd::{dedup, sort::iter_cmp},
    config::{Config, Delimiter},
    select::SelectColumns,
    util,
};

#[allow(dead_code)]
#[derive(Deserialize)]
struct Args {
    arg_input:        Option<String>,
    flag_select:      SelectColumns,
    flag_ignore_case: bool,
    flag_all:         bool,
    flag_no_headers:  bool,
    flag_delimiter:   Option<Delimiter>,
    flag_progressbar: bool,
    flag_json:        bool,
    flag_pretty_json: bool,
}

#[derive(Serialize, Deserialize)]
struct SortCheckStruct {
    sorted:          bool,
    record_count:    u64,
    unsorted_breaks: u64,
    dupe_count:      i64,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let ignore_case = args.flag_ignore_case;
    let rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = rconfig.reader()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
    let record_count;

    // prep progress bar
    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    let show_progress =
        (args.flag_progressbar || util::get_envvar_flag("QSV_PROGRESSBAR")) && !rconfig.is_stdin();
    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    let progress = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr_with_hz(5));
    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    {
        record_count = if show_progress {
            let count = util::count_rows(&rconfig)?;
            util::prep_progress(&progress, count);
            count
        } else {
            progress.set_draw_target(ProgressDrawTarget::hidden());
            0
        };
    }
    #[cfg(feature = "datapusher_plus")]
    {
        record_count = 0;
    }

    let do_json = args.flag_json | args.flag_pretty_json;

    let mut record = ByteRecord::new();
    let mut next_record = ByteRecord::new();
    let mut sorted = true;
    let mut scan_ctr: u64 = 0;
    let mut dupe_count: u64 = 0;
    let mut unsorted_breaks: u64 = 0;

    rdr.read_byte_record(&mut record)?;
    loop {
        #[cfg(any(feature = "feature_capable", feature = "lite"))]
        if show_progress {
            progress.inc(1);
        }
        scan_ctr += 1;
        let more_records = rdr.read_byte_record(&mut next_record)?;
        if !more_records {
            break;
        }
        let a = sel.select(&record);
        let b = sel.select(&next_record);
        let comparison = if ignore_case {
            dedup::iter_cmp_ignore_case(a, b)
        } else {
            iter_cmp(a, b)
        };

        match comparison {
            cmp::Ordering::Equal => {
                dupe_count += 1;
            },
            cmp::Ordering::Less => {
                record.clone_from(&next_record);
            },
            cmp::Ordering::Greater => {
                sorted = false;
                if args.flag_all || do_json {
                    unsorted_breaks += 1;
                    record.clone_from(&next_record);
                } else {
                    break;
                }
            },
        }
    } // end loop

    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    if show_progress {
        if sorted {
            progress.set_message(format!(
                " - ALL {} records checked. {} duplicates found. Sorted.",
                HumanCount(record_count),
                HumanCount(dupe_count),
            ));
        } else if args.flag_all || do_json {
            progress.set_message(format!(
                " - ALL {} records checked. {} unsorted breaks. NOT Sorted.",
                HumanCount(record_count),
                HumanCount(unsorted_breaks),
            ));
        } else {
            progress.set_message(format!(
                " - {} of {} records checked before aborting. {} duplicates found so far. NOT \
                 sorted.",
                HumanCount(scan_ctr),
                HumanCount(record_count),
                HumanCount(dupe_count),
            ));
        }
        util::finish_progress(&progress);
    }

    if do_json {
        let sortcheck_struct = SortCheckStruct {
            sorted,
            record_count: if record_count == 0 {
                scan_ctr
            } else {
                record_count
            },
            unsorted_breaks,
            dupe_count: if sorted { dupe_count as i64 } else { -1 },
        };
        // it's OK to have unwrap here as we know sortcheck_struct is valid json
        if args.flag_pretty_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&sortcheck_struct).unwrap()
            );
        } else {
            println!("{}", serde_json::to_string(&sortcheck_struct).unwrap());
        }
    }

    if !sorted {
        return fail!("not sorted");
    }

    Ok(())
}
