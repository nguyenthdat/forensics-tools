use dynfmt2::Format;
use foldhash::{HashMap, HashMapExt};
use serde::Deserialize;

use crate::{
    CliResult,
    config::{Config, Delimiter},
    select::SelectColumns,
    util,
    util::replace_column_value,
};

#[derive(Deserialize)]
struct Args {
    arg_column:      SelectColumns,
    arg_input:       Option<String>,
    flag_start:      u64,
    flag_increment:  u64,
    flag_formatstr:  String,
    flag_output:     Option<String>,
    flag_no_headers: bool,
    flag_delimiter:  Option<Delimiter>,
}

type Values = HashMap<String, String>;
type ValuesNum = HashMap<String, u64>;

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_column);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.flag_output.as_ref()).writer()?;

    let headers = rdr.byte_headers()?.clone();
    let column_index = match rconfig.selection(&headers) {
        Ok(sel) => {
            let sel_len = sel.len();
            if sel_len > 1 {
                return fail_incorrectusage_clierror!(
                    "{sel_len} columns selected. Only one column can be selected for \
                     pseudonymisation."
                );
            }
            // safety: we checked that sel.len() == 1
            *sel.iter().next().unwrap()
        },
        Err(e) => return fail_clierror!("{e}"),
    };

    if !rconfig.no_headers {
        wtr.write_record(&headers)?;
    }

    let mut record = csv::StringRecord::new();
    let mut counter: u64 = args.flag_start;
    let increment = args.flag_increment;
    let mut curr_counter: u64 = 0;
    let mut overflowed = false;

    if args.flag_formatstr == "{}" {
        // we don't need to use dynfmt2::SimpleCurlyFormat if the format string is "{}"
        let mut values_num = ValuesNum::with_capacity(1000);

        while rdr.read_record(&mut record)? {
            let value = record[column_index].to_owned();
            let new_value = values_num.entry(value.clone()).or_insert_with(|| {
                curr_counter = counter;
                (counter, overflowed) = counter.overflowing_add(increment);
                curr_counter
            });
            if overflowed {
                return fail_incorrectusage_clierror!(
                    "Overflowed. The counter is larger than u64::MAX {}. The last valid counter \
                     is {curr_counter}.",
                    u64::MAX
                );
            }
            record = replace_column_value(&record, column_index, &new_value.to_string());

            wtr.write_record(&record)?;
        }
    } else {
        // we need to use dynfmt2::SimpleCurlyFormat if the format string is not "{}"

        // first, validate the format string
        if !args.flag_formatstr.contains("{}")
            || dynfmt2::SimpleCurlyFormat
                .format(&args.flag_formatstr, [0])
                .is_err()
        {
            return fail_incorrectusage_clierror!(
                "Invalid format string: \"{}\". The format string must contain a single \"{{}}\" \
                 which will be replaced with the incremental identifier.",
                args.flag_formatstr
            );
        }

        let mut values = Values::with_capacity(1000);
        while rdr.read_record(&mut record)? {
            let value = record[column_index].to_owned();

            // safety: we checked that the format string contains "{}"
            let new_value = values.entry(value.clone()).or_insert_with(|| {
                curr_counter = counter;
                (counter, overflowed) = counter.overflowing_add(increment);
                dynfmt2::SimpleCurlyFormat
                    .format(&args.flag_formatstr, [curr_counter])
                    .unwrap()
                    .to_string()
            });
            if overflowed {
                return fail_incorrectusage_clierror!(
                    "Overflowed. The counter is larger than u64::MAX({}). The last valid counter \
                     is {curr_counter}.",
                    u64::MAX
                );
            }

            record = replace_column_value(&record, column_index, new_value);
            wtr.write_record(&record)?;
        }
    }

    Ok(wtr.flush()?)
}
