use std::{str::FromStr, sync::OnceLock};

use dynfmt2::Format;
use log::debug;
use rayon::{
    iter::{IndexedParallelIterator, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use regex::Regex;
use serde::Deserialize;
use smallvec::SmallVec;
use strum_macros::EnumString;

use crate::{
    CliResult,
    clitypes::CliError,
    config::{Config, Delimiter},
    regex_oncelock,
    select::SelectColumns,
    util,
    util::replace_column_value,
};

#[derive(Clone, EnumString, PartialEq)]
#[strum(use_phf)]
#[strum(ascii_case_insensitive)]
#[allow(non_camel_case_types)]
enum Operations {
    Copy,
    Escape,
    Len,
    Lower,
    Ltrim,
    Mltrim,
    Mrtrim,
    Mtrim,
    Regex_Replace,
    Replace,
    Round,
    Rtrim,
    Squeeze,
    Squeeze0,
    Strip_Prefix,
    Strip_Suffix,
    Trim,
    Upper,
}

#[derive(Deserialize)]
struct Args {
    arg_column:       SelectColumns,
    cmd_operations:   bool,
    arg_operations:   String,
    cmd_dynfmt:       bool,
    cmd_emptyreplace: bool,
    arg_input:        Option<String>,
    flag_rename:      Option<String>,
    flag_comparand:   String,
    flag_replacement: String,
    flag_formatstr:   String,
    flag_batch:       usize,
    flag_jobs:        Option<usize>,
    flag_new_column:  Option<String>,
    flag_output:      Option<String>,
    flag_no_headers:  bool,
    flag_delimiter:   Option<Delimiter>,
}

static REGEX_REPLACE: OnceLock<Regex> = OnceLock::new();
static ROUND_PLACES: OnceLock<u32> = OnceLock::new();

// default number of decimal places to round to
const DEFAULT_ROUND_PLACES: u32 = 3;

const NULL_VALUE: &str = "<null>";

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
    // safety: we just checked that sel is not empty in the previous line
    let column_index = *sel.iter().next().unwrap();

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

    // for dynfmt, safe_headers are the "safe" version of colnames - alphanumeric only,
    // all other chars replaced with underscore
    // dynfmt_fields are the columns used in the dynfmt --formatstr option
    // we prep it so we only populate the lookup vec with the index of these columns
    // so SimpleCurlyFormat is performant
    let dynfmt_template = if args.cmd_dynfmt {
        if args.flag_no_headers {
            return fail_incorrectusage_clierror!("dynfmt/calcconv subcommand requires headers.");
        }

        let mut dynfmt_template_wrk = args.flag_formatstr.clone();
        let mut dynfmt_fields = Vec::new();

        // first, get the fields used in the dynfmt template
        let formatstr_re: &'static Regex = crate::regex_oncelock!(r"\{(?P<key>\w+)?\}");
        for format_fields in formatstr_re.captures_iter(&args.flag_formatstr) {
            // safety: we already checked that the regex match is valid
            dynfmt_fields.push(format_fields.name("key").unwrap().as_str());
        }
        // we sort the fields so we can do binary_search
        dynfmt_fields.sort_unstable();

        // now, get the indices of the columns for the lookup vec
        let (safe_headers, _) = util::safe_header_names(&headers, false, false, None, "", true);
        for (i, field) in safe_headers.iter().enumerate() {
            if dynfmt_fields.binary_search(&field.as_str()).is_ok() {
                let field_with_curly = format!("{{{field}}}");
                let field_index = format!("{{{i}}}");
                dynfmt_template_wrk = dynfmt_template_wrk.replace(&field_with_curly, &field_index);
            }
        }
        debug!("dynfmt_fields: {dynfmt_fields:?}  dynfmt_template: {dynfmt_template_wrk}");
        dynfmt_template_wrk
    } else {
        String::new()
    };

    #[derive(PartialEq)]
    enum ApplydpSubCmd {
        Operations,
        DynFmt,
        EmptyReplace,
    }

    let mut ops_vec = SmallVec::<[Operations; 4]>::new();

    let applydp_cmd = if args.cmd_operations {
        match validate_operations(
            &args.arg_operations.split(',').collect(),
            &args.flag_comparand,
            &args.flag_replacement,
            &args.flag_new_column,
            &args.flag_formatstr,
        ) {
            Ok(operations_vec) => ops_vec = operations_vec,
            Err(e) => return Err(e),
        }
        ApplydpSubCmd::Operations
    } else if args.cmd_dynfmt {
        ApplydpSubCmd::DynFmt
    } else if args.cmd_emptyreplace {
        ApplydpSubCmd::EmptyReplace
    } else {
        return fail!("Unknown applydp subcommand.");
    };

    if !rconfig.no_headers {
        if let Some(new_column) = &args.flag_new_column {
            headers.push_field(new_column);
        }
        wtr.write_record(&headers)?;
    }

    // if there is a regex_replace operation and replacement is <NULL> case-insensitive,
    // we set it to empty string
    let flag_replacement = if applydp_cmd == ApplydpSubCmd::Operations
        && ops_vec.contains(&Operations::Regex_Replace)
        && args.flag_replacement.to_ascii_lowercase() == NULL_VALUE
    {
        String::new()
    } else {
        args.flag_replacement
    };
    let flag_comparand = args.flag_comparand;
    let flag_new_column = args.flag_new_column;

    // amortize memory allocation by reusing record
    #[allow(unused_assignments)]
    let mut batch_record = csv::StringRecord::new();

    // reuse batch buffers
    let batchsize: usize = if args.flag_batch == 0 {
        util::count_rows(&rconfig)? as usize
    } else {
        args.flag_batch
    };
    let mut batch = Vec::with_capacity(batchsize);
    let mut batch_results = Vec::with_capacity(batchsize);

    util::njobs(args.flag_jobs);

    // main loop to read CSV and construct batches for parallel processing.
    // each batch is processed via Rayon parallel iterator.
    // loop exits when batch is empty.
    'batch_loop: loop {
        for _ in 0..batchsize {
            match rdr.read_record(&mut batch_record) {
                Ok(has_data) => {
                    if has_data {
                        batch.push(std::mem::take(&mut batch_record));
                    } else {
                        // nothing else to add to batch
                        break;
                    }
                },
                Err(e) => {
                    return fail_clierror!("Error reading file: {e}");
                },
            }
        }

        if batch.is_empty() {
            // break out of infinite loop when at EOF
            break 'batch_loop;
        }

        // do actual applydp command via Rayon parallel iterator
        batch
            .par_iter()
            .with_min_len(1024)
            .map(|record_item| {
                let mut record = record_item.clone();
                match applydp_cmd {
                    ApplydpSubCmd::Operations => {
                        let mut cell = String::new();
                        for col_index in sel.iter() {
                            record[*col_index].clone_into(&mut cell);
                            applydp_operations(
                                &ops_vec,
                                &mut cell,
                                &flag_comparand,
                                &flag_replacement,
                            );
                            if flag_new_column.is_some() {
                                record.push_field(&cell);
                            } else {
                                record = replace_column_value(&record, *col_index, &cell);
                            }
                        }
                    },
                    ApplydpSubCmd::EmptyReplace => {
                        let mut cell = String::new();
                        for col_index in sel.iter() {
                            record[*col_index].clone_into(&mut cell);
                            if cell.trim().is_empty() {
                                cell = flag_replacement.clone();
                            }
                            if flag_new_column.is_some() {
                                record.push_field(&cell);
                            } else {
                                record = replace_column_value(&record, *col_index, &cell);
                            }
                        }
                    },
                    ApplydpSubCmd::DynFmt => {
                        let mut cell = record[column_index].to_owned();
                        if !cell.is_empty() {
                            let mut record_vec: Vec<String> = Vec::with_capacity(record.len());
                            for field in &record {
                                record_vec.push(field.to_string());
                            }
                            if let Ok(formatted) =
                                dynfmt2::SimpleCurlyFormat.format(&dynfmt_template, record_vec)
                            {
                                cell = formatted.to_string();
                            }
                        }
                        if flag_new_column.is_some() {
                            record.push_field(&cell);
                        } else {
                            record = replace_column_value(&record, column_index, &cell);
                        }
                    },
                }

                record
            })
            .collect_into_vec(&mut batch_results);

        // rayon collect() guarantees original order, so we can just append results each batch
        for result_record in &batch_results {
            wtr.write_record(result_record)?;
        }

        batch.clear();
    } // end batch loop

    Ok(wtr.flush()?)
}

// validate applydp operations for required options
// and prepare operations enum vec
fn validate_operations(
    operations: &Vec<&str>,
    flag_comparand: &str,
    flag_replacement: &str,
    flag_new_column: &Option<String>,
    flag_formatstr: &str,
) -> Result<SmallVec<[Operations; 4]>, CliError> {
    let mut copy_invokes = 0_u8;
    let mut regex_replace_invokes = 0_u8;
    let mut replace_invokes = 0_u8;
    let mut strip_invokes = 0_u8;

    let mut ops_vec = SmallVec::with_capacity(operations.len());

    for op in operations {
        let Ok(operation) = Operations::from_str(op) else {
            return fail_incorrectusage_clierror!("Unknown '{op}' operation");
        };
        match operation {
            Operations::Copy => {
                if flag_new_column.is_none() {
                    return fail_incorrectusage_clierror!(
                        "--new_column (-c) is required for copy operation."
                    );
                }
                copy_invokes = copy_invokes.saturating_add(1);
            },
            Operations::Mtrim | Operations::Mltrim | Operations::Mrtrim => {
                if flag_comparand.is_empty() {
                    return fail_incorrectusage_clierror!(
                        "--comparand (-C) is required for match trim operations."
                    );
                }
            },
            Operations::Regex_Replace => {
                if flag_comparand.is_empty() || flag_replacement.is_empty() {
                    return fail_incorrectusage_clierror!(
                        "--comparand (-C) and --replacement (-R) are required for regex_replace \
                         operation."
                    );
                }
                if regex_replace_invokes == 0 {
                    let re = match regex::Regex::new(flag_comparand) {
                        Ok(re) => re,
                        Err(err) => {
                            return fail_clierror!("regex_replace expression error: {err:?}");
                        },
                    };
                    let _ = REGEX_REPLACE.set(re);
                }
                regex_replace_invokes = regex_replace_invokes.saturating_add(1);
            },
            Operations::Replace => {
                if flag_comparand.is_empty() || flag_replacement.is_empty() {
                    return fail_incorrectusage_clierror!(
                        "--comparand (-C) and --replacement (-R) are required for replace \
                         operation."
                    );
                }
                replace_invokes = replace_invokes.saturating_add(1);
            },
            Operations::Strip_Prefix | Operations::Strip_Suffix => {
                if flag_comparand.is_empty() {
                    return fail_incorrectusage_clierror!(
                        "--comparand (-C) is required for strip operations."
                    );
                }
                strip_invokes = strip_invokes.saturating_add(1);
            },
            Operations::Round => {
                if ROUND_PLACES
                    .set(
                        flag_formatstr
                            .parse::<u32>()
                            .unwrap_or(DEFAULT_ROUND_PLACES),
                    )
                    .is_err()
                {
                    return fail!("Cannot initialize Round precision.");
                };
            },
            _ => {},
        }
        ops_vec.push(operation);
    }
    if copy_invokes > 1 || regex_replace_invokes > 1 || replace_invokes > 1 || strip_invokes > 1 {
        return fail_incorrectusage_clierror!(
            "you can only use copy({copy_invokes}), regex_replace({regex_replace_invokes}), \
             replace({replace_invokes}), and strip({strip_invokes}) ONCE per operation series."
        );
    };

    Ok(ops_vec) // no validation errors
}

#[inline]
fn applydp_operations(
    ops_vec: &SmallVec<[Operations; 4]>,
    cell: &mut String,
    comparand: &str,
    replacement: &str,
) {
    for op in ops_vec {
        match op {
            Operations::Len => {
                *cell = itoa::Buffer::new().format(cell.len()).to_owned();
            },
            Operations::Lower => {
                *cell = cell.to_lowercase();
            },
            Operations::Upper => {
                *cell = cell.to_uppercase();
            },
            Operations::Squeeze => {
                let squeezer: &'static Regex = regex_oncelock!(r"\s+");
                *cell = squeezer.replace_all(cell, " ").into_owned();
            },
            Operations::Squeeze0 => {
                let squeezer: &'static Regex = regex_oncelock!(r"\s+");
                *cell = squeezer.replace_all(cell, "").into_owned();
            },
            Operations::Trim => {
                *cell = String::from(cell.trim());
            },
            Operations::Ltrim => {
                *cell = String::from(cell.trim_start());
            },
            Operations::Rtrim => {
                *cell = String::from(cell.trim_end());
            },
            Operations::Mtrim => {
                let chars_to_trim: &[char] = &comparand.chars().collect::<Vec<_>>();
                *cell = String::from(cell.trim_matches(chars_to_trim));
            },
            Operations::Mltrim => {
                *cell = String::from(cell.trim_start_matches(comparand));
            },
            Operations::Mrtrim => {
                *cell = String::from(cell.trim_end_matches(comparand));
            },
            Operations::Escape => {
                *cell = cell.escape_default().to_string();
            },
            Operations::Strip_Prefix => {
                if let Some(stripped) = cell.strip_prefix(comparand) {
                    *cell = String::from(stripped);
                }
            },
            Operations::Strip_Suffix => {
                if let Some(stripped) = cell.strip_suffix(comparand) {
                    *cell = String::from(stripped);
                }
            },
            Operations::Replace => {
                *cell = cell.replace(comparand, replacement);
            },
            Operations::Regex_Replace => {
                // safety: we set REGEX_REPLACE in validate_operations()
                let regexreplace = REGEX_REPLACE.get().unwrap();
                *cell = regexreplace.replace_all(cell, replacement).into_owned();
            },
            Operations::Round => {
                if let Ok(num) = cell.parse::<f64>() {
                    // safety: we set ROUND_PLACES in validate_operations()
                    *cell = util::round_num(num, *ROUND_PLACES.get().unwrap());
                }
            },
            Operations::Copy => {}, // copy is a noop
        }
    }
}
