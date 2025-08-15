use std::collections::HashMap;

use foldhash::fast::RandomState;
use serde::{Deserialize, Serialize};

use crate::{
    CliResult,
    config::{Config, Delimiter},
    util,
};

#[derive(Deserialize)]
struct Args {
    arg_input:      Option<String>,
    flag_mode:      String,
    flag_reserved:  String,
    flag_prefix:    String,
    flag_output:    Option<String>,
    flag_delimiter: Option<Delimiter>,
}

#[derive(PartialEq)]
enum SafeNameMode {
    Always,
    Conditional,
    Verify,
    VerifyVerbose,
    VerifyVerboseJSON,
    VerifyVerbosePrettyJSON,
}

#[derive(Serialize, Deserialize)]
struct SafeNamesStruct {
    header_count:      usize,
    duplicate_count:   usize,
    duplicate_headers: Vec<String>,
    unsafe_headers:    Vec<String>,
    safe_headers:      Vec<String>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    // set SafeNames Mode
    let first_letter = args.flag_mode.chars().next().unwrap_or_default();
    let safenames_mode = match first_letter {
        'c' | 'C' => SafeNameMode::Conditional,
        'a' | 'A' => SafeNameMode::Always,
        'v' => SafeNameMode::Verify,
        'V' => SafeNameMode::VerifyVerbose,
        'j' => SafeNameMode::VerifyVerboseJSON,
        'J' => SafeNameMode::VerifyVerbosePrettyJSON,
        _ => {
            return fail_clierror!("Invalid mode: {}", args.flag_mode);
        },
    };

    let reserved_names_vec: Vec<String> = args
        .flag_reserved
        .split(',')
        .map(str::to_lowercase)
        .collect();

    let rconfig = Config::new(args.arg_input.as_ref()).delimiter(args.flag_delimiter);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.flag_output.as_ref()).writer()?;
    let old_headers = rdr.byte_headers()?;

    let mut headers = csv::StringRecord::from_byte_record_lossy(old_headers.clone());

    // trim enclosing quotes and spaces from headers as it messes up safenames
    // csv library will automatically add quotes when necessary when we write it
    let mut noquote_headers = csv::StringRecord::new();
    for header in &headers {
        noquote_headers.push_field(header.trim_matches(|c| c == '"' || c == ' '));
    }

    let (safe_headers, changed_count) = util::safe_header_names(
        &noquote_headers,
        true,
        safenames_mode == SafeNameMode::Conditional,
        Some(reserved_names_vec).as_ref(),
        &args.flag_prefix,
        false,
    );
    if let SafeNameMode::Conditional | SafeNameMode::Always = safenames_mode {
        headers.clear();
        for header_name in safe_headers {
            headers.push_field(&header_name);
        }

        // write CSV with safe headers
        wtr.write_record(headers.as_byte_record())?;
        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            wtr.write_record(&record)?;
        }
        wtr.flush()?;

        eprintln!("{changed_count}");
    } else {
        // Verify or VerifyVerbose Mode
        let mut safenames_vec: Vec<String> = Vec::new();
        let mut unsafenames_vec: Vec<String> = Vec::new();
        let mut checkednames_map: HashMap<String, u16, RandomState> = HashMap::default();
        let mut temp_string;

        for header_name in &headers {
            if safe_headers.contains(&header_name.to_string()) {
                if !safenames_vec.contains(&header_name.to_string()) {
                    safenames_vec.push(header_name.to_string());
                }
            } else {
                unsafenames_vec.push(header_name.to_string());
            }

            temp_string = header_name.to_string();
            if let Some(count) = checkednames_map.get(&temp_string) {
                checkednames_map.insert(temp_string, count + 1);
            } else {
                checkednames_map.insert(temp_string, 1);
            }
        }

        let headers_count = headers.len();
        let dupe_count = checkednames_map.values().filter(|&&v| v > 1).count();
        let unsafe_count = unsafenames_vec.len();
        let safe_count = safenames_vec.len();

        let safenames_struct = SafeNamesStruct {
            header_count:      headers_count,
            duplicate_count:   dupe_count,
            duplicate_headers: checkednames_map
                .iter()
                .filter(|&(_, &v)| v > 1)
                .map(|(k, v)| format!("{k}:{v}"))
                .collect(),
            unsafe_headers:    unsafenames_vec.clone(),
            safe_headers:      safenames_vec.clone(),
        };
        match safenames_mode {
            SafeNameMode::VerifyVerbose => {
                eprintln!(
                    r#"{num_headers} header/s
{dupe_count} duplicate/s: {dupe_headers:?}
{unsafe_count} unsafe header/s: {unsafenames_vec:?}
{num_safeheaders} safe header/s: {safenames_vec:?}"#,
                    dupe_headers = safenames_struct.duplicate_headers.join(", "),
                    num_headers = headers_count,
                    num_safeheaders = safe_count
                );
            },
            SafeNameMode::VerifyVerboseJSON | SafeNameMode::VerifyVerbosePrettyJSON => {
                if safenames_mode == SafeNameMode::VerifyVerbosePrettyJSON {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&safenames_struct).unwrap()
                    );
                } else {
                    println!("{}", serde_json::to_string(&safenames_struct).unwrap());
                }
            },
            _ => eprintln!("{unsafe_count}"),
        }
    }

    Ok(())
}
