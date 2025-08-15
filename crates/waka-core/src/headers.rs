use std::{io, path::PathBuf};

use serde::Deserialize;
use tabwriter::TabWriter;

use crate::{CliResult, config::Delimiter, util};

#[derive(Deserialize)]
struct Args {
    arg_input:       Vec<PathBuf>,
    flag_just_names: bool,
    flag_just_count: bool,
    flag_intersect:  bool,
    flag_trim:       bool,
    flag_delimiter:  Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let mut args: Args = util::get_args(USAGE, argv)?;
    let tmpdir = tempfile::tempdir()?;
    args.arg_input = util::process_input(args.arg_input, &tmpdir, "")?;
    let configs = util::many_configs(&args.arg_input, args.flag_delimiter, true, false)?;

    let num_inputs = configs.len();
    let mut headers: Vec<Vec<u8>> = vec![];
    for conf in configs {
        let mut rdr = conf.reader()?;
        for header in rdr.byte_headers()? {
            if !args.flag_intersect || !headers.iter().any(|h| &**h == header) {
                headers.push(header.to_vec());
            }
        }
    }

    let mut wtr: Box<dyn io::Write> = if args.flag_just_names || args.flag_just_count {
        Box::new(io::stdout())
    } else {
        Box::new(TabWriter::new(io::stdout()))
    };
    if args.flag_just_count {
        writeln!(wtr, "{}", headers.len())?;
    } else {
        for (i, header) in headers.iter().enumerate() {
            if num_inputs == 1 && !args.flag_just_names {
                write!(&mut wtr, "{}\t", i + 1)?;
            }
            if args.flag_trim {
                wtr.write_all(
                    std::string::String::from_utf8_lossy(header)
                        .trim_matches(|c| c == '"' || c == ' ')
                        .as_bytes(),
                )?;
            } else {
                wtr.write_all(header)?;
            }
            wtr.write_all(b"\n")?;
        }
    }
    Ok(wtr.flush()?)
}
