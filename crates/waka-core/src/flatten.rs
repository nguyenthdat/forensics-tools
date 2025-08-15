use std::{
    borrow::Cow,
    io::{self, BufWriter, Write},
};

use serde::Deserialize;
use tabwriter::TabWriter;

use crate::{
    CliResult,
    config::{Config, DEFAULT_WTR_BUFFER_CAPACITY, Delimiter},
    util,
};

#[derive(Deserialize)]
struct Args {
    arg_input:            Option<String>,
    flag_condense:        Option<usize>,
    flag_field_separator: Option<String>,
    flag_separator:       String,
    flag_no_headers:      bool,
    flag_delimiter:       Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);
    let mut rdr = rconfig.reader()?;
    let headers = rdr.byte_headers()?.clone();

    let stdoutlock = io::stdout().lock();
    let bufwtr = BufWriter::with_capacity(DEFAULT_WTR_BUFFER_CAPACITY, stdoutlock);
    let mut wtr = TabWriter::new(bufwtr);

    let mut first = true;
    let mut record = csv::ByteRecord::new();
    let separator_flag = !args.flag_separator.is_empty();
    let separator = args.flag_separator;
    let field_separator_flag = args.flag_field_separator.is_some();
    let field_separator = args.flag_field_separator.unwrap_or_default().into_bytes();

    while rdr.read_byte_record(&mut record)? {
        if !first && separator_flag {
            writeln!(&mut wtr, "{separator}")?;
        }
        first = false;
        for (i, (header, field)) in headers.iter().zip(&record).enumerate() {
            if rconfig.no_headers {
                write!(&mut wtr, "{i}")?;
            } else {
                wtr.write_all(header)?;
            }
            wtr.write_all(b"\t")?;
            if field_separator_flag {
                wtr.write_all(&field_separator)?;
            }
            wtr.write_all(&util::condense(Cow::Borrowed(field), args.flag_condense))?;
            wtr.write_all(b"\n")?;
        }
    }
    wtr.flush()?;
    Ok(())
}
