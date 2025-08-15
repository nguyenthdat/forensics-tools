use serde::Deserialize;

use crate::{
    CliResult,
    config::{Config, Delimiter},
    util,
};

#[derive(Deserialize)]
struct Args {
    arg_input:       Option<String>,
    flag_output:     Option<String>,
    flag_no_headers: bool,
    flag_delimiter:  Option<Delimiter>,
    flag_memcheck:   bool,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.flag_output.as_ref()).writer()?;

    if let Some(mut idx_file) = rconfig.indexed()? {
        // we have an index, no need to check avail mem,
        // we're reading the file in reverse streaming
        rconfig.write_headers(&mut rdr, &mut wtr)?;
        let mut record = csv::ByteRecord::new();
        let mut pos = idx_file.count().saturating_sub(1);

        while idx_file.seek(pos).is_ok() {
            idx_file.read_byte_record(&mut record)?;
            wtr.write_byte_record(&record)?;
            pos -= 1;
        }
    } else {
        // we don't have an index, we need to read the entire file into memory
        // we're loading the entire file into memory, we need to check avail mem
        if let Some(ref path) = rconfig.path {
            util::mem_file_check(path, false, args.flag_memcheck)?;
        }

        let mut all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
        all.reverse();

        rconfig.write_headers(&mut rdr, &mut wtr)?;
        for r in all {
            wtr.write_byte_record(&r)?;
        }
    }

    Ok(wtr.flush()?)
}
