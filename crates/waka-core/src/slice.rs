use std::{fs, path::PathBuf};

use serde::Deserialize;

use crate::{
    config::{Config, Delimiter},
    index::Indexed,
    util,
};

#[allow(clippy::unsafe_derive_deserialize)]
#[derive(Deserialize)]
struct Args {
    arg_input:       Option<String>,
    flag_start:      Option<isize>,
    flag_end:        Option<usize>,
    flag_len:        Option<usize>,
    flag_index:      Option<isize>,
    flag_json:       bool,
    flag_output:     Option<String>,
    flag_no_headers: bool,
    flag_delimiter:  Option<Delimiter>,
    flag_invert:     bool,
}

pub fn run(argv: &[&str]) -> anyhow::Result<()> {
    let mut args: Args = util::get_args("", argv)?;

    let tmpdir = tempfile::tempdir()?;
    let work_input = util::process_input(
        vec![PathBuf::from(
            // if no input file is specified, read from stdin "-"
            args.arg_input.clone().unwrap_or_else(|| "-".to_string()),
        )],
        &tmpdir,
        "",
    )?;

    // safety: there's at least one valid element in work_input
    let input_filename = work_input[0]
        .canonicalize()?
        .into_os_string()
        .into_string()
        .unwrap();

    args.arg_input = Some(input_filename);

    match args.rconfig().indexed()? {
        Some(idxed) => args.with_index(idxed),
        _ => args.no_index(),
    }
}

impl Args {
    fn no_index(&self) -> anyhow::Result<()> {
        let mut rdr = self.rconfig().reader()?;

        let (start, end) = self.range()?;
        if self.flag_json {
            let headers = rdr.byte_headers()?.clone();
            let records = rdr.byte_records().enumerate().filter_map(move |(i, r)| {
                let should_include = if self.flag_invert {
                    i < start || i >= end
                } else {
                    i >= start && i < end
                };
                if should_include {
                    Some(r.unwrap())
                } else {
                    None
                }
            });
            util::write_json(
                self.flag_output.as_ref(),
                self.flag_no_headers,
                &headers,
                records,
            )
        } else {
            let mut wtr = self.wconfig().writer()?;
            self.rconfig().write_headers(&mut rdr, &mut wtr)?;

            for (i, r) in rdr.byte_records().enumerate() {
                if self.flag_invert == (i < start || i >= end) {
                    wtr.write_byte_record(&r?)?;
                }
            }
            Ok(wtr.flush()?)
        }
    }

    fn with_index(&self, mut indexed_file: Indexed<fs::File, fs::File>) -> anyhow::Result<()> {
        let (start, end) = self.range()?;
        if end - start == 0 && !self.flag_invert {
            return Ok(());
        }

        if self.flag_json {
            let headers = indexed_file.byte_headers()?.clone();
            let total_rows = util::count_rows(&self.rconfig())?;
            let records = if self.flag_invert {
                let mut records: Vec<csv::ByteRecord> =
                    Vec::with_capacity(start + (total_rows as usize - end));
                // Get records before start
                indexed_file.seek(0)?;
                for r in indexed_file.byte_records().take(start) {
                    records.push(r.unwrap());
                }

                // Get records after end
                indexed_file.seek(end as u64)?;
                for r in indexed_file.byte_records().take(total_rows as usize - end) {
                    records.push(r.unwrap());
                }
                records
            } else {
                indexed_file.seek(start as u64)?;
                indexed_file
                    .byte_records()
                    .take(end - start)
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>()
            };
            util::write_json(
                self.flag_output.as_ref(),
                self.flag_no_headers,
                &headers,
                records.into_iter(),
            )
        } else {
            let mut wtr = self.wconfig().writer()?;
            self.rconfig().write_headers(&mut *indexed_file, &mut wtr)?;

            let total_rows = util::count_rows(&self.rconfig())? as usize;
            if self.flag_invert {
                // Get records before start
                indexed_file.seek(0)?;
                for r in indexed_file.byte_records().take(start) {
                    wtr.write_byte_record(&r?)?;
                }

                // Get records after end
                indexed_file.seek(end as u64)?;
                for r in indexed_file.byte_records().take(total_rows - end) {
                    wtr.write_byte_record(&r?)?;
                }
            } else {
                indexed_file.seek(start as u64)?;
                for r in indexed_file.byte_records().take(end - start) {
                    wtr.write_byte_record(&r?)?;
                }
            }
            Ok(wtr.flush()?)
        }
    }

    fn range(&self) -> anyhow::Result<(usize, usize)> {
        let mut start = None;
        if let Some(start_arg) = self.flag_start {
            if start_arg < 0 {
                start = Some(
                    (util::count_rows(&self.rconfig())? as usize)
                        .abs_diff(start_arg.unsigned_abs()),
                );
            } else {
                start = Some(start_arg as usize);
            }
        }
        let index = if let Some(flag_index) = self.flag_index {
            if flag_index < 0 {
                let index = (util::count_rows(&self.rconfig())? as usize)
                    .abs_diff(flag_index.unsigned_abs());
                Some(index)
            } else {
                Some(flag_index as usize)
            }
        } else {
            None
        };
        Ok(util::range(start, self.flag_end, self.flag_len, index)?)
    }

    fn rconfig(&self) -> Config {
        Config::new(self.arg_input.as_ref())
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
    }

    fn wconfig(&self) -> Config {
        Config::new(self.flag_output.as_ref())
    }
}
