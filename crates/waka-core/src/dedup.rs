use std::cmp;

use anyhow::anyhow;
use bon::Builder;
use csv::ByteRecord;
use rayon::slice::ParallelSliceMut;

use crate::{
    config::{Config, Delimiter},
    select::SelectColumns,
    sort::{iter_cmp, iter_cmp_num},
    util,
};
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug, Into))]
pub struct Args {
    #[builder(into)]
    pub arg_input:         Option<String>,
    pub flag_select:       SelectColumns,
    pub flag_numeric:      bool,
    pub flag_ignore_case:  bool,
    pub flag_sorted:       bool,
    #[builder(into)]
    pub flag_dupes_output: Option<String>,
    #[builder(into)]
    pub flag_output:       Option<String>,
    pub flag_no_headers:   bool,
    pub flag_delimiter:    Option<Delimiter>,
    pub flag_jobs:         Option<usize>,
    pub flag_memcheck:     bool,
}

#[derive(Debug, Clone, PartialEq, Hash, Copy, Eq)]
pub enum ComparisonMode {
    Numeric,
    IgnoreCase,
    Normal,
}

pub fn run(args: Args) -> anyhow::Result<usize> {
    let compare_mode = if args.flag_numeric {
        ComparisonMode::Numeric
    } else if args.flag_ignore_case {
        ComparisonMode::IgnoreCase
    } else {
        ComparisonMode::Normal
    };

    let rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.flag_output.as_ref()).writer()?;
    let dupes_output = args.flag_dupes_output.is_some();
    let mut dupewtr = Config::new(args.flag_dupes_output.as_ref()).writer()?;

    let headers = rdr.byte_headers()?;
    if dupes_output {
        dupewtr.write_byte_record(headers)?;
    }
    let sel = rconfig.selection(headers)?;

    rconfig.write_headers(&mut rdr, &mut wtr)?;
    let mut dupe_count = 0_usize;

    if args.flag_sorted {
        let mut record = ByteRecord::new();
        let mut next_record = ByteRecord::new();

        rdr.read_byte_record(&mut record)?;
        loop {
            let more_records = rdr.read_byte_record(&mut next_record)?;
            if !more_records {
                wtr.write_byte_record(&record)?;
                break;
            }
            let a = sel.select(&record);
            let b = sel.select(&next_record);
            let comparison = match compare_mode {
                ComparisonMode::Normal => iter_cmp(a, b),
                ComparisonMode::Numeric => iter_cmp_num(a, b),
                ComparisonMode::IgnoreCase => iter_cmp_ignore_case(a, b),
            };
            match comparison {
                cmp::Ordering::Equal => {
                    dupe_count += 1;
                    if dupes_output {
                        dupewtr.write_byte_record(&record)?;
                    }
                },
                cmp::Ordering::Less => {
                    wtr.write_byte_record(&record)?;
                    record.clone_from(&next_record);
                },
                cmp::Ordering::Greater => {
                    return Err(anyhow!(
                        r#"Aborting! Input not sorted! Current record is greater than Next record.
  Compare mode: {compare_mode:?};  Select columns index/es (0-based): {sel:?}
  Current: {record:?}
     Next: {next_record:?}
"#
                    ));
                },
            }
        }
    } else {
        // we're loading the entire file into memory, we need to check avail mem
        if let Some(path) = rconfig.path.clone() {
            util::mem_file_check(&path, false, args.flag_memcheck)?;
        }

        util::njobs(args.flag_jobs);

        let mut all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
        match compare_mode {
            ComparisonMode::Normal => {
                all.par_sort_by(|r1, r2| {
                    let a = sel.select(r1);
                    let b = sel.select(r2);
                    iter_cmp(a, b)
                });
            },
            ComparisonMode::Numeric => {
                all.par_sort_by(|r1, r2| {
                    let a = sel.select(r1);
                    let b = sel.select(r2);
                    iter_cmp_num(a, b)
                });
            },
            ComparisonMode::IgnoreCase => {
                all.par_sort_by(|r1, r2| {
                    let a = sel.select(r1);
                    let b = sel.select(r2);
                    iter_cmp_ignore_case(a, b)
                });
            },
        }

        for (current, current_record) in all.iter().enumerate() {
            let a = sel.select(current_record);
            if let Some(next_record) = all.get(current + 1) {
                let b = sel.select(next_record);
                match compare_mode {
                    ComparisonMode::Normal => {
                        if iter_cmp(a, b) == cmp::Ordering::Equal {
                            dupe_count += 1;
                            if dupes_output {
                                dupewtr.write_byte_record(current_record)?;
                            }
                        } else {
                            wtr.write_byte_record(current_record)?;
                        }
                    },
                    ComparisonMode::Numeric => {
                        if iter_cmp_num(a, b) == cmp::Ordering::Equal {
                            dupe_count += 1;
                            if dupes_output {
                                dupewtr.write_byte_record(current_record)?;
                            }
                        } else {
                            wtr.write_byte_record(current_record)?;
                        }
                    },
                    ComparisonMode::IgnoreCase => {
                        if iter_cmp_ignore_case(a, b) == cmp::Ordering::Equal {
                            dupe_count += 1;
                            if dupes_output {
                                dupewtr.write_byte_record(current_record)?;
                            }
                        } else {
                            wtr.write_byte_record(current_record)?;
                        }
                    },
                }
            } else {
                wtr.write_byte_record(current_record)?;
            }
        }
    }

    dupewtr.flush()?;
    wtr.flush()?;

    Ok(dupe_count)
}

/// Try comparing `a` and `b` ignoring the case
#[inline]
pub fn iter_cmp_ignore_case<'a, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    L: Iterator<Item = &'a [u8]>,
    R: Iterator<Item = &'a [u8]>,
{
    loop {
        match (next_no_case(&mut a), next_no_case(&mut b)) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(&y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}

#[inline]
fn next_no_case<'a, X>(xs: &mut X) -> Option<String>
where
    X: Iterator<Item = &'a [u8]>,
{
    xs.next()
        .and_then(|bytes| simdutf8::basic::from_utf8(bytes).ok())
        .map(str::to_lowercase)
}
