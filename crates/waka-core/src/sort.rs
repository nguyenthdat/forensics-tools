use std::{cmp, str::FromStr};

use anyhow::anyhow;
// use fastrand; //DevSkim: ignore DS148264
use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use rand_hc::Hc128Rng;
use rand_xoshiro::Xoshiro256Plus;
use rayon::slice::ParallelSliceMut;
use serde::Deserialize;
use simdutf8::basic::from_utf8;
use strum_macros::EnumString;

use self::Number::{Float, Int};
use crate::{
    config::{Config, Delimiter},
    dedup::iter_cmp_ignore_case,
    select::SelectColumns,
    util,
};

#[derive(Deserialize)]
struct Args {
    arg_input:        Option<String>,
    flag_select:      SelectColumns,
    flag_numeric:     bool,
    flag_natural:     bool,
    flag_reverse:     bool,
    flag_ignore_case: bool,
    flag_unique:      bool,
    flag_random:      bool,
    flag_seed:        Option<u64>,
    flag_rng:         String,
    flag_jobs:        Option<usize>,
    flag_faster:      bool,
    flag_output:      Option<String>,
    flag_no_headers:  bool,
    flag_delimiter:   Option<Delimiter>,
    flag_memcheck:    bool,
}

#[derive(Debug, EnumString, PartialEq)]
#[strum(ascii_case_insensitive)]
enum RngKind {
    Standard,
    Faster,
    Cryptosecure,
}

pub fn run(argv: &[&str]) -> anyhow::Result<()> {
    let args: Args = util::get_args("", argv)?;
    let numeric = args.flag_numeric;
    let natural = args.flag_natural;
    let reverse = args.flag_reverse;
    let random = args.flag_random;
    let faster = args.flag_faster;
    let rconfig = Config::builder()
        .maybe_path(args.arg_input.as_ref())
        .build()
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let Ok(rng_kind) = RngKind::from_str(&args.flag_rng) else {
        return Err(anyhow!(
            "Invalid RNG algorithm `{}`. Supported RNGs are: standard, faster, cryptosecure.",
            args.flag_rng
        ));
    };

    // we're loading the entire file into memory, we need to check avail memory
    if let Some(path) = rconfig.path.clone() {
        // we only check if we're doing a stable sort and its not --random
        // coz with --faster option, the sort algorithm sorts in-place (non-allocating)
        if !faster && !random {
            util::mem_file_check(&path, false, args.flag_memcheck)?;
        }
    }

    let mut rdr = rconfig.reader()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    util::njobs(args.flag_jobs);

    // Seeding RNG
    let seed = args.flag_seed;

    let ignore_case = args.flag_ignore_case;

    let mut all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
    // Tuple ordering and boolean flag meanings:
    // numeric: Sort numerically
    // natural: Sort in natural order https://en.wikipedia.org/wiki/Natural_sort_order
    // reverse: Sort in reverse order
    // random: Sort randomly
    // faster: Use faster parallel "unstable" sorting algorithm by using
    //   non-allocating, par_sort_unstable_by
    //   https://docs.rs/rayon/latest/rayon/slice/trait.ParallelSliceMut.html#method.par_sort_unstable_by
    // if all flags are false (the default), then we do a stable parallel, lexicographical sort
    match (numeric, natural, reverse, random, faster) {
        // --random sort
        (_, _, _, true, _) => {
            match rng_kind {
                RngKind::Standard => {
                    if let Some(val) = seed {
                        let mut rng = StdRng::seed_from_u64(val); //DevSkim: ignore DS148264
                        all.shuffle(&mut rng); //DevSkim: ignore DS148264
                    } else {
                        let mut rng = ::rand::rng();
                        all.shuffle(&mut rng); //DevSkim: ignore DS148264
                    }
                },
                RngKind::Faster => {
                    let mut rng = match args.flag_seed {
                        None => Xoshiro256Plus::from_os_rng(),
                        Some(sd) => Xoshiro256Plus::seed_from_u64(sd), // DevSkim: ignore DS148264
                    };
                    SliceRandom::shuffle(&mut *all, &mut rng); //DevSkim: ignore DS148264
                },
                RngKind::Cryptosecure => {
                    let seed_32 = match args.flag_seed {
                        None => rand::rng().random::<[u8; 32]>(),
                        Some(seed) => {
                            let seed_u8 = seed.to_le_bytes();
                            let mut seed_32 = [0u8; 32];
                            seed_32[..8].copy_from_slice(&seed_u8);
                            seed_32
                        },
                    };
                    let mut rng: Hc128Rng = match args.flag_seed {
                        None => Hc128Rng::from_os_rng(),
                        Some(_) => Hc128Rng::from_seed(seed_32),
                    };
                    SliceRandom::shuffle(&mut *all, &mut rng);
                },
            }
        },

        // default stable parallel sort
        (false, false, false, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_ignore_case(a, b)
            } else {
                iter_cmp(a, b)
            }
        }),
        // default --faster unstable, non-allocating parallel sort
        (false, false, false, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_ignore_case(a, b)
            } else {
                iter_cmp(a, b)
            }
        }),

        // --natural stable parallel natural sort
        (false, true, false, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(a, b)
            } else {
                iter_cmp_natural(a, b)
            }
        }),
        // --natural --faster unstable, non-allocating parallel natural sort
        (false, true, false, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(a, b)
            } else {
                iter_cmp_natural(a, b)
            }
        }),

        // --numeric stable parallel numeric sort
        (true, false, false, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(a, b)
        }),
        // --numeric --faster unstable, non-allocating, parallel numeric sort
        (true, false, false, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(a, b)
        }),

        // --reverse stable parallel sort
        (false, false, true, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_ignore_case(b, a)
            } else {
                iter_cmp(b, a)
            }
        }),
        // --reverse --faster unstable parallel sort
        (false, false, true, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_ignore_case(b, a)
            } else {
                iter_cmp(b, a)
            }
        }),

        // --natural --reverse stable parallel natural sort
        (false, true, true, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(b, a)
            } else {
                iter_cmp_natural(b, a)
            }
        }),
        // --natural --reverse --faster unstable parallel natural sort
        (false, true, true, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(b, a)
            } else {
                iter_cmp_natural(b, a)
            }
        }),

        // --numeric --reverse stable sort
        (true, false, true, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(b, a)
        }),
        // --numeric --reverse --faster unstable sort
        (true, false, true, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(b, a)
        }),

        // --numeric --natural stable sort (natural takes precedence over numeric)
        (true, true, false, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(a, b)
            } else {
                iter_cmp_natural(a, b)
            }
        }),
        // --numeric --natural --faster unstable sort
        (true, true, false, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(a, b)
            } else {
                iter_cmp_natural(a, b)
            }
        }),

        // --numeric --natural --reverse stable sort
        (true, true, true, false, false) => all.par_sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(b, a)
            } else {
                iter_cmp_natural(b, a)
            }
        }),
        // --numeric --natural --reverse --faster unstable sort
        (true, true, true, false, true) => all.par_sort_unstable_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            if ignore_case {
                iter_cmp_natural_ignore_case(b, a)
            } else {
                iter_cmp_natural(b, a)
            }
        }),
    }

    let mut wtr = Config::builder()
        .maybe_path(args.flag_output.as_ref())
        .build()
        .writer()?;
    let mut prev: Option<csv::ByteRecord> = None;
    rconfig.write_headers(&mut rdr, &mut wtr)?;
    if args.flag_unique {
        for r in all {
            match prev {
                Some(other_r) => {
                    let comparison = if numeric {
                        iter_cmp_num(sel.select(&r), sel.select(&other_r))
                    } else if natural {
                        if ignore_case {
                            iter_cmp_natural_ignore_case(sel.select(&r), sel.select(&other_r))
                        } else {
                            iter_cmp_natural(sel.select(&r), sel.select(&other_r))
                        }
                    } else if ignore_case {
                        iter_cmp_ignore_case(sel.select(&r), sel.select(&other_r))
                    } else {
                        iter_cmp(sel.select(&r), sel.select(&other_r))
                    };
                    match comparison {
                        cmp::Ordering::Equal => (),
                        _ => {
                            wtr.write_byte_record(&r)?;
                        },
                    }
                },
                None => {
                    wtr.write_byte_record(&r)?;
                },
            }
            prev = Some(r);
        }
    } else {
        for r in all {
            wtr.write_byte_record(&r)?;
        }
    }
    Ok(wtr.flush()?)
}

/// Order `a` and `b` lexicographically using `Ord`
#[inline]
pub fn iter_cmp<A, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    A: Ord,
    L: Iterator<Item = A>,
    R: Iterator<Item = A>,
{
    loop {
        match (a.next(), b.next()) {
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

/// Try parsing `a` and `b` as numbers when ordering
#[inline]
pub fn iter_cmp_num<'a, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    L: Iterator<Item = &'a [u8]>,
    R: Iterator<Item = &'a [u8]>,
{
    loop {
        match (next_num(&mut a), next_num(&mut b)) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match compare_num(x, y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}

/// Order `a` and `b` using natural sort order
#[inline]
pub fn iter_cmp_natural<'a, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    L: Iterator<Item = &'a [u8]>,
    R: Iterator<Item = &'a [u8]>,
{
    loop {
        match (a.next(), b.next()) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => {
                let comparison = compare_natural_strings(x, y);
                match comparison {
                    cmp::Ordering::Equal => (),
                    non_eq => return non_eq,
                }
            },
        }
    }
}

/// Order `a` and `b` using natural sort order, ignoring case
#[inline]
pub fn iter_cmp_natural_ignore_case<'a, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    L: Iterator<Item = &'a [u8]>,
    R: Iterator<Item = &'a [u8]>,
{
    loop {
        match (a.next(), b.next()) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => {
                let comparison = compare_natural_strings_ignore_case(x, y);
                match comparison {
                    cmp::Ordering::Equal => (),
                    non_eq => return non_eq,
                }
            },
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Number {
    Int(i64),
    Float(f64),
}

#[inline]
fn compare_num(n1: Number, n2: Number) -> cmp::Ordering {
    match (n1, n2) {
        (Int(i1), Int(i2)) => i1.cmp(&i2),
        #[allow(clippy::cast_precision_loss)]
        (Int(i1), Float(f2)) => compare_float(i1 as f64, f2),
        #[allow(clippy::cast_precision_loss)]
        (Float(f1), Int(i2)) => compare_float(f1, i2 as f64),
        (Float(f1), Float(f2)) => compare_float(f1, f2),
    }
}

#[allow(clippy::inline_always)]
// This function is part of a performance-critical hot path. Inlining it
// avoids the overhead of a function call, improving performance.
#[inline(always)]
fn compare_float(f1: f64, f2: f64) -> cmp::Ordering {
    f1.partial_cmp(&f2).unwrap_or(cmp::Ordering::Equal)
}

#[inline]
fn next_num<'a, X>(xs: &mut X) -> Option<Number>
where
    X: Iterator<Item = &'a [u8]>,
{
    match xs.next() {
        Some(bytes) => {
            if let Ok(i) = atoi_simd::parse::<i64>(bytes) {
                Some(Number::Int(i))
            } else {
                // If parsing as i64 failed, try parsing as f64
                if let Ok(f) = from_utf8(bytes).unwrap().parse::<f64>() {
                    Some(Number::Float(f))
                } else {
                    None
                }
            }
        },
        None => None,
    }
}

#[inline]
fn compare_natural_strings(a: &[u8], b: &[u8]) -> cmp::Ordering {
    compare_natural_bytes(a, b, false)
}

#[inline]
fn compare_natural_strings_ignore_case(a: &[u8], b: &[u8]) -> cmp::Ordering {
    compare_natural_bytes(a, b, true)
}

#[inline]
fn compare_natural_bytes(a: &[u8], b: &[u8], ignore_case: bool) -> cmp::Ordering {
    let mut a_pos = 0;
    let mut b_pos = 0;

    let mut a_byte;
    let mut b_byte;

    let mut num_comparison;
    let mut char_comparison;

    let mut a_num;
    let mut b_num;
    let mut a_end;
    let mut b_end;

    let mut a_char;
    let mut b_char;

    while a_pos < a.len() && b_pos < b.len() {
        a_byte = a[a_pos];
        b_byte = b[b_pos];

        // If both are ASCII digits, collect the full numbers and compare them
        if a_byte.is_ascii_digit() && b_byte.is_ascii_digit() {
            (a_num, a_end) = collect_number_from_bytes(a, a_pos);
            (b_num, b_end) = collect_number_from_bytes(b, b_pos);

            num_comparison = a_num.cmp(&b_num);
            if num_comparison != cmp::Ordering::Equal {
                return num_comparison;
            }

            a_pos = a_end;
            b_pos = b_end;
        } else if a_byte.is_ascii_digit() {
            // Digits come before non-digits
            return cmp::Ordering::Less;
        } else if b_byte.is_ascii_digit() {
            // Digits come before non-digits
            return cmp::Ordering::Greater;
        } else {
            // Both are non-digits, compare normally
            a_char = if ignore_case {
                a_byte.to_ascii_lowercase()
            } else {
                a_byte
            };
            b_char = if ignore_case {
                b_byte.to_ascii_lowercase()
            } else {
                b_byte
            };

            char_comparison = a_char.cmp(&b_char);
            if char_comparison != cmp::Ordering::Equal {
                return char_comparison;
            }
            a_pos += 1;
            b_pos += 1;
        }
    }

    // If we've exhausted one string but not the other
    a_pos.cmp(&b_pos)
}

#[inline]
fn collect_number_from_bytes(bytes: &[u8], start: usize) -> (i64, usize) {
    let mut pos = start;

    // Find the end of the digit sequence
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }

    // Parse the number using SIMD-optimized parsing
    let num = atoi_simd::parse::<i64>(&bytes[start..pos]).unwrap_or(0);
    (num, pos)
}
