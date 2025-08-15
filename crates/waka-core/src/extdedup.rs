use std::{
    fs,
    io::{self, BufRead, Write, stdin, stdout},
    path::PathBuf,
};

use indicatif::HumanCount;
use serde::Deserialize;
use sysinfo::System;

use crate::{
    CliResult, config,
    config::{Config, Delimiter},
    odhtcache,
    select::SelectColumns,
    util,
};

#[derive(Deserialize)]
struct Args {
    arg_input:           Option<String>,
    flag_select:         Option<SelectColumns>,
    arg_output:          Option<String>,
    flag_no_headers:     bool,
    flag_delimiter:      Option<Delimiter>,
    flag_no_output:      bool,
    flag_dupes_output:   Option<String>,
    flag_human_readable: bool,
    flag_memory_limit:   Option<u64>,
    flag_temp_dir:       Option<String>,
    flag_quiet:          bool,
}

const MEMORY_LIMITED_BUFFER: u64 = 100 * 1_000_000; // 100 MB

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    // Set the memory buffer size for the on-disk hash table based on --memory-limit
    // and system capabilities.
    let mem_limited_buffer_bytes = calculate_memory_limit(args.flag_memory_limit);
    log::info!("{mem_limited_buffer_bytes} bytes used for memory buffer for on-disk hash table...");

    let quiet = args.flag_quiet;
    let human_readable = args.flag_human_readable;

    let dupes_count = if args.flag_select.is_some() {
        dedup_csv(args, mem_limited_buffer_bytes)?
    } else {
        dedup_lines(args, mem_limited_buffer_bytes)?
    };

    if quiet {
        return Ok(());
    }

    eprintln!(
        "{}",
        if human_readable {
            HumanCount(dupes_count).to_string()
        } else {
            dupes_count.to_string()
        }
    );

    Ok(())
}

fn dedup_csv(args: Args, mem_limited_buffer: u64) -> Result<u64, crate::clitypes::CliError> {
    let rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select.unwrap());

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.arg_output.as_ref()).writer()?;
    let dupes_output = args.flag_dupes_output.is_some();
    let mut dupewtr = Config::new(args.flag_dupes_output.as_ref()).writer()?;

    let headers = rdr.byte_headers()?.clone();
    if dupes_output {
        let mut dupe_headers = csv::ByteRecord::new();
        dupe_headers.push_field(b"dupe_rowno");
        dupe_headers.extend(headers.iter());
        dupewtr.write_byte_record(&dupe_headers)?;
    }

    let temp_dir = args.flag_temp_dir.map(PathBuf::from);
    let mut dedup_cache = odhtcache::ExtDedupCache::new(mem_limited_buffer, temp_dir);
    let mut dupes_count = 0_u64;
    let sel = rconfig.selection(&headers)?;

    rconfig.write_headers(&mut rdr, &mut wtr)?;

    // Pre-allocate and reuse buffers
    let mut key = String::with_capacity(20);
    let mut utf8_string = String::with_capacity(20);
    let mut dupe_row = csv::ByteRecord::new();
    let mut curr_row = csv::ByteRecord::new();

    for (row_idx, row) in rdr.byte_records().enumerate() {
        curr_row.clone_from(&row?);
        key.clear();
        for field in sel.select(&curr_row) {
            if let Ok(s_utf8) = simdutf8::basic::from_utf8(field) {
                key.push_str(s_utf8);
            } else {
                utf8_string.clear();
                utf8_string.push_str(&String::from_utf8_lossy(field));
                key.push_str(&utf8_string);
            }
        }

        if dedup_cache.contains(&key) {
            dupes_count += 1;
            if dupes_output {
                dupe_row.clear();
                dupe_row.push_field(itoa::Buffer::new().format(row_idx + 1).as_bytes());
                dupe_row.extend(curr_row.iter());
                dupewtr.write_byte_record(&dupe_row)?;
            }
        } else {
            dedup_cache.insert(&key);
            wtr.write_byte_record(&curr_row)?;
        }
    }

    dupewtr.flush()?;
    wtr.flush()?;

    Ok(dupes_count)
}

fn dedup_lines(args: Args, mem_limited_buffer: u64) -> Result<u64, crate::clitypes::CliError> {
    let input_reader: Box<dyn BufRead> = match &args.arg_input {
        Some(input_path) => {
            if input_path.to_lowercase().ends_with(".sz") {
                return fail_clierror!(
                    "Input file cannot be a .sz file. Use 'qsv snappy decompress' first."
                );
            }
            let file = fs::File::open(input_path)?;
            Box::new(io::BufReader::with_capacity(
                config::DEFAULT_RDR_BUFFER_CAPACITY,
                file,
            ))
        },
        None => Box::new(io::BufReader::new(stdin().lock())),
    };
    let mut output_writer: Box<dyn Write> = match &args.arg_output {
        Some(output_path) => Box::new(io::BufWriter::with_capacity(
            config::DEFAULT_WTR_BUFFER_CAPACITY,
            fs::File::create(output_path)?,
        )),
        None => Box::new(io::BufWriter::with_capacity(
            config::DEFAULT_WTR_BUFFER_CAPACITY,
            stdout().lock(),
        )),
    };
    let mut write_dupes = false;
    #[cfg(target_family = "unix")]
    let mut dupes_writer = match args.flag_dupes_output {
        Some(dupes_output) => {
            write_dupes = true;
            io::BufWriter::with_capacity(
                config::DEFAULT_WTR_BUFFER_CAPACITY,
                fs::File::create(dupes_output)?,
            )
        },
        _ => io::BufWriter::with_capacity(
            config::DEFAULT_WTR_BUFFER_CAPACITY,
            fs::File::create("/dev/null")?,
        ),
    };
    #[cfg(target_family = "windows")]
    let mut dupes_writer = if let Some(dupes_output) = args.flag_dupes_output {
        write_dupes = true;
        io::BufWriter::with_capacity(
            config::DEFAULT_WTR_BUFFER_CAPACITY,
            fs::File::create(dupes_output)?,
        )
    } else {
        io::BufWriter::with_capacity(
            config::DEFAULT_WTR_BUFFER_CAPACITY,
            fs::File::create("nul")?,
        )
    };
    let temp_dir = args.flag_temp_dir.map(PathBuf::from);
    let mut dedup_cache = odhtcache::ExtDedupCache::new(mem_limited_buffer, temp_dir);
    let mut dupes_count = 0_u64;
    let mut line_work = String::with_capacity(1024);
    for (row_idx, line) in input_reader.lines().enumerate() {
        line_work.clone_from(&line?);
        if dedup_cache.contains(&line_work) {
            dupes_count += 1;
            if write_dupes {
                writeln!(dupes_writer, "{row_idx}\t{line_work}")?;
            }
        } else {
            dedup_cache.insert(&line_work);
            if args.flag_no_output {
                continue;
            }
            writeln!(output_writer, "{line_work}")?;
        }
    }
    dupes_writer.flush()?;
    output_writer.flush()?;

    Ok(dupes_count)
}

/// Determines the memory buffer size to use for on-disk hash table based on
/// the provided flag and the system's total memory.
///
/// # Arguments
///
/// * `flag_memory_limit` - An optional u64 value representing the user-specified memory limit.
///
/// # Returns
///
/// A u64 value representing the calculated memory limit in bytes.
///
/// # Behavior
///
/// - If the system is not supported, it returns a predefined `MEMORY_LIMITED_BUFFER` value.
/// - If `flag_memory_limit` is None, it returns the `MEMORY_LIMITED_BUFFER`.
/// - If `flag_memory_limit` is Some(limit):
///   - For limit <= 50, it's treated as a percentage of total system memory.
///   - For limit > 50, it's treated as megabytes, but capped at 90% of total system memory.
pub fn calculate_memory_limit(flag_memory_limit: Option<u64>) -> u64 {
    if !sysinfo::IS_SUPPORTED_SYSTEM {
        return MEMORY_LIMITED_BUFFER;
    }

    let mut sys = System::new();
    sys.refresh_memory();
    let total_memory = sys.total_memory();

    #[allow(clippy::cast_precision_loss)]
    match flag_memory_limit {
        Some(limit) if limit <= 50 => ((total_memory as f64 * limit as f64) / 100.0) as u64,
        Some(limit) => {
            let limit_bytes = limit.saturating_mul(1_000_000); // Convert MB to bytes
            let ninety_percent_total = (total_memory as f64 * 0.9) as u64;
            std::cmp::min(limit_bytes, ninety_percent_total)
        },
        None => MEMORY_LIMITED_BUFFER,
    }
}

#[test]
fn test_extdedup_mem_check() {
    // check to see if sysinfo return meminfo without segfaulting
    let mut sys = System::new();
    sys.refresh_memory();
    let mem10percent = (sys.total_memory() * 1000) / 10; // 10 percent of total memory
    assert!(mem10percent > 0);
}
