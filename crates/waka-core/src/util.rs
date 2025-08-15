#[cfg(target_family = "unix")]
use std::os::unix::process::ExitStatusExt;
use std::{
    borrow::Cow,
    cmp::min,
    collections::HashMap,
    env,
    fmt::Write as _,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str,
    sync::{Arc, OnceLock},
    time::SystemTime,
};

use anyhow::anyhow;
use csv::ByteRecord;
use docopt::Docopt;
use filetime::FileTime;
use polars::prelude::Schema;
use reqwest::Client;
use serde::de::{Deserialize, DeserializeOwned, Deserializer, Error};
use sysinfo::System;
use tracing_subscriber::layer::SubscriberExt as _;
use zip::read::root_dir_common_filter;

use crate::{
    config::{
        self, Config, DEFAULT_RDR_BUFFER_CAPACITY, DEFAULT_WTR_BUFFER_CAPACITY, Delimiter,
        SpecialFormat, get_special_format,
    },
    count::polars_count_input,
    select::SelectColumns,
    stats::{self, JsonTypes, STATSDATA_TYPES_MAP, StatsData},
};

#[macro_export]
macro_rules! regex_oncelock {
    ($re:literal $(,)?) => {{
        static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        #[allow(clippy::regex_creation_in_loops)] // false positive as we use oncelock
        RE.get_or_init(|| regex::Regex::new($re).expect("Invalid regex"))
    }};
}

// leave at least 20% of the available memory free
const DEFAULT_FREEMEMORY_HEADROOM_PCT: u8 = 20;

const DEFAULT_BATCH_SIZE: usize = 50_000;

const DEFAULT_STATSCACHE_MODE: &str = "auto";

static ROW_COUNT: OnceLock<Option<u64>> = OnceLock::new();

static JOBS_TO_USE: OnceLock<usize> = OnceLock::new();

pub type ByteString = Vec<u8>;

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StatsMode {
    Schema,
    Frequency,
    FrequencyForceStats,
    PolarsSchema,
    Outliers,
    None,
}

#[derive(serde::Deserialize, Clone)]
pub struct SchemaArgs {
    pub flag_enum_threshold:  u64,
    pub flag_ignore_case:     bool,
    pub flag_strict_dates:    bool,
    pub flag_pattern_columns: SelectColumns,
    pub flag_dates_whitelist: String,
    pub flag_prefer_dmy:      bool,
    pub flag_force:           bool,
    pub flag_stdout:          bool,
    pub flag_jobs:            Option<usize>,
    pub flag_polars:          bool,
    pub flag_no_headers:      bool,
    pub flag_delimiter:       Option<Delimiter>,
    pub arg_input:            Option<String>,
    pub flag_memcheck:        bool,
}

#[inline]
pub fn num_cpus() -> usize {
    num_cpus::get()
}

const CARGO_BIN_NAME: &str = match option_env!("CARGO_BIN_NAME") {
    Some(name) => name,
    None => env!("CARGO_PKG_NAME"), // fallback to package name
};

const TARGET: &str = match option_env!("TARGET") {
    Some(target) => target,
    None => "Unknown_target",
};

const WAKA_POLARS_REV: &str = match option_env!("WAKA_POLARS_REV") {
    Some(rev) => rev,
    None => "",
};

// Add constant for whitespace visualization
// the whitespace markers as as defined in
// https://doc.rust-lang.org/reference/whitespace.html
const WHITESPACE_MARKERS: &[(char, &str)] = &[
    // common whitespace markers other than space
    ('\t', "《→》"), // tab
    ('\n', "《¶》"), // newline
    ('\r', "《⏎》"), // carriage return
    // more obscure whitespace markers
    ('\u{000B}', "《⋮》"), // vertical tab
    ('\u{000C}', "《␌》"), // form feed
    ('\u{0009}', "《↹》"), // horizontal tab
    ('\u{0085}', "《␤》"), // next line
    ('\u{200E}', "《␎》"), // left-to-right mark
    ('\u{200F}', "《␏》"), // right-to-left mark
    ('\u{2028}', "《␊》"), // line separator
    ('\u{2029}', "《␍》"), // paragraph separator
    // additional common whitespace markers beyond
    // https://doc.rust-lang.org/reference/whitespace.html
    ('\u{00A0}', "《⍽》"),     // non-breaking space
    ('\u{2003}', "《emsp》"),  // em space
    ('\u{2007}', "《figsp》"), // figure space
    ('\u{200B}', "《zwsp》"),  // zero width space
];

#[cfg(unix)]
pub fn reset_sigpipe() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

pub fn visualize_whitespace(s: &str) -> String {
    // Check if string is all spaces
    let is_all_spaces = s.chars().all(|c| c == ' ');

    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        if c == ' ' {
            if is_all_spaces {
                // Only use space marker if entire string is spaces
                result.push_str("《_》");
            } else {
                result.push(c);
            }
        } else if let Some((_, replacement)) = WHITESPACE_MARKERS.iter().find(|(ws, _)| *ws == c) {
            result.push_str(replacement);
        } else {
            result.push(c);
        }
    }
    result
}

pub fn max_jobs() -> usize {
    let num_cpus = num_cpus();
    let max_jobs = match env::var("WAKA_MAX_JOBS") {
        Ok(val) => val.parse::<usize>().unwrap_or(1_usize),
        Err(_) => num_cpus,
    };
    if (1..=num_cpus).contains(&max_jobs) {
        max_jobs
    } else {
        num_cpus
    }
}

/// Given a desired number of cores to use
/// returns number of cores to actually use and set
/// rayon global thread pool size accordingly.
/// If desired is None, zero, or greater than available cores,
/// returns max_jobs, which is equal to number of available cores
/// If desired is Some and less than available cores,
/// returns desired number of cores
pub fn njobs(flag_jobs: Option<usize>) -> usize {
    let njobs_result = JOBS_TO_USE.get_or_init(|| {
        let max_jobs = max_jobs();
        let jobs_to_use = flag_jobs.map_or(max_jobs, |jobs| {
            if jobs == 0 || jobs > max_jobs {
                max_jobs
            } else {
                jobs
            }
        });
        match rayon::ThreadPoolBuilder::new()
            .num_threads(jobs_to_use)
            .build_global()
        {
            Err(e) => {
                tracing::warn!("Failed to set global thread pool size to {jobs_to_use}: {e}");
            },
            _ => {
                tracing::info!("Using {jobs_to_use} jobs...");
            },
        }
        jobs_to_use
    });
    *njobs_result
}

pub fn timeout_secs(timeout: u16) -> anyhow::Result<u64> {
    let timeout = match env::var("QSV_TIMEOUT") {
        Ok(val) => val.parse::<u16>().unwrap_or(30_u16),
        Err(_) => timeout,
    };

    if timeout > 3600 {
        return Err(anyhow!(
            "Timeout cannot be more than 3,600 seconds (1 hour): {timeout}"
        ));
    } else if timeout == 0 {
        return Err(anyhow!("Timeout cannot be zero."));
    }
    tracing::info!("TIMEOUT: {timeout}");
    Ok(timeout as u64)
}

pub fn version() -> String {
    let mut enabled_features = String::new();

    enabled_features.push_str("apply;");
    enabled_features.push_str("foreach;");
    enabled_features.push_str("geocode;");
    enabled_features.push_str("to;");

    #[allow(clippy::const_is_empty)]
    if WAKA_POLARS_REV.is_empty() {
        enabled_features.push_str(format!("polars-{};", polars::VERSION).as_str());
    } else {
        enabled_features
            .push_str(format!("polars-{}:{};", polars::VERSION, WAKA_POLARS_REV).as_str());
    }

    enabled_features.push('-');

    // get max_file_size & memory info. max_file_size is based on QSV_FREEMEMORY_HEADROOM_PCT
    // setting and is only enforced when qsv is running in "non-streaming" mode (i.e. needs to
    // load the entire file into memory).
    let mut sys = System::new();
    sys.refresh_memory();
    let avail_mem = sys.available_memory();
    let total_mem = sys.total_memory();
    let free_swap = sys.free_swap();
    let max_file_size = mem_file_check(Path::new(""), true, false).unwrap_or(0) as u64;

    let malloc_kind = "mimalloc";

    let (qsvtype, maj, min, pat, pre, rustversion) = (
        option_env!("CARGO_BIN_NAME"),
        option_env!("CARGO_PKG_VERSION_MAJOR"),
        option_env!("CARGO_PKG_VERSION_MINOR"),
        option_env!("CARGO_PKG_VERSION_PATCH"),
        option_env!("CARGO_PKG_VERSION_PRE"),
        option_env!("CARGO_PKG_RUST_VERSION"),
    );
    if let (Some(qsvtype), Some(maj), Some(min), Some(pat), Some(pre), Some(rustversion)) =
        (qsvtype, maj, min, pat, pre, rustversion)
    {
        if pre.is_empty() {
            format!(
                "{qsvtype} {maj}.{min}.{pat}-{malloc_kind}-{enabled_features}{maxjobs}-{numcpus};\
                 {max_file_size}-{free_swap}-{avail_mem}-{total_mem} ({TARGET} compiled with Rust \
                 {rustversion})",
                maxjobs = max_jobs(),
                numcpus = num_cpus(),
            )
        } else {
            format!(
                "{qsvtype} {maj}.{min}.\
                 {pat}-{pre}-{malloc_kind}-{enabled_features}{maxjobs}-{numcpus};\
                 {max_file_size}-{free_swap}-{avail_mem}-{total_mem} ({TARGET} compiled with Rust \
                 {rustversion})",
                maxjobs = max_jobs(),
                numcpus = num_cpus(),
            )
        }
    } else {
        String::new()
    }
}

const OTHER_ENV_VARS: &[&str] = &["all_proxy", "no_proxy", "http_proxy", "https_proxy"];

pub fn show_env_vars() {
    let mut env_var_set = false;
    for (n, v) in env::vars_os() {
        // safety: we know that the env::vars_os() will not fail
        let env_var = n.into_string().unwrap();
        if env_var.starts_with("QSV_")
            || env_var.starts_with("MIMALLOC_")
            || OTHER_ENV_VARS.contains(&env_var.to_ascii_lowercase().as_str())
        {
            env_var_set = true;
            tracing::info!("{env_var}: {v:?}");
        }
        if env_var.starts_with("POLARS_") {
            env_var_set = true;
            tracing::info!("{env_var}: {v:?}");
        }
    }
    if !env_var_set {
        tracing::info!("No waka-relevant environment variables set.");
    }
}

#[inline]
pub fn count_rows(conf: &Config) -> anyhow::Result<u64> {
    // Check if ROW_COUNT is already initialized to avoid redundant counting
    if let Some(count) = ROW_COUNT.get() {
        return Ok(count.unwrap_or(0));
    }

    // If not, try using index if available
    if let Some(idx) = conf.indexed().unwrap_or(None) {
        return Ok(idx.count());
    }
    // index does not exist or is stale

    // Otherwise, count records by using polars mem-mapped reader if available
    // If polars is not enabled, count records by iterating through records
    // Do this only once per invocation and cache the result in ROW_COUNT,
    // so we don't have to re-count rows every time we need to know the
    // rowcount for CSVs that don't have an index.
    ROW_COUNT
        .get_or_init(|| {
            // Try different counting methods in order of preference
            count_rows_with_best_method(conf)
        })
        .ok_or_else(|| anyhow!("Unable to get row count"))
}

fn count_rows_with_best_method(conf: &Config) -> Option<u64> {
    if !conf.no_headers {
        // Try polars first for files with headers
        if let Ok(polars_count) = polars_count_input(conf, false) {
            // If count is greater than 0, return the polars accelerated count
            // as sometimes, polars returns a zero count even if the file is not empty
            // and the file is a proper CSV file.
            // Otherwise, double-check with the "regular" CSV reader
            if polars_count > 0 {
                return Some(polars_count);
            }
        }
    }

    // Fall back to CSV reader
    count_with_csv_reader(conf)
}

fn count_with_csv_reader(conf: &Config) -> Option<u64> {
    conf.clone()
        .skip_format_check(true)
        .reader()
        .ok()
        .map(|mut rdr| {
            let mut count = 0_u64;
            let mut record = csv::ByteRecord::new();
            while rdr.read_byte_record(&mut record).unwrap_or_default() {
                count += 1;
            }
            count
        })
}

/// Count rows using "regular" CSV reader
/// we don't use polars mem-mapped reader here
/// even if it's available
#[inline]
pub fn count_rows_regular(conf: &Config) -> anyhow::Result<u64> {
    if let Some(idx) = conf.indexed().unwrap_or(None) {
        Ok(idx.count())
    } else {
        // index does not exist or is stale,
        let count_opt =
            ROW_COUNT.get_or_init(|| match conf.clone().skip_format_check(true).reader() {
                Ok(mut rdr) => {
                    let mut count = 0_u64;
                    let mut _record = csv::ByteRecord::new();
                    #[allow(clippy::used_underscore_binding)]
                    while rdr.read_byte_record(&mut _record).unwrap_or_default() {
                        count += 1;
                    }
                    Some(count)
                },
                _ => None,
            });

        match *count_opt {
            Some(count) => Ok(count),
            None => Err(anyhow!("Unable to get row count")),
        }
    }
}

pub fn count_lines_in_file(file: &str) -> anyhow::Result<u64> {
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    let line_count = reader.lines().count() as u64;
    Ok(line_count)
}

pub fn get_args<T>(usage: &str, argv: &[&str]) -> anyhow::Result<T>
where
    T: DeserializeOwned,
{
    Docopt::new(usage)
        .and_then(|d| {
            d.argv(argv.iter().copied())
                .version(Some(version()))
                .deserialize()
        })
        .map_err(From::from)
}

#[inline]
pub fn many_configs(
    inps: &[PathBuf],
    delim: Option<Delimiter>,
    no_headers: bool,
    flexible: bool,
) -> anyhow::Result<Vec<Config>> {
    let mut inps = inps
        .iter()
        .map(|p| p.to_str().unwrap_or("-").to_owned())
        .collect::<Vec<_>>();
    if inps.is_empty() {
        inps.push("-".to_owned()); // stdin
    }
    let confs = inps
        .into_iter()
        .map(|p| {
            Config::new(Some(p).as_ref())
                .delimiter(delim)
                .no_headers(no_headers)
                .flexible(flexible)
        })
        .collect::<Vec<_>>();
    errif_greater_one_stdin(&confs)?;
    Ok(confs)
}

pub fn errif_greater_one_stdin(inps: &[Config]) -> anyhow::Result<()> {
    let nstd = inps.iter().filter(|inp| inp.is_stdin()).count();
    if nstd > 1 {
        return Err(anyhow!("At most one <stdin> input is allowed."));
    }
    Ok(())
}

pub const fn chunk_size(nitems: usize, njobs: usize) -> usize {
    if nitems < njobs {
        nitems
    } else {
        nitems / njobs
    }
}

pub const fn num_of_chunks(nitems: usize, chunk_size: usize) -> usize {
    if chunk_size == 0 {
        return nitems;
    }
    let mut n = nitems / chunk_size;
    if !nitems.is_multiple_of(chunk_size) {
        n += 1;
    }
    n
}

pub fn file_metadata(md: &fs::Metadata) -> (u64, u64) {
    use filetime::FileTime;
    let last_modified = FileTime::from_last_modification_time(md).unix_seconds() as u64;
    let fsize = md.len();
    (last_modified, fsize)
}

/// Check if there is enough memory to process the file.
/// Return the maximum file size that can be processed.
/// If the file is larger than the maximum file size, return an error.
/// If memcheck is true, check memory in CONSERVATIVE mode (i.e., Filesize < AVAIL memory + SWAP -
/// headroom) If memcheck is false, check memory in NORMAL mode (i.e., Filesize < TOTAL memory -
/// headroom)
pub fn mem_file_check(
    path: &Path,
    version_check: bool,
    conservative_memcheck: bool,
) -> anyhow::Result<i64> {
    // if we're NOT calling this from the version() and the file doesn't exist,
    // we don't need to check memory as file existence is checked before this function is called.
    // If we do get here with a non-existent file, that means we're using stdin,
    // so this check doesn't apply, so we return -1
    if !path.exists() && !version_check {
        return Ok(-1_i64);
    }

    let conservative_memcheck_work = get_envvar_flag("WAKA_MEMORY_CHECK") || conservative_memcheck;

    let mut mem_pct = env::var("QSV_FREEMEMORY_HEADROOM_PCT")
        .unwrap_or_else(|_| DEFAULT_FREEMEMORY_HEADROOM_PCT.to_string())
        .parse::<u8>()
        .unwrap_or(DEFAULT_FREEMEMORY_HEADROOM_PCT);

    // if QSV_FREEMEMORY_HEADROOM_PCT is 0, we skip the memory check
    if mem_pct == 0 {
        return Ok(i64::MAX);
    }

    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let avail_mem = sys.available_memory();
    let free_swap = sys.free_swap();
    let total_mem = sys.total_memory();

    // for safety, we don't want to go below 10% memory headroom
    // nor above 90% memory headroom as its too memory-restrictive
    mem_pct = mem_pct.clamp(10, 90);

    #[allow(clippy::cast_precision_loss)]
    let max_avail_mem = if conservative_memcheck_work {
        ((avail_mem + free_swap) as f32 * ((100 - mem_pct) as f32 / 100.0_f32)) as u64
    } else {
        (total_mem as f32 * ((100 - mem_pct) as f32 / 100.0_f32)) as u64
    };

    // if we're calling this from version(), we don't need to check the file size
    if !version_check {
        let file_metadata =
            fs::metadata(path).map_err(|e| anyhow!("Failed to get file size: {e}"))?;
        let fsize = file_metadata.len();
        if fsize > max_avail_mem {
            return Err(anyhow!(
                "Not enough memory to process the file. qsv running in non-streaming {mode} mode. \
                 Total memory: {total_mem} Available memory: {avail_mem}. Free swap: {free_swap} \
                 Max Available memory/Max input file size: {max_avail_mem}. \
                 QSV_FREEMEMORY_HEADROOM_PCT: {mem_pct}%. File size: {fsize}.",
                mode = if conservative_memcheck_work {
                    "CONSERVATIVE"
                } else {
                    "NORMAL"
                },
            ));
        }
    }

    Ok(max_avail_mem as i64)
}

#[inline]
pub fn condense(val: Cow<[u8]>, n: Option<usize>) -> Cow<[u8]> {
    match n {
        None => val,
        Some(n) => {
            let mut is_short_utf8 = false;
            if let Ok(s) = simdutf8::basic::from_utf8(&val) {
                if n >= s.chars().count() {
                    is_short_utf8 = true;
                } else {
                    let mut s = s.chars().take(n).collect::<String>();
                    s.push_str("...");
                    return Cow::Owned(s.into_bytes());
                }
            }
            if is_short_utf8 || n >= (*val).len() {
                // already short enough
                val
            } else {
                // This is a non-Unicode string, so we just trim on bytes.
                let mut s = val[0..n].to_vec();
                s.extend(b"...".iter().copied());
                Cow::Owned(s)
            }
        },
    }
}

pub fn idx_path(csv_path: &Path) -> PathBuf {
    // safety: we know the path has a filename
    let mut p = csv_path
        .to_path_buf()
        .into_os_string()
        .into_string()
        .unwrap();
    p.push_str(".idx");
    PathBuf::from(&p)
}

pub type Idx = Option<usize>;

pub fn range(start: Idx, end: Idx, len: Idx, index: Idx) -> anyhow::Result<(usize, usize)> {
    match (start, end, len, index) {
        (None, None, None, Some(i)) => Ok((i, i + 1)),
        (_, _, _, Some(_)) => Err(anyhow!(
            "--index cannot be used with --start, --end or --len"
        )),
        (_, Some(_), Some(_), None) => {
            Err(anyhow!("--end and --len cannot be used at the same time."))
        },
        (_, None, None, None) => Ok((start.unwrap_or(0), usize::MAX)),
        (_, Some(e), None, None) => {
            let s = start.unwrap_or(0);
            if s > e {
                Err(anyhow!(
                    "The end of the range ({e}) must be greater than or\nequal to the start of \
                     the range ({s})."
                ))
            } else {
                Ok((s, e))
            }
        },
        (_, None, Some(l), None) => {
            let s = start.unwrap_or(0);
            Ok((s, s + l))
        },
    }
}

/// Represents a filename template of the form `"{}.csv"`, where `"{}"` is
/// the place to insert the part of the filename generated by `qsv`.
#[derive(Clone)]
pub struct FilenameTemplate {
    prefix: String,
    suffix: String,
}

impl FilenameTemplate {
    /// Generate a new filename using `unique_value` to replace the `"{}"`
    /// in the template.
    pub fn filename(&self, unique_value: &str) -> String {
        format!("{}{unique_value}{}", &self.prefix, &self.suffix)
    }

    /// Create a new, writable file in directory `path` with a filename
    /// using `unique_value` to replace the `"{}"` in the template.  Note
    /// that we do not output headers; the caller must do that if
    /// desired.
    pub fn writer<P>(
        &self,
        path: P,
        unique_value: &str,
    ) -> std::io::Result<csv::Writer<Box<dyn std::io::Write + 'static>>>
    where
        P: AsRef<Path>,
    {
        let filename = self.filename(unique_value);
        let full_path = path.as_ref().join(filename);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let spath = Some(full_path.display().to_string());
        Config::new(spath.as_ref()).writer()
    }
}

impl<'de> Deserialize<'de> for FilenameTemplate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<FilenameTemplate, D::Error> {
        let raw = String::deserialize(d)?;
        let chunks = raw.split("{}").collect::<Vec<_>>();
        if chunks.len() == 2 {
            Ok(FilenameTemplate {
                prefix: chunks[0].to_owned(),
                suffix: chunks[1].to_owned(),
            })
        } else {
            Err(D::Error::custom(
                "The --filename argument must contain one '{}'.",
            ))
        }
    }
}

pub fn init_logger() -> anyhow::Result<(String, Option<tracing_appender::non_blocking::WorkerGuard>)>
{
    use std::fs;

    use tracing_log::LogTracer;
    use tracing_subscriber::{EnvFilter, fmt};

    // If logging is off, do nothing.
    let level = env::var("WAKA_LOG_LEVEL").unwrap_or_else(|_| "off".to_string());
    if level.eq_ignore_ascii_case("off") {
        // still install the LogTracer so `log` macros are routed (but no subscriber = no output)
        let _ = LogTracer::init();
        let qsv_args = String::new();
        return Ok((qsv_args, None));
    }

    let log_dir = env::var("WAKA_LOG_DIR").unwrap_or_else(|_| ".".to_string());
    fs::create_dir_all(&log_dir)
        .map_err(|e| anyhow!("Failed to create WAKA_LOG_DIR '{}': {e}", log_dir))?;

    // Bridge `log` crate macros (log::info!, log::debug!, log_enabled!, etc.) to tracing
    let _ = LogTracer::init();

    // Build the filter from WAKA_LOG_LEVEL (e.g., "info", "debug", "warn", with optional module
    // filters)
    let env_filter = EnvFilter::try_new(&level).unwrap_or_else(|_| EnvFilter::new("info"));

    // Rolling file appender (daily). File name matches the binary name, similar to flexi_logger
    // default. Note: this changes rotation policy from size-based to time-based (daily).
    let file_name = format!("{CARGO_BIN_NAME}.log");
    let appender = tracing_appender::rolling::daily(&log_dir, file_name);

    // Honor WAKA_LOG_UNBUFFERED: use blocking (direct) writer vs. non-blocking background writer.
    let (make_writer, guard_opt) = if get_envvar_flag("WAKA_LOG_UNBUFFERED") {
        (
            // Type-erased writer
            tracing_subscriber::fmt::writer::BoxMakeWriter::new(appender),
            None,
        )
    } else {
        let (nb, guard) = tracing_appender::non_blocking(appender);
        (
            tracing_subscriber::fmt::writer::BoxMakeWriter::new(nb),
            Some(guard),
        )
    };

    // UTC timestamps similar to .use_utc()
    let timer = tracing_subscriber::fmt::time::UtcTime::rfc_3339();

    // Build and install the global subscriber
    let subscriber = tracing_subscriber::registry().with(env_filter).with(
        fmt::layer()
            .with_timer(timer)
            .with_ansi(false)
            .with_writer(make_writer)
            .with_level(true)
            .with_target(true),
    );

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| anyhow!("Failed to set global tracing subscriber: {e}"))?;

    // Only capture args if info is enabled
    let qsv_args: String = if tracing::enabled!(tracing::Level::INFO) {
        env::args().skip(1).collect::<Vec<_>>().join(" ")
    } else {
        String::new()
    };

    tracing::info!("START: {qsv_args}");
    Ok((qsv_args, guard_opt))
}

pub fn safe_header_names(
    headers: &csv::StringRecord,
    check_first_char: bool,
    conditional: bool,
    reserved_names: Option<&Vec<String>>,
    unsafe_prefix: &str,
    keep_case: bool,
) -> (Vec<String>, u16) {
    // Create "safe" var/key names - to support dynfmt/url-template, valid python vars & db-safe
    // column names. Fold to lowercase if keep_case is false. Trim leading & trailing whitespace.
    // Replace whitespace/non-alphanumeric) with _. If name starts with a number & check_first_char
    // is true, prepend the unsafe_prefix. If a column with the same name already exists,
    // append a sequence suffix (e.g. _n). Names are limited to 60 characters in length.
    // Empty names are replaced with unsafe_prefix as well.

    // If conditional = true & reserved_names is none, only rename the header if its not safe
    let prefix = if unsafe_prefix.is_empty() {
        "_"
    } else {
        unsafe_prefix
    };
    let safename_regex = regex_oncelock!(r"[^A-Za-z0-9]");
    let mut changed_count = 0_u16;
    let mut name_vec: Vec<String> = Vec::with_capacity(headers.len());
    let mut safe_name: String;
    let mut safename_always: String;
    let mut safename_candidate: String;
    let mut final_candidate: String;
    let mut buf_wrk = String::new();

    for header_name in headers {
        let reserved_found = if let Some(reserved_names_vec) = reserved_names {
            if keep_case {
                header_name.clone_into(&mut buf_wrk);
            } else {
                to_lowercase_into(header_name, &mut buf_wrk);
            }
            reserved_names_vec
                .iter()
                .any(|reserved_name| reserved_name == &buf_wrk)
        } else {
            false
        };
        safe_name = if conditional && is_safe_name(header_name) && !reserved_found {
            header_name.to_string()
        } else {
            safename_always = if header_name.is_empty() {
                prefix.to_string()
            } else {
                safename_regex
                    .replace_all(header_name.trim(), "_")
                    .to_string()
            };
            if check_first_char && safename_always.as_bytes()[0].is_ascii_digit() {
                safename_always = format!("{prefix}{safename_always}");
            }

            safename_candidate = if reserved_found {
                tracing::warn!("\"{safename_always}\" is a reserved name: {reserved_names:?}");
                format!("reserved_{safename_always}")
            } else {
                safename_always
            };

            final_candidate = safename_candidate[..safename_candidate
                .chars()
                .map(char::len_utf8)
                .take(60)
                .sum()]
                .to_string();

            final_candidate = if keep_case {
                final_candidate
            } else {
                final_candidate.to_lowercase()
            };

            if prefix != "_" && final_candidate.starts_with('_') {
                final_candidate = format!("{prefix}{final_candidate}");
            }
            final_candidate
        };
        let mut sequence_suffix = 2_u16;
        let mut candidate_name = safe_name.clone();
        while name_vec.contains(&candidate_name) {
            candidate_name = format!("{safe_name}_{sequence_suffix}");
            sequence_suffix += 1;
        }
        if candidate_name.ne(header_name) {
            changed_count += 1;
        }
        name_vec.push(candidate_name);
    }
    tracing::debug!("safe header names: {name_vec:?}");
    (name_vec, changed_count)
}

#[inline]
pub fn is_safe_name(header_name: &str) -> bool {
    if header_name.trim().is_empty()
        || header_name.trim_start_matches('_').is_empty()
        || header_name.len() > 60
    {
        return false;
    }
    let first_character = header_name.trim_start_matches('_').as_bytes()[0];
    if first_character.is_ascii_digit() || first_character.is_ascii_whitespace() {
        return false;
    }
    let safename_re = regex_oncelock!(r"^[\w\-\s]+$");
    safename_re.is_match(header_name)
}

pub fn log_end(mut qsv_args: String, now: std::time::Instant) {
    use crate::config::TEMP_FILE_DIR;

    if let Some(temp_dir) = TEMP_FILE_DIR.get() {
        // if polars is enabled, we need to remove the temporary directory
        // after the command finishes. This is using unwrap_or_default()
        // to avoid panics if the directory is already deleted.
        std::fs::remove_dir_all(temp_dir).unwrap_or_default();
    }
    if tracing::enabled!(tracing::Level::INFO) {
        let ellipsis = if qsv_args.len() > 24 {
            utf8_truncate(&mut qsv_args, 24);
            "..."
        } else {
            ""
        };
        tracing::info!(
            "END \"{qsv_args}{ellipsis}\" elapsed: {}",
            now.elapsed().as_secs_f32()
        );
    }
}

/// Truncates a UTF-8 encoded string to a maximum byte length while preserving valid UTF-8 encoding.
///
/// This function ensures that the truncation happens at valid UTF-8 character boundaries to avoid
/// splitting multi-byte characters. It modifies the input string in place.
///
/// # Arguments
///
/// * `input` - A mutable reference to the String to truncate
/// * `maxsize` - The maximum desired length in bytes
///
/// taken from https://gist.github.com/dginev/f6da5e94335d545e0a7b
pub fn utf8_truncate(input: &mut String, maxsize: usize) {
    let mut utf8_maxsize = input.len();
    if utf8_maxsize >= maxsize {
        {
            let mut char_iter = input.char_indices();
            while utf8_maxsize >= maxsize {
                (utf8_maxsize, _) = char_iter.next_back().unwrap_or_default();
            }
        } // Extra {} wrap to limit the immutable borrow of char_indices()
        input.truncate(utf8_maxsize);
    }
}

pub struct ColumnNameParser {
    chars: Vec<char>,
    pos:   usize,
}

impl ColumnNameParser {
    pub fn new(s: &str) -> ColumnNameParser {
        ColumnNameParser {
            chars: s.chars().collect(),
            pos:   0,
        }
    }

    pub fn parse(&mut self) -> anyhow::Result<Vec<String>> {
        let mut new_cols_name = vec![];
        loop {
            if self.cur().is_none() {
                break;
            }
            if self.cur() == Some('"') {
                self.bump();
                new_cols_name.push(self.parse_quoted_name()?);
            } else {
                new_cols_name.push(self.parse_name());
            }
            self.bump();
        }
        Ok(new_cols_name)
    }

    fn cur(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    const fn bump(&mut self) {
        if self.pos < self.chars.len() {
            self.pos += 1;
        }
    }

    fn is_end_of_field(&self) -> bool {
        self.cur().is_none_or(|c| c == ',')
    }

    fn parse_quoted_name(&mut self) -> anyhow::Result<String> {
        let mut name = String::new();
        loop {
            match self.cur() {
                None => {
                    return Err(anyhow!("Unclosed quote, missing \"."));
                },
                Some('"') => {
                    self.bump();
                    if self.cur() == Some('"') {
                        self.bump();
                        name.push('"');
                        name.push('"');
                        continue;
                    }
                    break;
                },
                Some(c) => {
                    name.push(c);
                    self.bump();
                },
            }
        }
        Ok(name)
    }

    fn parse_name(&mut self) -> String {
        let mut name = String::new();
        loop {
            if self.is_end_of_field() {
                break;
            }
            // safety: we know that the cur() will not be None as we checked above
            name.push(self.cur().unwrap());
            self.bump();
        }
        name
    }
}

#[inline]
/// Rounds a floating point number to a specified number of decimal places.
///
/// This function takes a 64-bit floating point number and rounds it to the specified number of
/// decimal places using "Bankers Rounding" (Midpoint Nearest Even) strategy. It returns the result
/// as a String.
///
/// # Arguments
///
/// * `dec_f64` - The floating point number to round
/// * `places` - The number of decimal places to round to. If set to 9999, no rounding is performed.
///
/// # Returns
///
/// * A String containing the rounded number with trailing zeros removed and -0.0 normalized to 0.0
pub fn round_num(dec_f64: f64, places: u32) -> String {
    use rust_decimal::{Decimal, RoundingStrategy};

    if dec_f64.is_nan() {
        return String::new();
    }

    // if places is the sentinel value 9999, we don't round, just return the number as is
    if places == 9999 {
        return ryu::Buffer::new().format(dec_f64).to_owned();
    }

    // use from_f64_retain, so we have all the excess bits before rounding with
    // round_dp_with_strategy as from_f64 will prematurely round when it drops the excess bits
    let Some(dec_num) = Decimal::from_f64_retain(dec_f64) else {
        return String::new();
    };

    // round using Midpoint Nearest Even Rounding Strategy AKA "Bankers Rounding."
    // https://docs.rs/rust_decimal/latest/rust_decimal/enum.RoundingStrategy.html#variant.MidpointNearestEven
    // we also normalize to remove trailing zeroes and to change -0.0 to 0.0.
    dec_num
        .round_dp_with_strategy(places, RoundingStrategy::MidpointNearestEven)
        .normalize()
        .to_string()
}

#[inline]
/// Transforms a byte slice into a ByteString with optional case-insensitive conversion.
///
/// This function takes a byte slice and attempts to convert it to a UTF-8 string. If successful,
/// it trims whitespace and optionally converts to lowercase. If the input is not valid UTF-8,
/// it returns the original bytes unchanged.
///
/// It's fine-tuned for speed and memory usage, using simdutf8 for UTF-8 validation and
/// to_lowercase_into for non-allocating, in-place lowercase conversion.
///
/// # Arguments
///
/// * `bs` - The input byte slice to transform
/// * `casei` - If true, converts the string to lowercase. If false, leaves case unchanged.
///
/// # Returns
///
/// * A `ByteString` (Vec<u8>) containing the transformed bytes
pub fn transform(bs: &[u8], casei: bool) -> ByteString {
    if let Ok(s) = simdutf8::basic::from_utf8(bs) {
        if casei {
            let mut buffer = String::with_capacity(bs.len());
            to_lowercase_into(s.trim(), &mut buffer);
            buffer.into_bytes()
        } else {
            s.trim().as_bytes().to_vec()
        }
    } else {
        bs.to_vec()
    }
}

pub fn load_dotenv() -> anyhow::Result<()> {
    // First, check if there is a QSV_DOTENV_PATH environment variable set
    // if there is, use that as the .env file.
    // Second, use the default .env file in the current directory.
    // If there is no .env file in the current directory, check if there is
    // an .env file with the same filestem as the binary, in the same directory as the binary.
    // If there is, use that. Failing that, qsv proceeds with its default settings and
    // whatever manually set environment variables are present.

    if let Ok(dotenv_path) = std::env::var("WAKA_DOTENV_PATH") {
        // <NONE> is a sentinel value to disable dotenv processing
        if dotenv_path == "<NONE>" {
            tracing::warn!("dotenv processing disabled with WAKA_DOTENV_PATH=<NONE>");
            return Ok(());
        }

        let canonical_dotenv_path = std::fs::canonicalize(dotenv_path)?;
        if let Err(e) = dotenvy::from_filename_override(canonical_dotenv_path.clone()) {
            return Err(anyhow!(
                "Cannot process .env file set in WAKA_DOTENV_PATH - {}: {e}",
                canonical_dotenv_path.display()
            ));
        }
        tracing::info!("Using .env file: {}", canonical_dotenv_path.display());
        return Ok(());
    }

    // check if there is an .env file in the current directory
    if dotenvy::dotenv_override().is_ok() {
        tracing::info!(
            "Using .env file in current directory: {}",
            std::env::current_dir()?.display()
        );
    } else {
        // no .env file in the current directory or it was invalid
        // now check if there is an .env file with the same name as the executable
        // in the same directory as the executable
        let qsv_binary_path = std::env::current_exe()?;

        let qsv_dir = qsv_binary_path
            .parent()
            .ok_or(anyhow!("No parent directory"))?;

        // safety: we know that the file_stem() will not be None as we checked above
        let qsv_binary_filestem = qsv_binary_path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut qsv_binary_envprofile = qsv_dir.to_path_buf();
        qsv_binary_envprofile.set_file_name(format!("{qsv_binary_filestem}.env"));

        if std::path::Path::new(&qsv_binary_envprofile).exists() {
            tracing::info!(
                "Using binary .env file: {}",
                qsv_binary_envprofile.display()
            );
            if let Err(e) = dotenvy::from_filename_override(qsv_binary_envprofile.clone()) {
                return Err(anyhow!(
                    "Cannot process binary .env file - {}: {e}",
                    qsv_binary_envprofile.display()
                ));
            }
        } else {
            // there is no binary .env file, just use the default settings
            // and whatever manually set environment variables are present
            tracing::info!(
                "No valid .env file found. Proceeding with default settings and current \
                 environment variable settings."
            );
        }
    }

    Ok(())
}

#[inline]
pub fn get_envvar_flag(key: &str) -> bool {
    if let Ok(tf_val) = std::env::var(key) {
        let tf_val = tf_val.to_lowercase();
        match tf_val {
            s if s == "true" || s == "t" || s == "1" || s == "yes" || s == "y" => true,
            s if s == "false" || s == "f" || s == "0" || s == "no" || s == "n" => false,
            _ => false,
        }
    } else {
        false
    }
}

/// Validates if a file is actually a Snappy-compressed file before attempting decompression
fn is_valid_snappy_file(path: &PathBuf) -> anyhow::Result<bool> {
    let mut file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(&mut file);

    // Try to create a FrameDecoder and read the first few bytes
    // This will fail immediately if the file doesn't have a valid Snappy header
    let decoder = snap::read::FrameDecoder::new(&mut reader);
    let mut buffer = Vec::with_capacity(50);

    match decoder.take(50).read_to_end(&mut buffer) {
        Ok(_) => {
            // Successfully read some bytes, this is likely a valid Snappy file
            tracing::debug!("File {} appears to be a valid Snappy file", path.display());
            Ok(true)
        },
        Err(e) => {
            // Failed to read, this is not a valid Snappy file
            tracing::debug!("File {} is not a valid Snappy file: {}", path.display(), e);
            Ok(false)
        },
    }
}

pub fn decompress_snappy_file(
    path: &PathBuf,
    tmpdir: &tempfile::TempDir,
) -> anyhow::Result<String> {
    // First, validate that this is actually a Snappy file
    if !is_valid_snappy_file(path)? {
        return Err(anyhow!(
            r#"File '{}' has an .sz extension but is not a valid Snappy-compressed file.
This might be a temporary file or incorrectly named file.
Consider renaming the file or using a different input."#,
            path.display()
        ));
    }

    // Proceed with decompression since we've validated the file
    let mut snappy_file = std::fs::File::open(path.clone())?;
    let mut snappy_reader = snap::read::FrameDecoder::new(&mut snappy_file);
    // safety: we know that the file_stem() will not be None as we opened the file above
    let file_stem = Path::new(&path).file_stem().unwrap().to_str().unwrap();
    let decompressed_filepath = tmpdir
        .path()
        .join(format!("qsv_temp_decompressed__{file_stem}"));
    let mut decompressed_file = std::fs::File::create(decompressed_filepath.clone())?;

    match std::io::copy(&mut snappy_reader, &mut decompressed_file) {
        Ok(num_bytes) => {
            decompressed_file.flush()?;
            tracing::debug!(
                "Successfully decompressed Snappy file: {} ({} bytes)",
                path.display(),
                num_bytes
            );
            Ok(format!("{}", decompressed_filepath.display()))
        },
        Err(e) => {
            // Clean up the partially created file
            let _ = std::fs::remove_file(&decompressed_filepath);
            Err(anyhow!(
                "Failed to decompress Snappy file '{}': {}. The file may be corrupted or \
                 incomplete.",
                path.display(),
                e
            ))
        },
    }
}

/// this is a non-allocating to_lowercase that uses an existing buffer
/// and should be faster than the allocating std::to_lowercase
#[inline]
pub fn to_lowercase_into(s: &str, buf: &mut String) {
    buf.clear();
    for c in s.chars() {
        for lc in c.to_lowercase() {
            buf.push(lc);
        }
    }
}

/// load the first BUFFER*8 (1024k) bytes of the file and check if it is utf8
pub fn isutf8_file(path: &Path) -> anyhow::Result<bool> {
    let metadata = std::fs::metadata(path)?;
    let buffer_len = config::DEFAULT_RDR_BUFFER_CAPACITY * 8;
    let file_size = metadata.len() as usize;
    let bytes_to_read: usize = if file_size < buffer_len {
        file_size
    } else {
        buffer_len
    };

    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::with_capacity(bytes_to_read);
    reader.read_to_end(&mut buffer)?;

    Ok(simdutf8::basic::from_utf8(&buffer).is_ok())
}

// check if a file is supported by process_input
fn is_supported_file(path: &Path) -> bool {
    // If QSV_SKIP_FORMAT_CHECK is set, consider all files as supported
    if get_envvar_flag("QSV_SKIP_FORMAT_CHECK") {
        return true;
    }

    let ext = path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::to_lowercase)
        .unwrap_or_default();
    match ext.as_str() {
        "csv" | "ssv" | "tsv" | "tab" => true,
        _ => get_special_format(path) != SpecialFormat::Unknown,
    }
}

/// Process the input files and return a vector of paths to the input files.
///
/// If the input is empty, try to copy stdin to a file named stdin in the passed temp directory.
/// If the input is empty and stdin is empty, return an error.
/// If it's not empty, check the input files if they exist, and return an error if they don't.
///
/// If the input is a directory, add all the files in the directory to the input.
/// If the input is a zip file, add all the files in the zip file to the input.
/// If the input is a file with the extension ".infile-list", read the file & add each line as a
/// file to the input.
/// If the input is a file, add the file to the input.
/// If the input are snappy compressed files, uncompress them before adding them to the input.
pub fn process_input(
    arg_input: Vec<PathBuf>,
    tmpdir: &tempfile::TempDir,
    custom_empty_stdin_errmsg: &str,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut processed_input = Vec::with_capacity(arg_input.len());

    let work_input = if arg_input.len() == 1 {
        let input_path = &arg_input[0];
        if input_path.is_dir() {
            // if the input is a directory, add all the supported files in the directory to the
            // input
            std::fs::read_dir(input_path)?
                .map(|entry| entry.map(|e| e.path()))
                .filter_map(|path| path.ok().filter(|p| is_supported_file(p)))
                .collect::<Vec<_>>()
        } else if input_path.is_file() {
            // if the input is a file and has the extension "infile-list" case-insensitive,
            // read the file. Each line is a file path
            if input_path
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                .map(str::to_lowercase)
                == Some("infile-list".to_string())
            {
                let mut input_file = std::fs::File::open(input_path)?;
                let mut input_file_contents = String::new();
                let mut canonical_invalid_path = PathBuf::new();
                let mut invalid_files = 0_u32;
                input_file.read_to_string(&mut input_file_contents)?;
                let infile_list_vec = input_file_contents
                    .lines()
                    .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
                    .map(PathBuf::from)
                    .filter_map(|path| {
                        if path.exists() {
                            Some(path)
                        } else {
                            // note that we're warn logging if files do not exist for
                            // each line in the infile-list file
                            // even though we're returning an error on the FIRST file that
                            // doesn't exist in the next section. This is because
                            // we want to log ALL the invalid file paths in the infile-list
                            // file, not just the first one.
                            invalid_files += 1;
                            canonical_invalid_path = path.canonicalize().unwrap_or_default();
                            tracing::warn!(
                                ".infile-list file '{}': '{}' does not exist",
                                path.display(),
                                canonical_invalid_path.display()
                            );
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                tracing::info!(
                    ".infile-list file parsed. Filecount - valid:{} invalid:{invalid_files}",
                    infile_list_vec.len()
                );
                infile_list_vec
            } else {
                // if the input is not an ".infile-list" file, add the file to the input
                arg_input
            }
        } else {
            arg_input
        }
    } else {
        arg_input
    };

    let mut stdin_path = PathBuf::new();
    let mut stdin_file_created = false;

    // check the input files
    for path in work_input {
        // check if the path is "-" (stdin)
        if path == PathBuf::from("-") {
            if !stdin_file_created {
                // if stdin was not copied to a file, copy stdin to a file named "stdin"
                let tmp_filename = tmpdir.path().join("stdin.csv");
                let mut tmp_file = std::fs::File::create(&tmp_filename)?;
                std::io::copy(&mut std::io::stdin(), &mut tmp_file)?;
                tmp_file.flush()?;
                stdin_file_created = true;
                stdin_path = tmp_filename;
            }
            processed_input.push(stdin_path.clone());
            continue;
        } else if !path.exists() {
            return Err(anyhow!("Input file '{}' does not exist", path.display()));
        }

        // is the input file snappy compressed?
        if path.extension().and_then(std::ffi::OsStr::to_str) == Some("sz") {
            // if so, decompress the file
            let decompressed_filepath = decompress_snappy_file(&path, tmpdir)?;

            // rename the decompressed file to the original filename, but still
            // inside the temp directory. this is so that the decompressed file can be
            // processed as if it was the original file without the "sz" extension
            let original_filepath = path.with_extension("");
            // safety: we know the path has a filename
            let original_filename = original_filepath.file_name().unwrap();

            let final_decompressed_filepath = tmpdir.path().join(original_filename);
            std::fs::rename(&decompressed_filepath, &final_decompressed_filepath)?;

            processed_input.push(final_decompressed_filepath);
        }
        // is the input file a zip archive?
        else if path
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .map(str::to_lowercase)
            == Some("zip".to_string())
        {
            // if so, extract all files from the zip archive to the temp directory
            tracing::info!("Extracting files from zip archive: {}", path.display());

            // Create a subdirectory in the temp directory for this zip file
            // safety: we know the path has a filename
            let zip_filename = path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .replace(".zip", "");
            let zip_extract_dir = tmpdir.path().join(&zip_filename);
            std::fs::create_dir_all(&zip_extract_dir)?;

            // Open the zip file
            let zip_file = std::fs::File::open(&path)?;
            let mut archive = zip::ZipArchive::new(zip_file)?;

            // Extract all files from the zip archive
            for i in 0..archive.len() {
                let mut zip_entry = archive.by_index(i)?;
                let entry_path = zip_entry.name().to_string();

                // Skip directories and common system files
                if entry_path.ends_with('/')
                    || !root_dir_common_filter(std::path::Path::new(&entry_path))
                {
                    tracing::info!("  Skipping system file or directory: {entry_path}");
                    continue;
                }

                // Create the full path for the extracted file
                let file_path = zip_extract_dir.join(&entry_path);

                // Create parent directories if they don't exist
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                // Extract the file
                let mut outfile = std::fs::File::create(&file_path)?;
                std::io::copy(&mut zip_entry, &mut outfile)?;

                tracing::info!("  Extracted file: {}", file_path.display());

                // Add the extracted file to the processed input if it's a supported format
                if is_supported_file(&file_path) {
                    processed_input.push(file_path);
                } else {
                    tracing::info!("  Skipping unsupported file type: {}", file_path.display());
                }
            }

            tracing::info!("Extracted {} files from zip archive", archive.len());
        } else {
            processed_input.push(path);
        }
    }

    if processed_input.is_empty() {
        if custom_empty_stdin_errmsg.is_empty() {
            return Err(anyhow!(
                "No data on stdin. Please provide at least one input file or pipe data to stdin."
            ));
        }
        return Err(anyhow!("{custom_empty_stdin_errmsg}"));
    }
    tracing::debug!("processed input file/s: {processed_input:?}");
    Ok(processed_input)
}

#[inline]
pub fn replace_column_value(
    record: &csv::StringRecord,
    column_index: usize,
    new_value: &str,
) -> csv::StringRecord {
    record
        .into_iter()
        .enumerate()
        .map(|(i, v)| if i == column_index { new_value } else { v })
        .collect()
}

/// format a SystemTime from a file's metadata to a string using the format specifier
#[inline]
pub fn format_systemtime(time: SystemTime, format_specifier: &str) -> String {
    // safety: we know the duration since UNIX EPOCH is always positive
    // as we're using this helper to format file metadata SystemTime
    // So if the duration is negative, then a file was created before UNIX EPOCH
    // which is impossible as the UNIX EPOCH is the start of time for file systems
    // we use expect here as we want it to panic if the file was created before UNIX EPOCH
    let timestamp = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH")
        .as_secs();

    let datetime = chrono::DateTime::from_timestamp(timestamp as i64, 0).unwrap_or_default();
    format!("{datetime}", datetime = datetime.format(format_specifier))
}

pub fn create_json_writer(
    output: Option<&String>,
    buffer_capacity: usize,
) -> std::io::Result<Box<dyn Write + Send + 'static>> {
    // create a JSON writer
    // if flag_output is None or "-" then write to stdout
    let output = output.as_ref().map_or("-", |s| s.as_str());
    let buffer_size = if buffer_capacity == 0 {
        config::DEFAULT_WTR_BUFFER_CAPACITY
    } else {
        buffer_capacity
    };
    let writer: Box<dyn Write + Send + 'static> = match output {
        "-" => Box::new(std::io::BufWriter::with_capacity(
            buffer_size,
            std::io::stdout(),
        )),
        "stderr" => Box::new(std::io::BufWriter::with_capacity(
            buffer_size,
            std::io::stderr(),
        )),
        _ => Box::new(std::io::BufWriter::with_capacity(
            buffer_size,
            fs::File::create(output)?,
        )),
    };
    Ok(writer)
}

/// iterate over the CSV ByteRecords and write them to the JSON file
pub fn write_json(
    output: Option<&String>,
    no_headers: bool,
    headers: &csv::ByteRecord,
    records: impl Iterator<Item = csv::ByteRecord>,
) -> anyhow::Result<()> {
    let mut json_wtr = create_json_writer(output, config::DEFAULT_WTR_BUFFER_CAPACITY * 4)?;

    let header_vec: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(col_idx, b)| {
            if no_headers {
                col_idx.to_string()
            } else if let Ok(val) = simdutf8::basic::from_utf8(b) {
                val.to_owned()
            } else {
                String::from_utf8_lossy(b).to_string()
            }
        })
        .collect();

    // Write the opening bracket for the JSON array
    write!(json_wtr, "[")?;
    let mut is_first = true;

    let rec_len = header_vec.len().saturating_sub(1);
    let mut temp_val;
    let null_val = "null".to_string();
    let mut json_string_val: serde_json::Value;

    for record in records {
        if is_first {
            is_first = false;
        } else {
            // Write a comma before each record except the first one
            write!(json_wtr, ",")?;
        }
        write!(json_wtr, "{{")?;
        for (idx, b) in record.iter().enumerate() {
            temp_val = if let Ok(val) = simdutf8::basic::from_utf8(b) {
                val.to_owned()
            } else {
                String::from_utf8_lossy(b).to_string()
            };
            if temp_val.is_empty() {
                temp_val.clone_from(&null_val);
            } else {
                // we round-trip the value to serde_json
                // to escape the string properly per JSON spec
                json_string_val = serde_json::Value::String(temp_val);
                temp_val = json_string_val.to_string();
            }
            // safety: idx is always in bounds
            // so we can get_unchecked here
            if idx < rec_len {
                unsafe {
                    write!(
                        &mut json_wtr,
                        r#""{key}":{value},"#,
                        key = header_vec.get_unchecked(idx),
                        value = temp_val
                    )?;
                }
            } else {
                // last column in the JSON record, no comma
                unsafe {
                    write!(
                        &mut json_wtr,
                        r#""{key}":{value}"#,
                        key = header_vec.get_unchecked(idx),
                        value = temp_val
                    )?;
                }
            }
        }
        write!(json_wtr, "}}")?;
    }
    // Write the closing bracket for the JSON array
    writeln!(json_wtr, "]")?;

    Ok(json_wtr.flush()?)
}

/// write a single csv::ByteRecord to a JSON record writer
/// if no_headers is true, the column index (0-based) is used as the key
/// if no_headers is false, the header is used as the key
/// if is_first is true, a comma is not written before the record
/// if is_first is false, a comma is written before the record
/// is_first is passed as a mutable reference so that it can be updated
/// in this helper function efficiently
/// in this way, we can stream JSON records to a writer
pub fn write_json_record<W: std::io::Write>(
    json_wtr: &mut W,
    no_headers: bool,
    headers: &csv::ByteRecord,
    record: &csv::ByteRecord,
    is_first: &mut bool,
) -> std::io::Result<()> {
    let header_vec: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(col_idx, b)| {
            if no_headers {
                col_idx.to_string()
            } else {
                String::from_utf8_lossy(b).to_string()
            }
        })
        .collect();

    let rec_len = header_vec.len().saturating_sub(1);
    let mut temp_val;
    let mut json_string_val: serde_json::Value;
    let null_val = "null".to_string();

    if *is_first {
        write!(json_wtr, "{{")?;
        *is_first = false;
    } else {
        write!(json_wtr, ",{{")?;
    }
    for (idx, b) in record.iter().enumerate() {
        if let Ok(val) = simdutf8::basic::from_utf8(b) {
            temp_val = val.to_owned();
        } else {
            temp_val = String::from_utf8_lossy(b).to_string();
        }
        if temp_val.is_empty() {
            temp_val.clone_from(&null_val);
        } else {
            json_string_val = serde_json::Value::String(temp_val);
            temp_val = json_string_val.to_string();
        }
        if idx < rec_len {
            unsafe {
                write!(
                    json_wtr,
                    r#""{key}":{value},"#,
                    key = header_vec.get_unchecked(idx),
                    value = temp_val
                )?;
            }
        } else {
            unsafe {
                write!(
                    json_wtr,
                    r#""{key}":{value}"#,
                    key = header_vec.get_unchecked(idx),
                    value = temp_val
                )?;
            }
        }
    }
    Ok(write!(json_wtr, "}}")?)
}

/// get stats records from stats.csv.data.jsonl file, or if its invalid, by running the stats
/// command returns tuple (`csv_fields`, `csv_stats`, `stats_col_index_map`)
pub fn get_stats_records(
    args: &SchemaArgs,
    requested_mode: StatsMode,
) -> anyhow::Result<(ByteRecord, Vec<StatsData>, HashMap<String, String>)> {
    const DATASET_STATS_PREFIX: &str = r#"{"field":"qsv__"#;

    let env_mode = env::var("QSV_STATSCACHE_MODE")
        .unwrap_or_else(|_| DEFAULT_STATSCACHE_MODE.to_string())
        .to_ascii_lowercase();

    if !["auto", "force", "none"].contains(&env_mode.as_str()) {
        return Err(anyhow!(
            "Invalid QSV_STATSCACHE_MODE value: {env_mode}. Must be one of: auto, force, none"
        ));
    }

    if requested_mode == StatsMode::None
        || env_mode == "none"
        || args.arg_input.is_none()
        || args.arg_input.as_ref() == Some(&"-".to_string())
        // safety: we know that by this point, args.arg_input is not None as
        // the earlier is_none() check would have short-circuited already
        || get_special_format(Path::new(args.arg_input.as_ref().unwrap())) != SpecialFormat::Unknown
    {
        // if stdin or StatsMode::None,
        // we're just doing frequency old school w/o cardinality
        return Ok((ByteRecord::new(), Vec::new(), HashMap::new()));
    }

    let input_path = args
        .arg_input
        .as_ref()
        .ok_or(anyhow!("No input provided"))?;
    let canonical_input_path = Path::new(input_path).canonicalize()?;
    let statsdata_path = canonical_input_path.with_extension("stats.csv.data.jsonl");

    let stats_data_current = if statsdata_path.exists() {
        let statsdata_metadata = std::fs::metadata(&statsdata_path)?;

        let input_metadata = std::fs::metadata(input_path)?;

        let statsdata_mtime = FileTime::from_last_modification_time(&statsdata_metadata);
        let input_mtime = FileTime::from_last_modification_time(&input_metadata);
        if statsdata_mtime > input_mtime {
            tracing::info!("Valid stats.csv.data.jsonl file found!");
            true
        } else {
            tracing::info!(
                "stats.csv.data.jsonl file is older than input file. Regenerating stats jsonl."
            );
            false
        }
    } else {
        tracing::info!(
            "stats.csv.data.jsonl file does not exist: {}",
            statsdata_path.display()
        );
        false
    };

    if requested_mode == StatsMode::Frequency && env_mode != "auto" && !stats_data_current {
        // if the stats.data file is not current,
        // we're also doing frequency old school w/o cardinality
        // unless env_mode auto overrides
        return Ok((ByteRecord::new(), Vec::new(), HashMap::new()));
    }

    // get the headers from the input file
    let mut rdr = csv::Reader::from_path(input_path)?;
    let csv_fields = rdr.byte_headers()?.clone();
    drop(rdr);

    let mut stats_data_loaded = false;
    let mut csv_stats: Vec<StatsData> = Vec::with_capacity(csv_fields.len());
    let mut dataset_stats: HashMap<String, String> = HashMap::with_capacity(4);

    // if stats_data file exists and is current, use it
    if stats_data_current && !args.flag_force {
        let statsdatajson_rdr =
            BufReader::with_capacity(DEFAULT_RDR_BUFFER_CAPACITY, File::open(statsdata_path)?);

        let mut curr_line: String;
        let mut s_slice: Vec<u8>;

        for line in statsdatajson_rdr.lines() {
            curr_line = line?;
            s_slice = curr_line.as_bytes().to_vec();
            if curr_line.starts_with(DATASET_STATS_PREFIX) {
                // Parse dataset stats record
                let v: serde_json::Value = simd_json::serde::from_slice(&mut s_slice)
                    .map_err(|e| anyhow!("Failed to parse dataset stats JSON: {e}"))?;
                let field = &v["field"];
                let value = v["qsv__value"].clone();

                dataset_stats.insert(
                    field
                        .as_str()
                        .unwrap_or_default()
                        .trim_matches('"')
                        .to_string(),
                    value.to_string(),
                );
            } else {
                // Parse regular stats record
                match simd_json::from_slice::<StatsData>(&mut s_slice) {
                    Ok(stats) => csv_stats.push(stats),
                    Err(e) => eprintln!("error parsing stats: {e}"),
                }
            }
        }
        stats_data_loaded = !csv_stats.is_empty();
    }

    // otherwise, run stats command to generate stats.csv.data.jsonl file
    if !stats_data_loaded {
        let stats_args = stats::Args {
            arg_input:             args.arg_input.as_ref().map(String::from),
            flag_select:           crate::select::SelectColumns::parse("").unwrap(),
            flag_everything:       false,
            flag_typesonly:        false,
            flag_infer_boolean:    false,
            flag_boolean_patterns: String::new(),
            flag_mode:             false,
            flag_cardinality:      true,
            flag_median:           false,
            flag_quartiles:        false,
            flag_mad:              false,
            flag_percentiles:      false,
            flag_percentile_list:  "5,10,40,60,90,95".to_string(),
            flag_nulls:            false,
            flag_round:            4,
            flag_infer_dates:      true,
            flag_dates_whitelist:  args.flag_dates_whitelist.to_string(),
            flag_prefer_dmy:       args.flag_prefer_dmy,
            flag_force:            args.flag_force,
            flag_jobs:             Some(njobs(args.flag_jobs)),
            flag_stats_jsonl:      true,
            flag_cache_threshold:  1, // force the creation of stats cache files
            flag_output:           None,
            flag_no_headers:       args.flag_no_headers,
            flag_delimiter:        args.flag_delimiter,
            flag_memcheck:         args.flag_memcheck,
            flag_vis_whitespace:   false,
            flag_dataset_stats:    true,
        };

        let tempfile = tempfile::Builder::new().suffix(".stats.csv").tempfile()?;
        // safety: we just created a tempfile, which is guaranteed to have a path
        let tempfile_path = tempfile.path().to_str().unwrap().to_string();

        let statsdatajson_path = &canonical_input_path.with_extension("stats.csv.data.jsonl");

        let input = stats_args.arg_input.unwrap_or_else(|| "-".to_string());

        // we do rustfmt::skip here as it was breaking the stats cmdline along strange
        // boundaries, causing CI errors.
        // This is because we're using tab characters (/t) to separate args to fix #2294,
        #[rustfmt::skip]
        let mut stats_args_str = match requested_mode {
            StatsMode::Schema => {
                // mode is StatsMode::Schema
                // we're generating schema, so we need cardinality and to infer-dates
                format!(
                    "stats\t{input}\t--round\t4\t--cardinality\
                    \t--infer-dates\t--dates-whitelist\t{dates_whitelist}\
                    \t--stats-jsonl\t--force\t--output\t{tempfile_path}",
                    dates_whitelist = stats_args.flag_dates_whitelist
                )
            },
            StatsMode::Frequency => {
                // StatsMode::Frequency
                // we're doing frequency, so we just need cardinality
                format!("stats\t{input}\t--cardinality\t--stats-jsonl\t--output\t{tempfile_path}")
            },
            StatsMode::FrequencyForceStats => {
                // StatsMode::FrequencyForceStats
                // we're doing frequency, so we need cardinality from a --forced stats run
                format!(
                    "stats\t{input}\t--cardinality\t--stats-jsonl\t--force\t--output\t{tempfile_path}"
                )
            },
            StatsMode::PolarsSchema => {
                // StatsMode::PolarsSchema
                // we need data types, ranges & cardinality
                format!("stats\t{input}\t--cardinality\t--stats-jsonl\t--output\t{tempfile_path}")
            },
            StatsMode::Outliers => {
                // StatsMode::Outliers
                // we need data types, ranges, cardinality, quartiles, mad and modes/antimodes
                format!("stats\t{input}\t--cardinality\t--quartiles\t--mad\t--mode\t--stats-jsonl\t--output\t{tempfile_path}")
            },
            StatsMode::None => unreachable!(), // we returned early on None earlier
        };
        if args.flag_prefer_dmy {
            stats_args_str = format!("{stats_args_str}\t--prefer-dmy");
        }
        if args.flag_no_headers {
            stats_args_str = format!("{stats_args_str}\t--no-headers");
        }
        if let Some(delimiter) = args.flag_delimiter {
            let delim = delimiter.as_byte() as char;
            stats_args_str = format!("{stats_args_str}\t--delimiter\t{delim}");
        }
        if args.flag_memcheck {
            stats_args_str = format!("{stats_args_str}\t--memcheck");
        }
        if let Some(jobs) = stats_args.flag_jobs {
            stats_args_str = format!("{stats_args_str}\t--jobs\t{jobs}");
        }
        if stats_args.flag_nulls {
            stats_args_str = format!("{stats_args_str}\t--nulls");
        }

        if env_mode == "force" && !stats_args_str.contains("--force") {
            stats_args_str = format!("{stats_args_str}\t--force");
        }

        let stats_args_vec: Vec<&str> = stats_args_str.split('\t').collect();

        let qsv_bin = std::env::current_exe()?;
        let mut stats_cmd = std::process::Command::new(qsv_bin);
        if requested_mode == StatsMode::Outliers {
            // set the max length for antimodes
            stats_cmd.env("QSV_ANTIMODES_LEN", "0").args(stats_args_vec);
        } else {
            stats_cmd.args(stats_args_vec);
        }
        let status = stats_cmd.output()?.status;
        if !status.success() {
            let status_code = status.code();
            if let Some(code) = status_code {
                return Err(anyhow!("waka stats exited with code: {code}"));
            }
            #[cfg(target_family = "unix")]
            {
                if let Some(signal) = status.signal() {
                    return Err(anyhow!("waka stats terminated with signal: {signal}"));
                }
                return Err(anyhow!("waka stats terminated by unknown cause"));
            }
            #[cfg(not(target_family = "unix"))]
            {
                return Err(anyhow!("waka stats terminated by unknown cause"));
            }
        }

        // create a stats data jsonl from the output of the stats command
        csv_to_jsonl(&tempfile_path, &STATSDATA_TYPES_MAP, statsdatajson_path)?;

        let statsdatajson_rdr =
            BufReader::with_capacity(DEFAULT_RDR_BUFFER_CAPACITY, File::open(statsdatajson_path)?);

        let mut curr_line: String;
        let mut s_slice: Vec<u8>;
        for line in statsdatajson_rdr.lines() {
            curr_line = line?;
            s_slice = curr_line.as_bytes().to_vec();
            if curr_line.starts_with(DATASET_STATS_PREFIX) {
                // Parse dataset stats record
                let v: serde_json::Value = simd_json::serde::from_slice(&mut s_slice)
                    .map_err(|e| anyhow!("Failed to parse dataset stats JSONL: {e}"))?;
                let field = &v["field"];
                let value = v["qsv__value"].clone();

                dataset_stats.insert(
                    field
                        .as_str()
                        .unwrap_or_default()
                        .trim_matches('"')
                        .to_string(),
                    value.to_string(),
                );
            } else {
                // Parse regular stats record
                match simd_json::from_slice::<StatsData>(&mut s_slice) {
                    Ok(stats) => csv_stats.push(stats),
                    Err(e) => eprintln!("error parsing stats: {e}"),
                }
            }
        }
    }

    // ensure csv_fields and csv_stats have the same length
    // as csv_fields may have the extra "qsv__value" field for dataset stats
    Ok((
        csv_fields.iter().take(csv_stats.len()).collect(),
        csv_stats,
        dataset_stats,
    ))
}

pub fn csv_to_jsonl(
    input_csv: &str,
    csv_types: &phf::Map<&'static str, JsonTypes>,
    output_jsonl: &PathBuf,
) -> anyhow::Result<()> {
    let file = File::open(input_csv)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let headers = rdr.headers()?;
    let key_vec: Vec<String> = headers
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let output = File::create(output_jsonl)?;
    let mut writer = BufWriter::new(output);

    let mut json_object = serde_json::Map::with_capacity(key_vec.len());
    let mut record = csv::StringRecord::new();
    let mut json_line: String;

    while rdr.read_record(&mut record)? {
        json_object.clear();

        for (i, val) in record.iter().enumerate() {
            let key = unsafe { key_vec.get_unchecked(i) };
            let data_type = csv_types.get(key).unwrap_or(&JsonTypes::String);
            let value = if val.is_empty() {
                continue;
            } else {
                match *data_type {
                    JsonTypes::String => serde_json::Value::String(val.to_owned()),
                    JsonTypes::Int => {
                        if let Ok(num) = val.parse::<u64>() {
                            serde_json::Value::Number(serde_json::Number::from(num))
                        } else {
                            serde_json::Value::String(val.to_owned())
                        }
                    },
                    JsonTypes::Float => {
                        if let Ok(num) = val.parse::<f64>() {
                            if let Some(n) = serde_json::Number::from_f64(num) {
                                serde_json::Value::Number(n)
                            } else {
                                serde_json::Value::Number(
                                    serde_json::Number::from_f64(0.0).unwrap_or_else(|| {
                                        // safety: we know that 0.0 is a valid f64
                                        serde_json::Number::from_f64(0.0).unwrap()
                                    }),
                                )
                            }
                        } else {
                            // serde_json::Value::String(val.to_owned())
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(0.0)
                                    // safety: we know that 0.0 is a valid f64
                                    .unwrap_or_else(|| serde_json::Number::from_f64(0.0).unwrap()),
                            )
                        }
                    },
                    JsonTypes::Bool => {
                        serde_json::Value::Bool(val.parse::<bool>().unwrap_or(false))
                    },
                }
            };
            json_object.insert(key.to_string(), value);
        }

        json_line = serde_json::to_string(&json_object)?;
        writeln!(writer, "{json_line}")?;
    }

    Ok(writer.flush()?)
}

/// get the optimal batch size
/// if CSV is not indexed and ROW_COUNT is not set, return DEFAULT_BATCH_SIZE
/// if batch_size is 0, return the number of rows in the CSV, effectively disabling batching
/// if batch_size is 1, force batch_size to be set to "optimal_size", even though
/// its not recommended (number of rows is too small for parallel processing)
/// if batch_size is equal to DEFAULT_BATCH_SIZE, return the optimal_size
/// failing everything above, return the requested batch_size
#[inline]
pub fn optimal_batch_size(rconfig: &Config, batch_size: usize, num_jobs: usize) -> usize {
    if batch_size > 1 && batch_size < DEFAULT_BATCH_SIZE {
        return DEFAULT_BATCH_SIZE;
    }

    let num_rows = match ROW_COUNT.get() {
        Some(count) => count.unwrap() as usize,
        None => match rconfig.indexed() {
            Ok(Some(idx)) => idx.count() as usize,
            _ => {
                return DEFAULT_BATCH_SIZE;
            },
        },
    };

    if batch_size == 0 {
        // disable batching, handle all rows in one batch
        num_rows
    } else if (num_rows > DEFAULT_BATCH_SIZE && (batch_size == DEFAULT_BATCH_SIZE))
        || batch_size == 1
    {
        // the optimal batch size is the number of rows divided by the number of jobs
        if num_rows.is_multiple_of(num_jobs) {
            // there is no remainder as num_rows is divisible by num_jobs
            num_rows / num_jobs
        } else {
            // there is a remainder, we add 1 to the batch size
            // this is to ensure that all rows are processed
            (num_rows / num_jobs) + 1
        }
    } else {
        batch_size
    }
}

/// Expand the tilde (`~`) from within the provided path.
pub fn expand_tilde(path: impl AsRef<Path>) -> Option<PathBuf> {
    let p = path.as_ref();

    let expanded = if p.starts_with("~") {
        let mut base = directories::BaseDirs::new()?.home_dir().to_path_buf();

        if !p.ends_with("~") {
            base.extend(p.components().skip(1));
        }
        base
    } else {
        p.to_path_buf()
    };
    Some(expanded)
}

/// Loads a Polars schema from a pschema.json file if it exists.
///
/// # Arguments
///
/// * `path` - The path to the input file
///
/// # Returns
///
/// * `Option<Arc<Schema>>` - The loaded schema if the file exists and can be parsed, None otherwise
fn load_schema_from_file(path: &Path) -> Result<Option<Arc<Schema>>, Box<dyn std::error::Error>> {
    // Use only the input file prefix to create the schema file path
    // e.g. data.tsv.gz, data.parquet, data.ssv should look for a schema file
    // named data.pschema.json
    // TODO: replace this with std::path::file_prefix once its stabilized
    // https://github.com/rust-lang/rust/pull/129114
    let fileprefix = path
        .file_name()
        .and_then(|fname| fname.to_str())
        .map(|s| s.split('.').next().unwrap_or(""))
        .unwrap_or_default();
    let schema_file = path.with_file_name(format!("{fileprefix}.pschema.json"));

    if schema_file.exists() {
        // Load the schema from the pschema.json file
        let file = File::open(&schema_file)?;
        let mut buf_reader = BufReader::new(file);
        let mut schema_json = String::with_capacity(100);
        buf_reader.read_to_string(&mut schema_json)?;
        let schema: Schema = serde_json::from_str(&schema_json)?;
        Ok(Some(Arc::new(schema)))
    } else {
        Ok(None)
    }
}

/// Converts files in special formats (Parquet, Avro, Arrow IPC, JSONL, JSON, or compressed CSV)
/// into a standard delimited text file. The output file extension will be:
/// - .tsv for tab-delimited
/// - .ssv for semicolon-delimited
/// - .csv for comma-delimited
///
/// # Arguments
///
/// * `path` - The path to the input file.
/// * `format` - The format of the input file.
/// * `delim` - The delimiter to use for the output CSV file.
///
/// # Returns
///
/// A `Result` containing the path to the temporary CSV file.
/// The caller is responsible for deleting the temporary file.
pub fn convert_special_format(
    path: &Path,
    format: SpecialFormat,
    delim: u8,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use polars::{
        io::avro::AvroReader,
        prelude::{
            CsvParseOptions, CsvReadOptions, CsvWriter, IpcReader, JsonLineReader, JsonReader,
            ParquetReader, SerReader, SerWriter,
        },
    };

    // Check if there's a pschema.json file with the same filestem
    // the Polars schema will be used in parsing
    // JSON/JSONL and compressed CSV files only
    let schema = if let SpecialFormat::Avro | SpecialFormat::Parquet | SpecialFormat::Ipc = format {
        None
    } else {
        load_schema_from_file(path)?
    };

    let mut extension = ".csv";
    // Create a reader based on the file format and convert to DataFrame
    let mut df = match format {
        SpecialFormat::Avro => AvroReader::new(BufReader::new(File::open(path)?)).finish()?,
        SpecialFormat::Parquet => ParquetReader::new(BufReader::new(File::open(path)?)).finish()?,
        SpecialFormat::Ipc => IpcReader::new(BufReader::new(File::open(path)?)).finish()?,
        SpecialFormat::Jsonl => {
            let df = JsonLineReader::new(BufReader::new(File::open(path)?));
            if let Some(schema) = schema {
                df.with_schema(schema).finish()?
            } else {
                df.finish()?
            }
        },
        SpecialFormat::Json => {
            let df = JsonReader::new(BufReader::new(File::open(path)?));
            if let Some(schema) = schema {
                df.with_schema(schema).finish()?
            } else {
                df.finish()?
            }
        },
        SpecialFormat::CompressedCsv
        | SpecialFormat::CompressedTsv
        | SpecialFormat::CompressedSsv => {
            let separator = match format {
                SpecialFormat::CompressedTsv => {
                    extension = ".tsv";
                    b'\t'
                },
                SpecialFormat::CompressedSsv => {
                    extension = ".ssv";
                    b';'
                },
                _ => delim,
            };

            // Create base CSV read options with the appropriate separator
            let base_options = CsvReadOptions::default()
                .with_parse_options(CsvParseOptions::default().with_separator(separator));

            // Try reading the compressed file with a schema if available
            let reader = CsvReadOptions::default()
                .try_into_reader_with_file_path(Some(path.to_path_buf()))?
                .with_options(if let Some(schema) = schema {
                    base_options.clone().with_schema(Some(schema))
                } else {
                    // it failed, try to infer it with 1,000 rows
                    base_options.clone().with_infer_schema_length(Some(1_000))
                });

            if let Ok(df) = reader.finish() {
                df
            } else {
                // Got an error. Try again with a larger infer schema length of 10,000 rows
                tracing::warn!(
                    "Falling back to reading file \"{}\" without a schema. 2nd try using infer \
                     schema length of 10,000 rows.",
                    path.display()
                );

                let reader_2ndtry = CsvReadOptions::default()
                    .try_into_reader_with_file_path(Some(path.to_path_buf()))?
                    .with_options(base_options.clone().with_infer_schema_length(Some(10_000)));

                if let Ok(df) = reader_2ndtry.finish() {
                    df
                } else {
                    tracing::warn!(
                        "Still failing. 3rd try - scanning the whole file to infer schema."
                    );

                    // Try one last time without an infer schema length, scanning the whole file
                    let reader_3rdtry = CsvReadOptions::default()
                        .try_into_reader_with_file_path(Some(path.to_path_buf()))?
                        .with_options(base_options.with_infer_schema_length(None));

                    reader_3rdtry.finish()?
                }
            }
        },
        SpecialFormat::Mft => todo!("Implement MFT format support"),
        SpecialFormat::Unknown => return Err("Unknown format".into()),
    };

    // Get or initialize temp directory that persists until program exit
    // safety: we know that the tempfile::TempDir::new() will not ordinarily fail
    // otherwise, we have a bigger problem
    let temp_dir =
        crate::config::TEMP_FILE_DIR.get_or_init(|| tempfile::TempDir::new().unwrap().keep());

    // Create temp file with appropriate extension
    let mut temp_file = tempfile::Builder::new()
        .suffix(extension)
        .tempfile_in(temp_dir)?;

    // Get QSV_POLARS_FORMAT_FLOAT_PRECISION env var
    let precision = crate::config::POLARS_FLOAT_PRECISION.get_or_init(|| {
        std::env::var("QSV_POLARS_FLOAT_PRECISION")
            .ok()
            .and_then(|s| s.parse().ok())
    });

    // Write DataFrame to CSV with specified delimiter/separator
    CsvWriter::new(BufWriter::new(&temp_file))
        .with_separator(delim)
        .with_float_precision(*precision)
        .finish(&mut df)?;
    temp_file.flush()?;

    let path = temp_file.path().to_path_buf();
    temp_file.keep()?; // Prevent auto-deletion

    Ok(path)
}

pub fn infer_polars_schema(
    delimiter: Option<config::Delimiter>,
    debuglog_flag: bool,
    table: &Path,
    schema_file: &std::path::PathBuf,
) -> anyhow::Result<bool> {
    let schema_args = SchemaArgs {
        flag_enum_threshold:  0,
        flag_ignore_case:     false,
        flag_strict_dates:    false,
        // we still get all the stats columns so we can use the stats cache
        flag_pattern_columns: crate::select::SelectColumns::parse("").unwrap(),
        flag_dates_whitelist: String::new(),
        flag_prefer_dmy:      false,
        flag_force:           false,
        flag_stdout:          false,
        flag_jobs:            Some(njobs(None)),
        flag_polars:          false,
        flag_no_headers:      false,
        flag_delimiter:       delimiter,
        arg_input:            Some(table.to_string_lossy().into_owned()),
        flag_memcheck:        false,
    };
    let (csv_fields, csv_stats, _) = get_stats_records(&schema_args, StatsMode::PolarsSchema)?;
    let mut schema = polars::prelude::Schema::with_capacity(csv_stats.len());
    for (idx, stat) in csv_stats.iter().enumerate() {
        // safety: we know that the get(idx) will not be None as we are using an iterator
        schema.insert(
            polars::prelude::PlSmallStr::from_str(
                simdutf8::basic::from_utf8(csv_fields.get(idx).unwrap()).unwrap(),
            ),
            {
                let datatype = &stat.r#type;
                #[allow(clippy::match_same_arms)]
                match datatype.as_str() {
                    "String" => polars::datatypes::DataType::String,
                    "Integer" => {
                        // safety: integer types are guaranteed to have a min and max
                        let min = stat.min.as_ref().unwrap();
                        let max = stat.max.as_ref().unwrap();

                        // Check if all values are non-negative to
                        // use unsigned types
                        if let (Ok(min_val), Ok(max_val)) = (min.parse::<i64>(), max.parse::<i64>())
                        {
                            if min_val >= 0 {
                                // Use smallest unsigned type that can hold
                                // the max value
                                if max_val <= u8::MAX as i64 {
                                    polars::datatypes::DataType::UInt8
                                } else if max_val <= u16::MAX as i64 {
                                    polars::datatypes::DataType::UInt16
                                } else if max_val <= u32::MAX as i64 {
                                    polars::datatypes::DataType::UInt32
                                } else {
                                    polars::datatypes::DataType::UInt64
                                }
                            } else {
                                // Use signed types for negative values
                                if min_val >= i32::MIN as i64 && max_val <= i32::MAX as i64 {
                                    polars::datatypes::DataType::Int32
                                } else {
                                    polars::datatypes::DataType::Int64
                                }
                            }
                        } else {
                            // Fallback to Int64 if parsing fails
                            polars::datatypes::DataType::Int64
                        }
                    },
                    "Float" => {
                        // safety: float types are guaranteed to have a min and max
                        let min = stat.min.as_ref().unwrap();
                        let max = stat.max.as_ref().unwrap();
                        let precision = stat.max_precision.unwrap_or(0);

                        // As we use f64 internally, its unlikely that we have more
                        // than 16 digits of precision, but we do this anyway to
                        // document it as the polars engine does support it
                        if precision > 16 {
                            // For very high precision, use Decimal type
                            polars::datatypes::DataType::Decimal(
                                Some(precision as usize),
                                // polars will infer scale from the data if None
                                None,
                            )
                        } else if precision > 7
                            || min.parse::<f32>().is_err()
                            || max.parse::<f32>().is_err()
                        {
                            polars::datatypes::DataType::Float64
                        } else {
                            polars::datatypes::DataType::Float32
                        }
                    },
                    "Boolean" => polars::datatypes::DataType::Boolean,
                    "Date" => polars::datatypes::DataType::Date,
                    _ => polars::datatypes::DataType::String,
                }
            },
        );
    }
    let stats_schema = std::sync::Arc::new(schema);
    let stats_schema_json = serde_json::to_string_pretty(&stats_schema)?;
    let mut file = std::io::BufWriter::new(File::create(schema_file)?);
    file.write_all(stats_schema_json.as_bytes())?;
    file.flush()?;
    if debuglog_flag {
        tracing::debug!("Saved stats_schema to file: {}", schema_file.display());
    }
    Ok(true)
}

#[bon::builder]
pub async fn download_file(
    url: &str,
    path: PathBuf,
    download_timeout: Option<u16>,
    sample_size: Option<u64>,
) -> anyhow::Result<()> {
    use futures_util::StreamExt;

    let download_timeout = match download_timeout {
        Some(t) => std::time::Duration::from_secs(timeout_secs(t).unwrap_or(30)),
        None => std::time::Duration::from_secs(30),
    };

    // setup the reqwest client
    let client = match Client::builder()
        .brotli(true)
        .gzip(true)
        .deflate(true)
        .zstd(true)
        .use_rustls_tls()
        .http2_adaptive_window(true)
        .connection_verbose(
            tracing::enabled!(tracing::Level::DEBUG) || tracing::enabled!(tracing::Level::TRACE),
        )
        .read_timeout(download_timeout)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return Err(anyhow!("Cannot build reqwest client: {e}."));
        },
    };

    let res = client.get(url).send().await?;

    let total_size = match res.content_length() {
        Some(l) => l,
        None => {
            // if we can't get the content length, set it to sentinel value
            u64::MAX
        },
    };

    let sample_size = sample_size.unwrap_or(0);

    // download chunks
    let mut file = BufWriter::with_capacity(DEFAULT_WTR_BUFFER_CAPACITY, File::create(path)?);
    let mut downloaded: u64 = 0;
    let mut stream = res.bytes_stream();

    while let Some(item) = stream.next().await {
        let chunk = item?;
        file.write_all(&chunk)?;
        let new = min(downloaded + (chunk.len() as u64), total_size);
        downloaded = new;

        if sample_size > 0 && downloaded >= sample_size {
            break;
        }
    }

    Ok(file.flush()?)
}
