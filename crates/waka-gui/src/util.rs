use std::{
    collections::HashSet,
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::anyhow;
use eframe::egui::{self, Ui};
use epaint::{Color32, Shape, Stroke};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
use num_cpus;
use polars_sql::SQLContext;
use ustr::Ustr;
use waka_core::{
    config::{Config, Delimiter},
    sqlp::OutputMode,
};

const RW_BUFFER_CAPACITY: usize = 1_000_000; // 1 MB

// To mimic Excel-like sort, we try to sort numerically when a field looks like an integer.
// Otherwise we sort case-insensitive as text. We also insert a column separator between
// composite keys to avoid collisions like ["ab","c"] vs ["a","bc"].
const SORT_COL_SEP: char = '\u{1F}'; // unit separator, unlikely in data
const INT_PAD: usize = 39; // support up to 39-digit integers lexicographically

fn encode_integer_key(buf: &mut String, s: &str) -> bool {
    // accept optional leading '-' and digits only
    if s.is_empty() {
        return false;
    }
    let (neg, digits) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else {
        (false, s)
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // too many digits? treat as text instead of risking overflow
    if digits.len() > INT_PAD {
        return false;
    }

    // build zero-padded magnitude
    let mut mag = String::with_capacity(INT_PAD);
    for _ in 0..(INT_PAD - digits.len()) {
        mag.push('0');
    }
    mag.push_str(digits);

    // prefix encodes sign so that negatives < positives
    if neg {
        buf.push('0'); // bucket for negative numbers
        // 9's complement so that lex ascending matches numeric ascending for negatives
        for ch in mag.bytes() {
            let d = ch - b'0';
            buf.push(char::from(b'9' - d));
        }
    } else {
        buf.push('1'); // bucket for positive numbers (and zero)
        buf.push_str(&mag);
    }
    true
}

fn append_excel_like_key(out: &mut String, field_str: &str) {
    // Try integer first; if not, treat as text (case-insensitive)
    if !encode_integer_key(out, field_str.trim()) {
        out.push('2'); // bucket for text
        out.push_str(&field_str.to_ascii_lowercase());
    }
}

pub fn display_name(path: &str) -> Ustr {
    Path::new(path)
        .file_name()
        .map(|s| Ustr::from(&s.to_string_lossy()))
        .unwrap_or_else(|| Ustr::from(path))
}

pub fn norm<'a>(s: &'a str, casei: bool) -> std::borrow::Cow<'a, str> {
    if casei {
        std::borrow::Cow::Owned(s.to_ascii_lowercase())
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// External sort that returns a sorted vector of 0-based row indices (data rows only),
/// preserving Excel-like behavior (header stays at top when `no_headers` is false).
/// Requires an existing qsv index (.idx) for `input_path`.
#[allow(clippy::too_many_arguments)]
pub fn external_sort_row_indices_by_columns(
    input_path: &str,
    columns: &[usize],
    reverse: bool,
    delimiter: Option<Delimiter>,
    tmp_dir: &str,
    memory_limit: Option<u64>,
    jobs: Option<usize>,
    no_headers: bool,
) -> anyhow::Result<Vec<u64>> {
    if !Path::new(tmp_dir).exists() {
        return Err(anyhow!("tmp-dir '{tmp_dir}' does not exist"));
    }

    // same ext-sort setup as CSV variant
    let mem_limited_buffer_bytes = 1024 * 1024 * 1024; // 1 GB default
    tracing::info!("{mem_limited_buffer_bytes} bytes used for in memory mergesort buffer...");
    let threads = jobs.unwrap_or_else(|| num_cpus::get().max(1));
    let sorter: ExternalSorter<String, io::Error, LimitedBufferBuilder> =
        match ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new(tmp_dir))
            .with_buffer(LimitedBufferBuilder::new(
                mem_limited_buffer_bytes as usize,
                true,
            ))
            .with_rw_buf_size(RW_BUFFER_CAPACITY)
            .with_threads_number(threads)
            .build()
        {
            Ok(sorter) => sorter,
            Err(e) => return Err(anyhow!("cannot create external sorter: {e}")),
        };

    let rconfig = Config::builder()
        .path(&input_path.to_string())
        .build()
        .delimiter(delimiter)
        .no_headers(no_headers);

    let idxfile = match rconfig.indexed() {
        Ok(Some(idx)) => idx,
        _ => return Err(anyhow!("extsort CSV mode requires an index")),
    };

    let mut input_rdr = rconfig.reader()?;

    // Write "sortkey|{padded_position}" temp file
    let linewtr_tfile = tempfile::NamedTempFile::new_in(tmp_dir)?;
    let mut line_wtr = io::BufWriter::with_capacity(RW_BUFFER_CAPACITY, linewtr_tfile.as_file());

    let headers = input_rdr.byte_headers()?.clone();
    let max_col = headers.len();
    let cols: Vec<usize> = columns.iter().copied().filter(|&c| c < max_col).collect();
    if cols.is_empty() {
        return Err(anyhow!("no valid column indices provided for sort"));
    }

    let mut sort_key = String::with_capacity(64);
    let mut utf8_string = String::with_capacity(64);
    let mut curr_row = csv::ByteRecord::new();

    let rowcount = idxfile.count();
    let width = rowcount.to_string().len();

    for row in input_rdr.byte_records() {
        curr_row.clone_from(&row?);
        sort_key.clear();
        for (j, &ci) in cols.iter().enumerate() {
            if j > 0 {
                sort_key.push(SORT_COL_SEP);
            }
            let field = curr_row.get(ci).unwrap_or(b"");
            if let Ok(s_utf8) = simdutf8::basic::from_utf8(field) {
                append_excel_like_key(&mut sort_key, s_utf8);
            } else {
                utf8_string.clear();
                utf8_string.push_str(&String::from_utf8_lossy(field));
                append_excel_like_key(&mut sort_key, &utf8_string);
            }
        }
        let idx_position = curr_row.position().unwrap();
        writeln!(line_wtr, "{sort_key}|{:01$}", idx_position.line(), width)?;
    }
    line_wtr.flush()?;

    let line_rdr = io::BufReader::with_capacity(
        RW_BUFFER_CAPACITY,
        std::fs::File::open(linewtr_tfile.path())?,
    );

    let compare = |a: &String, b: &String| {
        if reverse {
            a.cmp(b).reverse()
        } else {
            a.cmp(b)
        }
    };
    let sorted = sorter
        .sort_by(line_rdr.lines(), compare)
        .map_err(|e| anyhow!("cannot do external sort: {e:?}"))?;

    // Materialize sorted order to a temp file (so we can read positions sequentially)
    let sorted_tfile = tempfile::NamedTempFile::new_in(tmp_dir)?;
    let mut sorted_line_wtr =
        io::BufWriter::with_capacity(RW_BUFFER_CAPACITY, sorted_tfile.as_file());
    for item in sorted.map(Result::unwrap) {
        sorted_line_wtr.write_all(format!("{item}\n").as_bytes())?;
    }
    sorted_line_wtr.flush()?;

    // Decode positions into 0-based data row indices
    let position_delta: u64 = if no_headers { 1 } else { 2 };
    let mut out: Vec<u64> = Vec::with_capacity(rowcount as usize);
    let mut line = String::new();
    let sorted_lines = std::fs::File::open(sorted_tfile.path())?;
    let sorted_line_rdr = io::BufReader::with_capacity(RW_BUFFER_CAPACITY, sorted_lines);
    for l in sorted_line_rdr.lines() {
        line.clone_from(&l?);
        let pos = atoi_simd::parse::<u64>(&line.as_bytes()[line.len() - width..])
            .map_err(|_| anyhow!("Failed to retrieve position: invalid integer"))?;
        out.push(pos.saturating_sub(position_delta));
    }

    // Clean up
    drop(sorted_line_wtr);
    sorted_tfile.close()?;
    drop(line_wtr);
    linewtr_tfile.close()?;

    Ok(out)
}

// Draw a small up/down triangle as a clickable icon button (font-independent).
pub fn sort_triangle_button(ui: &mut Ui, up: bool, active: bool) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(14.0, 14.0), egui::Sense::click());
    let r = rect.shrink2(egui::vec2(2.5, 2.5));
    let (p1, p2, p3) = if up {
        (
            egui::pos2(r.center().x, r.top()),
            egui::pos2(r.left(), r.bottom()),
            egui::pos2(r.right(), r.bottom()),
        )
    } else {
        (
            egui::pos2(r.left(), r.top()),
            egui::pos2(r.right(), r.top()),
            egui::pos2(r.center().x, r.bottom()),
        )
    };
    let fill = if active {
        Color32::from_rgb(0, 200, 120)
    } else if response.hovered() {
        Color32::from_gray(210)
    } else {
        Color32::from_gray(170)
    };
    let stroke = Stroke::new(0.9, Color32::from_gray(60));
    ui.painter()
        .add(Shape::convex_polygon(vec![p1, p2, p3], fill, stroke));
    response
}

// Draw a compact funnel (filter) icon as a clickable button.
pub fn filter_icon_button(ui: &mut Ui, active: bool) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(16.0, 14.0), egui::Sense::click());
    let r = rect.shrink2(egui::vec2(2.0, 1.5));

    let fill = if active {
        Color32::from_rgb(0, 200, 120)
    } else if response.hovered() {
        Color32::from_gray(210)
    } else {
        Color32::from_gray(170)
    };
    let stroke = Stroke::new(1.0, Color32::from_gray(60));

    // Top trapezoid
    let top_h = (r.height() * 0.55).clamp(6.0, 9.0);
    let stem_w = (r.width() * 0.18).clamp(2.0, 3.0);
    let mid_y = r.top() + top_h;
    let cx = r.center().x;
    let trapezoid = vec![
        egui::pos2(r.left(), r.top()),
        egui::pos2(r.right(), r.top()),
        egui::pos2(cx + stem_w, mid_y),
        egui::pos2(cx - stem_w, mid_y),
    ];
    ui.painter()
        .add(Shape::convex_polygon(trapezoid, fill, stroke));

    // Stem rectangle
    let stem_h = (r.height() - top_h - 1.0).max(3.0);
    let stem = vec![
        egui::pos2(cx - stem_w, mid_y),
        egui::pos2(cx + stem_w, mid_y),
        egui::pos2(cx + stem_w, mid_y + stem_h),
        egui::pos2(cx - stem_w, mid_y + stem_h),
    ];
    ui.painter().add(Shape::convex_polygon(stem, fill, stroke));

    response
}

pub fn close_button(ui: &mut Ui, emphasize: bool) -> egui::Response {
    let desired = egui::vec2(18.0, 18.0);
    let (rect, response) = ui.allocate_exact_size(desired, egui::Sense::click());

    let color = if emphasize {
        Color32::from_white_alpha(230)
    } else {
        Color32::from_white_alpha(110)
    };
    let stroke = Stroke::new(1.6, color);

    // Draw a crisp X
    let r = rect.shrink(4.0);
    let painter = ui.painter();
    painter.line_segment([r.left_top(), r.right_bottom()], stroke);
    painter.line_segment([r.right_top(), r.left_bottom()], stroke);

    response
}

#[inline]
pub fn lower_ascii_into<'a>(buf: &'a mut Vec<u8>, src: &[u8]) -> &'a [u8] {
    buf.clear();
    buf.reserve(src.len());
    for &b in src {
        buf.push(b.to_ascii_lowercase());
    }
    &buf[..]
}

// --- Performance helpers for filtering on large files ---
#[inline]
pub fn selected_set_bytes(selected: &[Ustr], case_insensitive: bool) -> HashSet<Vec<u8>> {
    let mut set = HashSet::with_capacity(selected.len());
    for v in selected {
        let mut bytes = v.as_str().as_bytes().to_vec();
        if case_insensitive {
            for b in &mut bytes {
                *b = b.to_ascii_lowercase();
            }
        }
        set.insert(bytes);
    }
    set
}

// ---- Library-friendly API for programmatic SQL queries ----
#[derive(Clone)]
pub struct SqlpLibArgs {
    pub inputs:          Vec<PathBuf>,
    pub sql:             String,
    /// Output format: one of "csv", "json", "jsonl", "parquet", "arrow", "avro", or "none"
    pub format:          String,
    /// Field delimiter to use for CSV output (and for CSV input parsing)
    pub delimiter:       u8,
    pub try_parsedates:  bool,
    pub infer_len:       usize,
    pub cache_schema:    bool,
    pub decimal_comma:   bool,
    pub datetime_format: Option<String>,
    pub date_format:     Option<String>,
    pub time_format:     Option<String>,
    pub float_precision: Option<usize>,
    /// For parquet/arrow/avro outputs
    pub compression:     String,
    pub compress_level:  Option<i32>,
    pub statistics:      bool,
    /// If set, write the query result to this path; otherwise, write to stdout
    pub output_path:     Option<PathBuf>,
    /// Suppress returning the shape to stderr if true
    pub quiet:           bool,
}

pub struct SqlpLibResult {
    pub rows:        usize,
    pub cols:        usize,
    pub output_path: Option<PathBuf>,
    pub elapsed_ms:  u128,
}

fn parse_output_mode(s: &str) -> OutputMode {
    match s.to_ascii_lowercase().as_str() {
        "json" => OutputMode::Json,
        "jsonl" => OutputMode::Jsonl,
        "parquet" => OutputMode::Parquet,
        "arrow" => OutputMode::Arrow,
        "avro" => OutputMode::Avro,
        "none" => OutputMode::None,
        _ => OutputMode::Csv,
    }
}

/// Run a Polars SQL query programmatically using the same engine as `qsv sqlp`.
/// This registers each input CSV as a table named by its file stem and also as `_t_N` aliases.
/// Returns the result shape and optional output path and elapsed time.
pub fn run_sqlp(lib_args: SqlpLibArgs) -> anyhow::Result<SqlpLibResult> {
    todo!()
}
