use anyhow::anyhow;
use eframe::egui::{self, Ui};
use epaint::{Color32, Shape, Stroke};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
use qsv::{
    cmd::extdedup::calculate_memory_limit,
    config::{Config, Delimiter},
    util,
};
use std::io::{self, BufRead, Write};
use std::path::Path;
use ustr::Ustr;

const RW_BUFFER_CAPACITY: usize = 1_000_000; // 1 MB

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

/// Library-friendly external CSV sort that writes the sorted rows to `output_path`.
/// Columns are 0-based indices into the CSV. Requires an index (same as CLI).
/// If `no_headers` is false, the header row is preserved as the first line.
#[allow(dead_code)]
pub fn external_sort_csv_by_columns(
    input_path: &str,
    output_path: &Path,
    columns: &[usize],
    reverse: bool,
    delimiter: Option<Delimiter>,
    tmp_dir: &str,
    memory_limit: Option<u64>,
    jobs: Option<usize>,
    no_headers: bool,
) -> anyhow::Result<()> {
    // Validate tmp dir
    if !Path::new(tmp_dir).exists() {
        return Err(anyhow!("tmp-dir '{tmp_dir}' does not exist"));
    }

    // Build sorter with limits similar to CLI
    let mem_limited_buffer_bytes = calculate_memory_limit(memory_limit);
    tracing::info!("{mem_limited_buffer_bytes} bytes used for in memory mergesort buffer...");

    let sorter: ExternalSorter<String, io::Error, LimitedBufferBuilder> =
        match ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new(tmp_dir))
            .with_buffer(LimitedBufferBuilder::new(
                mem_limited_buffer_bytes as usize,
                true,
            ))
            .with_rw_buf_size(RW_BUFFER_CAPACITY)
            .with_threads_number(util::njobs(jobs))
            .build()
        {
            Ok(sorter) => sorter,
            Err(e) => {
                return Err(anyhow!("cannot create external sorter: {e}"));
            }
        };

    // Reader config mirrors CLI behavior
    let rconfig = Config::new(Some(&input_path.to_string()))
        .delimiter(delimiter)
        .no_headers(no_headers);

    // Require an index in CSV mode (same as CLI)
    let mut idxfile = match rconfig.indexed() {
        Ok(idx) => {
            if idx.is_none() {
                return Err(anyhow!("extsort CSV mode requires an index"));
            }
            idx.unwrap()
        }
        _ => {
            return Err(anyhow!("extsort CSV mode requires an index"));
        }
    };

    let mut input_rdr = rconfig.reader()?;

    // Prepare a temp text file of "sort_key|{padded_position}"
    let linewtr_tfile = tempfile::NamedTempFile::new_in(tmp_dir)?;
    let mut line_wtr = io::BufWriter::with_capacity(RW_BUFFER_CAPACITY, linewtr_tfile.as_file());

    let headers = input_rdr.byte_headers()?.clone();

    // Pre-validate and clamp requested columns to header width
    let max_col = headers.len();
    let cols: Vec<usize> = columns.iter().copied().filter(|&c| c < max_col).collect();
    if cols.is_empty() {
        return Err(anyhow!("no valid column indices provided for sort"));
    }

    // Working buffers
    let mut sort_key = String::with_capacity(64);
    let mut utf8_string = String::with_capacity(64);
    let mut curr_row = csv::ByteRecord::new();

    let rowcount = idxfile.count();
    let width = rowcount.to_string().len();

    for row in input_rdr.byte_records() {
        curr_row.clone_from(&row?);
        sort_key.clear();
        for &ci in &cols {
            let field = curr_row.get(ci).unwrap_or(b"");
            if let Ok(s_utf8) = simdutf8::basic::from_utf8(field) {
                sort_key.push_str(s_utf8);
            } else {
                utf8_string.clear();
                utf8_string.push_str(&String::from_utf8_lossy(field));
                sort_key.push_str(&utf8_string);
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

    // External sort of the temp key file
    let sorted = match sorter.sort_by(line_rdr.lines(), compare) {
        Ok(sorted) => sorted,
        Err(e) => {
            return Err(anyhow!("cannot do external sort: {e:?}"));
        }
    };

    // Materialize sorted keys into another temp file for sequential reading
    let sorted_tfile = tempfile::NamedTempFile::new_in(tmp_dir)?;
    let mut sorted_line_wtr =
        io::BufWriter::with_capacity(RW_BUFFER_CAPACITY, sorted_tfile.as_file());

    for item in sorted.map(Result::unwrap) {
        sorted_line_wtr.write_all(format!("{item}\n").as_bytes())?;
    }
    sorted_line_wtr.flush()?;

    // Drop the unsorted temp file
    drop(line_wtr);
    linewtr_tfile.close()?;

    // Now write the final sorted CSV to `output_path`
    let out_str = output_path.to_string_lossy().to_string();
    let mut sorted_csv_wtr = Config::new(Some(&out_str)).writer()?;

    let position_delta: u64 = if no_headers {
        1
    } else {
        // Write the header row if --no-headers is false
        let byte_headers = headers;
        sorted_csv_wtr.write_byte_record(&byte_headers)?;
        2
    };

    // amortize allocations
    let mut record_wrk = csv::ByteRecord::new();
    let mut line = String::new();

    let sorted_lines = std::fs::File::open(sorted_tfile.path())?;
    let sorted_line_rdr = io::BufReader::with_capacity(RW_BUFFER_CAPACITY, sorted_lines);
    for l in sorted_line_rdr.lines() {
        line.clone_from(&l?);
        let Ok(position) = atoi_simd::parse::<u64>(&line.as_bytes()[line.len() - width..]) else {
            return Err(anyhow!("Failed to retrieve position: invalid integer"));
        };

        idxfile
            .seek(position.saturating_sub(position_delta))
            .map_err(|e| anyhow!("Failed to seek to position {position}: {e}"))?;

        idxfile.read_byte_record(&mut record_wrk)?;
        sorted_csv_wtr.write_byte_record(&record_wrk)?;
    }
    sorted_csv_wtr.flush()?;

    // Cleanup
    drop(sorted_line_wtr);
    sorted_tfile.close()?;

    Ok(())
}

/// External sort that returns a sorted vector of 0-based row indices (data rows only),
/// preserving Excel-like behavior (header stays at top when `no_headers` is false).
/// Requires an existing qsv index (.idx) for `input_path`.
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
    let mem_limited_buffer_bytes = calculate_memory_limit(memory_limit);
    tracing::info!("{mem_limited_buffer_bytes} bytes used for in memory mergesort buffer...");
    let sorter: ExternalSorter<String, io::Error, LimitedBufferBuilder> =
        match ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new(tmp_dir))
            .with_buffer(LimitedBufferBuilder::new(
                mem_limited_buffer_bytes as usize,
                true,
            ))
            .with_rw_buf_size(RW_BUFFER_CAPACITY)
            .with_threads_number(util::njobs(jobs))
            .build()
        {
            Ok(sorter) => sorter,
            Err(e) => return Err(anyhow!("cannot create external sorter: {e}")),
        };

    let rconfig = Config::new(Some(&input_path.to_string()))
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
        for &ci in &cols {
            let field = curr_row.get(ci).unwrap_or(b"");
            if let Ok(s_utf8) = simdutf8::basic::from_utf8(field) {
                sort_key.push_str(s_utf8);
            } else {
                utf8_string.clear();
                utf8_string.push_str(&String::from_utf8_lossy(field));
                sort_key.push_str(&utf8_string);
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
    let (rect, response) = ui.allocate_exact_size(egui::vec2(18.0, 18.0), egui::Sense::click());
    let r = rect.shrink2(egui::vec2(4.0, 4.0));
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
    let stroke = Stroke::new(1.0, Color32::from_gray(60));
    ui.painter()
        .add(Shape::convex_polygon(vec![p1, p2, p3], fill, stroke));
    response
}
