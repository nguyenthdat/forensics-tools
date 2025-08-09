use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;

use bon::Builder;
use eframe::egui::{self, Button, DragValue, Frame, RichText, ScrollArea, TextEdit, Ui};
use egui_extras::{Column, TableBuilder};
use epaint::{Color32, CornerRadius, Margin, Stroke};
use qsv::config::Config;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use ustr::{Ustr, ustr};

use crate::util;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFilter {
    pub enabled: bool,
    pub include: bool, // include selected values when true; else exclude them
    pub case_insensitive: bool, // Aa toggle
    pub selected: Vec<Ustr>, // chosen values in this column
    #[serde(skip)]
    pub distinct_cache: Option<Vec<Ustr>>, // lazily populated (sampled)
    #[serde(skip)]
    pub search: Ustr, // search within the dropdown
    // Regex filtering
    pub use_regex: bool,  // enable regex filter
    pub regex_text: Ustr, // pattern text (persisted)
    #[serde(skip)]
    pub regex_error: Option<Ustr>, // last regex compile error (ui only)
}

impl Default for ColumnFilter {
    fn default() -> Self {
        Self {
            enabled: true,
            include: true,
            case_insensitive: false,
            selected: Vec::new(),
            distinct_cache: None,
            search: ustr(""),
            use_regex: false,
            regex_text: ustr(""),
            regex_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct FilePreview {
    pub file_path: Ustr,
    pub headers: Vec<Ustr>,
    pub preview_rows: Vec<Vec<Ustr>>,
    pub filters: Vec<ColumnFilter>,
    pub page: usize, // 0-based, per-file current page
    pub total_rows: Option<u64>,
    pub load_error: Option<Ustr>,
    #[serde(skip)]
    pub filtered_indices: Option<Vec<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct DataTableArea {
    pub files: Vec<FilePreview>,
    pub current_file: usize,
    pub toal_rows: usize, // kept for backward-compat
    pub rows_per_page: usize,
    pub page: usize, // 0-based
    #[serde(skip)]
    pub pending_reload: bool,
}

impl Default for DataTableArea {
    fn default() -> Self {
        Self {
            files: Vec::new(),
            current_file: 0,
            toal_rows: 0,
            rows_per_page: 50,
            page: 0,
            pending_reload: false,
        }
    }
}

impl DataTableArea {
    fn count_rows_for_path(path: &str) -> u64 {
        let cfg = Config::new(Some(&path.to_string()));
        // Try index first
        if let Ok(Some(idx)) = cfg.indexed() {
            return idx.count();
        }
        // Fallback: scan the file (faster: use byte_records to avoid string allocations)
        if let Ok(mut rdr) = cfg.reader() {
            let mut cnt: u64 = 0;
            for rec in rdr.byte_records() {
                if rec.is_ok() {
                    cnt = cnt.saturating_add(1);
                }
            }
            cnt
        } else {
            0
        }
    }

    /// Render the preview table with a header that stays pinned vertically
    /// while sharing the same horizontal scroll as the body.
    pub fn show_preview_table(&mut self, ui: &mut Ui) {
        let (headers, rows) = match self.current_fp() {
            Some(fp) => (fp.headers.clone(), fp.preview_rows.clone()),
            None => return,
        };
        // Track if any filter popup is open this frame
        let mut any_filter_popup_open = false;
        let col_width: f32 = 180.0;
        let ncols = headers.len().max(1);

        // Single horizontal scroll area that wraps both header and body
        ScrollArea::horizontal()
            .id_salt("dt_preview_hscroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                // Use a shared id so header & body reuse the same column state (widths, resize)
                ui.push_id("dt_preview_table", |ui| {
                    // --- HEADER (pinned vertically) ---
                    let mut header_tbl = TableBuilder::new(ui)
                        .striped(false)
                        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                        .resizable(true);
                    for _ in 0..ncols {
                        header_tbl = header_tbl.column(Column::initial(col_width).clip(true));
                    }
                    header_tbl.header(22.0, |mut header| {
                        for (ci, h) in headers.iter().enumerate() {
                            header.col(|ui| {
                                ui.horizontal(|ui| {
                                    // label (leave room for filter button)
                                    ui.add_sized(
                                        egui::vec2(col_width - 24.0, 20.0),
                                        egui::Label::new(
                                            RichText::new(h.as_str())
                                                .strong()
                                                .size(12.0)
                                                .color(Color32::WHITE),
                                        ),
                                    );

                                    // filter dropdown button (menu_button replacement)
                                    let mut apply_now = false;
                                    let mut clear_now = false;

                                    let active = self
                                        .current_fp()
                                        .and_then(|fp| fp.filters.get(ci))
                                        .map(|f| !f.selected.is_empty())
                                        .unwrap_or(false);

                                    let btn_text =
                                        RichText::new("Filter ▾").size(12.0).color(if active {
                                            Color32::from_rgb(0, 200, 120)
                                        } else {
                                            Color32::from_gray(230)
                                        });
                                    ui.menu_button(btn_text, |ui| {
                                        any_filter_popup_open = true;
                                        self.ensure_distinct_for_col(ci);
                                        if let Some(fp) = self.current_fp_mut() {
                                            let f = &mut fp.filters[ci];

                                            // Sticky top controls: case sensitivity + regex
                                            ui.horizontal(|ui| {
                                                if ui
                                                    .checkbox(&mut f.case_insensitive, "Aa")
                                                    .on_hover_text("Case-insensitive")
                                                    .changed()
                                                {
                                                    apply_now = true;
                                                }
                                                if ui
                                                    .checkbox(&mut f.use_regex, ".*")
                                                    .on_hover_text("Use regex filter")
                                                    .changed()
                                                {
                                                    // Re-validate the current pattern when toggled
                                                    if f.use_regex && !f.regex_text.is_empty() {
                                                        let mut b = RegexBuilder::new(
                                                            f.regex_text.as_str(),
                                                        );
                                                        b.case_insensitive(f.case_insensitive);
                                                        match b.build() {
                                                            Ok(_) => f.regex_error = None,
                                                            Err(e) => {
                                                                f.regex_error =
                                                                    Some(ustr(&e.to_string()))
                                                            }
                                                        }
                                                    }
                                                    apply_now = true;
                                                }
                                            });
                                            ui.add_space(4.0);

                                            if f.use_regex {
                                                let mut rbuf = f.regex_text.to_string();
                                                let edited = ui
                                                    .add(TextEdit::singleline(&mut rbuf).hint_text(
                                                        "Regex pattern (e.g. ^foo.*bar$)",
                                                    ))
                                                    .changed();
                                                if edited {
                                                    f.regex_text = ustr(&rbuf);
                                                    // Try to compile with current case setting; show error if invalid
                                                    let mut b =
                                                        RegexBuilder::new(f.regex_text.as_str());
                                                    b.case_insensitive(f.case_insensitive);
                                                    match b.build() {
                                                        Ok(_) => {
                                                            f.regex_error = None;
                                                            apply_now = true;
                                                        }
                                                        Err(e) => {
                                                            f.regex_error =
                                                                Some(ustr(&e.to_string()));
                                                        }
                                                    }
                                                }
                                                if let Some(err) = &f.regex_error {
                                                    ui.label(
                                                        RichText::new(format!(
                                                            "⚠ Invalid regex: {}",
                                                            err
                                                        ))
                                                        .color(Color32::from_rgb(220, 90, 90))
                                                        .size(11.0),
                                                    );
                                                }
                                                ui.add_space(4.0);
                                            }

                                            let mut buf = f.search.to_string();
                                            if ui
                                                .add(
                                                    TextEdit::singleline(&mut buf)
                                                        .hint_text("Search values..."),
                                                )
                                                .changed()
                                            {
                                                f.search = ustr(&buf);
                                                // Apply on typing, but reload is deferred by pending_reload
                                                apply_now = true;
                                            }
                                            ui.add_space(4.0);

                                            let mut values: Vec<Ustr> =
                                                f.distinct_cache.clone().unwrap_or_default();
                                            if !f.search.is_empty() {
                                                let s = f.search.as_str().to_ascii_lowercase();
                                                values.retain(|v| {
                                                    v.as_str().to_ascii_lowercase().contains(&s)
                                                });
                                            }

                                            let mut selected_set: std::collections::HashSet<Ustr> =
                                                f.selected.iter().cloned().collect();

                                            egui::ScrollArea::vertical().max_height(180.0).show(
                                                ui,
                                                |ui| {
                                                    for val in values {
                                                        let mut checked =
                                                            selected_set.contains(&val);
                                                        if ui
                                                            .checkbox(&mut checked, val.as_str())
                                                            .clicked()
                                                        {
                                                            if checked {
                                                                selected_set.insert(val.clone());
                                                            } else {
                                                                selected_set.remove(&val);
                                                            }
                                                            // Apply immediately on item click
                                                            apply_now = true;
                                                        }
                                                    }
                                                },
                                            );

                                            f.selected = selected_set.into_iter().collect();

                                            ui.separator();
                                            ui.horizontal(|ui| {
                                                // Sticky toggles at bottom too
                                                if ui
                                                    .checkbox(&mut f.case_insensitive, "Aa")
                                                    .on_hover_text("Case-insensitive")
                                                    .changed()
                                                {
                                                    apply_now = true;
                                                }
                                                if ui
                                                    .checkbox(&mut f.use_regex, ".*")
                                                    .on_hover_text("Use regex")
                                                    .changed()
                                                {
                                                    apply_now = true;
                                                }
                                                ui.add_space(8.0);

                                                if ui.button("Select all").clicked() {
                                                    if let Some(all) = &f.distinct_cache {
                                                        f.selected = all.clone();
                                                    }
                                                    // Apply immediately but keep popup open
                                                    apply_now = true;
                                                }
                                                if ui.button("Clear").clicked() {
                                                    f.selected.clear();
                                                    // Apply immediately but keep popup open
                                                    clear_now = true;
                                                }
                                                if ui.button("Apply").clicked() {
                                                    apply_now = true;
                                                }
                                            });
                                        }
                                    });

                                    if apply_now || clear_now {
                                        // Apply filter changes immediately but defer the heavy table reload
                                        self.apply_filters_for_current_file();
                                        self.pending_reload = true;
                                    }
                                });
                            });
                        }
                    });

                    ui.add_space(4.0);

                    // --- BODY (vertically scrollable only) ---
                    ScrollArea::vertical()
                        .id_salt("dt_preview_vscroll")
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            let mut body_tbl = TableBuilder::new(ui)
                                .striped(true)
                                .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                                .resizable(true);
                            for _ in 0..ncols {
                                body_tbl = body_tbl.column(Column::initial(col_width).clip(true));
                            }

                            let row_h = 20.0;
                            body_tbl.body(|body| {
                                body.rows(row_h, rows.len(), |mut row| {
                                    let r = &rows[row.index()];
                                    // Ensure consistent column count: pad/truncate to headers length
                                    for ci in 0..ncols {
                                        row.col(|ui| {
                                            let txt = r.get(ci).map(|s| s.as_str()).unwrap_or("");
                                            // Single-line, clipped cell content
                                            let label =
                                                egui::Label::new(RichText::new(txt).size(12.0))
                                                    .truncate();
                                            ui.add_sized(egui::vec2(col_width, row_h - 2.0), label);
                                        });
                                    }
                                });
                            });
                        });
                    // If any filter popup is open, keep the table stable so typing/clicking doesn't close it.
                    if self.pending_reload && !any_filter_popup_open {
                        self.reload_current_preview_page();
                        self.pending_reload = false;
                    }
                });
            });
    }

    pub fn reload_current_preview_page(&mut self) {
        // Work with locals to avoid borrowing conflicts while updating self later.
        let rows_per_page = self.rows_per_page;

        let Some(fp) = self.current_fp_mut() else {
            return;
        };
        let mut new_page = fp.page;
        let path_str = fp.file_path.to_string();
        let cfg = Config::new(Some(&path_str));

        // Prepare rows buffer up front
        fp.preview_rows.clear();
        fp.preview_rows.reserve(rows_per_page);

        // Track legacy total rows update for self after we drop the fp borrow.
        let mut new_total_rows: Option<usize> = None;

        // Prefer using the qsv index for fast paging; fallback to streaming reader
        if let Ok(Some(mut idx)) = cfg.indexed() {
            // headers: only (re)read into cache if empty
            if fp.headers.is_empty() {
                if let Ok(hdrs) = idx.headers() {
                    fp.headers = hdrs.iter().map(ustr).collect();
                } else if let Ok(bhdrs) = idx.byte_headers() {
                    fp.headers = bhdrs
                        .iter()
                        .map(|b| ustr(&String::from_utf8_lossy(b)))
                        .collect();
                }
            }

            // count rows once per file (if not already counted)
            if fp.total_rows.is_none() {
                let total = Self::count_rows_for_path(&path_str);
                fp.total_rows = Some(total);
                new_total_rows = Some(total as usize); // update self after dropping fp
                // Clamp page within new total
                let total_pages =
                    ((total as usize).saturating_add(rows_per_page - 1)) / rows_per_page;
                if total_pages == 0 {
                    new_page = 0;
                } else if new_page >= total_pages {
                    new_page = total_pages - 1;
                }
            } else {
                new_total_rows = Some(fp.total_rows.unwrap_or(0) as usize);
            }

            if let Some(ref filt) = fp.filtered_indices {
                let total = filt.len();
                new_total_rows = Some(total);
                let total_pages = if rows_per_page == 0 {
                    0
                } else {
                    total.div_ceil(rows_per_page)
                };
                if total_pages == 0 {
                    new_page = 0;
                } else if new_page >= total_pages {
                    new_page = total_pages - 1;
                }

                let start_idx = new_page.saturating_mul(rows_per_page);
                let end_idx = (start_idx + rows_per_page).min(total);
                let slice = &filt[start_idx..end_idx];

                // seek in contiguous chunks to minimize random seeks
                let mut i = 0;
                while i < slice.len() {
                    let base = slice[i];
                    let mut len = 1usize;
                    while i + len < slice.len() && slice[i + len] == base + len as u64 {
                        len += 1;
                    }
                    if let Err(e) = idx.seek(base) {
                        fp.load_error = Some(ustr(&format!("Index seek error: {e}")));
                        break;
                    }
                    for rec_res in idx.byte_records().take(len) {
                        match rec_res {
                            Ok(brec) => {
                                let mut row = Vec::with_capacity(fp.headers.len().max(brec.len()));
                                row.extend(brec.iter().map(|b| ustr(&String::from_utf8_lossy(b))));
                                fp.preview_rows.push(row);
                            }
                            Err(e) => {
                                fp.load_error = Some(ustr(&format!("Row read error: {e}")));
                                break;
                            }
                        }
                    }
                    i += len;
                }
            } else {
                let start = new_page.saturating_mul(rows_per_page);
                if let Err(e) = idx.seek(start as u64) {
                    fp.load_error = Some(ustr(&format!("Index seek error: {e}")));
                } else {
                    for rec_res in idx.byte_records().take(rows_per_page) {
                        match rec_res {
                            Ok(brec) => {
                                let mut row = Vec::with_capacity(fp.headers.len().max(brec.len()));
                                row.extend(brec.iter().map(|b| ustr(&String::from_utf8_lossy(b))));
                                fp.preview_rows.push(row);
                            }
                            Err(e) => {
                                fp.load_error = Some(ustr(&format!("Row read error: {e}")));
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            match cfg.reader() {
                Ok(mut rdr) => {
                    // headers: only (re)read into cache if empty
                    if fp.headers.is_empty() {
                        if let Ok(hdrs) = rdr.headers() {
                            fp.headers = hdrs.iter().map(ustr).collect();
                        }
                    } else {
                        // Ensure CSV header row is consumed so records() yields data rows.
                        let _ = rdr.headers();
                    }

                    // count rows once per file (if not already counted)
                    if fp.total_rows.is_none() {
                        let total = Self::count_rows_for_path(&path_str);
                        fp.total_rows = Some(total);
                        new_total_rows = Some(total as usize); // update self after dropping fp
                        // Clamp page within new total
                        let total_pages =
                            ((total as usize).saturating_add(rows_per_page - 1)) / rows_per_page;
                        if total_pages == 0 {
                            new_page = 0;
                        } else if new_page >= total_pages {
                            new_page = total_pages - 1;
                        }
                    } else {
                        new_total_rows = Some(fp.total_rows.unwrap_or(0) as usize);
                    }

                    if let Some(ref filt) = fp.filtered_indices {
                        let total = filt.len();
                        new_total_rows = Some(total);
                        let total_pages = if rows_per_page == 0 {
                            0
                        } else {
                            total.div_ceil(rows_per_page)
                        };
                        if total_pages == 0 {
                            new_page = 0;
                        } else if new_page >= total_pages {
                            new_page = total_pages - 1;
                        }

                        let start_idx = new_page.saturating_mul(rows_per_page);
                        let end_idx = (start_idx + rows_per_page).min(total);
                        let mut wanted_iter = filt[start_idx..end_idx].iter().copied();
                        let mut next = wanted_iter.next();

                        for (ri, rec_res) in rdr.records().enumerate() {
                            match next {
                                Some(want) if ri as u64 == want => {
                                    if let Ok(rec) = rec_res {
                                        let mut row =
                                            Vec::with_capacity(fp.headers.len().max(rec.len()));
                                        row.extend(rec.iter().map(ustr));
                                        fp.preview_rows.push(row);
                                    }
                                    next = wanted_iter.next();
                                    if next.is_none() {
                                        break;
                                    }
                                }
                                Some(want) if (ri as u64) < want => continue,
                                _ => {
                                    if next.is_none() {
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        let start = new_page.saturating_mul(rows_per_page);
                        for rec_res in rdr.records().skip(start).take(rows_per_page) {
                            match rec_res {
                                Ok(rec) => {
                                    let mut row =
                                        Vec::with_capacity(fp.headers.len().max(rec.len()));
                                    row.extend(rec.iter().map(ustr));
                                    fp.preview_rows.push(row);
                                }
                                Err(e) => {
                                    fp.load_error = Some(ustr(&format!("Row read error: {e}")));
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    fp.load_error = Some(ustr(&format!("Unable to open file: {e}")));
                }
            }
        }

        // End the mutable borrow of the file before mutating other fields on self.
        fp.page = new_page;
        let _ = fp;

        if let Some(tr) = new_total_rows {
            self.toal_rows = tr; // keep legacy field updated
        }
    }

    pub fn handle_file_drop(&mut self, ctx: &egui::Context) {
        // Accept multiple files now
        let dropped = ctx.input(|i| i.raw.dropped_files.clone());
        if dropped.is_empty() {
            return;
        }
        for f in dropped {
            if let Some(path) = f.path {
                self.load_preview(path);
            }
        }
    }

    pub fn current_fp(&self) -> Option<&FilePreview> {
        self.files.get(self.current_file)
    }

    pub fn current_fp_mut(&mut self) -> Option<&mut FilePreview> {
        self.files.get_mut(self.current_file)
    }

    pub fn load_preview(&mut self, path: PathBuf) {
        let file_path = Ustr::from(&path.to_string_lossy());
        // Avoid reloading same file
        if let Some(idx) = self.files.iter().position(|fp| fp.file_path == file_path) {
            self.current_file = idx;
            // refresh preview for current pagination settings
            self.page = 0; // reset to first page when reselecting
            self.reload_current_preview_page();
            return;
        }

        let mut fp = FilePreview {
            file_path,
            headers: Vec::new(),
            preview_rows: Vec::new(),
            filters: Vec::new(),
            page: 0,
            total_rows: None,
            load_error: None,
            filtered_indices: None,
        };

        // Count first so we can clamp paging appropriately (byte_records for speed)
        let total = Self::count_rows_for_path(fp.file_path.as_ref());
        fp.total_rows = Some(total);
        self.toal_rows = total as usize;
        self.page = 0;

        let cfg = Config::new(Some(&fp.file_path.to_string()));
        match cfg.reader() {
            Ok(mut rdr) => {
                if let Ok(hdrs) = rdr.headers() {
                    fp.headers = hdrs.iter().map(ustr).collect();
                } else {
                    fp.load_error = Some(ustr("Cannot read headers"));
                }
                if fp.load_error.is_none() {
                    fp.filters = vec![ColumnFilter::default(); fp.headers.len()];
                }

                if fp.load_error.is_none() {
                    let rows_per_page = self.rows_per_page;
                    fp.preview_rows.reserve(rows_per_page);
                    for rec_res in rdr.records().take(rows_per_page) {
                        match rec_res {
                            Ok(rec) => {
                                let mut row = Vec::with_capacity(fp.headers.len().max(rec.len()));
                                row.extend(rec.iter().map(ustr));
                                fp.preview_rows.push(row);
                            }
                            Err(e) => {
                                fp.load_error = Some(ustr(&format!("Row read error: {e}")));
                                break;
                            }
                        }
                    }

                    if fp.preview_rows.is_empty() && fp.load_error.is_none() {
                        fp.load_error = Some(ustr("No data rows found."));
                    }
                }
            }
            Err(e) => {
                fp.load_error = Some(ustr(&format!("Unable to open file: {e}")));
            }
        }

        self.files.push(fp);
        self.current_file = self.files.len() - 1;
    }

    // Draw a crisp vector "X" close button, for font fallback safety.
    fn close_button(ui: &mut Ui, emphasize: bool) -> egui::Response {
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

    pub fn show_file_tabs(&mut self, ui: &mut Ui) {
        if self.files.is_empty() {
            return;
        }

        Frame::new()
            .fill(Color32::from_rgb(45, 45, 45))
            .inner_margin(Margin::symmetric(8, 6))
            .show(ui, |ui| {
                ScrollArea::horizontal()
                    .id_salt("file_tabs_scroll")
                    .auto_shrink([false, true])
                    .show(ui, |ui| {
                        let mut clicked_idx: Option<usize> = None;
                        let mut close_idx: Option<usize> = None;

                        ui.horizontal(|ui| {
                            for (idx, fp) in self.files.iter().enumerate() {
                                let selected = idx == self.current_file;
                                let name = util::display_name(&fp.file_path);

                                let tab_fill = if selected {
                                    Color32::from_rgb(60, 60, 60)
                                } else {
                                    Color32::from_rgb(40, 40, 40)
                                };
                                let tab_stroke = if selected {
                                    Stroke::new(1.5, Color32::from_rgb(100, 100, 100))
                                } else {
                                    Stroke::new(1.0, Color32::from_rgb(70, 70, 70))
                                };

                                // Compact tab: no icon, tighter paddings, same colors
                                let accent = Color32::from_rgb(0, 120, 215);
                                let ir = Frame::new()
                                    .fill(tab_fill)
                                    .stroke(tab_stroke)
                                    .corner_radius(CornerRadius {
                                        nw: 6,
                                        ne: 6,
                                        sw: 0,
                                        se: 0,
                                    })
                                    .inner_margin(Margin::symmetric(8, 4))
                                    .show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            // filename label (smaller font, narrow height)
                                            let text_color = if selected {
                                                Color32::WHITE
                                            } else {
                                                Color32::from_rgb(210, 210, 210)
                                            };
                                            let label = egui::Label::new(
                                                RichText::new(name.clone())
                                                    .size(11.0)
                                                    .color(text_color),
                                            )
                                            .truncate();
                                            let resp = ui
                                                .add_sized(egui::vec2(120.0, 14.0), label)
                                                .on_hover_text(&*fp.file_path);
                                            if resp.clicked() {
                                                clicked_idx = Some(idx);
                                            }

                                            // Close button at far right (keep small spacing)
                                            ui.add_space(6.0);
                                            let show_close = selected || resp.hovered();
                                            let close_resp = Self::close_button(ui, show_close)
                                                .on_hover_text("Close");
                                            if close_resp.clicked() {
                                                close_idx = Some(idx);
                                            }
                                        });
                                    });

                                // Make the whole tab clickable
                                let tab_hit = ui.interact(
                                    ir.response.rect,
                                    ui.id().with(("tab", idx)),
                                    egui::Sense::click(),
                                );
                                if tab_hit.clicked() {
                                    clicked_idx = Some(idx);
                                }

                                // Draw top accent underline for the active tab
                                if selected {
                                    let rect = ir.response.rect;
                                    let y = rect.top() + 1.0;
                                    ui.painter().line_segment(
                                        [
                                            egui::pos2(rect.left() + 6.0, y),
                                            egui::pos2(rect.right() - 6.0, y),
                                        ],
                                        Stroke::new(1.5, accent),
                                    );
                                }

                                ui.add_space(4.0);
                            }
                        });

                        if let Some(i) = clicked_idx {
                            self.current_file = i;
                            self.reload_current_preview_page();
                        }

                        if let Some(i) = close_idx {
                            // Remove the file and adjust current index safely
                            self.files.remove(i);
                            if self.files.is_empty() {
                                self.current_file = 0;
                            } else if self.current_file >= self.files.len() {
                                self.current_file = self.files.len() - 1;
                            } else if self.current_file > i {
                                self.current_file -= 1;
                            }
                            self.reload_current_preview_page();
                        }
                    });
            });

        ui.add_space(4.0);
    }

    pub fn clear_all_filters_current_file(&mut self) {
        if let Some(fp) = self.current_fp_mut() {
            for f in &mut fp.filters {
                f.selected.clear();
                f.search = ustr("");
                f.use_regex = false;
                f.regex_text = ustr("");
                f.regex_error = None;
            }
            fp.filtered_indices = None;
            fp.page = 0;
        }
    }

    pub fn show_pagination_controls(&mut self, ui: &mut Ui) {
        {
            // Pull current values without holding a mutable borrow during UI
            let (mut page, total_rows) = match self.current_fp() {
                Some(fp) => {
                    let filtered = fp.filtered_indices.as_ref().map(|v| v.len());
                    let base_total = fp.total_rows.unwrap_or(self.toal_rows as u64) as usize;
                    (fp.page, filtered.unwrap_or(base_total))
                }
                None => return,
            };
            let rows_per_page = self.rows_per_page;
            let total_pages = if rows_per_page == 0 {
                0
            } else {
                total_rows.div_ceil(rows_per_page)
            };

            let mut reload_needed = false;

            ui.horizontal(|ui| {
                // Page navigation
                if ui.add_enabled(page > 0, Button::new("⏮ First")).clicked() {
                    page = 0;
                    reload_needed = true;
                }
                if ui.add_enabled(page > 0, Button::new("◀ Prev")).clicked() {
                    page = page.saturating_sub(1);
                    reload_needed = true;
                }
                ui.label(format!(
                    "Page {}/{}",
                    if total_pages == 0 { 0 } else { page + 1 },
                    total_pages.max(1)
                ));
                if ui
                    .add_enabled(page + 1 < total_pages, Button::new("Next ▶"))
                    .clicked()
                {
                    page += 1;
                    reload_needed = true;
                }
                if ui
                    .add_enabled(page + 1 < total_pages, Button::new("Last ⏭"))
                    .clicked()
                {
                    if total_pages > 0 {
                        page = total_pages - 1;
                    }
                    reload_needed = true;
                }

                ui.separator();

                // Rows per page controls
                ui.label("Rows/page:");
                let mut rpp_changed = false;
                if ui.button("–").clicked() && self.rows_per_page > 5 {
                    self.rows_per_page = (self.rows_per_page - 5).max(5);
                    rpp_changed = true;
                }
                let mut rpp = self.rows_per_page as i64;
                let r = ui.add(DragValue::new(&mut rpp).range(5..=5000).speed(1));
                if r.changed() {
                    self.rows_per_page = rpp as usize;
                    rpp_changed = true;
                }
                if ui.button("+").clicked() {
                    self.rows_per_page = (self.rows_per_page + 5).min(5000);
                    rpp_changed = true;
                }

                if rpp_changed {
                    page = 0;
                    reload_needed = true;
                }

                ui.separator();
                ui.label(format!("Rows: {}", total_rows));
            });

            if reload_needed {
                if let Some(fp) = self.current_fp_mut() {
                    fp.page = page;
                }
                self.reload_current_preview_page();
                // keep legacy field loosely in sync for any old callers
                self.page = page;
            }
        }
    }

    // Populate the unique values cache for a column (uses index if available)
    fn ensure_distinct_for_col(&mut self, col: usize) {
        let Some(fp) = self.current_fp_mut() else {
            return;
        };
        if col >= fp.headers.len() {
            return;
        }
        if fp
            .filters
            .get(col)
            .and_then(|f| f.distinct_cache.as_ref())
            .is_some()
        {
            return;
        }

        let path_str = fp.file_path.to_string();
        let cfg = Config::new(Some(&path_str));
        let mut set: BTreeSet<Ustr> = BTreeSet::new();
        let limit = 2_000usize; // keep menus snappy

        if let Ok(Some(mut idx)) = cfg.indexed() {
            for rec_res in idx.byte_records() {
                if let Ok(brec) = rec_res {
                    if let Some(val) = brec.get(col) {
                        set.insert(ustr(&String::from_utf8_lossy(val)));
                        if set.len() >= limit {
                            break;
                        }
                    }
                }
            }
        } else if let Ok(mut rdr) = cfg.reader() {
            for rec_res in rdr.records() {
                if let Ok(rec) = rec_res {
                    if let Some(val) = rec.get(col) {
                        set.insert(ustr(val));
                        if set.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }

        let distinct: Vec<Ustr> = set.into_iter().collect();
        if let Some(f) = fp.filters.get_mut(col) {
            f.distinct_cache = Some(distinct);
        }
    }

    pub fn apply_filters_for_current_file(&mut self) {
        let Some(fp) = self.current_fp_mut() else {
            return;
        };
        if fp.filters.is_empty() {
            return;
        }

        // Build active filters (now supporting regex)
        let mut active: Vec<(usize, HashSet<String>, bool /*casei*/, Option<Regex>)> = Vec::new();
        for (i, f) in fp.filters.iter().enumerate() {
            let casei = f.case_insensitive;
            let set: HashSet<String> = if !f.selected.is_empty() {
                f.selected
                    .iter()
                    .map(|v| util::norm(v, casei).to_string())
                    .collect()
            } else {
                HashSet::new()
            };

            let rx = if f.use_regex {
                let pat = f.regex_text.as_str();
                if !pat.is_empty() {
                    let mut b = RegexBuilder::new(pat);
                    b.case_insensitive(casei);
                    match b.build() {
                        Ok(r) => Some(r),
                        Err(_) => None, // ignore invalid regex at apply time
                    }
                } else {
                    None
                }
            } else {
                None
            };

            if !set.is_empty() || rx.is_some() {
                active.push((i, set, casei, rx));
            }
        }

        if active.is_empty() {
            fp.filtered_indices = None;
            fp.page = 0;
            return;
        }

        let path_str = fp.file_path.to_string();
        let cfg = Config::new(Some(&path_str));
        let mut out: Vec<u64> = Vec::new();

        if let Ok(Some(mut idx)) = cfg.indexed() {
            for (ri, rec_res) in idx.byte_records().enumerate() {
                if let Ok(brec) = rec_res {
                    let mut keep = true;
                    for (col, set, casei, rx) in active.iter() {
                        let val = brec
                            .get(*col)
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .unwrap_or_default();
                        // If there are selected values, enforce membership
                        if !set.is_empty() {
                            let key = util::norm(&val, *casei);
                            if !set.contains(key.as_ref()) {
                                keep = false;
                                break;
                            }
                        }
                        // If a regex is active, enforce regex match (on original value)
                        if let Some(r) = rx {
                            if !r.is_match(&val) {
                                keep = false;
                                break;
                            }
                        }
                    }
                    if keep {
                        out.push(ri as u64);
                    }
                }
            }
        } else if let Ok(mut rdr) = cfg.reader() {
            for (ri, rec_res) in rdr.records().enumerate() {
                if let Ok(rec) = rec_res {
                    let mut keep = true;
                    for (col, set, casei, rx) in active.iter() {
                        let val = rec.get(*col).unwrap_or("");
                        // If there are selected values, enforce membership
                        if !set.is_empty() {
                            let key = util::norm(val, *casei);
                            if !set.contains(key.as_ref()) {
                                keep = false;
                                break;
                            }
                        }
                        // If a regex is active, enforce regex match (on original value)
                        if let Some(r) = rx {
                            if !r.is_match(val) {
                                keep = false;
                                break;
                            }
                        }
                    }
                    if keep {
                        out.push(ri as u64);
                    }
                }
            }
        }

        fp.filtered_indices = Some(out);
        fp.page = 0;
    }
}
