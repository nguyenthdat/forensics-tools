use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;

use anyhow::anyhow;
use bon::Builder;
use csv::Writer;
use csvs_convert::{
    Options, csvs_to_ods_with_options, csvs_to_parquet_with_options, csvs_to_xlsx_with_options,
};
use eframe::egui::{
    self, Align, Button, DragValue, Frame, Layout, Popup, PopupCloseBehavior, RectAlign, RichText,
    ScrollArea, TextEdit, Ui,
};
use egui_extras::{Column, TableBuilder};
use epaint::{Color32, CornerRadius, Margin, Stroke};
use qsv::config::Config;
use regex::{Regex, RegexBuilder};
use rfd::FileDialog;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use ustr::{Ustr, ustr};

use crate::util;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Csv,
    Xlsx,
    Ods,
    Parquet,
    Json,
}

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
    #[serde(skip)]
    pub compiled_regex: Option<Regex>, // cached compiled regex (ui/runtime only)
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
            compiled_regex: None,
        }
    }
}

impl ColumnFilter {
    pub fn rebuild_regex(&mut self) {
        if self.use_regex && !self.regex_text.is_empty() {
            let mut b = RegexBuilder::new(self.regex_text.as_str());
            b.case_insensitive(self.case_insensitive);
            match b.build() {
                Ok(rx) => {
                    self.regex_error = None;
                    self.compiled_regex = Some(rx);
                }
                Err(e) => {
                    self.regex_error = Some(ustr(&e.to_string()));
                    self.compiled_regex = None;
                }
            }
        } else {
            // When regex is disabled or pattern is empty, clear cache and error
            self.compiled_regex = None;
            self.regex_error = None;
        }
    }
}

#[derive(Debug, Clone, Builder)]
pub struct FilePreview {
    pub file_path: Ustr,
    pub headers: Vec<Ustr>,
    pub preview_rows: Vec<Vec<Ustr>>,
    pub filters: Vec<ColumnFilter>,
    pub page: usize, // 0-based, per-file current page
    pub total_rows: Option<u64>,
    pub load_error: Option<Ustr>,
    pub filtered_indices: Option<Vec<u64>>,
    pub sorted_indices: Option<Vec<u64>>,
}

#[derive(Debug, Clone, Builder)]
pub struct DataTableArea {
    pub files: Vec<FilePreview>,
    pub current_file: usize,
    pub toal_rows: usize, // kept for backward-compat
    pub rows_per_page: usize,
    pub page: usize, // 0-based
    pub export_format: ExportFormat,
    pub export_only_filtered: bool,
    pub export_status: Option<Ustr>,
    pub pending_reload: bool,
    pub sort_col: Option<usize>,
    pub sort_desc: bool,
}

impl Default for DataTableArea {
    fn default() -> Self {
        Self {
            files: Vec::new(),
            current_file: 0,
            toal_rows: 0,
            rows_per_page: 50,
            page: 0,
            export_format: ExportFormat::Csv,
            export_only_filtered: true,
            export_status: None,
            pending_reload: false,
            sort_col: None,
            sort_desc: false,
        }
    }
}

impl DataTableArea {
    /// Render the preview table with a header that stays pinned vertically
    /// while sharing the same horizontal scroll as the body.
    pub fn show_preview_table(&mut self, ui: &mut Ui) {
        let (headers, file_id) = match self.current_fp() {
            Some(fp) => (fp.headers.clone(), fp.file_path.clone()),
            None => return,
        };
        let col_width: f32 = 180.0;
        let ncols = headers.len().max(1);

        // One table with header + scrollable body so column widths stay in sync
        ScrollArea::horizontal()
            .id_salt(("dt_preview_hscroll", file_id.as_str()))
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.push_id(("dt_preview_table", file_id.as_str()), |ui| {
                    let max_h = ui.available_height();
                    let mut tbl = TableBuilder::new(ui)
                        .id_salt(("dt_preview_shared", file_id.as_str()))
                        .striped(true)
                        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                        .resizable(true)
                        .min_scrolled_height(0.0) // allow small tables
                        .max_scroll_height(max_h); // fill remaining vertical space
                    for _ in 0..ncols {
                        tbl = tbl.column(Column::initial(col_width).clip(true));
                    }

                    // Header (pinned)
                    let table = tbl.header(22.0, |mut header| {
                        for (ci, h) in headers.iter().enumerate() {
                            header.col(|ui| {
                                // Allocate a single row area and split into: [label        |   controls]
                                let avail = ui.available_width().max(0.0);
                                let controls_w = 56.0; // reserve space so Filter + ▲▼ are not clipped
                                let label_w = (avail - controls_w).max(0.0);
                                ui.allocate_ui_with_layout(
                                    egui::vec2(avail, 20.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        // --- Left: header label (clipped/truncated)
                                        let header_label = egui::Label::new(
                                            RichText::new(h.as_str())
                                                .strong()
                                                .size(12.0)
                                                .color(Color32::WHITE),
                                        )
                                        .truncate();
                                        ui.add_sized(egui::vec2(label_w, 20.0), header_label);

                                        // --- Right: controls (Filter ▾ button + ▲ ▼ sort buttons)
                                        ui.scope(|ui| {
                                            // tighter spacing just for these header controls
                                            let spacing = &mut ui.style_mut().spacing;
                                            spacing.item_spacing.x = 2.0;
                                            spacing.item_spacing.y = 0.0;
                                            spacing.button_padding = egui::vec2(2.0, 1.0);

                                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                                // Filter button (keep text; constrain min width so it stays visible)
                                                let active = self
                                                    .current_fp()
                                                    .and_then(|fp| fp.filters.get(ci))
                                                    .map(|f| !f.selected.is_empty())
                                                    .unwrap_or(false);
                                                let btn_resp = util::filter_icon_button(ui, active).on_hover_text("Filter");
                                                let popup_id = ui.make_persistent_id(("col_filter_popup", ci));
                                                if btn_resp.clicked() {
                                                    Popup::toggle_id(ui.ctx(), popup_id);
                                                }

                                                // Sort buttons (vector triangles so we don't depend on font glyphs)
                                                let is_active_sort = self.sort_col == Some(ci);
                                                let desc_resp = util::sort_triangle_button(ui, false, is_active_sort && self.sort_desc)
                                                    .on_hover_text("Sort descending");
                                                if desc_resp.clicked() {
                                                    self.on_sort_click(ci, true);
                                                }
                                                let asc_resp = util::sort_triangle_button(ui, true, is_active_sort && !self.sort_desc)
                                                    .on_hover_text("Sort ascending");
                                                if asc_resp.clicked() {
                                                    self.on_sort_click(ci, false);
                                                }

                                                // Keep a bit of breathing room from the label
                                                ui.add_space(1.0);

                                                // Filter popup anchored to `btn_resp`
                                                let mut apply_now = false;
                                                let mut clear_now = false;
                                                Popup::from_response(&btn_resp)
                                                    .layout(Layout::top_down_justified(Align::LEFT))
                                                    .open_memory(None)
                                                    .close_behavior(PopupCloseBehavior::CloseOnClickOutside)
                                                    .id(popup_id)
                                                    .align(RectAlign::BOTTOM_START)
                                                    .width(btn_resp.rect.width())
                                                    .show(|ui: &mut Ui| {
                                                        ui.set_min_width(ui.available_width());
                                                        self.ensure_distinct_for_col(ci);
                                                        if let Some(fp) = self.current_fp_mut() {
                                                            let f = &mut fp.filters[ci];

                                                            if f.use_regex {
                                                                let mut rbuf = f.regex_text.to_string();
                                                                let edited = ui
                                                                    .add(
                                                                        TextEdit::singleline(&mut rbuf)
                                                                            .hint_text("Regex pattern (e.g. ^foo.*bar$)"),
                                                                    )
                                                                    .changed();
                                                                if edited {
                                                                    f.regex_text = ustr(&rbuf);
                                                                    f.rebuild_regex();
                                                                    if f.regex_error.is_none() {
                                                                        apply_now = true;
                                                                    }
                                                                }
                                                                if let Some(err) = &f.regex_error {
                                                                    ui.label(
                                                                        RichText::new(format!("⚠ Invalid regex: {}", err))
                                                                            .color(Color32::from_rgb(220, 90, 90))
                                                                            .size(11.0),
                                                                    );
                                                                }
                                                                ui.add_space(4.0);
                                                            }

                                                            let mut buf = f.search.to_string();
                                                            if ui
                                                                .add(TextEdit::singleline(&mut buf).hint_text("Search values..."))
                                                                .changed()
                                                            {
                                                                f.search = ustr(&buf);
                                                            }
                                                            ui.add_space(4.0);

                                                            let values_slice: &[Ustr] = f
                                                                .distinct_cache
                                                                .as_ref()
                                                                .map(|v| v.as_slice())
                                                                .unwrap_or(&[]);
                                                            let search_lower = f.search.as_str().to_ascii_lowercase();

                                                            let mut selected_set: std::collections::HashSet<Ustr> =
                                                                f.selected.iter().cloned().collect();
                                                            egui::ScrollArea::vertical().max_height(180.0).show(ui, |ui| {
                                                                for val in values_slice.iter() {
                                                                    if !search_lower.is_empty()
                                                                        && !val.as_str().to_ascii_lowercase().contains(&search_lower)
                                                                    {
                                                                        continue;
                                                                    }

                                                                    let mut checked = selected_set.contains(val);
                                                                    if ui.checkbox(&mut checked, val.as_str()).clicked() {
                                                                        if checked {
                                                                            selected_set.insert(val.clone());
                                                                        } else {
                                                                            selected_set.remove(val);
                                                                        }
                                                                        apply_now = true;
                                                                    }
                                                                }
                                                            });
                                                            f.selected = selected_set.into_iter().collect();

                                                            ui.separator();
                                                            ui.horizontal(|ui| {
                                                                if ui
                                                                    .checkbox(&mut f.case_insensitive, "Aa")
                                                                    .on_hover_text("Case-insensitive")
                                                                    .changed()
                                                                {
                                                                    f.rebuild_regex();
                                                                    apply_now = true;
                                                                }
                                                                if ui
                                                                    .checkbox(&mut f.use_regex, ".*")
                                                                    .on_hover_text("Use regex")
                                                                    .changed()
                                                                {
                                                                    f.rebuild_regex();
                                                                    apply_now = true;
                                                                }
                                                                ui.add_space(8.0);

                                                                if ui.button("Select all").clicked() {
                                                                    if let Some(all) = &f.distinct_cache {
                                                                        f.selected = all.clone();
                                                                    }
                                                                    apply_now = true;
                                                                }
                                                                if ui.button("Clear").clicked() {
                                                                    f.selected.clear();
                                                                    clear_now = true;
                                                                }
                                                                if ui.button("Apply").clicked() {
                                                                    apply_now = true;
                                                                }
                                                            });
                                                        }
                                                    });

                                                if apply_now || clear_now {
                                                    self.apply_filters_for_current_file();
                                                    self.reload_current_preview_page();
                                                    self.pending_reload = false;
                                                }
                                            });
                                        });
                                    },
                                );
                            });
                        }
                    });

                    // Body (scrolls under the pinned header; widths stay in sync with header)
                    let row_h = 20.0;
                    table.body(|body| {
                        if let Some(fp_ref) = self.current_fp() {
                            let rows_ref = &fp_ref.preview_rows;
                            body.rows(row_h, rows_ref.len(), |mut row| {
                                let r = &rows_ref[row.index()];
                                for ci in 0..ncols {
                                    row.col(|ui| {
                                        let txt = r.get(ci).map(|s| s.as_str()).unwrap_or("");
                                        let label = egui::Label::new(RichText::new(txt).size(12.0))
                                            .truncate();
                                        ui.add_sized(
                                            egui::vec2(ui.available_width(), row_h - 2.0),
                                            label,
                                        );
                                    });
                                }
                            });
                        }
                    });

                    // If something else scheduled a reload, run it even while the popup is open.
                    if self.pending_reload {
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
                let total = util::count_rows_for_path(&path_str);
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

            // Compose effective indices from overlay sort and filters (if any).
            let composed: Option<Vec<u64>>;
            let eff_slice: Option<&[u64]>;
            match (fp.sorted_indices.as_ref(), fp.filtered_indices.as_ref()) {
                (Some(sort), Some(filt)) => {
                    let set: std::collections::HashSet<u64> = filt.iter().copied().collect();
                    let mut v: Vec<u64> = Vec::with_capacity(filt.len());
                    for &i in sort.iter() {
                        if set.contains(&i) {
                            v.push(i);
                        }
                    }
                    composed = Some(v);
                    eff_slice = composed.as_ref().map(|v| v.as_slice());
                }
                (Some(sort), None) => {
                    eff_slice = Some(sort.as_slice());
                }
                (None, Some(filt)) => {
                    eff_slice = Some(filt.as_slice());
                }
                (None, None) => {
                    eff_slice = None;
                }
            }

            if let Some(slice_all) = eff_slice {
                let total = slice_all.len();
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
                let slice = &slice_all[start_idx..end_idx];

                // seek in contiguous chunks to minimize random seeks
                let mut i = 0usize;
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
                // Unsorted & unfiltered fast page-seek
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
                        let total = util::count_rows_for_path(&path_str);
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

                    // Compose effective indices from overlay sort and filters (if any).
                    let composed: Option<Vec<u64>>;
                    let eff_slice: Option<&[u64]>;
                    match (fp.sorted_indices.as_ref(), fp.filtered_indices.as_ref()) {
                        (Some(sort), Some(filt)) => {
                            let set: std::collections::HashSet<u64> =
                                filt.iter().copied().collect();
                            let mut v: Vec<u64> = Vec::with_capacity(filt.len());
                            for &i in sort.iter() {
                                if set.contains(&i) {
                                    v.push(i);
                                }
                            }
                            composed = Some(v);
                            eff_slice = composed.as_ref().map(|v| v.as_slice());
                        }
                        (Some(sort), None) => {
                            eff_slice = Some(sort.as_slice());
                        }
                        (None, Some(filt)) => {
                            eff_slice = Some(filt.as_slice());
                        }
                        (None, None) => {
                            eff_slice = None;
                        }
                    }

                    if let Some(slice_all) = eff_slice {
                        let total = slice_all.len();
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
                        let mut wanted_iter = slice_all[start_idx..end_idx].iter().copied();
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
                        // Unsorted & unfiltered: simple skip/take
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
            sorted_indices: None,
        };

        // Count first so we can clamp paging appropriately (byte_records for speed)
        let total = util::count_rows_for_path(fp.file_path.as_ref());
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
                                        // Return the close button's rect so we can avoid treating its clicks as tab clicks
                                        let mut close_rect = egui::Rect::NAN;
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
                                            let close_resp = util::close_button(ui, show_close)
                                                .on_hover_text("Close");
                                            close_rect = close_resp.rect;
                                            if close_resp.clicked() {
                                                close_idx = Some(idx);
                                            }
                                        });
                                        close_rect
                                    });

                                // Make the whole tab clickable, but ignore clicks on the close button area
                                let tab_hit = ui.interact(
                                    ir.response.rect,
                                    ui.id().with(("tab", idx)),
                                    egui::Sense::click(),
                                );
                                if tab_hit.clicked() {
                                    // If the click occurred over the close button, treat it as a close, not a select.
                                    let click_pos = tab_hit
                                        .interact_pointer_pos()
                                        .or_else(|| ui.input(|i| i.pointer.interact_pos()))
                                        .unwrap_or_default();
                                    if ir.inner.contains(click_pos) {
                                        close_idx = Some(idx);
                                    } else {
                                        clicked_idx = Some(idx);
                                    }
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

    /// Write rows (filtered or all) of the current file to a CSV writer.
    fn write_rows_to_csv_writer<W: std::io::Write>(
        &self,
        fp: &FilePreview,
        mut wtr: Writer<W>,
        only_filtered: bool,
    ) -> anyhow::Result<()> {
        // Write headers that we cache in-memory
        wtr.write_record(fp.headers.iter().map(|u| u.as_str()))?;

        let path_str = fp.file_path.to_string();
        let cfg = Config::new(Some(&path_str));

        // If we have filtered indices and only_filtered is true, restrict to them; else stream all rows.
        if only_filtered {
            if let Some(ref filt) = fp.filtered_indices {
                // Fast-path using the qsv index when available
                if let Ok(Some(mut idx)) = cfg.indexed() {
                    // Iterate contiguous chunks to minimize seeking, mirroring reload logic
                    let slice = &filt[..];
                    let mut i = 0usize;
                    while i < slice.len() {
                        let base = slice[i];
                        let mut len = 1usize;
                        while i + len < slice.len() && slice[i + len] == base + len as u64 {
                            len += 1;
                        }
                        idx.seek(base)
                            .map_err(|e| anyhow!("Index seek error: {e}"))?;
                        for rec_res in idx.byte_records().take(len) {
                            let brec = rec_res?;
                            wtr.write_byte_record(&brec)?;
                        }
                        i += len;
                    }
                    wtr.flush().map_err(|e| anyhow!("Flush failed: {e}"))?;
                    return Ok(());
                }

                // Fallback: stream from reader and pick wanted rows
                if let Ok(mut rdr) = cfg.reader() {
                    // Ensure header row is consumed before records()
                    let _ = rdr.headers();
                    let mut wanted_iter = filt.iter().copied();
                    let mut next = wanted_iter.next();
                    for (ri, rec_res) in rdr.records().enumerate() {
                        match next {
                            Some(want) if ri as u64 == want => {
                                let rec = rec_res.map_err(|e| anyhow!("Row read error: {e}"))?;
                                wtr.write_record(rec.iter())
                                    .map_err(|e| anyhow!("Write row failed: {e}"))?;
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
                    wtr.flush().map_err(|e| anyhow!("Flush failed: {e}"))?;
                    return Ok(());
                } else {
                    return Err(anyhow!("Unable to open CSV reader for filtered export"));
                }
            }
            // If only_filtered is requested but no filters are active, fall through to export all rows.
        }

        // No active filters (or exporting all rows): stream everything
        if let Ok(mut rdr) = cfg.reader() {
            // Write remaining rows as-is
            for rec_res in rdr.records() {
                let rec = rec_res.map_err(|e| anyhow!("Row read error: {e}"))?;
                wtr.write_record(rec.iter())?;
            }
            wtr.flush().map_err(|e| anyhow!("Flush failed: {e}"))?;
            Ok(())
        } else {
            Err(anyhow!("Unable to open CSV reader"))
        }
    }

    /// Create a temporary CSV (UTF-8, comma-delimited) with the current (optionally filtered) rows.
    fn make_temp_csv_for_current(&self, only_filtered: bool) -> anyhow::Result<PathBuf> {
        let Some(fp) = self.current_fp() else {
            return Err(anyhow!("Cannot create temp CSV: no file selected"));
        };
        // Build a temp filename that carries a human-readable stem and .csv suffix,
        // so downstream converters have a resource name/title.
        let base = crate::util::display_name(&fp.file_path);
        let mut stem: String = base
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        if stem.is_empty() {
            stem = "export".into();
        }
        if stem.len() > 31 {
            stem.truncate(31); // friendly sheet/resource name
        }

        let mut tmp = tempfile::Builder::new()
            .prefix(&stem)
            .suffix(".csv")
            .tempfile()?;
        {
            let wtr = Writer::from_writer(&mut tmp);
            self.write_rows_to_csv_writer(fp, wtr, only_filtered)?;
        }
        let path = tmp.into_temp_path();
        // Persist the file so it survives once the NamedTempFile is dropped.
        let kept = path.keep()?;
        Ok(kept)
    }

    /// Write rows (filtered or all) of the current file to a JSON writer as an array of objects.
    fn write_rows_to_json_writer<W: std::io::Write>(
        &self,
        fp: &FilePreview,
        mut out: W,
        only_filtered: bool,
    ) -> anyhow::Result<()> {
        let headers: Vec<&str> = fp.headers.iter().map(|u| u.as_str()).collect();
        let path_str = fp.file_path.to_string();
        let cfg = Config::new(Some(&path_str));

        // helper to emit one object
        let mut first = true;
        write!(&mut out, "[")?;
        let mut emit_obj = |vals: &[String]| -> anyhow::Result<()> {
            if !first {
                write!(&mut out, ",")?;
            }
            first = false;
            let mut obj = JsonMap::with_capacity(headers.len());
            for (i, key) in headers.iter().enumerate() {
                let v = vals.get(i).map(|s| s.as_str()).unwrap_or("");
                obj.insert((*key).to_string(), JsonValue::String(v.to_string()));
            }
            serde_json::to_writer(&mut out, &JsonValue::Object(obj))?;
            Ok(())
        };

        // If we have filtered indices and only_filtered is true, restrict to them; else stream all rows.
        if only_filtered {
            if let Some(ref filt) = fp.filtered_indices {
                if let Ok(Some(mut idx)) = cfg.indexed() {
                    // iterate contiguous chunks (mirrors paging logic)
                    let slice = &filt[..];
                    let mut i = 0usize;
                    while i < slice.len() {
                        let base = slice[i];
                        let mut len = 1usize;
                        while i + len < slice.len() && slice[i + len] == base + len as u64 {
                            len += 1;
                        }
                        idx.seek(base)
                            .map_err(|e| anyhow!("Index seek error: {e}"))?;
                        for rec_res in idx.byte_records().take(len) {
                            let brec = rec_res?;
                            let mut vals: Vec<String> =
                                Vec::with_capacity(headers.len().max(brec.len()));
                            vals.extend((0..headers.len()).map(|ci| {
                                brec.get(ci)
                                    .map(|b| String::from_utf8_lossy(b).into_owned())
                                    .unwrap_or_default()
                            }));
                            emit_obj(&vals)?;
                        }
                        i += len;
                    }
                    write!(&mut out, "]")?;
                    out.flush()?;
                    return Ok(());
                }
                // Fallback: stream from reader and pick wanted rows
                if let Ok(mut rdr) = cfg.reader() {
                    // Ensure header is consumed
                    let _ = rdr.headers();
                    let mut wanted_iter = filt.iter().copied();
                    let mut next = wanted_iter.next();
                    for (ri, rec_res) in rdr.records().enumerate() {
                        match next {
                            Some(want) if ri as u64 == want => {
                                let rec = rec_res?;
                                let mut vals: Vec<String> =
                                    Vec::with_capacity(headers.len().max(rec.len()));
                                vals.extend(
                                    (0..headers.len())
                                        .map(|ci| rec.get(ci).unwrap_or("").to_string()),
                                );
                                emit_obj(&vals)?;
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
                    write!(&mut out, "]")?;
                    out.flush()?;
                    return Ok(());
                } else {
                    return Err(anyhow!("Unable to open CSV reader for filtered export"));
                }
            }
        }

        // No active filters (or exporting all rows): stream everything
        if let Ok(mut rdr) = cfg.reader() {
            for rec_res in rdr.records() {
                let rec = rec_res?;
                let mut vals: Vec<String> = Vec::with_capacity(headers.len().max(rec.len()));
                vals.extend((0..headers.len()).map(|ci| rec.get(ci).unwrap_or("").to_string()));
                emit_obj(&vals)?;
            }
            write!(&mut out, "]")?;
            out.flush()?;
            Ok(())
        } else {
            Err(anyhow!("Unable to open CSV reader"))
        }
    }

    fn export_current_to_json_path(
        &self,
        dest: &PathBuf,
        only_filtered: bool,
    ) -> anyhow::Result<()> {
        let Some(fp) = self.current_fp() else {
            return Err(anyhow!("Cannot export JSON: no file selected"));
        };
        let mut file = std::fs::File::create(dest)?;
        self.write_rows_to_json_writer(fp, &mut file, only_filtered)
    }

    fn export_current_to_csv_path(
        &self,
        dest: &PathBuf,
        only_filtered: bool,
    ) -> anyhow::Result<()> {
        let Some(fp) = self.current_fp() else {
            return Err(anyhow!("Cannot export CSV: no file selected"));
        };
        let file = std::fs::File::create(dest)?;
        let wtr = Writer::from_writer(file);
        self.write_rows_to_csv_writer(fp, wtr, only_filtered)
    }

    fn export_current_to_xlsx_path(
        &self,
        dest: &PathBuf,
        only_filtered: bool,
    ) -> anyhow::Result<()> {
        let temp_csv = self.make_temp_csv_for_current(only_filtered)?;
        let options = Options::builder()
            .delimiter(Some(b',')) // we wrote comma-delimited temp CSV
            .threads(1)
            .build();
        let _ =
            csvs_to_xlsx_with_options(dest.to_string_lossy().to_string(), vec![temp_csv], options)?;

        Ok(())
    }

    fn export_current_to_ods_path(
        &self,
        dest: &PathBuf,
        only_filtered: bool,
    ) -> anyhow::Result<()> {
        let temp_csv = self.make_temp_csv_for_current(only_filtered)?;
        let options = Options::builder()
            .delimiter(Some(b',')) // we wrote comma-delimited temp CSV
            .threads(1)
            .build();
        let _ =
            csvs_to_ods_with_options(dest.to_string_lossy().to_string(), vec![temp_csv], options)?;

        Ok(())
    }

    fn export_current_to_parquet_dir(
        &self,
        dest_dir: &PathBuf,
        only_filtered: bool,
    ) -> anyhow::Result<()> {
        let temp_csv = self.make_temp_csv_for_current(only_filtered)?;
        let options = Options::builder()
            .delimiter(Some(b',')) // we wrote comma-delimited temp CSV
            .threads(1)
            .build();
        let _ = csvs_to_parquet_with_options(
            dest_dir.to_string_lossy().to_string(),
            vec![temp_csv],
            options,
        )?;
        Ok(())
    }

    fn default_export_filename(&self, ext: &str) -> String {
        if let Some(fp) = self.current_fp() {
            let base = crate::util::display_name(&fp.file_path);
            format!("{}_filtered.{}", base, ext)
        } else {
            format!("export_filtered.{}", ext)
        }
    }

    /// Render the export popup anchored to `anchor`. Call this from the existing
    /// blue "Export data" button instead of creating another button here.
    pub fn show_export_popup(&mut self, ui: &mut Ui, anchor: &egui::Response) {
        let popup_id = ui.make_persistent_id("export_popup");
        if anchor.clicked() {
            egui::Popup::toggle_id(ui.ctx(), popup_id);
        }
        egui::Popup::from_response(anchor)
            .open_memory(None)
            .close_behavior(PopupCloseBehavior::CloseOnClickOutside)
            .id(popup_id)
            .show(|ui| {
                ui.set_min_width(260.0);
                ui.label(RichText::new("Export current file").strong());
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    ui.label("Format:");
                    ui.radio_value(&mut self.export_format, ExportFormat::Csv, "CSV");
                    ui.radio_value(&mut self.export_format, ExportFormat::Xlsx, "XLSX");
                    ui.radio_value(&mut self.export_format, ExportFormat::Ods, "ODS");
                    ui.radio_value(&mut self.export_format, ExportFormat::Parquet, "Parquet");
                    ui.radio_value(&mut self.export_format, ExportFormat::Json, "JSON");
                });
                ui.add_space(4.0);
                ui.checkbox(&mut self.export_only_filtered, "Only export filtered rows");
                ui.add_space(6.0);

                if let Some(msg) = &self.export_status {
                    ui.label(RichText::new(msg.as_str()).color(Color32::from_rgb(160, 200, 160)));
                    ui.add_space(6.0);
                }

                ui.horizontal(|ui| {
                    if ui.button("Save As…").clicked() {
                        // Dispatch per selected format
                        let result: anyhow::Result<()> = match self.export_format {
                            ExportFormat::Csv => {
                                if let Some(path) = FileDialog::new()
                                    .add_filter("CSV", &["csv"])
                                    .set_file_name(self.default_export_filename("csv"))
                                    .save_file()
                                {
                                    self.export_current_to_csv_path(
                                        &path,
                                        self.export_only_filtered,
                                    )
                                } else {
                                    Ok(())
                                }
                            }
                            ExportFormat::Xlsx => {
                                if let Some(path) = FileDialog::new()
                                    .add_filter("Excel Workbook", &["xlsx"])
                                    .set_file_name(self.default_export_filename("xlsx"))
                                    .save_file()
                                {
                                    self.export_current_to_xlsx_path(
                                        &path,
                                        self.export_only_filtered,
                                    )
                                } else {
                                    Ok(())
                                }
                            }
                            ExportFormat::Ods => {
                                if let Some(path) = FileDialog::new()
                                    .add_filter("OpenDocument Spreadsheet", &["ods"])
                                    .set_file_name(self.default_export_filename("ods"))
                                    .save_file()
                                {
                                    self.export_current_to_ods_path(
                                        &path,
                                        self.export_only_filtered,
                                    )
                                } else {
                                    Ok(())
                                }
                            }
                            ExportFormat::Parquet => {
                                if let Some(dir) = FileDialog::new().pick_folder() {
                                    self.export_current_to_parquet_dir(
                                        &dir,
                                        self.export_only_filtered,
                                    )
                                } else {
                                    Ok(())
                                }
                            }
                            ExportFormat::Json => {
                                if let Some(path) = FileDialog::new()
                                    .add_filter("JSON", &["json"])
                                    .set_file_name(self.default_export_filename("json"))
                                    .save_file()
                                {
                                    self.export_current_to_json_path(
                                        &path,
                                        self.export_only_filtered,
                                    )
                                } else {
                                    Ok(())
                                }
                            }
                        };

                        self.export_status = Some(ustr(&match result {
                            Ok(()) => "✅ Export complete".to_string(),
                            Err(e) => format!("⚠ Export failed: {e}"),
                        }));
                    }
                    if ui.button("Close").clicked() {
                        egui::Popup::close_id(ui.ctx(), popup_id);
                    }
                });
            });
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
                ui.separator();
                // Removed: self.show_export_controls(ui);
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

        // Build active filters once (avoid per-row allocations)
        // We'll prepare two variants: one optimized for byte records (index),
        // and one for string records (reader).
        struct ActiveBytes {
            col: usize,
            set: Option<HashSet<Vec<u8>>>, // normalized per case-insensitive flag
            casei: bool,
            regex: Option<Regex>,
        }

        let active_any = fp
            .filters
            .iter()
            .any(|f| !f.selected.is_empty() || f.use_regex);
        if !active_any {
            fp.filtered_indices = None;
            fp.page = 0;
            return;
        }

        let path_str = fp.file_path.to_string();
        let cfg = Config::new(Some(&path_str));
        let mut out: Vec<u64> = Vec::new();

        // Try fast byte-indexed path
        if let Ok(Some(mut idx)) = cfg.indexed() {
            // Prepare ActiveBytes using byte-normalized sets
            let mut active_b: Vec<ActiveBytes> = Vec::new();
            active_b.reserve(fp.filters.len());
            for (i, f) in fp.filters.iter().enumerate() {
                if f.selected.is_empty() && !f.use_regex {
                    continue;
                }
                let set = if !f.selected.is_empty() {
                    Some(util::selected_set_bytes(&f.selected, f.case_insensitive))
                } else {
                    None
                };
                let rx = if f.use_regex {
                    f.compiled_regex.clone()
                } else {
                    None
                };
                active_b.push(ActiveBytes {
                    col: i,
                    set,
                    casei: f.case_insensitive,
                    regex: rx,
                });
            }

            if active_b.is_empty() {
                fp.filtered_indices = None;
                fp.page = 0;
                return;
            }

            // Scan using byte records; do membership checks on &[u8] to avoid String allocations.
            let mut scratch: Vec<u8> = Vec::new();
            for (ri, rec_res) in idx.byte_records().enumerate() {
                let Ok(brec) = rec_res else { continue };
                let mut keep = true;
                for af in active_b.iter() {
                    let val_bytes = brec.get(af.col).unwrap_or(&[]);

                    // Set membership (bytes)
                    if let Some(set) = af.set.as_ref() {
                        let matched = if af.casei {
                            let lowered = util::lower_ascii_into(&mut scratch, val_bytes);
                            set.contains::<[u8]>(lowered)
                        } else {
                            set.contains::<[u8]>(val_bytes)
                        };
                        if !matched {
                            keep = false;
                            break;
                        }
                    }

                    // Regex (needs &str); only run if still keeping
                    if keep {
                        if let Some(rx) = af.regex.as_ref() {
                            let v: std::borrow::Cow<'_, str> = match std::str::from_utf8(val_bytes)
                            {
                                Ok(s) => std::borrow::Cow::Borrowed(s),
                                Err(_) => std::borrow::Cow::Owned(
                                    String::from_utf8_lossy(val_bytes).into_owned(),
                                ),
                            };
                            if !rx.is_match(&v) {
                                keep = false;
                                break;
                            }
                        }
                    }
                }
                if keep {
                    out.push(ri as u64);
                }
            }

            fp.filtered_indices = Some(out);
            fp.page = 0;
            return;
        }

        // Fallback: stream with CSV reader (string records).
        // Keep the existing normalization logic for correctness (Unicode-aware via util::norm).
        let mut active: Vec<(usize, HashSet<String>, bool /*casei*/, Option<Regex>)> = Vec::new();
        for (i, f) in fp.filters.iter().enumerate() {
            if f.selected.is_empty() && !f.use_regex {
                continue;
            }
            let casei = f.case_insensitive;
            let set: HashSet<String> = if !f.selected.is_empty() {
                f.selected
                    .iter()
                    .map(|v| crate::util::norm(v, casei).to_string())
                    .collect()
            } else {
                HashSet::new()
            };

            let rx = if f.use_regex {
                f.compiled_regex.clone()
            } else {
                None
            };

            active.push((i, set, casei, rx));
        }

        if active.is_empty() {
            fp.filtered_indices = None;
            fp.page = 0;
            return;
        }

        if let Ok(mut rdr) = cfg.reader() {
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
            fp.filtered_indices = Some(out);
            fp.page = 0;
        }
    }

    fn on_sort_click(&mut self, col: usize, descending: bool) {
        // Run sort and update UI state
        match self.sort_current_by_column(col, descending) {
            Ok(()) => {
                self.sort_col = Some(col);
                self.sort_desc = descending;
                self.export_status = Some(ustr("✅ Sorted"));
            }
            Err(e) => {
                self.export_status = Some(ustr(&format!("⚠ Sort failed: {e}")));
            }
        }
        // Force preview reload after sort
        self.reload_current_preview_page();
    }

    /// Sort the current file by a column using qsv's external sorter.
    fn sort_current_by_column(&mut self, col: usize, descending: bool) -> anyhow::Result<()> {
        let Some(fp_snapshot) = self.current_fp().cloned() else {
            return Err(anyhow!("No file loaded"));
        };
        let input_path = fp_snapshot.file_path.to_string();

        let tmp_dir = std::env::temp_dir();
        let indices = util::external_sort_row_indices_by_columns(
            &input_path,
            &[col],
            descending,
            None, // default delimiter
            tmp_dir.to_string_lossy().as_ref(),
            None,  // memory limit heuristic
            None,  // threads
            false, // assume headers present
        )?;

        if let Some(fp) = self.current_fp_mut() {
            fp.sorted_indices = Some(indices);
            fp.page = 0;
            fp.load_error = None;
            // don't touch file_path/headers/filters
        }
        Ok(())
    }
}
