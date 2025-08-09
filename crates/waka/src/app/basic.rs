use std::path::PathBuf;

use eframe::egui::{self, ComboBox, Frame, Ui};
use epaint::{Color32, CornerRadius, Margin, Stroke, StrokeKind};
use ustr::Ustr;

use crate::app::table::DataTableArea;

pub struct BasicEditor {
    data_table: DataTableArea,

    show_borders: bool,
    wrap_rows: bool,
    search_column: Ustr,
    search_query: Ustr,
}

impl BasicEditor {
    pub fn new() -> Self {
        Self {
            data_table: DataTableArea::default(),
            show_borders: true,
            wrap_rows: false,
            search_column: Ustr::from(""),
            search_query: Ustr::from(""),
        }
    }

    pub fn show(&mut self, ui: &mut Ui) {
        self.data_table.handle_file_drop(ui.ctx());

        Frame::new()
            .fill(egui::Color32::from_rgb(37, 37, 38))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    self.show_results_section(ui);
                });
            });
    }

    pub fn show_results_section(&mut self, ui: &mut Ui) {
        Frame::new()
            .fill(Color32::from_rgb(37, 37, 38))
            .inner_margin(Margin::symmetric(16, 8))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // File tabs
                    self.data_table.show_file_tabs(ui);
                    self.data_table.show_pagination_controls(ui);

                    // Table controls
                    self.show_table_controls(ui);

                    // Search controls
                    self.show_search_controls(ui);

                    // Results content placeholder
                    self.show_results_placeholder(ui);
                });
            });
    }

    pub fn show_table_controls(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            // Export button (placeholder)
            let export_button = egui::Button::new(
                egui::RichText::new("📤 Export data")
                    .color(egui::Color32::WHITE)
                    .size(12.0),
            )
            .fill(egui::Color32::from_rgb(0, 120, 215))
            .corner_radius(CornerRadius::same(4));
            if ui.add(export_button).clicked() {
                // TODO: implement export
            }

            // File selector if multiple files
            if !self.data_table.files.is_empty() {
                let cur_name = self
                    .data_table
                    .current_fp()
                    .map(|f| f.file_path.as_str())
                    .unwrap_or("<none>");
                ComboBox::from_id_salt("file_selector")
                    .selected_text(format!("📂 {}", cur_name))
                    .width(220.0)
                    .show_ui(ui, |ui| {
                        for (idx, fp) in self.data_table.files.iter().enumerate() {
                            if ui
                                .selectable_label(
                                    idx == self.data_table.current_file,
                                    &*fp.file_path,
                                )
                                .clicked()
                            {
                                self.data_table.current_file = idx;
                            }
                        }
                    });
            }

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                egui::ComboBox::from_label("Show/Hide Columns")
                    .selected_text("Show/Hide Columns")
                    .show_ui(ui, |ui| {
                        if let Some(fp) = self.data_table.current_fp() {
                            for h in &fp.headers {
                                ui.label(h.as_str());
                            }
                        } else {
                            ui.label("No file loaded");
                        }
                    });

                ui.add_space(16.0);
                ui.checkbox(&mut self.show_borders, "Borders");
                ui.add_space(8.0);
                ui.checkbox(&mut self.wrap_rows, "Wrap Rows");
            });
        });

        ui.add_space(8.0);
    }

    pub fn show_search_controls(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("Search column:")
                    .color(egui::Color32::WHITE)
                    .size(12.0),
            );

            let headers: Vec<Ustr> = self
                .data_table
                .current_fp()
                .map(|fp| fp.headers.clone())
                .unwrap_or_default();

            ComboBox::from_id_salt("search_column")
                .selected_text(&*self.search_column)
                .show_ui(ui, |ui| {
                    if headers.is_empty() {
                        ui.label("No headers");
                    } else {
                        for h in headers {
                            ui.selectable_value(&mut self.search_column, h, &*h);
                        }
                    }
                });

            ui.add_space(8.0);

            let hint = format!("Search query for {}...", self.search_column);
            ui.add(
                egui::TextEdit::singleline(&mut self.search_query.to_string())
                    .hint_text(hint)
                    .desired_width(200.0),
            );
        });

        ui.add_space(12.0);
    }

    pub fn show_results_placeholder(&mut self, ui: &mut Ui) {
        if self.data_table.files.is_empty() {
            let (rect, _resp) = ui.allocate_exact_size(
                egui::Vec2::new(ui.available_width(), 180.0),
                egui::Sense::hover(),
            );
            // Drag & drop state from egui input
            let dragging_files_in = ui.ctx().input(|i| !i.raw.hovered_files.is_empty());
            let files_being_dropped_now = ui.ctx().input(|i| !i.raw.dropped_files.is_empty());

            let bg = if files_being_dropped_now {
                Color32::from_rgb(30, 70, 30)
            } else if dragging_files_in {
                Color32::from_rgb(50, 50, 50)
            } else {
                Color32::from_rgb(45, 45, 45)
            };

            let stroke = Stroke::new(1.0, Color32::from_gray(90));
            ui.painter().rect(rect, 6.0, bg, stroke, StrokeKind::Inside);

            // Show explicit prompt while a file is being dragged in
            let text = if files_being_dropped_now {
                "Release to load CSV with first 50 rows preview"
            } else if dragging_files_in {
                "Drop CSV file(s) to preview first 50 rows"
            } else {
                "📁 Drag & drop CSV file(s) here to preview first 50 rows (slice 0..50)"
            };
            ui.put(
                rect.shrink2(egui::Vec2::new(8.0, 8.0)),
                egui::Label::new(
                    egui::RichText::new(text)
                        .color(egui::Color32::from_rgb(180, 180, 180))
                        .size(14.0)
                        .strong(),
                )
                .wrap(),
            );
            // Show most recent load error (none yet)
            return;
        }

        // Defer potential reload to avoid mutable borrow during immutable borrow of current_fp
        let mut reload_requested = false;

        // Snapshot needed data first to avoid holding an immutable borrow while mutating
        let (file_path, preview_len, current_idx, total_files, load_error, no_rows) = {
            let Some(fp) = self.data_table.current_fp() else {
                return;
            };
            (
                fp.file_path.clone(),
                fp.preview_rows.len(),
                self.data_table.current_file,
                self.data_table.files.len(),
                fp.load_error.clone(),
                fp.preview_rows.is_empty(),
            )
        };

        if let Some(err) = load_error {
            Frame::new()
                .fill(egui::Color32::from_rgb(45, 45, 45))
                .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(90, 40, 40)))
                .corner_radius(CornerRadius::same(4))
                .inner_margin(Margin::same(8))
                .show(ui, |ui| {
                    ui.colored_label(egui::Color32::RED, format!("Load error: {err}"));
                });
            return;
        }

        if no_rows {
            Frame::new()
                .fill(egui::Color32::from_rgb(45, 45, 45))
                .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(70, 70, 70)))
                .corner_radius(CornerRadius::same(4))
                .inner_margin(Margin::same(8))
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("No rows match current filters.")
                                .color(egui::Color32::from_rgb(200, 180, 150)),
                        );
                        if ui.button("Clear filters").clicked() {
                            self.data_table.clear_all_filters_current_file();
                            self.data_table.reload_current_preview_page();
                        }
                    });
                });
            // Do not return: still render the table header + filter menus below
        }

        Frame::new()
            .fill(egui::Color32::from_rgb(45, 45, 45))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(70, 70, 70)))
            .corner_radius(CornerRadius::same(4))
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(format!(
                            "🔎 Preview of {} (first {} rows)  •  File {}/{}",
                            file_path,
                            preview_len,
                            current_idx + 1,
                            total_files
                        ))
                        .color(egui::Color32::WHITE)
                        .size(12.0)
                        .strong(),
                    );
                    if ui.button("⟲ Reload").clicked() {
                        reload_requested = true;
                    }
                });
                ui.add_space(6.0);

                // Pinned header + shared horizontal scroll
                self.data_table.show_preview_table(ui);
            });

        if reload_requested
            && let Some(path) = self
                .data_table
                .current_fp()
                .map(|fp| PathBuf::from(&fp.file_path))
        {
            let idx = self.data_table.current_file;
            self.data_table.files.remove(idx);
            self.data_table.load_preview(path);
        }
    }
}
