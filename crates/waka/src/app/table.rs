use bon::Builder;
use eframe::egui::{self, Button, Frame, RichText, ScrollArea, Ui};
use epaint::{Color32, CornerRadius, Margin, Stroke};
use qsv::config::Config;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use ustr::{Ustr, ustr};

use crate::util;

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct FilePreview {
    pub file_path: Ustr,
    pub headers: Vec<Ustr>,
    pub preview_rows: Vec<Vec<Ustr>>,
    pub load_error: Option<Ustr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Default)]

pub struct DataTableArea {
    pub files: Vec<FilePreview>,
    pub current_file: usize,
    pub toal_rows: usize,
}

impl DataTableArea {
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
        if self.files.iter().any(|fp| fp.file_path == file_path) {
            // Switch to it
            if let Some(idx) = self.files.iter().position(|fp| fp.file_path == file_path) {
                self.current_file = idx;
            }
            return;
        }

        let mut fp = FilePreview {
            file_path,
            headers: Vec::new(),
            preview_rows: Vec::new(),
            load_error: None,
        };

        let cfg = Config::new(Some(&file_path.to_string()));
        match cfg.reader() {
            Ok(mut rdr) => {
                match rdr.headers() {
                    Ok(hdrs) => {
                        fp.headers = hdrs.iter().map(|s| ustr(s)).collect();
                    }
                    Err(e) => {
                        fp.load_error = Some(ustr(&format!("Cannot read headers: {e}")));
                    }
                }
                if fp.load_error.is_none() {
                    for rec_res in rdr.records().take(50) {
                        match rec_res {
                            Ok(rec) => {
                                fp.preview_rows.push(rec.iter().map(|s| ustr(s)).collect());
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

                                Frame::new()
                                    .fill(tab_fill)
                                    .stroke(tab_stroke)
                                    .corner_radius(CornerRadius {
                                        nw: 6,
                                        ne: 6,
                                        sw: 0,
                                        se: 0,
                                    })
                                    .inner_margin(Margin::symmetric(10, 6))
                                    .show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            let resp = ui
                                                .selectable_label(
                                                    selected,
                                                    RichText::new(name)
                                                        .size(12.0)
                                                        .color(Color32::WHITE),
                                                )
                                                .on_hover_text(&*fp.file_path);

                                            if resp.clicked() {
                                                clicked_idx = Some(idx);
                                            }

                                            ui.add_space(6.0);
                                            let close_resp = ui
                                                .add(
                                                    Button::new(RichText::new("✕").size(10.0))
                                                        .fill(Color32::TRANSPARENT),
                                                )
                                                .on_hover_text("Close");

                                            if close_resp.clicked() {
                                                close_idx = Some(idx);
                                            }
                                        });
                                    });

                                ui.add_space(6.0);
                            }
                        });

                        if let Some(i) = clicked_idx {
                            self.current_file = i;
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
                        }
                    });
            });

        ui.add_space(6.0);
    }
}
