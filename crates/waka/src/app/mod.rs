use eframe::egui;

use crate::{
    APP_VERSION,
    app::{ftsq::FtsEditor, sqlq::SqlEditor},
};

mod filer;
mod ftsq;
mod sqlq;
mod table;

pub enum WakaMode {
    Filer,
    FullTextSearch,
    Sql,
    Workflow,
}

pub struct WakaApp {
    sql_editor: SqlEditor,
    fts_editor: FtsEditor,
    current_mode: WakaMode,
}

impl WakaApp {
    pub fn new() -> Self {
        WakaApp {
            sql_editor: SqlEditor::new(),
            current_mode: WakaMode::Filer,
            fts_editor: FtsEditor::new(),
        }
    }
}

impl eframe::App for WakaApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Set dark theme
        ctx.set_visuals(egui::Visuals::dark());

        // Top panel with logo and navigation
        egui::TopBottomPanel::top("top_panel")
            .exact_height(60.0)
            .show(ctx, |ui| {
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    // Logo and app name
                    ui.add_space(16.0);

                    // Circle logo placeholder (you can replace with actual image)
                    let logo_response =
                        ui.allocate_response(egui::Vec2::splat(32.0), egui::Sense::hover());
                    ui.painter().circle_filled(
                        logo_response.rect.center(),
                        16.0,
                        egui::Color32::from_rgb(0, 150, 255), // Blue color
                    );
                    ui.painter().text(
                        logo_response.rect.center(),
                        egui::Align2::CENTER_CENTER,
                        "W",
                        egui::FontId::proportional(18.0),
                        egui::Color32::WHITE,
                    );

                    ui.add_space(8.0);

                    // App name and version
                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new("Waka DFIR Suite")
                                .size(16.0)
                                .color(egui::Color32::WHITE),
                        );
                        ui.label(
                            egui::RichText::new(format!("Version v{}", APP_VERSION))
                                .size(11.0)
                                .italics()
                                .color(egui::Color32::GRAY),
                        );
                    });

                    ui.add_space(32.0);

                    // Navigation buttons
                    ui.horizontal(|ui| {
                        // Workflow button
                        let workflow_color = if matches!(self.current_mode, WakaMode::Workflow) {
                            egui::Color32::from_rgb(0, 150, 255)
                        } else {
                            egui::Color32::GRAY
                        };

                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new("📋 Workflow")
                                        .color(workflow_color)
                                        .size(14.0),
                                )
                                .fill(egui::Color32::TRANSPARENT)
                                .stroke(egui::Stroke::NONE),
                            )
                            .clicked()
                        {
                            self.current_mode = WakaMode::Workflow;
                        }

                        ui.add_space(24.0);

                        // Toolbox button
                        let toolbox_color = if !matches!(self.current_mode, WakaMode::Workflow) {
                            egui::Color32::from_rgb(0, 150, 255)
                        } else {
                            egui::Color32::GRAY
                        };

                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new("🔧 Toolbox")
                                        .color(toolbox_color)
                                        .size(14.0),
                                )
                                .fill(egui::Color32::TRANSPARENT)
                                .stroke(egui::Stroke::NONE),
                            )
                            .clicked()
                        {
                            // Show toolbox submenu or switch to default tool
                            self.current_mode = WakaMode::Filer;
                        }
                    });

                    // Right side - tool selection (when in toolbox mode)
                    if !matches!(self.current_mode, WakaMode::Workflow) {
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.add_space(16.0);

                            // Tool selection buttons
                            ui.horizontal(|ui| {
                                if ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new("SQL Editor").size(12.0).color(
                                                if matches!(self.current_mode, WakaMode::Sql) {
                                                    egui::Color32::WHITE
                                                } else {
                                                    egui::Color32::GRAY
                                                },
                                            ),
                                        )
                                        .fill(if matches!(self.current_mode, WakaMode::Sql) {
                                            egui::Color32::from_rgb(0, 150, 255)
                                        } else {
                                            egui::Color32::TRANSPARENT
                                        })
                                        .corner_radius(4.0),
                                    )
                                    .clicked()
                                {
                                    self.current_mode = WakaMode::Sql;
                                }

                                if ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new("Full Text Search")
                                                .size(12.0)
                                                .color(
                                                    if matches!(
                                                        self.current_mode,
                                                        WakaMode::FullTextSearch
                                                    ) {
                                                        egui::Color32::WHITE
                                                    } else {
                                                        egui::Color32::GRAY
                                                    },
                                                ),
                                        )
                                        .fill(
                                            if matches!(self.current_mode, WakaMode::FullTextSearch)
                                            {
                                                egui::Color32::from_rgb(0, 150, 255)
                                            } else {
                                                egui::Color32::TRANSPARENT
                                            },
                                        )
                                        .corner_radius(4.0),
                                    )
                                    .clicked()
                                {
                                    self.current_mode = WakaMode::FullTextSearch;
                                }

                                if ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new("Filer").size(12.0).color(
                                                if matches!(self.current_mode, WakaMode::Filer) {
                                                    egui::Color32::WHITE
                                                } else {
                                                    egui::Color32::GRAY
                                                },
                                            ),
                                        )
                                        .fill(if matches!(self.current_mode, WakaMode::Filer) {
                                            egui::Color32::from_rgb(0, 150, 255)
                                        } else {
                                            egui::Color32::TRANSPARENT
                                        })
                                        .corner_radius(4.0),
                                    )
                                    .clicked()
                                {
                                    self.current_mode = WakaMode::Filer;
                                }
                            });
                        });
                    }
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            match self.current_mode {
                WakaMode::Filer => {
                    ui.label("File Explorer Mode");
                    // TODO: Add file explorer UI here
                }
                WakaMode::FullTextSearch => {
                    // ui.label("Full Text Search Mode");
                    // TODO: Add full text search UI here
                    self.fts_editor.show(ui);
                }
                WakaMode::Sql => {
                    self.sql_editor.show(ui);
                }
                WakaMode::Workflow => {
                    ui.label("Workflow Mode");
                    // TODO: Add workflow UI here
                }
            }
        });
    }
}
