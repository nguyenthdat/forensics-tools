use crate::{
    APP_ICON, APP_VERSION,
    app::{basic::BasicEditor, ftsq::FtsEditor, sqlq::SqlEditor},
};
use eframe::egui;
use egui_extras::image;
use epaint::TextureHandle;

mod basic;
mod ftsq;
mod sqlq;
mod table;

pub enum WakaMode {
    Basic,
    FullTextSearch,
    Sql,
    Workflow,
}

pub struct WakaApp {
    basic_editor: BasicEditor,
    sql_editor: SqlEditor,
    fts_editor: FtsEditor,
    current_mode: WakaMode,
    logo_tex: Option<TextureHandle>,
}

impl WakaApp {
    pub fn new() -> Self {
        WakaApp {
            basic_editor: BasicEditor::new(),
            sql_editor: SqlEditor::new(),
            current_mode: WakaMode::Basic,
            fts_editor: FtsEditor::new(),
            logo_tex: None,
        }
    }

    fn ensure_logo(&mut self, ctx: &egui::Context) {
        if self.logo_tex.is_some() {
            return;
        }

        // Decode PNG bytes into an egui::ColorImage and upload as a texture once
        let color_image = image::load_image_bytes(APP_ICON).expect("invalid PNG logo bytes");

        // Upload to a GPU texture once and keep the handle
        let tex = ctx.load_texture("waka_png_logo", color_image, egui::TextureOptions::LINEAR);
        self.logo_tex = Some(tex);
    }
}

impl eframe::App for WakaApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.ensure_logo(ctx);

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

                    // Draw the PNG logo (uploaded once)
                    let logo_response =
                        ui.allocate_response(egui::Vec2::splat(32.0), egui::Sense::hover());
                    if let Some(tex) = &self.logo_tex {
                        // Center the 32x32 image in the allocated rect
                        let rect = logo_response.rect;
                        let size = egui::vec2(32.0, 32.0);
                        let pos = egui::pos2(
                            rect.center().x - size.x * 0.5,
                            rect.center().y - size.y * 0.5,
                        );
                        ui.painter().image(
                            tex.id(),
                            egui::Rect::from_min_size(pos, size),
                            egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                            egui::Color32::WHITE,
                        );
                    } else {
                        // Fallback placeholder
                        ui.painter().circle_filled(
                            logo_response.rect.center(),
                            16.0,
                            egui::Color32::from_rgb(0, 150, 255),
                        );
                    }

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
                            self.current_mode = WakaMode::Basic;
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
                                                if matches!(self.current_mode, WakaMode::Basic) {
                                                    egui::Color32::WHITE
                                                } else {
                                                    egui::Color32::GRAY
                                                },
                                            ),
                                        )
                                        .fill(if matches!(self.current_mode, WakaMode::Basic) {
                                            egui::Color32::from_rgb(0, 150, 255)
                                        } else {
                                            egui::Color32::TRANSPARENT
                                        })
                                        .corner_radius(4.0),
                                    )
                                    .clicked()
                                {
                                    self.current_mode = WakaMode::Basic;
                                }
                            });
                        });
                    }
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            match self.current_mode {
                WakaMode::Basic => {
                    self.basic_editor.show(ui);
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
