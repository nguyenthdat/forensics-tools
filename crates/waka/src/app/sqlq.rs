use eframe::{egui, egui::Frame};
use epaint::{CornerRadius, Margin, Stroke, StrokeKind};
use polars_sql::keywords::{all_functions, all_keywords};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct SqlEditor {
    query: String,
    result: String,
    show_result: bool,
    suggestions: Vec<String>,
    show_suggestions: bool,
    selected_suggestion: usize,
    cursor_row: usize,
    cursor_col: usize,
    sql_keywords: Vec<&'static str>,
    sql_functions: Vec<&'static str>,
    current_word: String,
    text_edit_rect: Option<egui::Rect>,
    syntax_error: Option<String>,
    limit: i32,
    show_history: bool,
    editor_height_ratio: f32,
    separator_hover: bool,
    separator_drag: bool,
}

impl SqlEditor {
    pub fn new() -> Self {
        Self {
            query: String::new(),
            result: String::new(),
            show_result: false,
            suggestions: Vec::new(),
            show_suggestions: false,
            selected_suggestion: 0,
            cursor_row: 1,
            cursor_col: 1,
            sql_keywords: all_keywords(),
            sql_functions: all_functions(),
            current_word: String::new(),
            text_edit_rect: None,
            syntax_error: None,
            limit: 100,
            show_history: false,
            editor_height_ratio: 0.6,
            separator_hover: false,
            separator_drag: false,
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        // Main container with improved dark theme
        Frame::new()
            .fill(egui::Color32::from_rgb(24, 24, 27))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(45, 45, 48)))
            .corner_radius(CornerRadius::same(8))
            .inner_margin(Margin::same(4))
            .show(ui, |ui| {
                ui.vertical_centered_justified(|ui| {
                    // Top toolbar with better styling
                    self.show_toolbar(ui);

                    ui.add_space(2.0);

                    // Calculate available space for resizable content
                    let available_rect = ui.available_rect_before_wrap();
                    let total_height = available_rect.height() - 60.0; // Reserve space for toolbar and separator

                    // Editor area with resizable height
                    let editor_height = total_height * self.editor_height_ratio;
                    self.show_main_editor_area(ui, editor_height);

                    // Improved horizontal separator/splitter
                    self.show_resize_separator(ui);

                    // Results area (remaining space)
                    if self.show_result {
                        let remaining_height =
                            total_height * (1.0 - self.editor_height_ratio) - 20.0;
                        self.show_results_area(ui, remaining_height);
                    }

                    // Show syntax error if any
                    self.show_syntax_error(ui);

                    // Suggestions popup (overlay)
                    if self.show_suggestions && !self.suggestions.is_empty() {
                        self.show_suggestions_popup(ui);
                    }
                });
            });
    }

    fn show_toolbar(&mut self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(39, 39, 42))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(60, 60, 63)))
            .corner_radius(CornerRadius::same(6))
            .inner_margin(Margin::same(4))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(12.0);

                    // Query tab with improved styling
                    Frame::new()
                        .fill(egui::Color32::from_rgb(55, 55, 58))
                        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(75, 75, 78)))
                        .corner_radius(CornerRadius::same(6))
                        .inner_margin(Margin::same(6))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(egui::RichText::new("📊").size(14.0));
                                ui.add_space(4.0);
                                ui.label(
                                    egui::RichText::new("Query Editor")
                                        .color(egui::Color32::from_rgb(229, 229, 232))
                                        .size(13.0)
                                        .strong(),
                                );
                            });
                        });

                    ui.add_space(12.0);

                    // Enhanced Run button with gradient effect
                    let run_button = egui::Button::new(
                        egui::RichText::new("▶ Execute")
                            .color(egui::Color32::WHITE)
                            .size(13.0)
                            .strong(),
                    )
                    .fill(egui::Color32::from_rgb(34, 197, 94))
                    .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(22, 163, 74)))
                    .corner_radius(CornerRadius::same(6));

                    if ui.add_sized([80.0, 32.0], run_button).clicked() {
                        self.execute_query();
                    }

                    ui.add_space(16.0);

                    // Improved limit controls
                    ui.vertical_centered(|ui| {
                        ui.horizontal(|ui| {
                            ui.checkbox(&mut true, "");
                            ui.label(
                                egui::RichText::new("Row Limit")
                                    .color(egui::Color32::from_rgb(161, 161, 170))
                                    .size(12.0),
                            );
                        });
                    });

                    ui.add_sized(
                        [60.0, 24.0],
                        egui::DragValue::new(&mut self.limit)
                            .range(1..=10000)
                            .speed(10),
                    );

                    ui.add_space(16.0);

                    // Enhanced Format button
                    let format_button = egui::Button::new(
                        egui::RichText::new("🎨 Format")
                            .color(egui::Color32::from_rgb(229, 229, 232))
                            .size(12.0),
                    )
                    .fill(egui::Color32::from_rgb(55, 55, 58))
                    .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(75, 75, 78)))
                    .corner_radius(CornerRadius::same(6));

                    if ui.add_sized([80.0, 28.0], format_button).clicked() {
                        self.format_query();
                    }

                    ui.add_space(12.0);

                    // Enhanced history button
                    let history_button = egui::Button::new(
                        egui::RichText::new("📋 History")
                            .color(egui::Color32::from_rgb(229, 229, 232))
                            .size(12.0),
                    )
                    .fill(egui::Color32::from_rgb(55, 55, 58))
                    .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(75, 75, 78)))
                    .corner_radius(CornerRadius::same(6));

                    if ui.add_sized([80.0, 28.0], history_button).clicked() {
                        self.show_history = !self.show_history;
                    }

                    // Right side - enhanced settings
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add_space(12.0);

                        let settings_button =
                            egui::Button::new(egui::RichText::new("⚙️").size(16.0))
                                .fill(egui::Color32::from_rgb(55, 55, 58))
                                .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(75, 75, 78)))
                                .corner_radius(CornerRadius::same(6));

                        ui.add_sized([32.0, 32.0], settings_button);
                    });
                });
            });
    }

    fn show_main_editor_area(&mut self, ui: &mut egui::Ui, height: f32) {
        Frame::new()
            .fill(egui::Color32::from_rgb(30, 30, 33))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(60, 60, 63)))
            .corner_radius(CornerRadius::same(8))
            .inner_margin(Margin::same(2))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    // Enhanced line numbers with better contrast
                    self.show_line_numbers(ui, height);

                    // Vertical separator between line numbers and editor
                    ui.separator();

                    // Editor with improved syntax highlighting
                    self.show_highlighted_editor(ui, height);
                });
            });
    }

    fn show_resize_separator(&mut self, ui: &mut egui::Ui) {
        // Create an improved draggable horizontal separator
        let separator_height = 16.0;
        let available_width = ui.available_width();

        let (rect, response) = ui.allocate_exact_size(
            egui::Vec2::new(available_width, separator_height),
            egui::Sense::hover() | egui::Sense::drag(),
        );

        // Update hover and drag states
        self.separator_hover = response.hovered();
        self.separator_drag = response.dragged();

        // Choose colors based on state
        let (bg_color, accent_color, handle_color) = if self.separator_drag {
            (
                egui::Color32::from_rgb(70, 130, 180), // Bright blue when dragging
                egui::Color32::from_rgb(100, 150, 200),
                egui::Color32::WHITE,
            )
        } else if self.separator_hover {
            (
                egui::Color32::from_rgb(55, 55, 58), // Lighter when hovering
                egui::Color32::from_rgb(75, 75, 78),
                egui::Color32::from_rgb(200, 200, 200),
            )
        } else {
            (
                egui::Color32::from_rgb(45, 45, 48), // Default dark
                egui::Color32::from_rgb(60, 60, 63),
                egui::Color32::from_rgb(120, 120, 123),
            )
        };

        // Draw the separator background
        ui.painter()
            .rect_filled(rect, CornerRadius::same(4), bg_color);

        // Draw border
        ui.painter().rect_stroke(
            rect,
            CornerRadius::same(4),
            Stroke::new(1.0, accent_color),
            StrokeKind::Inside,
        );

        // Draw resize handle in the center
        let handle_rect = egui::Rect::from_center_size(rect.center(), egui::Vec2::new(60.0, 4.0));

        ui.painter()
            .rect_filled(handle_rect, CornerRadius::same(2), handle_color);

        // Add grip dots for better visual indication
        let dot_size = 2.0;
        let dot_spacing = 6.0;
        let dots_start_x = rect.center().x - (2.0 * dot_spacing);

        for i in 0..5 {
            let dot_x = dots_start_x + (i as f32 * dot_spacing);
            let dot_center = egui::Pos2::new(dot_x, rect.center().y);

            ui.painter().circle_filled(
                dot_center,
                dot_size,
                if self.separator_hover || self.separator_drag {
                    egui::Color32::from_rgb(180, 180, 180)
                } else {
                    egui::Color32::from_rgb(100, 100, 103)
                },
            );
        }

        // Change cursor when hovering over separator
        if response.hovered() {
            ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeVertical);
        }

        // Handle drag to resize with smooth animation
        if response.dragged() {
            let delta = response.drag_delta().y;
            let total_height = ui.available_rect_before_wrap().height();

            if total_height > 0.0 {
                let height_change = delta / total_height;
                self.editor_height_ratio += height_change;

                // Clamp the ratio to reasonable bounds with smooth limits
                self.editor_height_ratio = self.editor_height_ratio.clamp(0.15, 0.85);
            }
        }

        // Add helpful tooltip
        if response.hovered() {
            response.on_hover_text("Drag to resize editor and results panels");
        }
    }

    fn show_results_area(&mut self, ui: &mut egui::Ui, height: f32) {
        Frame::new()
            .fill(egui::Color32::from_rgb(27, 27, 30))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(60, 60, 63)))
            .corner_radius(CornerRadius::same(8))
            .inner_margin(Margin::same(2))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Enhanced results header
                    Frame::new()
                        .fill(egui::Color32::from_rgb(39, 39, 42))
                        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(60, 60, 63)))
                        .corner_radius(CornerRadius::same(6))
                        .inner_margin(Margin::same(4))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.add_space(12.0);
                                ui.label(egui::RichText::new("📊").size(14.0));
                                ui.add_space(4.0);
                                ui.label(
                                    egui::RichText::new("Query Results")
                                        .color(egui::Color32::from_rgb(229, 229, 232))
                                        .size(13.0)
                                        .strong(),
                                );

                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        ui.add_space(12.0);

                                        // Enhanced close button
                                        let close_button = egui::Button::new(
                                            egui::RichText::new("✕")
                                                .color(egui::Color32::WHITE)
                                                .size(12.0),
                                        )
                                        .fill(egui::Color32::from_rgb(239, 68, 68))
                                        .stroke(egui::Stroke::new(
                                            1.0,
                                            egui::Color32::from_rgb(220, 38, 38),
                                        ))
                                        .corner_radius(CornerRadius::same(4));

                                        if ui.add_sized([24.0, 24.0], close_button).clicked() {
                                            self.show_result = false;
                                        }
                                    },
                                );
                            });
                        });

                    ui.add_space(4.0);

                    // Enhanced results content with better scrolling
                    egui::ScrollArea::vertical()
                        .max_height(height - 60.0) // Leave space for header
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            ui.add_space(12.0);
                            Frame::new()
                                .fill(egui::Color32::from_rgb(24, 24, 27))
                                .corner_radius(CornerRadius::same(4))
                                .inner_margin(Margin::same(8))
                                .show(ui, |ui| {
                                    ui.label(
                                        egui::RichText::new(&self.result)
                                            .color(egui::Color32::from_rgb(229, 229, 232))
                                            .size(12.0)
                                            .family(egui::FontFamily::Monospace),
                                    );
                                });
                            ui.add_space(12.0);
                        });
                });
            });
    }

    fn show_line_numbers(&self, ui: &mut egui::Ui, height: f32) {
        let line_count = self.query.lines().count().max(1);

        Frame::new()
            .fill(egui::Color32::from_rgb(39, 39, 42))
            .show(ui, |ui| {
                ui.allocate_ui_with_layout(
                    egui::Vec2::new(60.0, height),
                    egui::Layout::top_down(egui::Align::RIGHT),
                    |ui| {
                        ui.add_space(12.0);
                        for line_num in 1..=line_count.max(20) {
                            let is_current = line_num == self.cursor_row;

                            ui.label(
                                egui::RichText::new(format!("{:3}", line_num))
                                    .color(if is_current {
                                        egui::Color32::from_rgb(229, 229, 232)
                                    } else {
                                        egui::Color32::from_rgb(113, 113, 122)
                                    })
                                    .size(12.0)
                                    .family(egui::FontFamily::Monospace),
                            );
                        }
                    },
                );
            });
    }

    fn show_highlighted_editor(&mut self, ui: &mut egui::Ui, height: f32) {
        Frame::new()
            .fill(egui::Color32::from_rgb(24, 24, 27))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Create a layered approach for syntax highlighting
                    let text_edit_id = egui::Id::new("sql_editor");

                    // First layer: Regular text editor (invisible text)
                    let response = ui.add_sized(
                        [ui.available_width(), height - 40.0], // Leave space for status
                        egui::TextEdit::multiline(&mut self.query)
                            .font(egui::FontId::monospace(14.0))
                            .background_color(egui::Color32::TRANSPARENT)
                            .text_color(egui::Color32::TRANSPARENT) // Make text invisible
                            .margin(Margin::same(12))
                            .id(text_edit_id),
                    );

                    // Store the text editor's rect for cursor positioning
                    self.text_edit_rect = Some(response.rect);

                    // Second layer: Render highlighted text overlay
                    let painter = ui.painter();
                    self.paint_syntax_highlighted_text(&painter, response.rect);

                    // Handle interactions
                    if response.has_focus() {
                        self.update_cursor_position();
                    }

                    if response.changed() {
                        self.update_suggestions();
                        self.validate_syntax();
                    }

                    // Handle keyboard input for suggestions
                    ui.input(|i| {
                        if self.show_suggestions && !self.suggestions.is_empty() {
                            if i.key_pressed(egui::Key::Tab) {
                                if let Some(suggestion) =
                                    self.suggestions.get(self.selected_suggestion).cloned()
                                {
                                    self.apply_suggestion(&suggestion);
                                }
                            } else if i.key_pressed(egui::Key::ArrowUp) {
                                if self.selected_suggestion > 0 {
                                    self.selected_suggestion -= 1;
                                }
                            } else if i.key_pressed(egui::Key::ArrowDown) {
                                if self.selected_suggestion < self.suggestions.len() - 1 {
                                    self.selected_suggestion += 1;
                                }
                            } else if i.key_pressed(egui::Key::Escape) {
                                self.show_suggestions = false;
                            }
                        }
                    });

                    // Enhanced status bar at bottom
                    self.show_editor_status(ui);
                });
            });
    }

    fn paint_syntax_highlighted_text(&self, painter: &egui::Painter, rect: egui::Rect) {
        if self.query.is_empty() {
            return;
        }

        let font_id = egui::FontId::monospace(14.0);
        let line_height = 18.0;
        let char_width = 8.4;

        let lines: Vec<&str> = self.query.lines().collect();

        for (line_idx, line) in lines.iter().enumerate() {
            let y_pos = rect.top() + 8.0 + (line_idx as f32 * line_height);
            let mut x_pos = rect.left() + 8.0;

            // Tokenize this line
            let words: Vec<&str> = line.split_whitespace().collect();
            let mut char_offset = 0;

            for word in words {
                // Find the actual position of this word in the line
                if let Some(word_start) = line[char_offset..].find(word) {
                    char_offset += word_start;
                    x_pos = rect.left() + 8.0 + (char_offset as f32 * char_width);
                }

                let color = self.get_token_color(word);

                painter.text(
                    egui::Pos2::new(x_pos, y_pos),
                    egui::Align2::LEFT_TOP,
                    word,
                    font_id.clone(),
                    color,
                );

                char_offset += word.len();
                // Skip to next word position (including spaces)
                while char_offset < line.len() && line.chars().nth(char_offset) == Some(' ') {
                    char_offset += 1;
                }
            }
        }
    }

    fn get_token_color(&self, word: &str) -> egui::Color32 {
        let clean_word = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
        let upper_word = clean_word.to_uppercase();

        if self.sql_keywords.contains(&upper_word.as_str()) {
            egui::Color32::from_rgb(86, 156, 214) // Blue for keywords
        } else if self.sql_functions.contains(&upper_word.as_str()) {
            egui::Color32::from_rgb(220, 220, 170) // Yellow for functions
        } else if word.starts_with("'") && word.ends_with("'") {
            egui::Color32::from_rgb(206, 145, 120) // Orange for strings
        } else if word.starts_with('"') && word.ends_with('"') {
            egui::Color32::from_rgb(206, 145, 120) // Orange for strings
        } else if word.chars().all(|c| c.is_numeric() || c == '.' || c == '-') && !word.is_empty() {
            egui::Color32::from_rgb(181, 206, 168) // Green for numbers
        } else if word.starts_with("--") || word.starts_with("/*") {
            egui::Color32::from_rgb(106, 153, 85) // Green for comments
        } else if "=<>!+-*/%()[]{},.;".contains(clean_word) {
            egui::Color32::from_rgb(212, 212, 212) // Light gray for operators
        } else {
            egui::Color32::WHITE // White for regular text
        }
    }

    fn show_editor_status(&self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(39, 39, 42))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(60, 60, 63)))
            .corner_radius(CornerRadius::same(4))
            .inner_margin(Margin::same(2))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(12.0);

                    // Enhanced status indicator
                    let status_color = if self.syntax_error.is_some() {
                        egui::Color32::from_rgb(239, 68, 68) // Red for errors
                    } else {
                        egui::Color32::from_rgb(34, 197, 94) // Green for ready
                    };

                    ui.painter().circle_filled(
                        ui.next_widget_position() + egui::Vec2::new(6.0, 8.0),
                        4.0,
                        status_color,
                    );

                    ui.add_space(16.0);

                    let status_text = if self.syntax_error.is_some() {
                        "Syntax Error"
                    } else {
                        "Ready"
                    };

                    ui.label(
                        egui::RichText::new(status_text)
                            .color(egui::Color32::from_rgb(229, 229, 232))
                            .size(11.0)
                            .strong(),
                    );

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add_space(12.0);

                        // Enhanced position info with better formatting
                        ui.label(
                            egui::RichText::new(format!(
                                "Line {}, Column {}",
                                self.cursor_row, self.cursor_col
                            ))
                            .color(egui::Color32::from_rgb(161, 161, 170))
                            .size(10.0)
                            .family(egui::FontFamily::Monospace),
                        );
                    });
                });
            });
    }

    fn show_syntax_error(&self, ui: &mut egui::Ui) {
        if let Some(error) = &self.syntax_error {
            Frame::new()
                .fill(egui::Color32::from_rgb(80, 40, 40))
                .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(220, 80, 80)))
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.add_space(8.0);
                        ui.label("❌");
                        ui.vertical(|ui| {
                            ui.label(
                                egui::RichText::new("Syntax Error:")
                                    .color(egui::Color32::from_rgb(220, 80, 80))
                                    .size(12.0)
                                    .strong(),
                            );
                            ui.label(
                                egui::RichText::new(error)
                                    .color(egui::Color32::from_rgb(255, 200, 200))
                                    .size(11.0)
                                    .family(egui::FontFamily::Monospace),
                            );
                        });
                    });
                });
        }
    }

    fn format_query(&mut self) {
        // Simple SQL formatting
        self.query = self
            .query
            .replace(" SELECT ", "\nSELECT ")
            .replace(" FROM ", "\nFROM ")
            .replace(" WHERE ", "\nWHERE ")
            .replace(" AND ", "\n  AND ")
            .replace(" OR ", "\n  OR ")
            .replace(" ORDER BY ", "\nORDER BY ")
            .replace(" GROUP BY ", "\nGROUP BY ")
            .replace(" HAVING ", "\nHAVING ");
    }

    fn validate_syntax(&mut self) {
        if self.query.trim().is_empty() {
            self.syntax_error = None;
            return;
        }

        match self.parse_sql(&self.query) {
            Ok(_) => self.syntax_error = None,
            Err(error) => {
                let clean_error = error
                    .replace("sql parser error: ", "")
                    .chars()
                    .take(100)
                    .collect::<String>();
                self.syntax_error = Some(clean_error);
            }
        }
    }

    fn update_cursor_position(&mut self) {
        let lines: Vec<&str> = self.query.lines().collect();
        self.cursor_row = lines.len().max(1);

        if let Some(last_line) = lines.last() {
            self.cursor_col = last_line.len() + 1;
        } else {
            self.cursor_col = 1;
        }
    }

    fn get_cursor_screen_position(&self) -> egui::Pos2 {
        if let Some(rect) = self.text_edit_rect {
            let font_size = 14.0;
            let line_height = font_size * 1.3;
            let char_width = font_size * 0.6;

            let x = rect.left() + 8.0 + (self.cursor_col as f32 - 1.0) * char_width;
            let y = rect.top() + 8.0 + (self.cursor_row as f32 - 1.0) * line_height;

            egui::Pos2::new(x, y)
        } else {
            egui::Pos2::new(400.0, 300.0)
        }
    }

    fn show_suggestions_popup(&mut self, ui: &mut egui::Ui) {
        let cursor_pos = self.get_cursor_screen_position();
        let popup_pos = cursor_pos + egui::Vec2::new(0.0, 20.0);

        let mut clicked_suggestion: Option<String> = None;

        egui::Area::new(egui::Id::new("sql_suggestions"))
            .fixed_pos(popup_pos)
            .show(ui.ctx(), |ui| {
                egui::Frame::popup(&ui.style())
                    .fill(egui::Color32::from_rgb(60, 60, 60))
                    .stroke(egui::Stroke::new(
                        1.0,
                        egui::Color32::from_rgb(100, 100, 100),
                    ))
                    .show(ui, |ui| {
                        ui.vertical(|ui| {
                            ui.set_min_width(200.0);

                            for (i, suggestion) in self.suggestions.iter().take(8).enumerate() {
                                let is_selected = i == self.selected_suggestion;

                                let response = ui.selectable_label(
                                    is_selected,
                                    egui::RichText::new(suggestion)
                                        .color(if is_selected {
                                            egui::Color32::WHITE
                                        } else {
                                            egui::Color32::from_rgb(220, 220, 220)
                                        })
                                        .family(egui::FontFamily::Monospace)
                                        .size(13.0),
                                );

                                if response.clicked() {
                                    clicked_suggestion = Some(suggestion.clone());
                                }
                            }

                            ui.separator();
                            ui.horizontal(|ui| {
                                ui.label("💡");
                                ui.label(
                                    egui::RichText::new("Press Tab to accept")
                                        .color(egui::Color32::from_rgb(170, 170, 170))
                                        .size(10.0),
                                );
                            });
                        });
                    });
            });

        if let Some(suggestion) = clicked_suggestion {
            self.apply_suggestion(&suggestion);
        }
    }

    fn update_suggestions(&mut self) {
        self.current_word = self.get_current_word();

        if self.current_word.len() >= 2 {
            self.suggestions = self.get_suggestions(&self.current_word);
            self.show_suggestions = !self.suggestions.is_empty();
            self.selected_suggestion = 0;
        } else {
            self.show_suggestions = false;
        }
    }

    fn get_current_word(&self) -> String {
        let words: Vec<&str> = self.query.split_whitespace().collect();
        words.last().map_or("", |v| v).to_uppercase()
    }

    fn get_suggestions(&self, prefix: &str) -> Vec<String> {
        let mut suggestions = Vec::new();
        let prefix_upper = prefix.to_uppercase();

        for keyword in &self.sql_keywords {
            if keyword.starts_with(&prefix_upper) {
                suggestions.push(keyword.to_string());
            }
        }

        for function in &self.sql_functions {
            if function.starts_with(&prefix_upper) {
                suggestions.push(format!("{}()", function));
            }
        }

        suggestions.sort();
        suggestions.truncate(8);
        suggestions
    }

    fn apply_suggestion(&mut self, suggestion: &str) {
        let words: Vec<&str> = self.query.split_whitespace().collect();
        if !words.is_empty() {
            let mut new_words = words[..words.len() - 1].to_vec();
            new_words.push(suggestion);
            self.query = new_words.join(" ") + " ";
        } else {
            self.query = format!("{} ", suggestion);
        }
        self.show_suggestions = false;
        self.validate_syntax();
    }

    fn parse_sql(&self, sql: &str) -> Result<Vec<Statement>, String> {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())
    }

    fn execute_query(&mut self) {
        if let Some(error) = &self.syntax_error {
            self.result = format!("❌ Cannot execute query with syntax error:\n{}", error);
        } else {
            self.result = format!("✅ Query executed successfully:\n{}", self.query);
        }
        self.show_result = true;
    }
}

#[derive(Debug, Clone)]
struct SqlToken {
    text: String,
    token_type: TokenType,
}

#[derive(Debug, Clone)]
enum TokenType {
    Keyword,
    Function,
    String,
    Number,
    Comment,
    Operator,
    Error,
    Regular,
}
