use eframe::{egui, egui::Frame};
use epaint::{CornerRadius, Margin};
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
            sql_keywords: all_functions(),
            sql_functions: all_keywords(),
            current_word: String::new(),
            text_edit_rect: None,
            syntax_error: None,
            limit: 100,
            show_history: false,
            editor_height_ratio: 0.6,
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        // Main container with dark background
        Frame::new()
            .fill(egui::Color32::from_rgb(30, 30, 30))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Top toolbar
                    self.show_toolbar(ui);

                    // Calculate available space for resizable content
                    let available_rect = ui.available_rect_before_wrap();
                    let total_height = available_rect.height();

                    // Editor area with resizable height
                    let editor_height = total_height * self.editor_height_ratio;
                    self.show_main_editor_area(ui, editor_height);

                    // Horizontal separator/splitter
                    self.show_resize_separator(ui);

                    // Results area (remaining space)
                    if self.show_result {
                        self.show_results_area(ui);
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
            .fill(egui::Color32::from_rgb(45, 45, 45))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(8.0);

                    // Query tab
                    Frame::new()
                        .fill(egui::Color32::from_rgb(60, 60, 60))
                        .corner_radius(CornerRadius::same(4))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.add_space(8.0);
                                ui.label("📊");
                                ui.label(
                                    egui::RichText::new("Query 5")
                                        .color(egui::Color32::WHITE)
                                        .size(12.0),
                                );
                                ui.add_space(4.0);
                            });
                        });

                    ui.add_space(8.0);

                    // Run button (highlighted green)
                    let run_button = egui::Button::new(
                        egui::RichText::new("▶ Run")
                            .color(egui::Color32::WHITE)
                            .size(12.0),
                    )
                    .fill(egui::Color32::from_rgb(76, 175, 80))
                    .corner_radius(CornerRadius::same(4));

                    if ui.add(run_button).clicked() {
                        self.execute_query();
                    }

                    ui.add_space(8.0);

                    // Limit checkbox and input
                    ui.checkbox(&mut true, "");
                    ui.label(
                        egui::RichText::new("Limit")
                            .color(egui::Color32::WHITE)
                            .size(11.0),
                    );

                    ui.add(
                        egui::DragValue::new(&mut self.limit)
                            .range(1..=10000)
                            .speed(1),
                    );

                    ui.add_space(8.0);

                    // Format button
                    let format_button = egui::Button::new(
                        egui::RichText::new("Format")
                            .color(egui::Color32::WHITE)
                            .size(11.0),
                    )
                    .fill(egui::Color32::from_rgb(60, 60, 60))
                    .corner_radius(CornerRadius::same(4));

                    if ui.add(format_button).clicked() {
                        self.format_query();
                    }

                    ui.add_space(8.0);

                    // View history button
                    let history_button = egui::Button::new(
                        egui::RichText::new("📋 View history")
                            .color(egui::Color32::WHITE)
                            .size(11.0),
                    )
                    .fill(egui::Color32::from_rgb(60, 60, 60))
                    .corner_radius(CornerRadius::same(4));

                    if ui.add(history_button).clicked() {
                        self.show_history = !self.show_history;
                    }

                    // Right side - settings
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add_space(8.0);

                        let settings_button = egui::Button::new("⚙")
                            .fill(egui::Color32::from_rgb(60, 60, 60))
                            .corner_radius(CornerRadius::same(4));

                        ui.add(settings_button);
                    });
                });
            });
    }

    fn show_main_editor_area(&mut self, ui: &mut egui::Ui, height: f32) {
        Frame::new()
            .fill(egui::Color32::from_rgb(40, 40, 40))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(60, 60, 60)))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    // Line numbers
                    self.show_line_numbers(ui, height);

                    // Editor with syntax highlighting overlay
                    self.show_highlighted_editor(ui, height);
                });
            });
    }

    fn show_resize_separator(&mut self, ui: &mut egui::Ui) {
        let separator_height = 16.0; // Make it taller for easier grabbing

        // Create an interactive area that spans the full width
        let (rect, response) = ui.allocate_exact_size(
            egui::Vec2::new(ui.available_width(), separator_height),
            egui::Sense::hover().union(egui::Sense::drag()),
        );

        // Visual feedback based on interaction state
        let (bg_color, grip_color) = if response.dragged() {
            // Active drag state
            (
                egui::Color32::from_rgb(70, 120, 180), // Blue tint when dragging
                egui::Color32::from_rgb(200, 200, 200),
            )
        } else if response.hovered() {
            // Hover state
            (
                egui::Color32::from_rgb(60, 60, 60), // Lighter on hover
                egui::Color32::from_rgb(180, 180, 180),
            )
        } else {
            // Default state
            (
                egui::Color32::from_rgb(45, 45, 45),
                egui::Color32::from_rgb(120, 120, 120),
            )
        };

        // Draw background
        ui.painter().rect_filled(rect, 2.0, bg_color);

        // Draw border
        ui.painter().rect_stroke(
            rect,
            2.0,
            egui::Stroke::new(0.5, egui::Color32::from_rgb(80, 80, 80)),
            egui::StrokeKind::Inside,
        );

        // Draw grip pattern in the center
        let center = rect.center();
        let grip_width = 40.0;
        let grip_height = 2.0;
        let line_spacing = 2.0;

        // Draw three horizontal lines as grip indicator
        for i in -1..=1 {
            let y_offset = i as f32 * (grip_height + line_spacing);
            let line_rect = egui::Rect::from_center_size(
                egui::Pos2::new(center.x, center.y + y_offset),
                egui::Vec2::new(grip_width, grip_height),
            );
            ui.painter().rect_filled(line_rect, 1.0, grip_color);
        }

        // Change cursor when hovering or dragging
        if response.hovered() || response.dragged() {
            ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeVertical);
        }

        // Handle drag to resize
        if response.dragged() {
            let delta = response.drag_delta().y;

            // Get the total available height from the main show function
            // We need to be more careful about how we calculate this
            let available_height = ui.available_height()
                + (ui.available_rect_before_wrap().height() - ui.available_height());

            if available_height > 200.0 {
                // Minimum total height check
                // Convert pixel delta to ratio change
                let height_change = delta / available_height;
                let new_ratio = self.editor_height_ratio + height_change;

                // Clamp to reasonable bounds
                let min_editor_ratio = 0.2; // At least 20% for editor
                let max_editor_ratio = 0.8; // At most 80% for editor

                self.editor_height_ratio = new_ratio.clamp(min_editor_ratio, max_editor_ratio);
            }
        }

        // Show tooltip on hover
        if response.hovered() {
            egui::Tooltip::for_enabled(&response.on_hover_ui_at_pointer(|ui| {
                ui.label("Drag to resize editor and results panels");
            }));
        }
    }

    fn show_results_area(&mut self, ui: &mut egui::Ui) {
        let remaining_height = ui.available_height();

        Frame::new()
            .fill(egui::Color32::from_rgb(35, 35, 35))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(60, 60, 60)))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Results header
                    Frame::new()
                        .fill(egui::Color32::from_rgb(50, 50, 50))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.add_space(8.0);
                                ui.label(
                                    egui::RichText::new("📊 Results")
                                        .color(egui::Color32::WHITE)
                                        .size(12.0)
                                        .strong(),
                                );

                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        ui.add_space(8.0);

                                        // Close button
                                        if ui.small_button("✕").clicked() {
                                            self.show_result = false;
                                        }
                                    },
                                );
                            });
                        });

                    // Results content
                    egui::ScrollArea::vertical()
                        .max_height(remaining_height - 40.0) // Leave space for header
                        .show(ui, |ui| {
                            ui.add_space(8.0);
                            ui.horizontal(|ui| {
                                ui.add_space(8.0);
                                ui.label(
                                    egui::RichText::new(&self.result)
                                        .color(egui::Color32::WHITE)
                                        .size(11.0)
                                        .family(egui::FontFamily::Monospace),
                                );
                            });
                            ui.add_space(8.0);
                        });
                });
            });
    }

    fn show_line_numbers(&self, ui: &mut egui::Ui, height: f32) {
        let line_count = self.query.lines().count().max(1);

        Frame::new()
            .fill(egui::Color32::from_rgb(50, 50, 50))
            .show(ui, |ui| {
                ui.allocate_ui_with_layout(
                    egui::Vec2::new(50.0, height),
                    egui::Layout::top_down(egui::Align::RIGHT),
                    |ui| {
                        ui.add_space(8.0);
                        for line_num in 1..=line_count.max(20) {
                            ui.label(
                                egui::RichText::new(format!("{:2}", line_num))
                                    .color(egui::Color32::from_rgb(130, 130, 130))
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
            .fill(egui::Color32::from_rgb(30, 30, 30))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Create a layered approach for syntax highlighting
                    let text_edit_id = egui::Id::new("sql_editor");

                    // First layer: Regular text editor (invisible text)
                    let response = ui.add_sized(
                        [ui.available_width(), height - 30.0], // Leave space for status
                        egui::TextEdit::multiline(&mut self.query)
                            .font(egui::FontId::monospace(14.0))
                            .background_color(egui::Color32::TRANSPARENT)
                            .text_color(egui::Color32::TRANSPARENT) // Make text invisible
                            .margin(Margin::same(8))
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

                    // Status bar at bottom
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
            .fill(egui::Color32::from_rgb(50, 50, 50))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(8.0);

                    // Ready status
                    ui.label("🟢");
                    ui.label(
                        egui::RichText::new("Ready")
                            .color(egui::Color32::WHITE)
                            .size(11.0),
                    );

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add_space(8.0);

                        // Position info
                        ui.label(
                            egui::RichText::new(format!(
                                "Ln {}, Col {}",
                                self.cursor_row, self.cursor_col
                            ))
                            .color(egui::Color32::from_rgb(170, 170, 170))
                            .size(10.0),
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
