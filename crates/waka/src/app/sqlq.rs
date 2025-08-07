use eframe::{egui, egui::Frame};
use epaint::Margin;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;

pub struct SqlEditor {
    query: String,
    result: String,
    show_result: bool,
    suggestions: Vec<String>,
    show_suggestions: bool,
    selected_suggestion: usize,
    cursor_row: usize,
    cursor_col: usize,
    sql_keywords: HashSet<&'static str>,
    sql_functions: HashSet<&'static str>,
    current_word: String,
    text_edit_rect: Option<egui::Rect>, // Store text editor position
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
            sql_keywords: Self::get_sql_keywords(),
            sql_functions: Self::get_sql_functions(),
            current_word: String::new(),
            text_edit_rect: None,
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        // Main container with dark background (smaller height)
        Frame::new()
            .fill(egui::Color32::from_rgb(30, 30, 30))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Top toolbar
                    self.show_toolbar(ui);

                    // Editor area with line numbers (smaller)
                    self.show_editor_with_line_numbers(ui);

                    // Suggestions popup (overlay) - show after editor to get correct position
                    if self.show_suggestions && !self.suggestions.is_empty() {
                        self.show_suggestions_popup(ui);
                    }
                });
            });
    }

    fn show_toolbar(&mut self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(40, 40, 40))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(8.0);

                    // File icon and name
                    ui.label("📄");
                    ui.label(
                        egui::RichText::new("console")
                            .color(egui::Color32::WHITE)
                            .size(12.0),
                    );
                    ui.label("×");

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add_space(8.0);

                        // Schema selector
                        egui::ComboBox::from_label("")
                            .selected_text("🗂️ <schema>")
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut "", "", "public");
                                ui.selectable_value(&mut "", "", "information_schema");
                            });

                        ui.separator();

                        // Control buttons
                        if ui.small_button("▶").clicked() {
                            self.execute_query();
                        }
                        if ui.small_button("⏸").clicked() {
                            // Pause execution
                        }
                        if ui.small_button("⏹").clicked() {
                            // Stop execution
                        }
                    });
                });
            });
    }

    fn show_editor_with_line_numbers(&mut self, ui: &mut egui::Ui) {
        // Make editor much smaller - only 30% of available height
        let available_height = ui.available_height() * 0.3;

        Frame::new()
            .fill(egui::Color32::from_rgb(30, 30, 30))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    // Line numbers column (smaller)
                    self.show_line_numbers(ui, available_height);

                    // Editor area
                    self.show_editor_area(ui, available_height);
                });
            });
    }

    fn show_line_numbers(&self, ui: &mut egui::Ui, height: f32) {
        let line_count = self.query.lines().count().max(1);

        Frame::new()
            .fill(egui::Color32::from_rgb(40, 40, 40))
            .show(ui, |ui| {
                ui.allocate_ui_with_layout(
                    egui::Vec2::new(40.0, height), // Smaller width
                    egui::Layout::top_down(egui::Align::RIGHT),
                    |ui| {
                        ui.add_space(4.0);
                        for line_num in 1..=line_count.max(10) {
                            // Show fewer lines
                            ui.label(
                                egui::RichText::new(format!("{:2}", line_num))
                                    .color(egui::Color32::from_rgb(120, 120, 120))
                                    .size(11.0) // Smaller font
                                    .family(egui::FontFamily::Monospace),
                            );
                        }
                    },
                );
            });
    }

    fn show_editor_area(&mut self, ui: &mut egui::Ui, height: f32) {
        Frame::new()
            .fill(egui::Color32::from_rgb(30, 30, 30))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Main text editor
                    let response = ui.add_sized(
                        [ui.available_width(), height],
                        egui::TextEdit::multiline(&mut self.query)
                            .font(egui::FontId::monospace(13.0)) // Smaller font
                            .background_color(egui::Color32::from_rgb(30, 30, 30))
                            .text_color(egui::Color32::WHITE)
                            .margin(Margin::same(6)), // Smaller margin
                    );

                    // Store the text editor's rect for cursor positioning
                    self.text_edit_rect = Some(response.rect);

                    // Update cursor position based on text content
                    if response.has_focus() {
                        self.update_cursor_position();
                    }

                    if response.changed() {
                        self.update_suggestions();
                    }

                    // Handle keyboard input for suggestions
                    ui.input(|i| {
                        if self.show_suggestions && !self.suggestions.is_empty() {
                            // Tab to accept suggestion
                            if i.key_pressed(egui::Key::Tab) {
                                if let Some(suggestion) =
                                    self.suggestions.get(self.selected_suggestion).cloned()
                                {
                                    self.apply_suggestion(&suggestion);
                                }
                            }
                            // Arrow keys to navigate suggestions
                            else if i.key_pressed(egui::Key::ArrowUp) {
                                if self.selected_suggestion > 0 {
                                    self.selected_suggestion -= 1;
                                }
                            } else if i.key_pressed(egui::Key::ArrowDown) {
                                if self.selected_suggestion < self.suggestions.len() - 1 {
                                    self.selected_suggestion += 1;
                                }
                            }
                            // Escape to hide suggestions
                            else if i.key_pressed(egui::Key::Escape) {
                                self.show_suggestions = false;
                            }
                        }
                    });

                    // Status bar (smaller)
                    self.show_status_bar(ui);
                });
            });
    }

    fn update_cursor_position(&mut self) {
        // Calculate cursor position based on text content
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
            // Font metrics for monospace font
            let font_size = 13.0;
            let line_height = font_size * 1.2; // Approximate line height
            let char_width = font_size * 0.6; // Approximate character width for monospace

            // Calculate position based on cursor row and column
            let x = rect.left() + 6.0 + (self.cursor_col as f32 - 1.0) * char_width; // 6.0 is margin
            let y = rect.top() + 6.0 + (self.cursor_row as f32 - 1.0) * line_height;

            egui::Pos2::new(x, y)
        } else {
            // Fallback to center of screen if no text edit rect
            egui::Pos2::new(400.0, 300.0)
        }
    }

    fn show_suggestions_popup(&mut self, ui: &mut egui::Ui) {
        // Position popup at typing cursor instead of mouse cursor
        let cursor_pos = self.get_cursor_screen_position();
        let popup_pos = cursor_pos + egui::Vec2::new(0.0, 20.0); // Offset below cursor

        let mut clicked_suggestion: Option<String> = None;

        egui::Area::new(egui::Id::new("sql_suggestions"))
            .fixed_pos(popup_pos)
            .show(ui.ctx(), |ui| {
                egui::Frame::popup(&ui.style())
                    .fill(egui::Color32::from_rgb(50, 50, 50))
                    .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(80, 80, 80)))
                    .show(ui, |ui| {
                        ui.vertical(|ui| {
                            ui.set_min_width(180.0); // Smaller popup

                            for (i, suggestion) in self.suggestions.iter().take(6).enumerate() {
                                // Show fewer suggestions
                                let is_selected = i == self.selected_suggestion;

                                let response = ui.selectable_label(
                                    is_selected,
                                    egui::RichText::new(suggestion)
                                        .color(if is_selected {
                                            egui::Color32::WHITE
                                        } else {
                                            egui::Color32::from_rgb(200, 200, 200)
                                        })
                                        .family(egui::FontFamily::Monospace)
                                        .size(12.0), // Smaller text
                                );

                                if response.clicked() {
                                    clicked_suggestion = Some(suggestion.clone());
                                }
                            }

                            // Show Tab hint at bottom
                            ui.separator();
                            ui.horizontal(|ui| {
                                ui.label("💡");
                                ui.label(
                                    egui::RichText::new("Press Tab to accept")
                                        .color(egui::Color32::from_rgb(150, 150, 150))
                                        .size(9.0),
                                );
                            });
                        });
                    });
            });

        if let Some(suggestion) = clicked_suggestion {
            self.apply_suggestion(&suggestion);
        }
    }

    fn show_status_bar(&self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(0, 122, 204))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(6.0);

                    // Validation status
                    match self.parse_sql(&self.query) {
                        Ok(_) if !self.query.trim().is_empty() => {
                            ui.label(
                                egui::RichText::new("✓ SQL Valid")
                                    .color(egui::Color32::WHITE)
                                    .size(10.0), // Smaller font
                            );
                        }
                        Err(_) if !self.query.trim().is_empty() => {
                            ui.label(
                                egui::RichText::new("✗ SQL Error")
                                    .color(egui::Color32::from_rgb(255, 100, 100))
                                    .size(10.0),
                            );
                        }
                        _ => {
                            ui.label(
                                egui::RichText::new("Ready")
                                    .color(egui::Color32::WHITE)
                                    .size(10.0),
                            );
                        }
                    }

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add_space(6.0);

                        // Character count
                        ui.label(
                            egui::RichText::new(format!("{} chars", self.query.len()))
                                .color(egui::Color32::WHITE)
                                .size(10.0),
                        );

                        ui.separator();

                        // Cursor position
                        ui.label(
                            egui::RichText::new(format!(
                                "Ln {}, Col {}",
                                self.cursor_row, self.cursor_col
                            ))
                            .color(egui::Color32::WHITE)
                            .size(10.0),
                        );
                    });
                });
            });
    }

    fn update_suggestions(&mut self) {
        // Get current word being typed
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

        // SQL Keywords
        for keyword in &self.sql_keywords {
            if keyword.starts_with(&prefix_upper) {
                suggestions.push(keyword.to_string());
            }
        }

        // SQL Functions
        for function in &self.sql_functions {
            if function.starts_with(&prefix_upper) {
                suggestions.push(format!("{}()", function));
            }
        }

        // Common SQL clauses
        let clauses = ["RELEASE", "INSERT", "INSERT INTO"];
        for clause in &clauses {
            if clause.starts_with(&prefix_upper) {
                suggestions.push(clause.to_string());
            }
        }

        suggestions.sort();
        suggestions.truncate(6); // Show fewer suggestions
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
    }

    fn parse_sql(&self, sql: &str) -> Result<Vec<Statement>, String> {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())
    }

    fn execute_query(&mut self) {
        // TODO: Implement actual SQL execution
        self.result = format!("Executed query:\n{}", self.query);
        self.show_result = true;
    }

    fn get_sql_keywords() -> HashSet<&'static str> {
        [
            "SELECT",
            "FROM",
            "WHERE",
            "AND",
            "OR",
            "NOT",
            "IN",
            "LIKE",
            "BETWEEN",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "ALTER",
            "DROP",
            "TABLE",
            "INDEX",
            "VIEW",
            "DATABASE",
            "SCHEMA",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "OUTER",
            "ON",
            "AS",
            "DISTINCT",
            "ORDER",
            "BY",
            "GROUP",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "IF",
            "EXISTS",
            "NULL",
            "IS",
            "ASC",
            "DESC",
            "PRIMARY",
            "KEY",
            "FOREIGN",
            "UNIQUE",
            "CHECK",
            "DEFAULT",
            "RELEASE",
        ]
        .iter()
        .cloned()
        .collect()
    }

    fn get_sql_functions() -> HashSet<&'static str> {
        [
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "CONCAT",
            "LENGTH",
            "UPPER",
            "LOWER",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "SUBSTRING",
            "REPLACE",
            "NOW",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "COALESCE",
            "ISNULL",
            "CAST",
            "ROUND",
            "ABS",
        ]
        .iter()
        .cloned()
        .collect()
    }
}
