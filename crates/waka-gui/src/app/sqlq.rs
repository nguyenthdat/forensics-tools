use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use eframe::{egui, egui::Frame};
use epaint::{CornerRadius, Margin};
use polars_sql::keywords::{all_functions, all_keywords};
use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

use crate::{app::basic::BasicEditor, util};

pub struct SqlEditor {
    query:               String,
    result:              String,
    show_result:         bool,
    suggestions:         Vec<String>,
    show_suggestions:    bool,
    selected_suggestion: usize,
    cursor_row:          usize,
    cursor_col:          usize,
    sql_keywords:        Vec<&'static str>,
    sql_functions:       Vec<&'static str>,
    current_word:        String,
    text_edit_rect:      Option<egui::Rect>,
    syntax_error:        Option<String>,
    limit:               i32,
    editor_height_ratio: f32,

    error_line:   Option<usize>,
    error_column: Option<usize>,
    error_length: Option<usize>,

    execution_time: String,
    row_count:      usize,
    basic_editor:   BasicEditor,
}

impl SqlEditor {
    fn execute_query(&mut self) {
        // Require at least one file to be present as a table
        if self.basic_editor.table.files.is_empty() {
            self.syntax_error = Some("Load at least one CSV file to query.".to_string());
            self.show_result = false;
            return;
        }

        let inputs: Vec<PathBuf> = self
            .basic_editor
            .table
            .files
            .iter()
            .map(|fp| PathBuf::from(fp.file_path.as_str()))
            .collect();

        // Write result to a temp CSV and then preview it using the existing table UI
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let out_path = std::env::temp_dir().join(format!("qsv_sqlp_result_{}.csv", millis));

        let lib_args = util::SqlpLibArgs {
            inputs,
            sql: self.query.clone(),
            format: "csv".to_string(),
            delimiter: b',',
            try_parsedates: false,
            infer_len: self.limit as usize,
            cache_schema: false,
            decimal_comma: false,
            datetime_format: None,
            date_format: None,
            time_format: None,
            float_precision: None,
            compression: "zstd".to_string(),
            compress_level: None,
            statistics: false,
            output_path: Some(out_path.clone()),
            quiet: true,
        };

        match util::run_sqlp(lib_args) {
            Ok(res) => {
                self.execution_time = format!("{}ms", res.elapsed_ms);
                self.row_count = res.rows;
                self.show_result = true;
                self.syntax_error = None;
                // Load the produced CSV into the results table
                self.basic_editor.table.load_preview(out_path);
            },
            Err(e) => {
                self.show_result = false;
                self.syntax_error = Some(format!("SQL execution failed: {}", e));
            },
        }
    }

    pub fn new() -> Self {
        Self {
            query:               "SELECT * FROM data\nLIMIT 1000".to_string(),
            result:              String::new(),
            show_result:         true,
            suggestions:         Vec::new(),
            show_suggestions:    false,
            selected_suggestion: 0,
            cursor_row:          1,
            cursor_col:          1,
            sql_keywords:        all_keywords(),
            sql_functions:       all_functions(),
            current_word:        String::new(),
            text_edit_rect:      None,
            syntax_error:        None,
            error_line:          None,
            error_column:        None,
            error_length:        None,
            limit:               1000,
            editor_height_ratio: 0.35,

            execution_time: "69ms".to_string(),
            row_count:      1000,
            basic_editor:   BasicEditor::new(),
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        // Main container with VS Code dark theme
        self.basic_editor.table.handle_file_drop(ui.ctx());

        Frame::new()
            .fill(egui::Color32::from_rgb(37, 37, 38)) // VS Code background
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Header section
                    self.show_header(ui);

                    // SQL Editor area
                    self.show_sql_editor_section(ui);

                    // Query controls
                    self.show_query_controls(ui);

                    // Execution status
                    self.show_execution_status(ui);

                    // Results area
                    if self.show_result {
                        self.show_results_section(ui);
                    }

                    // Suggestions popup (overlay)
                    if self.show_suggestions && !self.suggestions.is_empty() {
                        self.show_suggestions_popup(ui);
                    }
                });
            });
    }

    fn show_header(&mut self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(45, 45, 45))
            .inner_margin(Margin::symmetric(0, 12))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Top section with title and close button
                    ui.horizontal(|ui| {
                        ui.add_space(16.0);

                        // Collapsible arrow and title
                        ui.label(
                            egui::RichText::new("▼ Run a Polars SQL query")
                                .color(egui::Color32::WHITE)
                                .size(14.0)
                                .strong(),
                        );

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.add_space(16.0);
                            if ui.button("✕").clicked() {
                                // Handle close
                            }
                        });
                    });

                    ui.add_space(8.0);

                    // Instructions with bullet points
                    let instructions = [
                        "Run a Polars SQL query on your data using qsv sqlp.",
                        "Refer to your file as a table named <name of your CSV file>.",
                        "Save SQL query output to a file using qsv sqlp or qsv to or to the \
                         clipboard using qsv clipboard.",
                        "Important note: Decimal values may be truncated and very large SQL query \
                         outputs can cause issues.",
                    ];

                    for instruction in instructions {
                        ui.horizontal(|ui| {
                            ui.add_space(16.0);
                            ui.label(
                                egui::RichText::new(format!("• {}", instruction))
                                    .color(egui::Color32::from_rgb(200, 200, 200))
                                    .size(12.0),
                            );
                        });
                    }

                    ui.add_space(12.0);

                    // Query input label
                    ui.horizontal(|ui| {
                        ui.add_space(16.0);
                        ui.label(
                            egui::RichText::new("Enter your Polars SQL query:")
                                .color(egui::Color32::WHITE)
                                .size(13.0)
                                .strong(),
                        );
                    });
                });
            });
    }

    fn show_sql_editor_section(&mut self, ui: &mut egui::Ui) {
        let available_height = ui.available_height();
        let editor_height = available_height * self.editor_height_ratio;

        Frame::new()
            .fill(egui::Color32::from_rgb(30, 30, 30))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(70, 70, 70)))
            .corner_radius(CornerRadius::same(4))
            .inner_margin(Margin::symmetric(16, 8))
            .show(ui, |ui| {
                // Editor with syntax highlighting (no line numbers)
                self.show_highlighted_editor(ui, editor_height - 16.0);
            });
    }

    fn show_query_controls(&mut self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(40, 40, 40))
            .inner_margin(Margin::symmetric(16, 8))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    // Run SQL query button
                    let run_button = egui::Button::new(
                        egui::RichText::new("🛠 Run SQL query")
                            .color(egui::Color32::WHITE)
                            .size(13.0),
                    )
                    .fill(egui::Color32::from_rgb(0, 120, 215)) // VS Code blue
                    .corner_radius(CornerRadius::same(4));

                    if ui.add(run_button).clicked() {
                        self.execute_query();
                    }

                    ui.add_space(16.0);

                    // Decrease/Increase code size buttons
                    if ui
                        .button("🔍-")
                        .on_hover_text("Decrease code size")
                        .clicked()
                    {
                        // Font size decrease logic will be added later
                    }

                    if ui
                        .button("🔍+")
                        .on_hover_text("Increase code size")
                        .clicked()
                    {
                        // Font size increase logic will be added later
                    }

                    ui.add_space(16.0);

                    // Inference length
                    ui.label(
                        egui::RichText::new("Inference length:")
                            .color(egui::Color32::WHITE)
                            .size(12.0),
                    );

                    ui.add(
                        egui::DragValue::new(&mut self.limit)
                            .range(1..=10000)
                            .speed(10),
                    );
                });
            });
    }

    fn show_execution_status(&mut self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(50, 50, 50))
            .inner_margin(Margin::symmetric(16, 4))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(format!(
                            "Recent Polars SQL query's estimated elapsed time: {} | Row count: {}",
                            self.execution_time, self.row_count
                        ))
                        .color(egui::Color32::from_rgb(180, 180, 180))
                        .size(11.0),
                    );
                });
            });
    }

    fn show_results_section(&mut self, ui: &mut egui::Ui) {
        // Reuse BasicEditor's results section UI
        self.basic_editor.show_results_section(ui);
    }

    fn show_highlighted_editor(&mut self, ui: &mut egui::Ui, height: f32) {
        Frame::new()
            .fill(egui::Color32::from_rgb(30, 30, 30))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    let text_edit_id = egui::Id::new("sql_editor");

                    // Get the available area for the editor
                    let available_rect = ui.available_rect_before_wrap();
                    let editor_rect = egui::Rect::from_min_size(
                        available_rect.min,
                        egui::Vec2::new(available_rect.width(), height - 30.0),
                    );

                    // First, paint the syntax highlighted text as background
                    self.paint_syntax_highlighted_text(ui.painter(), editor_rect);

                    // Then overlay the transparent text editor for input handling
                    let response = ui
                        .scope_builder(egui::UiBuilder::new().max_rect(editor_rect), |ui| {
                            ui.add_sized(
                                [editor_rect.width() - 16.0, editor_rect.height() - 16.0],
                                egui::TextEdit::multiline(&mut self.query)
                                    .font(egui::FontId::monospace(13.0))
                                    .background_color(egui::Color32::TRANSPARENT)
                                    .text_color(egui::Color32::TRANSPARENT) // Keep text invisible
                                    .margin(Margin::same(8))
                                    .id(text_edit_id),
                            )
                        })
                        .inner;

                    // Store the text editor's rect for cursor positioning
                    self.text_edit_rect = Some(response.rect);

                    // Check for hover over error area and show tooltip
                    self.handle_error_hover(ui, &response);

                    // Handle interactions
                    if response.has_focus() {
                        self.update_cursor_position();
                    }

                    if response.changed() {
                        self.update_suggestions(ui);
                        self.validate_syntax();
                    }

                    // Handle keyboard input for suggestions
                    ui.input(|i| {
                        if self.show_suggestions && !self.suggestions.is_empty() {
                            let ctrl = i.modifiers.ctrl || i.modifiers.command; // Windows/Linux or macOS
                            if ctrl && i.key_pressed(egui::Key::J) {
                                if self.selected_suggestion + 1 < self.suggestions.len() {
                                    self.selected_suggestion += 1;
                                }
                            } else if ctrl && i.key_pressed(egui::Key::K) {
                                if self.selected_suggestion > 0 {
                                    self.selected_suggestion -= 1;
                                }
                            } else if i.key_pressed(egui::Key::Escape) {
                                self.show_suggestions = false;
                            } else if ctrl && i.key_pressed(egui::Key::Enter) {
                                if let Some(suggestion) =
                                    self.suggestions.get(self.selected_suggestion).cloned()
                                {
                                    self.apply_suggestion_ui(&suggestion, ui);
                                }
                            }
                        } else {
                            // Show/toggle suggestions explicitly without typing
                            let ctrl = i.modifiers.ctrl || i.modifiers.command;
                            if ctrl && i.key_pressed(egui::Key::Space) {
                                self.update_suggestions(ui);
                            }
                        }
                    });

                    // Status bar at bottom
                    if height > 50.0 {
                        self.show_editor_status(ui);
                    }
                });
            });
    }

    fn handle_error_hover(&self, ui: &mut egui::Ui, response: &egui::Response) {
        if let (Some(error_msg), Some(error_line), Some(error_col), Some(error_len)) = (
            &self.syntax_error,
            self.error_line,
            self.error_column,
            self.error_length,
        ) && response.hovered()
        {
            let hover_pos = ui.input(|i| i.pointer.hover_pos()).unwrap_or_default();

            // Calculate if hover is over the error area
            if let Some(rect) = self.text_edit_rect {
                let line_height = 17.0;
                let char_width = 7.8;

                let error_y = rect.top() + 8.0 + ((error_line - 1) as f32 * line_height);
                let error_x_start = rect.left() + 8.0 + ((error_col - 1) as f32 * char_width);
                let error_x_end = error_x_start + (error_len as f32 * char_width);

                // Check if mouse is hovering over error area
                if hover_pos.x >= error_x_start
                    && hover_pos.x <= error_x_end
                    && hover_pos.y >= error_y
                    && hover_pos.y <= error_y + line_height
                {
                    response.clone().on_hover_ui_at_pointer(|ui| {
                        ui.set_max_width(300.0);
                        Frame::new()
                            .fill(egui::Color32::from_rgb(80, 40, 40))
                            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(220, 80, 80)))
                            .corner_radius(CornerRadius::same(4))
                            .inner_margin(Margin::same(8))
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.label("❌");
                                    ui.vertical(|ui| {
                                        ui.label(
                                            egui::RichText::new("Syntax Error:")
                                                .color(egui::Color32::from_rgb(255, 200, 200))
                                                .size(12.0)
                                                .strong(),
                                        );
                                        ui.label(
                                            egui::RichText::new(error_msg)
                                                .color(egui::Color32::from_rgb(255, 220, 220))
                                                .size(11.0)
                                                .family(egui::FontFamily::Monospace),
                                        );
                                    });
                                });
                            });
                    });
                }
            }
        }
    }

    fn paint_syntax_highlighted_text(&self, painter: &egui::Painter, rect: egui::Rect) {
        if self.query.is_empty() {
            // Show placeholder text when empty
            painter.text(
                egui::Pos2::new(rect.left() + 8.0, rect.top() + 8.0),
                egui::Align2::LEFT_TOP,
                "-- Enter your SQL query here",
                egui::FontId::monospace(13.0),
                egui::Color32::from_rgb(100, 100, 100),
            );
            return;
        }

        let font_id = egui::FontId::monospace(13.0);
        let line_height = 17.0;
        let char_width = 7.8;

        // Split by lines, but include empty lines
        let lines: Vec<&str> = self.query.split('\n').collect();

        for (line_idx, line) in lines.iter().enumerate() {
            let y_pos = rect.top() + 8.0 + (line_idx as f32 * line_height);

            // Paint the line content
            if !line.is_empty() {
                self.paint_line_with_syntax(
                    painter,
                    line,
                    rect.left() + 8.0,
                    y_pos,
                    &font_id,
                    char_width,
                );
            }

            // Paint error underline if this line has an error
            if let (Some(error_line), Some(error_col), Some(error_len)) =
                (self.error_line, self.error_column, self.error_length)
                && line_idx + 1 == error_line
            {
                // Convert to 1-based line number
                let start_x = rect.left() + 8.0 + ((error_col - 1) as f32 * char_width);
                let end_x = start_x + (error_len as f32 * char_width);
                let underline_y = y_pos + 14.0; // Position underline below text

                // Draw wavy red underline
                self.paint_error_underline(painter, start_x, end_x, underline_y);
            }
        }
    }

    fn paint_error_underline(&self, painter: &egui::Painter, start_x: f32, end_x: f32, y: f32) {
        let color = egui::Color32::from_rgb(220, 80, 80);
        let stroke = egui::Stroke::new(1.5, color);

        // Draw a wavy line
        let wave_width = 4.0;
        let wave_height = 2.0;
        let mut x = start_x;

        while x < end_x - wave_width {
            let next_x = (x + wave_width).min(end_x);

            // Create a simple wave pattern
            painter.line_segment(
                [
                    egui::Pos2::new(x, y),
                    egui::Pos2::new(x + wave_width / 2.0, y - wave_height),
                ],
                stroke,
            );
            painter.line_segment(
                [
                    egui::Pos2::new(x + wave_width / 2.0, y - wave_height),
                    egui::Pos2::new(next_x, y),
                ],
                stroke,
            );

            x = next_x;
        }
    }

    fn paint_line_with_syntax(
        &self,
        painter: &egui::Painter,
        line: &str,
        start_x: f32,
        y_pos: f32,
        font_id: &egui::FontId,
        char_width: f32,
    ) {
        // Handle comments first (they override everything else)
        if let Some(comment_start) = line.find("--") {
            // Paint everything before the comment normally
            let before_comment = &line[..comment_start];
            if !before_comment.trim().is_empty() {
                let _x_pos =
                    self.paint_tokens(painter, before_comment, start_x, y_pos, font_id, char_width);
            }

            // Paint the comment
            let comment = &line[comment_start..];
            let comment_x = start_x + (comment_start as f32 * char_width);
            painter.text(
                egui::Pos2::new(comment_x, y_pos),
                egui::Align2::LEFT_TOP,
                comment,
                font_id.clone(),
                egui::Color32::from_rgb(106, 153, 85), // Green for comments
            );
            return;
        }

        // Paint tokens normally
        self.paint_tokens(painter, line, start_x, y_pos, font_id, char_width);
    }

    fn paint_tokens(
        &self,
        painter: &egui::Painter,
        text: &str,
        start_x: f32,
        y_pos: f32,
        font_id: &egui::FontId,
        char_width: f32,
    ) -> f32 {
        let mut x_pos = start_x;
        let chars: Vec<char> = text.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let start_i = i;

            // Skip whitespace
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }

            if i > start_i {
                let whitespace: String = chars[start_i..i].iter().collect();
                x_pos += whitespace.len() as f32 * char_width;
            }

            if i >= chars.len() {
                break;
            }

            // Collect word/token
            let word_start = i;

            if chars[i] == '\'' || chars[i] == '"' {
                // Handle string literals
                let quote = chars[i];
                i += 1;
                while i < chars.len() && chars[i] != quote {
                    i += 1;
                }
                if i < chars.len() {
                    i += 1; // Include closing quote
                }
            } else if chars[i].is_alphanumeric() || chars[i] == '_' {
                // Handle identifiers/keywords
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
            } else if chars[i].is_numeric()
                || (chars[i] == '.' && i + 1 < chars.len() && chars[i + 1].is_numeric())
            {
                // Handle numbers
                while i < chars.len() && (chars[i].is_numeric() || chars[i] == '.') {
                    i += 1;
                }
            } else {
                // Handle operators and punctuation
                i += 1;
            }

            let token: String = chars[word_start..i].iter().collect();
            if !token.is_empty() {
                let color = self.get_token_color(&token);

                painter.text(
                    egui::Pos2::new(x_pos, y_pos),
                    egui::Align2::LEFT_TOP,
                    &token,
                    font_id.clone(),
                    color,
                );

                x_pos += token.len() as f32 * char_width;
            }
        }

        x_pos
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
        Frame::new().show(ui, |ui| {
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

    fn validate_syntax(&mut self) {
        if self.query.trim().is_empty() {
            self.syntax_error = None;
            self.error_line = None;
            self.error_column = None;
            self.error_length = None;
            return;
        }

        match self.parse_sql(&self.query) {
            Ok(_) => {
                self.syntax_error = None;
                self.error_line = None;
                self.error_column = None;
                self.error_length = None;
            },
            Err(error) => {
                let clean_error = error
                    .replace("sql parser error: ", "")
                    .chars()
                    .take(200)
                    .collect::<String>();

                self.syntax_error = Some(clean_error.clone());

                // Try to extract line and column information from the error
                self.extract_error_position(&error);
            },
        }
    }

    fn extract_error_position(&mut self, error: &str) {
        // Try to parse error position from common SQL error formats
        // This is a simplified parser - real SQL parsers may have different formats

        // Look for patterns like "at line 1 column 5" or "Line: 1, Column: 5"
        if let Some(captures) = regex::Regex::new(r"(?i)line[:\s]*(\d+).*column[:\s]*(\d+)")
            .unwrap()
            .captures(error)
            && let (Ok(line), Ok(col)) =
                (captures[1].parse::<usize>(), captures[2].parse::<usize>())
        {
            self.error_line = Some(line);
            self.error_column = Some(col);
            self.error_length = Some(5); // Default error length
            return;
        }

        // If we can't parse the exact position, try to guess based on common errors
        if error.to_lowercase().contains("unexpected") {
            // Try to find the last word in the query as a rough estimate
            let lines: Vec<&str> = self.query.split('\n').collect();
            for (line_idx, line) in lines.iter().enumerate() {
                if let Some(last_word_start) = line.rfind(char::is_whitespace) {
                    self.error_line = Some(line_idx + 1);
                    self.error_column = Some(last_word_start + 2);
                    self.error_length = Some(line.len() - last_word_start - 1);
                    return;
                }
            }
        }

        // Default fallback - highlight the last non-empty line
        let lines: Vec<&str> = self.query.split('\n').collect();
        for (line_idx, line) in lines.iter().enumerate().rev() {
            if !line.trim().is_empty() {
                self.error_line = Some(line_idx + 1);
                self.error_column = Some(1);
                self.error_length = Some(line.len().max(1));
                break;
            }
        }
    }

    fn update_cursor_position(&mut self) {
        // More accurate cursor position tracking
        let lines: Vec<&str> = self.query.split('\n').collect();

        // Current row is the number of lines
        self.cursor_row = lines.len();

        // Current column is the length of the last line + 1
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
                egui::Frame::popup(ui.style())
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
                                    egui::RichText::new(
                                        "Ctrl+Enter to accept  •  Ctrl+Space to toggle",
                                    )
                                    .color(egui::Color32::from_rgb(170, 170, 170))
                                    .size(10.0),
                                );
                            });
                        });
                    });
            });

        if let Some(suggestion) = clicked_suggestion {
            self.apply_suggestion_ui(&suggestion, ui);
        }
    }

    fn update_suggestions(&mut self, ui: &egui::Ui) {
        let (prefix, _start, _end) = self.current_word_at_cursor(ui);
        self.current_word = prefix.clone();

        if self.current_word.len() >= 1 {
            self.suggestions = self.get_suggestions(&self.current_word);
            self.show_suggestions = !self.suggestions.is_empty();
            self.selected_suggestion = 0;
        } else {
            self.show_suggestions = false;
            self.suggestions.clear();
        }
    }

    fn get_suggestions(&self, prefix: &str) -> Vec<String> {
        let mut suggestions = Vec::new();
        let p = prefix.to_uppercase();

        for &kw in &self.sql_keywords {
            if kw.to_uppercase().starts_with(&p) {
                suggestions.push(kw.to_string());
            }
        }
        for &func in &self.sql_functions {
            if func.to_uppercase().starts_with(&p) {
                suggestions.push(format!("{}()", func));
            }
        }

        suggestions.sort();
        suggestions.dedup();
        suggestions.truncate(8);
        suggestions
    }

    fn caret_index(&self, ui: &egui::Ui) -> Option<usize> {
        let id = egui::Id::new("sql_editor");
        if let Some(state) = egui::TextEdit::load_state(ui.ctx(), id) {
            if let Some(cr) = state.cursor.char_range() {
                return Some(cr.primary.index);
            }
        }
        None
    }

    fn current_word_range_at(&self, idx: usize) -> (usize, usize) {
        let bytes = self.query.as_bytes();
        let len = bytes.len();
        let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

        let mut s = idx.min(len);
        while s > 0 && is_word(bytes[s - 1]) {
            s -= 1;
        }
        let mut e = idx.min(len);
        while e < len && is_word(bytes[e]) {
            e += 1;
        }
        (s, e)
    }

    fn current_word_at_cursor(&self, ui: &egui::Ui) -> (String, usize, usize) {
        if let Some(i) = self.caret_index(ui) {
            let (s, e) = self.current_word_range_at(i);
            return (self.query[s..e].to_string(), s, e);
        }
        // Fallback: end-of-text
        let last = self
            .query
            .split_whitespace()
            .last()
            .unwrap_or("")
            .to_string();
        (
            last.clone(),
            self.query.len().saturating_sub(last.len()),
            self.query.len(),
        )
    }

    fn apply_suggestion_ui(&mut self, suggestion: &str, ui: &egui::Ui) {
        let (_prefix, start, end) = self.current_word_at_cursor(ui);
        if start <= end && end <= self.query.len() {
            self.query.replace_range(start..end, suggestion);
            // add a trailing space for keywords (heuristic: not a function call)
            if !suggestion.ends_with(')') {
                let insert_at = start + suggestion.len();
                if insert_at <= self.query.len() {
                    self.query.insert(insert_at, ' ');
                }
            }
        }
        self.show_suggestions = false;
        self.validate_syntax();
    }

    fn parse_sql(&self, sql: &str) -> Result<Vec<Statement>, String> {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())
    }
}
