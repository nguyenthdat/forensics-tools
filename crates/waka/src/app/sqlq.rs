use eframe::egui;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSet;
use syntect::util::{LinesWithEndings, as_24_bit_terminal_escaped};

pub struct SqlEditor {
    query: String,
    result: String,
    show_result: bool,
    syntax_set: SyntaxSet,
    theme_set: ThemeSet,
    suggestions: Vec<String>,
    show_suggestions: bool,
    selected_suggestion: usize,
    last_cursor_pos: Option<usize>,
    sql_keywords: HashSet<&'static str>,
    sql_functions: HashSet<&'static str>,
}

impl SqlEditor {
    pub fn new() -> Self {
        let syntax_set = SyntaxSet::load_defaults_newlines();
        let theme_set = ThemeSet::load_defaults();

        Self {
            query: String::new(),
            result: String::new(),
            show_result: false,
            syntax_set,
            theme_set,
            suggestions: Vec::new(),
            show_suggestions: false,
            selected_suggestion: 0,
            last_cursor_pos: None,
            sql_keywords: Self::get_sql_keywords(),
            sql_functions: Self::get_sql_functions(),
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        ui.vertical(|ui| {
            ui.heading("SQL Query Editor");

            // Query validation status
            self.show_query_status(ui);

            // Query input area with enhanced features
            ui.label("Enter your SQL query:");

            let text_edit = egui::TextEdit::multiline(&mut self.query)
                .desired_rows(10)
                .desired_width(f32::INFINITY)
                .font(egui::TextStyle::Monospace)
                .code_editor();

            let response = ui.add(text_edit);

            // Handle autocomplete
            if response.changed() {
                self.update_autocomplete(ui);
            }

            // Show autocomplete popup
            if self.show_suggestions && !self.suggestions.is_empty() {
                self.show_autocomplete_popup(ui);
            }

            // Syntax highlighted preview
            ui.separator();
            ui.collapsing("Syntax Highlighted Preview", |ui| {
                self.show_syntax_highlighted_query(ui);
            });

            // Action buttons
            ui.horizontal(|ui| {
                if ui.button("▶ Execute Query").clicked() {
                    self.execute_query();
                }

                if ui.button("🗑 Clear").clicked() {
                    self.clear_editor();
                }

                if ui.button("✨ Format Query").clicked() {
                    self.format_query();
                }

                if ui.button("✅ Validate SQL").clicked() {
                    self.validate_query();
                }

                // Quick insert buttons
                ui.separator();
                if ui.small_button("SELECT").clicked() {
                    self.insert_template("SELECT * FROM table_name WHERE ");
                }
                if ui.small_button("INSERT").clicked() {
                    self.insert_template("INSERT INTO table_name (column1, column2) VALUES (?, ?)");
                }
                if ui.small_button("UPDATE").clicked() {
                    self.insert_template("UPDATE table_name SET column1 = ? WHERE ");
                }
            });

            // Results area with enhanced display
            if self.show_result {
                ui.separator();
                ui.heading("Query Result");

                egui::ScrollArea::vertical()
                    .max_height(250.0)
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut self.result)
                                .desired_rows(8)
                                .desired_width(f32::INFINITY)
                                .font(egui::TextStyle::Monospace)
                                .interactive(false),
                        );
                    });
            }
        });
    }

    fn show_query_status(&self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            // Query validation indicator
            match self.parse_sql(&self.query) {
                Ok(_) if !self.query.trim().is_empty() => {
                    ui.colored_label(egui::Color32::GREEN, "✅ Valid SQL");
                }
                Err(e) if !self.query.trim().is_empty() => {
                    ui.colored_label(egui::Color32::RED, format!("❌ SQL Error: {}", e));
                }
                _ => {
                    ui.colored_label(egui::Color32::GRAY, "⏳ Enter SQL query...");
                }
            }

            ui.separator();

            // Character count
            ui.label(format!("Characters: {}", self.query.len()));

            // Line count
            ui.label(format!("Lines: {}", self.query.lines().count()));
        });
    }

    fn show_syntax_highlighted_query(&self, ui: &mut egui::Ui) {
        if self.query.is_empty() {
            ui.label("No query to highlight");
            return;
        }

        // Use syntect for syntax highlighting
        if let Some(syntax) = self.syntax_set.find_syntax_by_extension("sql") {
            let theme = &self.theme_set.themes["base16-ocean.dark"];
            let mut highlight_lines = HighlightLines::new(syntax, theme);

            ui.horizontal_wrapped(|ui| {
                for line in LinesWithEndings::from(&self.query) {
                    let ranges: Vec<(Style, &str)> = highlight_lines
                        .highlight_line(line, &self.syntax_set)
                        .unwrap_or_default();

                    for (style, text) in ranges {
                        let color = egui::Color32::from_rgb(
                            style.foreground.r,
                            style.foreground.g,
                            style.foreground.b,
                        );
                        ui.colored_label(color, text);
                    }
                }
            });
        } else {
            // Fallback to simple highlighting
            self.show_simple_highlighted_query(ui);
        }
    }

    fn show_simple_highlighted_query(&self, ui: &mut egui::Ui) {
        let words: Vec<&str> = self.query.split_whitespace().collect();

        ui.horizontal_wrapped(|ui| {
            for word in words {
                let clean_word = word
                    .trim_matches(|c: char| !c.is_alphanumeric())
                    .to_uppercase();

                if self.sql_keywords.contains(clean_word.as_str()) {
                    ui.colored_label(egui::Color32::from_rgb(86, 156, 214), word);
                } else if self.sql_functions.contains(clean_word.as_str()) {
                    ui.colored_label(egui::Color32::from_rgb(220, 220, 170), word);
                } else if word.starts_with("'") && word.ends_with("'") {
                    ui.colored_label(egui::Color32::from_rgb(206, 145, 120), word);
                } else if word.chars().all(|c| c.is_numeric() || c == '.') {
                    ui.colored_label(egui::Color32::from_rgb(181, 206, 168), word);
                } else {
                    ui.label(word);
                }
                ui.label(" ");
            }
        });
    }

    fn update_autocomplete(&mut self, ui: &mut egui::Ui) {
        // Simple autocomplete based on current word
        let words: Vec<&str> = self.query.split_whitespace().collect();
        if let Some(last_word) = words.last() {
            if last_word.len() >= 2 {
                self.suggestions = self.get_suggestions(last_word);
                self.show_suggestions = !self.suggestions.is_empty();
                self.selected_suggestion = 0;
            } else {
                self.show_suggestions = false;
            }
        }
    }

    fn show_autocomplete_popup(&mut self, ui: &mut egui::Ui) {
        let mut selected_suggestion: Option<String> = None;

        ui.group(|ui| {
            ui.vertical(|ui| {
                ui.label("💡 Suggestions:");
                for (i, suggestion) in self.suggestions.iter().take(8).enumerate() {
                    let is_selected = i == self.selected_suggestion;
                    let response = ui.selectable_label(is_selected, suggestion);
                    if response.clicked() {
                        selected_suggestion = Some(suggestion.clone());
                    }
                }
            });
        });

        if let Some(suggestion) = selected_suggestion {
            self.apply_suggestion(&suggestion);
        }
    }

    fn get_suggestions(&self, prefix: &str) -> Vec<String> {
        let mut suggestions = Vec::new();
        let prefix_upper = prefix.to_uppercase();

        // Keywords
        for keyword in &self.sql_keywords {
            if keyword.starts_with(&prefix_upper) {
                suggestions.push(keyword.to_string());
            }
        }

        // Functions
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
    }

    fn parse_sql(&self, sql: &str) -> Result<Vec<Statement>, String> {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())
    }

    fn validate_query(&mut self) {
        match self.parse_sql(&self.query) {
            Ok(statements) => {
                self.result = format!(
                    "✅ SQL Validation Successful!\n\nParsed {} statement(s):\n{}",
                    statements.len(),
                    statements
                        .iter()
                        .enumerate()
                        .map(|(i, stmt)| format!(
                            "{}. {}",
                            i + 1,
                            format!("{:?}", stmt).chars().take(100).collect::<String>()
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                );
            }
            Err(e) => {
                self.result = format!("❌ SQL Validation Failed:\n\n{}", e);
            }
        }
        self.show_result = true;
    }

    fn format_query(&mut self) {
        // Basic SQL formatting using sqlparser
        match self.parse_sql(&self.query) {
            Ok(statements) => {
                self.query = statements
                    .iter()
                    .map(|stmt| format!("{}", stmt))
                    .collect::<Vec<_>>()
                    .join(";\n");
            }
            Err(_) => {
                // Fallback to simple formatting
                self.query = self
                    .query
                    .replace(" select ", " SELECT ")
                    .replace(" from ", " FROM ")
                    .replace(" where ", " WHERE ")
                    .replace(" and ", " AND ")
                    .replace(" or ", " OR ");
            }
        }
    }

    fn insert_template(&mut self, template: &str) {
        if !self.query.is_empty() && !self.query.ends_with('\n') {
            self.query.push('\n');
        }
        self.query.push_str(template);
    }

    fn clear_editor(&mut self) {
        self.query.clear();
        self.result.clear();
        self.show_result = false;
        self.show_suggestions = false;
    }

    fn execute_query(&mut self) {
        if self.query.trim().is_empty() {
            self.result = "❌ Error: Empty query".to_string();
            self.show_result = true;
            return;
        }

        // Validate SQL first
        match self.parse_sql(&self.query) {
            Ok(statements) => {
                // TODO: Implement actual SQL execution
                let query_type = self.get_query_type(&statements[0]);
                self.result = format!(
                    "✅ Query executed successfully!\n\nQuery Type: {}\nQuery:\n{}\n\n📊 Mock Results:\n{}\n\n⏱ Execution time: 0.003s",
                    query_type,
                    self.query,
                    self.generate_mock_result(&query_type)
                );
            }
            Err(e) => {
                self.result = format!("❌ SQL Error: {}", e);
            }
        }
        self.show_result = true;
    }

    fn get_query_type(&self, statement: &Statement) -> String {
        match statement {
            Statement::Query(_) => "SELECT".to_string(),
            Statement::Insert { .. } => "INSERT".to_string(),
            Statement::Update { .. } => "UPDATE".to_string(),
            Statement::Delete { .. } => "DELETE".to_string(),
            Statement::CreateTable { .. } => "CREATE TABLE".to_string(),
            _ => "OTHER".to_string(),
        }
    }

    fn generate_mock_result(&self, query_type: &str) -> String {
        match query_type {
            "SELECT" => "| id | name     | email           |\n|----|---------|-----------------|\n| 1  | John Doe | john@example.com|\n| 2  | Jane Doe | jane@example.com|",
            "INSERT" => "1 row inserted successfully",
            "UPDATE" => "2 rows updated successfully", 
            "DELETE" => "1 row deleted successfully",
            _ => "Operation completed successfully"
        }.to_string()
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
            "AUTO_INCREMENT",
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
