use eframe::egui::ComboBox;
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
    // New fields for the modern interface
    search_column: String,
    search_query: String,
    rows_per_page: usize,
    current_page: usize,
    show_borders: bool,
    wrap_rows: bool,
    execution_time: String,
    row_count: usize,
}

impl SqlEditor {
    pub fn new() -> Self {
        Self {
            query: "SELECT * FROM data\nLIMIT 1000".to_string(),
            result: String::new(),
            show_result: true,
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
            limit: 1000,
            show_history: false,
            editor_height_ratio: 0.35, // Smaller editor like in image
            search_column: "altnameid".to_string(),
            search_query: String::new(),
            rows_per_page: 10,
            current_page: 1,
            show_borders: true,
            wrap_rows: false,
            execution_time: "69ms".to_string(),
            row_count: 1000,
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        // Main container with VS Code dark theme
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

                    // Show syntax error if any
                    self.show_syntax_error(ui);

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
                                .strong()
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
                        "Refer to your file as a table named _t_1.",
                        "Save SQL query output to a file using qsv sqlp or qsv to or to the clipboard using qsv clipboard.",
                        "Important note: Decimal values may be truncated and very large SQL query outputs can cause issues.",
                    ];
                    
                    for instruction in instructions {
                        ui.horizontal(|ui| {
                            ui.add_space(16.0);
                            ui.label(
                                egui::RichText::new(format!("• {}", instruction))
                                    .color(egui::Color32::from_rgb(200, 200, 200))
                                    .size(12.0)
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
                                .strong()
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
                ui.horizontal(|ui| {
                    // Calculate line number width dynamically
                    let line_count = self.query.lines().count().max(1);
                    let max_line_number = line_count.max(20);
                    let digits = if max_line_number < 10 {
                        1
                    } else if max_line_number < 100 {
                        2
                    } else {
                        3
                    };
                    let char_width = 8.0;
                    let padding = 16.0;
                    let line_number_width = (digits as f32 * char_width) + padding;

                    // Line numbers with calculated width
                    ui.allocate_ui_with_layout(
                        egui::Vec2::new(line_number_width, editor_height - 32.0),
                        egui::Layout::top_down(egui::Align::LEFT),
                        |ui| {
                            self.show_line_numbers(ui, editor_height - 32.0);
                        },
                    );

                    // Editor with syntax highlighting
                    self.show_highlighted_editor(ui, editor_height - 32.0);
                });
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
                            .size(13.0)
                    )
                    .fill(egui::Color32::from_rgb(0, 120, 215)) // VS Code blue
                    .corner_radius(CornerRadius::same(4));
                    
                    if ui.add(run_button).clicked() {
                        self.execute_query();
                    }
                    
                    ui.add_space(16.0);
                    
                    // Decrease/Increase code size buttons
                    if ui.button("🔍-").on_hover_text("Decrease code size").clicked() {
                        // Font size decrease logic will be added later
                    }
                    
                    if ui.button("🔍+").on_hover_text("Increase code size").clicked() {
                        // Font size increase logic will be added later
                    }
                    
                    ui.add_space(16.0);
                    
                    // Inference length
                    ui.label(
                        egui::RichText::new("Inference length:")
                            .color(egui::Color32::WHITE)
                            .size(12.0)
                    );
                    
                    ui.add(
                        egui::DragValue::new(&mut self.limit)
                            .range(1..=10000)
                            .speed(10)
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
                        .size(11.0)
                    );
                });
            });
    }

        fn show_results_section(&mut self, ui: &mut egui::Ui) {
        Frame::new()
            .fill(egui::Color32::from_rgb(37, 37, 38))
            .inner_margin(Margin::symmetric(16, 8))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    // Table controls
                    self.show_table_controls(ui);
                    
                    // Search controls
                    self.show_search_controls(ui);
                    
                    // Results content placeholder
                    self.show_results_placeholder(ui);
                    
                    // Pagination
                    self.show_pagination(ui);
                });
            });
    }

    fn show_table_controls(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            // Export data button
            let export_button = egui::Button::new(
                egui::RichText::new("📤 Export data")
                    .color(egui::Color32::WHITE)
                    .size(12.0)
            )
            .fill(egui::Color32::from_rgb(0, 120, 215))
            .corner_radius(CornerRadius::same(4));
            
            if ui.add(export_button).clicked() {
                // Handle export - will be implemented when CSV loading is added
            }
            
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                // Show/Hide Columns dropdown
                egui::ComboBox::from_label("Show/Hide Columns")
                    .selected_text("Show/Hide Columns")
                    .show_ui(ui, |ui| {
                        // Placeholder for column headers - will be populated from CSV
                        ui.label("Column headers will be loaded from CSV");
                    });
                
                ui.add_space(16.0);
                
                // Borders toggle
                ui.checkbox(&mut self.show_borders, "Borders");
                
                ui.add_space(8.0);
                
                // Wrap Rows toggle
                ui.checkbox(&mut self.wrap_rows, "Wrap Rows");
            });
        });
        
        ui.add_space(8.0);
    }

    fn show_search_controls(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("Search column:")
                    .color(egui::Color32::WHITE)
                    .size(12.0)
            );
            
            ComboBox::from_id_salt("search_column")
                .selected_text(&self.search_column)
                .show_ui(ui, |ui| {
                    // Placeholder - will be populated from CSV headers
                    ui.selectable_value(&mut self.search_column, "altnameid".to_string(), "altnameid");
                });
            
            ui.add_space(8.0);
            
            ui.add(
                egui::TextEdit::singleline(&mut self.search_query)
                    .hint_text("Search query for altnameid...")
                    .desired_width(200.0)
            );
        });
        
        ui.add_space(12.0);
    }

    fn show_results_placeholder(&mut self, ui: &mut egui::Ui) {
        // Placeholder for results table - will be replaced with actual table when CSV is loaded
        Frame::new()
            .fill(egui::Color32::from_rgb(45, 45, 45))
            .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(70, 70, 70)))
            .corner_radius(CornerRadius::same(4))
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(60.0);
                    ui.label(
                        egui::RichText::new("📊 Results will appear here")
                            .color(egui::Color32::from_rgb(150, 150, 150))
                            .size(14.0)
                    );
                    ui.label(
                        egui::RichText::new("Run a query to see the data table")
                            .color(egui::Color32::from_rgb(120, 120, 120))
                            .size(12.0)
                    );
                    ui.add_space(60.0);
                });
            });
    }

    fn show_pagination(&mut self, ui: &mut egui::Ui) {
        ui.add_space(8.0);
        
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("Rows per page")
                    .color(egui::Color32::WHITE)
                    .size(12.0)
            );
            
            ComboBox::from_id_salt("rows_per_page")
                .selected_text(format!("{}", self.rows_per_page))
                .show_ui(ui, |ui| {
                    for &count in &[10, 25, 50, 100] {
                        ui.selectable_value(&mut self.rows_per_page, count, format!("{}", count));
                    }
                });
            
            ui.add_space(16.0);
            
            ui.label(
                egui::RichText::new("Page 1 of 100")
                    .color(egui::Color32::from_rgb(180, 180, 180))
                    .size(12.0)
            );
            
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                // Pagination controls
                if ui.button("»").clicked() {
                    // Last page
                }
                
                if ui.button("›").clicked() {
                    self.current_page += 1;
                }
                
                ui.add(
                    egui::DragValue::new(&mut self.current_page)
                        .range(1..=100)
                        .speed(1)
                );
                
                if ui.button("‹").clicked() && self.current_page > 1 {
                    self.current_page -= 1;
                }
                
                if ui.button("«").clicked() {
                    self.current_page = 1;
                }
            });
        });
        
        ui.add_space(8.0);
        
        // Footer note
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("Percentages and decimal values may be estimations. Data with large content may be truncated with ellipsis.")
                    .color(egui::Color32::from_rgb(150, 150, 150))
                    .size(10.0)
                    .italics()
            );
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
                    // Calculate line number width dynamically
                    let line_count = self.query.lines().count().max(1);
                    let max_line_number = line_count.max(20);
                    let digits = if max_line_number < 10 {
                        1
                    } else if max_line_number < 100 {
                        2
                    } else {
                        3
                    };
                    let char_width = 8.0;
                    let padding = 16.0;
                    let line_number_width = (digits as f32 * char_width) + padding;

                    // Line numbers with calculated width
                    ui.allocate_ui_with_layout(
                        egui::Vec2::new(line_number_width, height),
                        egui::Layout::top_down(egui::Align::LEFT),
                        |ui| {
                            self.show_line_numbers(ui, height);
                        },
                    );

                    // Editor with remaining width
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

        // Calculate how many lines can fit in the given height
        let line_height = 18.0; // Should match the line height in paint_syntax_highlighted_text
        let top_padding = 8.0;
        let available_height_for_lines = height - top_padding - 10.0; // Leave some bottom padding
        let max_visible_lines = (available_height_for_lines / line_height).floor() as usize;
        let lines_to_show = line_count.min(max_visible_lines).max(1);

        // Calculate the width needed based on the maximum line number
        let max_line_number = line_count.max(20); // Show at least 20 line numbers for consistency
        let digits = if max_line_number < 10 {
            1
        } else if max_line_number < 100 {
            2
        } else {
            3
        };
        let char_width = 8.0; // Approximate width of a monospace character
        let padding = 16.0; // Left and right padding
        let calculated_width = (digits as f32 * char_width) + padding;

        Frame::new()
            .fill(egui::Color32::from_rgb(50, 50, 50))
            .show(ui, |ui| {
                ui.allocate_ui_with_layout(
                    egui::Vec2::new(calculated_width, height),
                    egui::Layout::top_down(egui::Align::RIGHT),
                    |ui| {
                        ui.add_space(top_padding);

                        // Show line numbers that fit in the available space
                        for line_num in 1..=lines_to_show {
                            let line_color = if line_num <= line_count {
                                egui::Color32::from_rgb(130, 130, 130) // Normal line numbers
                            } else {
                                egui::Color32::from_rgb(80, 80, 80) // Dimmed for empty lines
                            };

                            ui.label(
                                egui::RichText::new(format!("{:width$}", line_num, width = digits))
                                    .color(line_color)
                                    .size(12.0)
                                    .family(egui::FontFamily::Monospace),
                            );
                        }

                        // If there are more lines than can be displayed, show an indicator
                        if line_count > max_visible_lines {
                            ui.add_space(4.0);
                            ui.label(
                                egui::RichText::new("⋮")
                                    .color(egui::Color32::from_rgb(100, 100, 100))
                                    .size(14.0),
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
                    let text_edit_id = egui::Id::new("sql_editor");
                    
                    // Get the available area for the editor
                    let available_rect = ui.available_rect_before_wrap();
                    let editor_rect = egui::Rect::from_min_size(
                        available_rect.min,
                        egui::Vec2::new(available_rect.width(), height - 30.0)
                    );

                    // First, paint the syntax highlighted text as background
                    self.paint_syntax_highlighted_text(ui.painter(), editor_rect);

                    // Then overlay the transparent text editor for input handling
                  let response = ui.scope_builder(egui::UiBuilder::new().max_rect(editor_rect), |ui| {
                        ui.add_sized(
                            [editor_rect.width() - 16.0, editor_rect.height() - 16.0],
                            egui::TextEdit::multiline(&mut self.query)
                                .font(egui::FontId::monospace(13.0))
                                .background_color(egui::Color32::TRANSPARENT)
                                .text_color(egui::Color32::TRANSPARENT) // Keep text invisible
                                .margin(Margin::same(8))
                                .id(text_edit_id),
                        )
                    }).inner;

                    // Store the text editor's rect for cursor positioning
                    self.text_edit_rect = Some(response.rect);

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

                    // Status bar at bottom (optional, can be removed if not needed)
                    if height > 50.0 {
                        self.show_editor_status(ui);
                    }
                });
            });
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

        let lines: Vec<&str> = self.query.lines().collect();

        for (line_idx, line) in lines.iter().enumerate() {
            let y_pos = rect.top() + 8.0 + (line_idx as f32 * line_height);
            
            if line.trim().is_empty() {
                continue; // Skip empty lines
            }

            // Parse the line for syntax highlighting
            self.paint_line_with_syntax(painter, line, rect.left() + 8.0, y_pos, &font_id, char_width);
        }
    }

    fn paint_line_with_syntax(&self, painter: &egui::Painter, line: &str, start_x: f32, y_pos: f32, font_id: &egui::FontId, char_width: f32) {
        // Handle comments first (they override everything else)
        if let Some(comment_start) = line.find("--") {
            // Paint everything before the comment normally
            let before_comment = &line[..comment_start];
            if !before_comment.trim().is_empty() {
                let _x_pos = self.paint_tokens(painter, before_comment, start_x, y_pos, font_id, char_width);
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

        fn paint_tokens(&self, painter: &egui::Painter, text: &str, start_x: f32, y_pos: f32, font_id: &egui::FontId, char_width: f32) -> f32 {
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
            } else if chars[i].is_numeric() || (chars[i] == '.' && i + 1 < chars.len() && chars[i + 1].is_numeric()) {
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
