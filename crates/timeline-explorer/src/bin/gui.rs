use anyhow::{Context as AnyhowContext, Result};
use eframe::{App, NativeOptions, egui};
use egui::Context;
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use polars_sql::SQLContext;
use rfd::FileDialog;
use std::{
    env,
    path::{Path, PathBuf},
};

fn main() -> eframe::Result<()> {
    let args: Vec<String> = env::args().collect();
    let file = args.get(1).map(PathBuf::from);

    let native_options = NativeOptions::default();
    eframe::run_native(
        "Timeline Explorer CSV Viewer",
        native_options,
        Box::new(move |_cc| Ok(Box::new(TimelineExplorerApp::new(file)))),
    )
}

struct TimelineExplorerApp {
    /// Lazily-scanned CSV. We keep it around so we can re-collect with filters.
    lf: Option<LazyFrame>,
    /// Materialised batch that's currentlys displayed (e.g. 1k rows).
    batch: Option<DataFrame>,
    /// Column names cache
    cols: Vec<String>,
    /// Text filter
    filter: String,
    /// SQL query text
    sql_query: String,
    /// Whether we're in SQL mode
    sql_mode: bool,
    /// How many rows have we collected so far (for pagination / virtual scroll)?
    offset: usize,
    /// Page size
    page: usize,
    /// Last file path (used for window title)
    path: Option<PathBuf>,
    /// Possible error message
    error: Option<String>,
}

impl TimelineExplorerApp {
    pub fn new(file: Option<PathBuf>) -> Self {
        let mut app = Self {
            lf: None,
            batch: None,
            cols: Vec::new(),
            filter: String::new(),
            sql_query: "SELECT * FROM data LIMIT 1000".to_string(),
            sql_mode: false,
            offset: 0,
            page: 1_000,
            path: None,
            error: None,
        };
        if let Some(p) = file {
            if let Err(e) = app.try_open_file(&p) {
                app.error = Some(e.to_string());
            }
        }
        app
    }

    fn open_file<P: AsRef<Path>>(&mut self, path: P) {
        if let Err(e) = self.try_open_file(&path) {
            self.error = Some(e.to_string());
        }
    }

    fn try_open_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path_buf = path.as_ref().to_path_buf();
        let path_str = path_buf
            .to_str()
            .with_context(|| format!("Invalid UTF-8 in path: {}", path_buf.display()))?;

        let mut lf = LazyCsvReader::new(PlPath::from_str(path_str))
            .with_has_header(true)
            .with_encoding(CsvEncoding::LossyUtf8)
            .with_try_parse_dates(true)
            .finish()
            .with_context(|| format!("Failed to read CSV file: {}", path_buf.display()))?;

        self.cols = lf
            .collect_schema()
            .with_context(|| "Failed to collect schema from CSV")?
            .iter_names()
            .map(|s| s.to_string())
            .collect();

        self.lf = Some(lf);
        self.path = Some(path_buf);
        self.offset = 0;
        self.sql_mode = false; // Reset to filter mode when opening new file
        self.error = None; // Clear any previous errors

        self.try_collect_batch()
            .with_context(|| "Failed to collect initial batch of data")?;

        Ok(())
    }

    /// Fetch a slice of rows according to current filter/offset.
    fn collect_batch(&mut self) {
        if let Err(e) = self.try_collect_batch() {
            self.error = Some(e.to_string());
        }
    }

    fn try_collect_batch(&mut self) -> Result<()> {
        if self.lf.is_none() {
            return Err(anyhow::anyhow!("No CSV file loaded"));
        }

        if self.sql_mode {
            self.try_execute_sql()
        } else {
            let lf = self.lf.as_ref().unwrap().clone();
            self.try_collect_filtered_batch(&lf)
        }
    }

    fn try_collect_filtered_batch(&mut self, lf: &LazyFrame) -> Result<()> {
        let mut q = lf.clone();

        if !self.filter.is_empty() {
            let filter_exprs: Vec<Expr> = self
                .cols
                .iter()
                .map(|c| {
                    col(c)
                        .cast(DataType::String)
                        .str()
                        .contains_literal(lit(&*self.filter))
                })
                .collect();

            let combined_filter = filter_exprs
                .into_iter()
                .reduce(|acc, e| acc.or(e))
                .with_context(|| "No columns available for filtering")?;

            q = q.filter(combined_filter);
        }

        q = q.slice(self.offset as i64, self.page as u32);

        let df = q
            .collect()
            .with_context(|| "Failed to collect filtered data")?;

        self.batch = Some(df);
        Ok(())
    }

    fn try_execute_sql(&mut self) -> Result<()> {
        let lf = self.lf.as_ref().with_context(|| "No CSV file loaded")?;

        // Create SQL context and register the dataframe as "data"
        let mut ctx = SQLContext::new();
        ctx.register("data", lf.clone());

        // Execute the SQL query
        let result_lf = ctx
            .execute(&self.sql_query)
            .with_context(|| "Failed to execute SQL query")?;

        let df = result_lf
            .collect()
            .with_context(|| "Failed to collect SQL query results")?;

        self.batch = Some(df);
        Ok(())
    }

    fn execute_sql(&mut self) {
        if let Err(e) = self.try_execute_sql() {
            self.error = Some(e.to_string());
        }
    }

    fn try_get_cell_value(&self, df: &DataFrame, col: &str, idx: usize) -> Result<String> {
        let column = df
            .column(col)
            .with_context(|| format!("Column '{}' not found", col))?;

        let value = column
            .get(idx)
            .with_context(|| format!("Row index {} out of bounds", idx))?;

        Ok(value.to_string())
    }
}

impl App for TimelineExplorerApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("menu").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("Open CSV…").clicked() {
                    if let Some(path) = FileDialog::new().add_filter("csv", &["csv"]).pick_file() {
                        self.open_file(&path);
                    }
                }
                if let Some(p) = &self.path {
                    ui.label(p.display().to_string());
                }

                ui.separator();

                // Mode selection
                ui.label("Mode:");
                if ui.selectable_label(!self.sql_mode, "Filter").clicked() {
                    self.sql_mode = false;
                    self.offset = 0;
                    self.collect_batch();
                }
                if ui.selectable_label(self.sql_mode, "SQL").clicked() {
                    self.sql_mode = true;
                    self.offset = 0;
                }
            });
        });

        // Query panel
        egui::TopBottomPanel::top("query").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if self.sql_mode {
                    ui.label("SQL Query:");
                    let response = ui.text_edit_singleline(&mut self.sql_query);
                    if (response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                        || ui.button("Execute").clicked()
                    {
                        self.execute_sql();
                    }
                    if ui.button("Clear").clicked() {
                        self.sql_query.clear();
                    }
                } else {
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.text_edit_singleline(&mut self.filter).lost_focus()
                            && ui.input(|i| i.key_pressed(egui::Key::Enter))
                        {
                            self.offset = 0;
                            self.collect_batch();
                        }
                        ui.label("Filter:");
                    });
                }
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(err) = &self.error {
                ui.colored_label(egui::Color32::RED, format!("Error: {}", err));
                ui.separator();
            }

            if let Some(df) = &self.batch {
                let n_rows = df.height();

                // Update columns for the current dataframe (important for SQL results)
                let current_cols: Vec<String> = df
                    .get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();

                let table = TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center));

                let table_with_columns = current_cols
                    .iter()
                    .fold(table, |acc, _| acc.column(Column::auto()));

                table_with_columns
                    .header(20.0, |mut header| {
                        for col in &current_cols {
                            header.col(|ui| {
                                ui.strong(col);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(18.0, n_rows, |mut row| {
                            let idx = row.index();
                            for col in &current_cols {
                                let value = self
                                    .try_get_cell_value(df, col, idx)
                                    .unwrap_or_else(|e| format!("Error: {}", e));
                                row.col(|ui| {
                                    ui.label(value);
                                });
                            }
                        });
                    });

                ui.separator();
                ui.horizontal(|ui| {
                    // Only show pagination for filter mode
                    if !self.sql_mode {
                        if ui.button("Prev").clicked() && self.offset >= self.page {
                            self.offset -= self.page;
                            self.collect_batch();
                        }
                        if ui.button("Next").clicked() {
                            self.offset += self.page;
                            self.collect_batch();
                        }
                        ui.label(format!(
                            "Showing rows {}..{}",
                            self.offset + 1,
                            self.offset + n_rows
                        ));
                    } else {
                        ui.label(format!("Query returned {} rows", n_rows));
                    }
                });
            } else {
                ui.centered_and_justified(|ui| {
                    ui.label("Open a CSV to begin");
                });
            }
        });
    }
}
