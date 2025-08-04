use anyhow::{Context as AnyhowContext, Result};
use eframe::{App, NativeOptions, egui};
use egui::Context;
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
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
        let lf = self.lf.as_ref().with_context(|| "No CSV file loaded")?;

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
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.text_edit_singleline(&mut self.filter).lost_focus()
                        && ui.input(|i| i.key_pressed(egui::Key::Enter))
                    {
                        self.offset = 0;
                        self.collect_batch();
                    }
                    ui.label("Filter:");
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(err) = &self.error {
                ui.colored_label(egui::Color32::RED, format!("Error: {}", err));
                ui.separator();
            }

            if let Some(df) = &self.batch {
                let n_rows = df.height();
                let table = TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center));

                let table_with_columns = self
                    .cols
                    .iter()
                    .fold(table, |acc, _| acc.column(Column::auto()));

                table_with_columns
                    .header(20.0, |mut header| {
                        for col in &self.cols {
                            header.col(|ui| {
                                ui.strong(col);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(18.0, n_rows, |mut row| {
                            let idx = row.index();
                            for col in &self.cols {
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
                });
            } else {
                ui.centered_and_justified(|ui| {
                    ui.label("Open a CSV to begin");
                });
            }
        });
    }
}
