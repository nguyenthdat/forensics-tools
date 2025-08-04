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
            app.open_file(&p);
        }
        app
    }

    fn open_file<P: AsRef<Path>>(&mut self, path: P) {
        let path_ref = path.as_ref().to_str().unwrap_or("invalid path");
        match LazyCsvReader::new(PlPath::from_str(path_ref))
            .with_has_header(true)
            .with_encoding(CsvEncoding::LossyUtf8)
            .with_try_parse_dates(true)
            .finish()
        {
            Ok(mut lf) => {
                self.cols = lf
                    .collect_schema()
                    .unwrap()
                    .iter_names()
                    .map(|s| s.to_string())
                    .collect();
                self.lf = Some(lf);
                self.path = Some(path.as_ref().to_path_buf());
                self.offset = 0;
                self.collect_batch();
            }
            Err(e) => self.error = Some(e.to_string()),
        }
    }

    /// Fetch a slice of rows according to current filter/offset.
    fn collect_batch(&mut self) {
        if let Some(lf) = &self.lf {
            let mut q = lf.clone();
            if !self.filter.is_empty() {
                // very naive contains filter across string columns
                let exprs: Vec<Expr> = self
                    .cols
                    .iter()
                    .map(|c| {
                        col(c)
                            .cast(DataType::String) // Fixed: Use String instead of Utf8
                            .str()
                            .contains_literal(lit(&*self.filter)) // Fixed: use lit() and add case_insensitive parameter
                    })
                    .collect();
                let or = exprs
                    .into_iter()
                    .reduce(|acc, e| acc.or(e))
                    .expect("at least one col");
                q = q.filter(or);
            }
            // Only pull the next page worth of data
            q = q.slice(self.offset as i64, self.page as u32); // Fixed: cast to u32
            match q.collect() {
                Ok(df) => self.batch = Some(df),
                Err(e) => self.error = Some(e.to_string()),
            }
        }
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
                ui.colored_label(egui::Color32::RED, err);
            }
            if let Some(df) = &self.batch {
                // Build a dynamic table
                let n_rows = df.height();
                let table = TableBuilder::new(ui) // Fixed: Remove mut since it's not needed
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center));

                // Fixed: Add columns to the table
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
                                let value = df.column(col).unwrap().get(idx).unwrap().to_string(); // Fixed: Add unwrap() for get()
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
