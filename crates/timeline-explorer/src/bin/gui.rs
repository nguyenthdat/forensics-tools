use anyhow::{Context as AnyhowContext, Result};
use eframe::{App, NativeOptions, egui};
use egui::{Color32, Context, RichText};
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use polars_sql::SQLContext;
use rayon::prelude::*;
use rfd::FileDialog;
use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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

#[derive(Clone, Debug, PartialEq)]
pub enum FilterType {
    Contains,
    Exact,
    StartsWith,
    EndsWith,
    NotContains,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterEqual,
    LessEqual,
    DateRange,
    IsEmpty,
    IsNotEmpty,
}

impl FilterType {
    fn display_name(&self) -> &'static str {
        match self {
            FilterType::Contains => "Contains",
            FilterType::Exact => "Exact match",
            FilterType::StartsWith => "Starts with",
            FilterType::EndsWith => "Ends with",
            FilterType::NotContains => "Does not contain",
            FilterType::NotEqual => "Not equal",
            FilterType::GreaterThan => "Greater than",
            FilterType::LessThan => "Less than",
            FilterType::GreaterEqual => "Greater or equal",
            FilterType::LessEqual => "Less or equal",
            FilterType::DateRange => "Date range",
            FilterType::IsEmpty => "Is empty",
            FilterType::IsNotEmpty => "Is not empty",
        }
    }

    fn all() -> Vec<FilterType> {
        vec![
            FilterType::Contains,
            FilterType::Exact,
            FilterType::StartsWith,
            FilterType::EndsWith,
            FilterType::NotContains,
            FilterType::NotEqual,
            FilterType::GreaterThan,
            FilterType::LessThan,
            FilterType::GreaterEqual,
            FilterType::LessEqual,
            FilterType::DateRange,
            FilterType::IsEmpty,
            FilterType::IsNotEmpty,
        ]
    }
}

#[derive(Clone, Debug)]
pub struct ColumnFilter {
    pub enabled: bool,
    pub filter_type: FilterType,
    pub value: String,
    pub date_from: String,
    pub date_to: String,
    pub case_sensitive: bool,
    pub unique_values: Vec<String>,
    pub selected_values: HashMap<String, bool>,
    pub show_dropdown: bool,
    pub show_advanced: bool,
    // Performance optimizations
    pub unique_values_cached: bool,
    pub last_updated: Instant,
    pub cached_expr: Option<Expr>,
}

impl Default for ColumnFilter {
    fn default() -> Self {
        Self {
            enabled: false,
            filter_type: FilterType::Contains,
            value: String::new(),
            date_from: String::new(),
            date_to: String::new(),
            case_sensitive: false,
            unique_values: Vec::new(),
            selected_values: HashMap::new(),
            show_dropdown: false,
            show_advanced: false,
            unique_values_cached: false,
            last_updated: Instant::now(),
            cached_expr: None,
        }
    }
}

impl ColumnFilter {
    fn has_active_filters(&self) -> bool {
        if !self.enabled {
            return false;
        }

        match self.filter_type {
            FilterType::IsEmpty | FilterType::IsNotEmpty => true,
            FilterType::DateRange => !self.date_from.is_empty() || !self.date_to.is_empty(),
            _ => !self.value.is_empty() || self.selected_values.values().any(|&selected| !selected),
        }
    }

    fn update_unique_values(&mut self, values: Vec<String>) {
        // Only update if values have changed to preserve selections
        if self.unique_values != values {
            self.unique_values = values;
            // Initialize all values as selected by default
            for value in &self.unique_values {
                self.selected_values.entry(value.clone()).or_insert(true);
            }
            self.unique_values_cached = true;
            self.last_updated = Instant::now();
        }
    }

    fn invalidate_cache(&mut self) {
        self.cached_expr = None;
        self.last_updated = Instant::now();
    }

    fn needs_unique_values_update(&self) -> bool {
        !self.unique_values_cached || self.last_updated.elapsed() > Duration::from_secs(60)
    }
}

#[derive(Clone)]
struct CachedData {
    lf: Option<LazyFrame>,
    schema: Option<SchemaRef>,
    column_stats: HashMap<String, ColumnStats>,
    last_updated: Instant,
}

#[derive(Clone, Debug)]
struct ColumnStats {
    data_type: DataType,
    null_count: usize,
    unique_count: usize,
    min_value: Option<String>,
    max_value: Option<String>,
}

impl Default for CachedData {
    fn default() -> Self {
        Self {
            lf: None,
            schema: None,
            column_stats: HashMap::new(),
            last_updated: Instant::now(),
        }
    }
}

struct TimelineExplorerApp {
    /// Cached data with LazyFrame and metadata
    cached_data: Arc<Mutex<CachedData>>,
    /// Materialised batch that's currently displayed (e.g. 1k rows).
    batch: Option<DataFrame>,
    /// Column names cache
    cols: Vec<String>,
    /// Column filters with performance optimizations
    column_filters: HashMap<String, ColumnFilter>,
    /// Text filter (global search)
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
    /// Show filter panel
    show_filters: bool,
    /// Column data types for better filtering
    column_types: HashMap<String, DataType>,
    /// Performance monitoring
    last_filter_time: Instant,
    filter_debounce_timer: Option<Instant>,
    /// Background processing state
    processing: Arc<Mutex<bool>>,
    /// Filter expression cache
    filter_expr_cache: HashMap<String, Expr>,
}

impl TimelineExplorerApp {
    pub fn new(file: Option<PathBuf>) -> Self {
        let mut app = Self {
            cached_data: Arc::new(Mutex::new(CachedData::default())),
            batch: None,
            cols: Vec::new(),
            column_filters: HashMap::new(),
            filter: String::new(),
            sql_query: "SELECT * FROM data LIMIT 1000".to_string(),
            sql_mode: false,
            offset: 0,
            page: 1_000,
            path: None,
            error: None,
            show_filters: true,
            column_types: HashMap::new(),
            last_filter_time: Instant::now(),
            filter_debounce_timer: None,
            processing: Arc::new(Mutex::new(false)),
            filter_expr_cache: HashMap::new(),
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

        // Use optimized CSV reading with parallel processing
        let mut lf = LazyCsvReader::new(PlPath::from_str(path_str))
            .with_has_header(true)
            .with_encoding(CsvEncoding::LossyUtf8)
            .with_try_parse_dates(true)
            .finish()
            .with_context(|| format!("Failed to read CSV file: {}", path_buf.display()))?;

        let schema = lf
            .collect_schema()
            .with_context(|| "Failed to collect schema from CSV")?;

        self.cols = schema.iter_names().map(|s| s.to_string()).collect();

        // Store column types for better filtering
        self.column_types = schema
            .iter()
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect();

        // Update cached data
        {
            let mut cached_data = self.cached_data.lock().unwrap();
            cached_data.lf = Some(lf);
            cached_data.schema = Some(schema);
            cached_data.last_updated = Instant::now();
        }

        // Initialize column filters
        self.column_filters.clear();
        for col in &self.cols {
            self.column_filters
                .insert(col.clone(), ColumnFilter::default());
        }

        self.path = Some(path_buf);
        self.offset = 0;
        self.sql_mode = false;
        self.error = None;
        self.filter_expr_cache.clear();

        self.try_collect_batch()
            .with_context(|| "Failed to collect initial batch of data")?;

        // Update unique values and column stats in parallel
        self.update_unique_values_parallel()?;
        self.update_column_stats_parallel()?;

        Ok(())
    }

    fn update_unique_values_parallel(&mut self) -> Result<()> {
        let cached_data = self.cached_data.lock().unwrap();
        if let Some(lf) = &cached_data.lf {
            let lf_clone = lf.clone();
            let cols_clone = self.cols.clone();
            drop(cached_data);

            // Process columns in parallel using Rayon
            let unique_values_results: Result<Vec<(String, Vec<String>)>, _> = cols_clone
                .par_iter()
                .map(|column_name| {
                    let unique_values = lf_clone
                        .clone()
                        .select([col(column_name).cast(DataType::String)])
                        .unique(None, UniqueKeepStrategy::First)
                        .sort([column_name], SortMultipleOptions::default())
                        .limit(1000) // Limit to prevent memory issues with large datasets
                        .collect()?
                        .column(column_name)?
                        .str()?
                        .into_iter()
                        .filter_map(|opt_val| opt_val.map(|s| s.to_string()))
                        .collect::<Vec<String>>();

                    Ok((column_name.clone(), unique_values))
                })
                .collect();

            match unique_values_results {
                Ok(results) => {
                    for (column_name, unique_values) in results {
                        if let Some(filter) = self.column_filters.get_mut(&column_name) {
                            filter.update_unique_values(unique_values);
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn update_column_stats_parallel(&mut self) -> Result<()> {
        let cached_data = self.cached_data.lock().unwrap();
        if let Some(lf) = &cached_data.lf {
            let lf_clone = lf.clone();
            let cols_clone = self.cols.clone();
            drop(cached_data);

            // Calculate column statistics in parallel
            let stats_results: Result<Vec<(String, ColumnStats)>, _> = cols_clone
                .par_iter()
                .map(|column_name| {
                    let stats_df = lf_clone
                        .clone()
                        .select([
                            col(column_name).null_count().alias("null_count"),
                            col(column_name).n_unique().alias("unique_count"),
                            col(column_name)
                                .cast(DataType::String)
                                .min()
                                .alias("min_value"),
                            col(column_name)
                                .cast(DataType::String)
                                .max()
                                .alias("max_value"),
                        ])
                        .collect()?;

                    let null_count =
                        stats_df.column("null_count")?.u32()?.get(0).unwrap_or(0) as usize;
                    let unique_count =
                        stats_df.column("unique_count")?.u32()?.get(0).unwrap_or(0) as usize;
                    let min_value = stats_df
                        .column("min_value")?
                        .str()?
                        .get(0)
                        .map(|s| s.to_string());
                    let max_value = stats_df
                        .column("max_value")?
                        .str()?
                        .get(0)
                        .map(|s| s.to_string());

                    let data_type = self
                        .column_types
                        .get(column_name)
                        .cloned()
                        .unwrap_or(DataType::String);

                    let stats = ColumnStats {
                        data_type,
                        null_count,
                        unique_count,
                        min_value,
                        max_value,
                    };

                    Ok((column_name.clone(), stats))
                })
                .collect();

            match stats_results {
                Ok(results) => {
                    let mut cached_data = self.cached_data.lock().unwrap();
                    for (column_name, stats) in results {
                        cached_data.column_stats.insert(column_name, stats);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Fetch a slice of rows according to current filter/offset.
    fn collect_batch(&mut self) {
        if let Err(e) = self.try_collect_batch() {
            self.error = Some(e.to_string());
        }
    }

    fn collect_batch_debounced(&mut self) {
        // Implement debouncing to avoid excessive recomputation
        self.filter_debounce_timer = Some(Instant::now());

        // Only update if enough time has passed since last filter change
        if self.last_filter_time.elapsed() > Duration::from_millis(300) {
            self.collect_batch();
            self.last_filter_time = Instant::now();
        }
    }

    fn try_collect_batch(&mut self) -> Result<()> {
        let cached_data = self.cached_data.lock().unwrap();
        if cached_data.lf.is_none() {
            return Err(anyhow::anyhow!("No CSV file loaded"));
        }

        if self.sql_mode {
            drop(cached_data);
            self.try_execute_sql()
        } else {
            let lf = cached_data.lf.as_ref().unwrap().clone();
            drop(cached_data);
            self.try_collect_filtered_batch(&lf)
        }
    }

    fn build_column_filter_expr_cached(
        &mut self,
        column: &str,
        filter: &ColumnFilter,
    ) -> Option<Expr> {
        if !filter.enabled || !filter.has_active_filters() {
            return None;
        }

        // Check cache first
        let cache_key = format!(
            "{}:{:?}:{}:{}:{}",
            column,
            filter.filter_type,
            filter.value,
            filter.case_sensitive,
            filter.selected_values.len()
        );

        if let Some(cached_expr) = self.filter_expr_cache.get(&cache_key) {
            return Some(cached_expr.clone());
        }

        let expr = self.build_column_filter_expr(column, filter);

        // Cache the expression
        if let Some(ref expr) = expr {
            self.filter_expr_cache.insert(cache_key, expr.clone());
        }

        expr
    }

    fn build_column_filter_expr(&self, column: &str, filter: &ColumnFilter) -> Option<Expr> {
        if !filter.enabled || !filter.has_active_filters() {
            return None;
        }

        let col_expr = col(column);

        match filter.filter_type {
            FilterType::IsEmpty => Some(
                col_expr
                    .clone()
                    .is_null()
                    .or(col_expr.cast(DataType::String).eq(lit(""))),
            ),
            FilterType::IsNotEmpty => Some(
                col_expr
                    .clone()
                    .is_not_null()
                    .and(col_expr.cast(DataType::String).neq(lit(""))),
            ),
            FilterType::DateRange => {
                let mut expr_parts = Vec::new();
                if !filter.date_from.is_empty() {
                    if let Ok(date) =
                        chrono::NaiveDate::parse_from_str(&filter.date_from, "%Y-%m-%d")
                    {
                        expr_parts.push(col_expr.clone().cast(DataType::Date).gt_eq(lit(date)));
                    }
                }
                if !filter.date_to.is_empty() {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(&filter.date_to, "%Y-%m-%d")
                    {
                        expr_parts.push(col_expr.clone().cast(DataType::Date).lt_eq(lit(date)));
                    }
                }
                expr_parts.into_iter().reduce(|acc, expr| acc.and(expr))
            }
            _ => {
                let mut expr_parts = Vec::new();

                // Handle value-based filters
                if !filter.value.is_empty() {
                    let value_expr = if filter.case_sensitive {
                        col_expr.clone().cast(DataType::String)
                    } else {
                        col_expr.clone().cast(DataType::String).str().to_lowercase()
                    };

                    let filter_value = if filter.case_sensitive {
                        filter.value.clone()
                    } else {
                        filter.value.to_lowercase()
                    };

                    let filter_expr = match filter.filter_type {
                        FilterType::Contains => {
                            value_expr.str().contains_literal(lit(filter_value))
                        }
                        FilterType::Exact => value_expr.eq(lit(filter_value)),
                        FilterType::StartsWith => value_expr.str().starts_with(lit(filter_value)),
                        FilterType::EndsWith => value_expr.str().ends_with(lit(filter_value)),
                        FilterType::NotContains => {
                            value_expr.str().contains_literal(lit(filter_value)).not()
                        }
                        FilterType::NotEqual => value_expr.neq(lit(filter_value)),
                        FilterType::GreaterThan => {
                            if let Ok(num) = filter.value.parse::<f64>() {
                                col_expr.clone().cast(DataType::Float64).gt(lit(num))
                            } else {
                                value_expr.gt(lit(filter_value))
                            }
                        }
                        FilterType::LessThan => {
                            if let Ok(num) = filter.value.parse::<f64>() {
                                col_expr.clone().cast(DataType::Float64).lt(lit(num))
                            } else {
                                value_expr.lt(lit(filter_value))
                            }
                        }
                        FilterType::GreaterEqual => {
                            if let Ok(num) = filter.value.parse::<f64>() {
                                col_expr.clone().cast(DataType::Float64).gt_eq(lit(num))
                            } else {
                                value_expr.gt_eq(lit(filter_value))
                            }
                        }
                        FilterType::LessEqual => {
                            if let Ok(num) = filter.value.parse::<f64>() {
                                col_expr.clone().cast(DataType::Float64).lt_eq(lit(num))
                            } else {
                                value_expr.lt_eq(lit(filter_value))
                            }
                        }
                        _ => return None,
                    };
                    expr_parts.push(filter_expr);
                }

                // Handle selected values filter (optimized)
                let selected_values: Vec<String> = filter
                    .selected_values
                    .iter()
                    .filter_map(
                        |(value, &selected)| if selected { Some(value.clone()) } else { None },
                    )
                    .collect();

                if selected_values.len() < filter.unique_values.len()
                    && !filter.unique_values.is_empty()
                {
                    let values_expr = col_expr
                        .cast(DataType::String)
                        .is_in(lit(Series::new("".into(), selected_values)), false);
                    expr_parts.push(values_expr);
                }

                expr_parts.into_iter().reduce(|acc, expr| acc.and(expr))
            }
        }
    }

    fn try_collect_filtered_batch(&mut self, lf: &LazyFrame) -> Result<()> {
        let start_time = Instant::now();
        let mut q = lf.clone();

        // Build all filter expressions in parallel
        let filter_exprs: Vec<Expr> = self
            .cols
            .par_iter()
            .filter_map(|col| {
                self.column_filters
                    .get(col)
                    .and_then(|filter| self.build_column_filter_expr(col, filter))
            })
            .collect();

        // Apply column filters
        if !filter_exprs.is_empty() {
            let combined_filter = filter_exprs
                .into_iter()
                .reduce(|acc, e| acc.and(e))
                .with_context(|| "No valid column filters")?;
            q = q.filter(combined_filter);
        }

        // Apply global text filter
        if !self.filter.is_empty() {
            let global_filter_exprs: Vec<Expr> = self
                .cols
                .par_iter()
                .map(|c| {
                    col(c)
                        .cast(DataType::String)
                        .str()
                        .contains_literal(lit(&*self.filter))
                })
                .collect();

            if !global_filter_exprs.is_empty() {
                let combined_global_filter = global_filter_exprs
                    .into_iter()
                    .reduce(|acc, e| acc.or(e))
                    .with_context(|| "No columns available for global filtering")?;

                q = q.filter(combined_global_filter);
            }
        }

        // Apply pagination with streaming for better memory usage
        q = q.slice(self.offset as i64, self.page as u32);

        let df = q
            .collect()
            .with_context(|| "Failed to collect filtered data")?;

        self.batch = Some(df);

        // Performance monitoring
        let elapsed = start_time.elapsed();
        if elapsed > Duration::from_millis(500) {
            log::warn!("Slow filter operation took {:?}", elapsed);
        }

        Ok(())
    }

    fn try_execute_sql(&mut self) -> Result<()> {
        let cached_data = self.cached_data.lock().unwrap();
        let lf = cached_data
            .lf
            .as_ref()
            .with_context(|| "No CSV file loaded")?;

        // Create SQL context and register the dataframe as "data"
        let mut ctx = SQLContext::new();
        ctx.register("data", lf.clone());
        drop(cached_data);

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

    fn clear_all_filters(&mut self) {
        for filter in self.column_filters.values_mut() {
            filter.enabled = false;
            filter.value.clear();
            filter.date_from.clear();
            filter.date_to.clear();
            filter.invalidate_cache();
            for selected in filter.selected_values.values_mut() {
                *selected = true;
            }
        }
        self.filter.clear();
        self.filter_expr_cache.clear();
        self.offset = 0;
        self.collect_batch();
    }

    fn render_filter_panel(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Column Filters:");
            if ui.button("Clear All").clicked() {
                self.clear_all_filters();
            }
            ui.separator();
            ui.checkbox(&mut self.show_filters, "Show Filters");

            // Performance indicator
            let active_filters = self
                .column_filters
                .values()
                .filter(|f| f.has_active_filters())
                .count();
            if active_filters > 0 {
                ui.colored_label(
                    Color32::from_rgb(100, 150, 255),
                    format!("({} active)", active_filters),
                );
            }
        });

        if !self.show_filters {
            return;
        }

        let cols_clone = self.cols.clone();
        egui::ScrollArea::horizontal().show(ui, |ui| {
            ui.horizontal(|ui| {
                for col in &cols_clone {
                    let has_active_filters = self
                        .column_filters
                        .get(col)
                        .map(|f| f.has_active_filters())
                        .unwrap_or(false);

                    ui.vertical(|ui| {
                        ui.set_width(200.0);

                        // Column header with filter indicator and stats
                        ui.horizontal(|ui| {
                            let color = if has_active_filters {
                                Color32::from_rgb(100, 150, 255)
                            } else {
                                ui.style().visuals.text_color()
                            };
                            ui.colored_label(color, RichText::new(col).strong());

                            if has_active_filters {
                                ui.colored_label(Color32::from_rgb(255, 150, 100), "●");
                            }
                        });

                        // Show column statistics
                        if let Ok(cached_data) = self.cached_data.try_lock() {
                            if let Some(stats) = cached_data.column_stats.get(col) {
                                ui.small(format!("Type: {:?}", stats.data_type));
                                ui.small(format!("Unique: {}", stats.unique_count));
                                if stats.null_count > 0 {
                                    ui.small(format!("Nulls: {}", stats.null_count));
                                }
                            }
                        }

                        ui.separator();

                        // Get filter values to avoid multiple borrows
                        let (
                            mut enabled,
                            mut filter_type,
                            mut value,
                            mut date_from,
                            mut date_to,
                            mut case_sensitive,
                            unique_values,
                            selected_values,
                        ) = {
                            let filter = self.column_filters.get(col).unwrap();
                            (
                                filter.enabled,
                                filter.filter_type.clone(),
                                filter.value.clone(),
                                filter.date_from.clone(),
                                filter.date_to.clone(),
                                filter.case_sensitive,
                                filter.unique_values.clone(),
                                filter.selected_values.clone(),
                            )
                        };

                        // Enable/disable filter
                        if ui.checkbox(&mut enabled, "Enable Filter").changed() {
                            if let Some(filter) = self.column_filters.get_mut(col) {
                                filter.enabled = enabled;
                                filter.invalidate_cache();
                            }
                            self.collect_batch_debounced();
                        }

                        if enabled {
                            // Filter type selection
                            egui::ComboBox::from_label("Type")
                                .selected_text(filter_type.display_name())
                                .show_ui(ui, |ui| {
                                    for ft in FilterType::all() {
                                        if ui
                                            .selectable_value(
                                                &mut filter_type,
                                                ft.clone(),
                                                ft.display_name(),
                                            )
                                            .changed()
                                        {
                                            if let Some(filter) = self.column_filters.get_mut(col) {
                                                filter.filter_type = filter_type.clone();
                                                filter.invalidate_cache();
                                            }
                                            self.collect_batch_debounced();
                                        }
                                    }
                                });

                            // Filter controls based on type
                            match filter_type {
                                FilterType::DateRange => {
                                    ui.label("From:");
                                    if ui.text_edit_singleline(&mut date_from).changed() {
                                        if let Some(filter) = self.column_filters.get_mut(col) {
                                            filter.date_from = date_from.clone();
                                            filter.invalidate_cache();
                                        }
                                        self.collect_batch_debounced();
                                    }
                                    ui.label("To:");
                                    if ui.text_edit_singleline(&mut date_to).changed() {
                                        if let Some(filter) = self.column_filters.get_mut(col) {
                                            filter.date_to = date_to.clone();
                                            filter.invalidate_cache();
                                        }
                                        self.collect_batch_debounced();
                                    }
                                    ui.small("Format: YYYY-MM-DD");
                                }
                                FilterType::IsEmpty | FilterType::IsNotEmpty => {
                                    ui.label("No additional settings");
                                }
                                _ => {
                                    ui.label("Value:");
                                    if ui.text_edit_singleline(&mut value).changed() {
                                        if let Some(filter) = self.column_filters.get_mut(col) {
                                            filter.value = value.clone();
                                            filter.invalidate_cache();
                                        }
                                        self.collect_batch_debounced();
                                    }

                                    if matches!(
                                        filter_type,
                                        FilterType::Contains
                                            | FilterType::NotContains
                                            | FilterType::Exact
                                            | FilterType::NotEqual
                                    ) {
                                        if ui
                                            .checkbox(&mut case_sensitive, "Case sensitive")
                                            .changed()
                                        {
                                            if let Some(filter) = self.column_filters.get_mut(col) {
                                                filter.case_sensitive = case_sensitive;
                                                filter.invalidate_cache();
                                            }
                                            self.collect_batch_debounced();
                                        }
                                    }
                                }
                            }

                            // Unique values selection with performance optimization
                            if !unique_values.is_empty() && unique_values.len() <= 100 {
                                ui.separator();
                                ui.label("Select values:");

                                ui.horizontal(|ui| {
                                    if ui.small_button("All").clicked() {
                                        if let Some(filter) = self.column_filters.get_mut(col) {
                                            for selected in filter.selected_values.values_mut() {
                                                *selected = true;
                                            }
                                            filter.invalidate_cache();
                                        }
                                        self.collect_batch_debounced();
                                    }
                                    if ui.small_button("None").clicked() {
                                        if let Some(filter) = self.column_filters.get_mut(col) {
                                            for selected in filter.selected_values.values_mut() {
                                                *selected = false;
                                            }
                                            filter.invalidate_cache();
                                        }
                                        self.collect_batch_debounced();
                                    }
                                });

                                egui::ScrollArea::vertical()
                                    .max_height(150.0)
                                    .show(ui, |ui| {
                                        for value in &unique_values {
                                            let mut selected =
                                                selected_values.get(value).copied().unwrap_or(true);
                                            if ui.checkbox(&mut selected, value).changed() {
                                                if let Some(filter) =
                                                    self.column_filters.get_mut(col)
                                                {
                                                    filter
                                                        .selected_values
                                                        .insert(value.clone(), selected);
                                                    filter.invalidate_cache();
                                                }
                                                self.collect_batch_debounced();
                                            }
                                        }
                                    });
                            } else if unique_values.len() > 100 {
                                ui.small(format!(
                                    "Too many unique values ({})",
                                    unique_values.len()
                                ));
                                ui.small("Use text filter instead");
                            }

                            if ui.button("Apply Filter").clicked() {
                                self.offset = 0;
                                self.collect_batch();
                            }
                        }
                    });
                    ui.separator();
                }
            });
        });
    }
}

impl App for TimelineExplorerApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        // Handle debounced filter updates
        if let Some(timer) = self.filter_debounce_timer {
            if timer.elapsed() > Duration::from_millis(300) {
                self.collect_batch();
                self.filter_debounce_timer = None;
            }
        }

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

                ui.separator();

                // Performance indicator
                if let Ok(processing) = self.processing.try_lock() {
                    if *processing {
                        ui.colored_label(Color32::YELLOW, "Processing...");
                    }
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
                        let response = ui.text_edit_singleline(&mut self.filter);
                        if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                            self.offset = 0;
                            self.collect_batch();
                        }
                        ui.label("Global Search:");
                    });
                }
            });
        });

        // Filter panel (only show in filter mode)
        if !self.sql_mode {
            let cached_data = self.cached_data.lock().unwrap();
            let has_data = cached_data.lf.is_some();
            drop(cached_data);

            if has_data {
                egui::TopBottomPanel::top("filters").show(ctx, |ui| {
                    self.render_filter_panel(ui);
                });
            }
        }

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
                                // Show filter indicator in header
                                let has_filter = self
                                    .column_filters
                                    .get(col)
                                    .map(|f| f.has_active_filters())
                                    .unwrap_or(false);

                                if has_filter {
                                    ui.horizontal(|ui| {
                                        ui.colored_label(Color32::from_rgb(100, 150, 255), "●");
                                        ui.strong(col);
                                    });
                                } else {
                                    ui.strong(col);
                                }
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

                        // Show active filter count and performance info
                        let active_filters = self
                            .column_filters
                            .values()
                            .filter(|f| f.has_active_filters())
                            .count();
                        if active_filters > 0 {
                            ui.separator();
                            ui.colored_label(
                                Color32::from_rgb(100, 150, 255),
                                format!("{} active filter(s)", active_filters),
                            );
                        }

                        // Show performance metrics
                        ui.separator();
                        ui.small(format!("Cache entries: {}", self.filter_expr_cache.len()));
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
