use polars::prelude::*;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tantivy::directory::MmapDirectory;
use tantivy::{Index, ReloadPolicy, TantivyDocument};

use super::schema::build_schema_from_df;

/// Create (or open) an index directory and fill it from a DataFrame.
/// Returns the opened `Index` so the caller can immediately search.
pub fn create_index_from_df(df: &DataFrame) -> anyhow::Result<Index> {
    // 1) Build schema
    let (schema, field_map) = build_schema_from_df(df);

    // 2) Create / open index on disk (use the provided directory)
    // Use an in‑memory directory during tests to skip slow disk I/O.

    let dir = MmapDirectory::create_from_tempdir()?;
    let index = Index::open_or_create(dir, schema.clone())?;

    // 3) Prepare a writer (adjust RAM budget as needed)
    let writer = Arc::new(index.writer(256 * 1024 * 1024)?); // 256 MB buffer
    let columns = df.get_columns();
    let series: Vec<&Series> = columns
        .iter()
        .map(|s: &Column| s.as_series().expect("Failed to convert to Series"))
        .collect();

    // Collect errors from parallel processing
    let errors: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));

    // 4) Build documents in parallel with improved error handling
    (0..df.height()).into_par_iter().for_each(|row_idx| {
        match create_document_for_row(row_idx, &series, &field_map) {
            Ok(document) => {
                // SAFETY: add_document is Send + Sync according to tantivy docs
                if let Err(e) = writer.add_document(document) {
                    let error_msg = format!("Failed to add document at row {}: {}", row_idx, e);
                    log::error!("{}", error_msg);
                    if let Ok(mut error_queue) = errors.lock() {
                        error_queue.push_back(error_msg);
                        // Limit error queue size to prevent memory issues
                        if error_queue.len() > 1000 {
                            error_queue.pop_front();
                        }
                    }
                }
            }
            Err(e) => {
                let error_msg = format!("Failed to create document for row {}: {}", row_idx, e);
                log::error!("{}", error_msg);
                if let Ok(mut error_queue) = errors.lock() {
                    error_queue.push_back(error_msg);
                    if error_queue.len() > 1000 {
                        error_queue.pop_front();
                    }
                }
            }
        }
    });

    // Check if we had too many errors
    let error_count = errors.lock().map(|e| e.len()).unwrap_or(0);
    if error_count > 0 {
        log::warn!(
            "Encountered {} errors during document creation",
            error_count
        );
        // If more than 10% of documents failed, consider it a critical error
        if error_count as f64 / df.height() as f64 > 0.1 {
            return Err(anyhow::anyhow!(
                "Too many errors during indexing: {}/{} documents failed",
                error_count,
                df.height()
            ));
        }
    }

    // 5) Commit once all threads are done
    Arc::try_unwrap(writer)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap IndexWriter - still has references"))?
        .commit()
        .map_err(|e| anyhow::anyhow!("Failed to commit index: {}", e))?;

    // 6) Create reader
    index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;

    Ok(index)
}

/// Create a document for a single row with proper error handling
fn create_document_for_row(
    row_idx: usize,
    columns: &[&Series],
    field_map: &std::collections::HashMap<String, tantivy::schema::Field>,
) -> anyhow::Result<TantivyDocument> {
    let mut document = TantivyDocument::default();

    // Add primary key
    let row_id_field = field_map
        .get("row_id")
        .ok_or_else(|| anyhow::anyhow!("Missing row_id field in schema"))?;
    document.add_u64(*row_id_field, row_idx as u64);

    for s in columns {
        let field_name = s.name();
        let field = field_map
            .get(field_name.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing field '{}' in schema", field_name))?;

        // Skip null values safely
        match s.dtype() {
            DataType::String => {
                if let Ok(str_series) = s.str() {
                    if let Some(txt) = str_series.get(row_idx) {
                        document.add_text(*field, txt);
                    }
                }
            }
            DataType::Int64 => {
                if let Ok(i64_series) = s.i64() {
                    if let Some(val) = i64_series.get(row_idx) {
                        document.add_i64(*field, val);
                    }
                }
            }
            DataType::Int32 => {
                if let Ok(i32_series) = s.i32() {
                    if let Some(val) = i32_series.get(row_idx) {
                        document.add_i64(*field, val as i64);
                    }
                }
            }
            DataType::UInt64 => {
                if let Ok(u64_series) = s.u64() {
                    if let Some(val) = u64_series.get(row_idx) {
                        document.add_u64(*field, val);
                    }
                }
            }
            DataType::UInt32 => {
                if let Ok(u32_series) = s.u32() {
                    if let Some(val) = u32_series.get(row_idx) {
                        document.add_u64(*field, val as u64);
                    }
                }
            }
            DataType::Float64 => {
                if let Ok(f64_series) = s.f64() {
                    if let Some(val) = f64_series.get(row_idx) {
                        if val.is_finite() {
                            document.add_f64(*field, val);
                        }
                    }
                }
            }
            DataType::Float32 => {
                if let Ok(f32_series) = s.f32() {
                    if let Some(val) = f32_series.get(row_idx) {
                        if val.is_finite() {
                            document.add_f64(*field, val as f64);
                        }
                    }
                }
            }
            DataType::Boolean => {
                if let Ok(bool_series) = s.bool() {
                    if let Some(val) = bool_series.get(row_idx) {
                        document.add_bool(*field, val);
                    }
                }
            }
            DataType::Date | DataType::Datetime(_, _) => {
                let any_value = s.get(row_idx)?;
                if let Ok(ts) = any_value.try_extract::<i64>() {
                    document.add_date(*field, tantivy::DateTime::from_timestamp_secs(ts));
                }
            }
            _ => {
                // Fallback: convert to string representation
                let any_value = s.get(row_idx)?;
                if !any_value.is_null() {
                    document.add_text(*field, &any_value.to_string());
                }
            }
        }
    }

    Ok(document)
}
