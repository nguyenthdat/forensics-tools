use std::path::Path;
use std::sync::Arc;

use csv::ReaderBuilder;
use rayon::prelude::*;
use tantivy::{DateTime as TantivyDateTime, Index, TantivyDocument, schema::FieldType};

use crate::index::schema::infer_schema;

pub fn index_csv<P: AsRef<Path>>(
    csv_path: P,
    index_dir: P,
    sample_rows: usize,
    writer_mem_mb: usize,
) -> anyhow::Result<Index> {
    // 1. determine schema from sample
    let (schema, fields) = infer_schema(&csv_path, sample_rows)?;

    // Get the idx field from the schema
    let idx_field = schema.get_field("idx")?;

    // 2. create index & writer
    let index = Index::create_in_dir(index_dir, schema.clone())?;
    let mut writer = index.writer((writer_mem_mb as usize) * 1_048_576)?; // MB → bytes

    // 3. read all CSV records into memory first
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(&csv_path)?;

    let records: Result<Vec<_>, _> = rdr.records().collect();
    let records = records?;

    // 4. process records in parallel and collect documents
    let schema_arc = Arc::new(schema);
    let fields_arc = Arc::new(fields);

    let documents: Vec<TantivyDocument> = records
        .into_par_iter()
        .enumerate()
        .filter_map(|(row_idx, rec)| {
            let mut doc = TantivyDocument::default();
            let schema = Arc::clone(&schema_arc);
            let fields = Arc::clone(&fields_arc);
            doc.add_u64(idx_field, row_idx as u64);
            for (idx, value) in rec.iter().enumerate() {
                if idx >= fields.len() {
                    break;
                }

                let field = fields[idx];
                let schema_field = schema.get_field_entry(field);

                if value.is_empty() {
                    continue; // skip empties
                }

                match schema_field.field_type() {
                    FieldType::I64(_) | FieldType::U64(_) => {
                        if let Ok(v) = value.parse::<i64>() {
                            doc.add_i64(field, v)
                        }
                    }
                    FieldType::F64(_) => {
                        if let Ok(v) = value.parse::<f64>() {
                            doc.add_f64(field, v)
                        }
                    }
                    FieldType::Bool(_) => {
                        if let Ok(v) = value.parse::<bool>() {
                            doc.add_bool(field, v)
                        }
                    }
                    FieldType::Date(_) => {
                        if let Ok(v) = chrono::DateTime::parse_from_rfc3339(value) {
                            let timestamp_nanos = v.timestamp();
                            doc.add_date(
                                field,
                                TantivyDateTime::from_timestamp_nanos(timestamp_nanos),
                            )
                        }
                    }
                    _ => doc.add_text(field, value),
                };
            }

            // Only return documents that have at least one field
            if doc.field_values().next().is_some() {
                Some(doc)
            } else {
                None
            }
        })
        .collect();

    // 5. add all documents to writer sequentially (writer is not thread-safe)
    for doc in documents {
        writer.add_document(doc)?;
    }

    // 6. commit & finish
    writer.commit()?;
    writer.wait_merging_threads()?; // optional but helpful for small datasets
    Ok(index)
}
