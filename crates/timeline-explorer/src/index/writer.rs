use std::path::Path;
use std::sync::Arc;

use csv::ReaderBuilder;
use rayon::prelude::*;
use tantivy::{DateTime as TantivyDateTime, Index, TantivyDocument, schema::FieldType};

use crate::index::schema::infer_schema_from_csv;

pub fn index_csv<P: AsRef<Path>>(
    csv_path: P,
    index_dir: P,
    sample_rows: usize,
    writer_mem_mb: usize,
) -> anyhow::Result<Index> {
    // 1. determine schema from sample
    let (schema, fields) = infer_schema_from_csv(&csv_path, sample_rows)?;

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

pub fn index_csv_streaming<P: AsRef<Path>>(
    csv_path: P,
    index_dir: P,
    sample_rows: usize,
    writer_mem_mb: usize,
) -> anyhow::Result<Index> {
    use lexical_core::parse as fast_parse;
    use speedate::DateTime as FastDate;

    // --- 1. schema -----------------------------------------------------------
    let (schema, fields) = infer_schema_from_csv(&csv_path, sample_rows)?;
    let idx_field = schema.get_field("idx")?;

    // --- 2. writer -----------------------------------------------------------
    let index = Index::create_in_dir(index_dir, schema.clone())?;
    let mut w = index.writer(writer_mem_mb * 1_048_576)?;

    // Pre-compute column meta once
    let col_meta: Vec<(tantivy::schema::Field, FieldType)> = fields
        .iter()
        .map(|&f| (f, schema.get_field_entry(f).field_type().clone()))
        .collect();

    // --- 3. CSV streaming ----------------------------------------------------
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&csv_path)?;
    let mut rec = csv::ByteRecord::new();

    while rdr.read_byte_record(&mut rec)? {
        let mut doc = TantivyDocument::default();
        doc.add_u64(idx_field, rdr.position().line() as u64); // unique row id

        for (i, bytes) in rec.iter().enumerate() {
            if bytes.is_empty() || i >= col_meta.len() {
                continue;
            }
            let (field, ftype) = &col_meta[i];

            match ftype {
                FieldType::I64(_) | FieldType::U64(_) => {
                    if let Ok(n) = fast_parse::<i64>(bytes) {
                        doc.add_i64(*field, n)
                    }
                }
                FieldType::F64(_) => {
                    if let Ok(n) = fast_parse::<f64>(bytes) {
                        doc.add_f64(*field, n)
                    }
                }
                FieldType::Bool(_) => {
                    if matches!(bytes, b"true" | b"TRUE" | b"1") {
                        doc.add_bool(*field, true)
                    } else if matches!(bytes, b"false" | b"FALSE" | b"0") {
                        doc.add_bool(*field, false)
                    }
                }
                FieldType::Date(_) => {
                    if let Ok(dt) = FastDate::parse_str(std::str::from_utf8(bytes)?) {
                        doc.add_date(
                            *field,
                            TantivyDateTime::from_timestamp_micros(dt.timestamp_ms()),
                        );
                    }
                }
                _ => doc.add_text(*field, std::str::from_utf8(bytes)?),
            }
        }

        if !doc.field_values().next().is_none() {
            w.add_document(doc)?; // already fan-out indexed internally
        }
    }

    // --- 4. commit -----------------------------------------------------------
    w.commit()?;
    w.wait_merging_threads()?;
    Ok(index)
}
