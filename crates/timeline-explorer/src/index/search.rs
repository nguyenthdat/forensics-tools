use polars::prelude::*;
use std::path::Path;
use tantivy::{Index, collector::TopDocs, query::QueryParser, schema::FieldType};

use crate::core::csv_loader::load_csv;

pub fn search_ids(
    index_dir: impl AsRef<Path>,
    query_str: &str,
    limit: usize,
) -> anyhow::Result<Vec<(f32, u64)>> {
    // ── open index ───────────────────────────────────────────
    let index = Index::open_in_dir(index_dir)?;
    let schema = index.schema();
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // ── build query parser on all text/JSON fields ───────────
    let default_fields = schema
        .fields()
        .filter_map(|(f, e)| {
            matches!(e.field_type(), FieldType::Str(_) | FieldType::JsonObject(_)).then_some(f)
        })
        .collect::<Vec<_>>();

    let query = QueryParser::for_index(&index, default_fields).parse_query(query_str)?;

    // ── run search, collect (score, DocAddress) ──────────────
    let hits = searcher.search(&query, &TopDocs::with_limit(limit))?;

    // ── map hits to (score, idx) using the fast-field ────────
    let mut out = Vec::with_capacity(hits.len());
    for (score, addr) in hits {
        let seg_reader = searcher.segment_reader(addr.segment_ord);
        let reader = seg_reader.fast_fields().u64("idx")?;
        let idx_val = reader.values.get_val(addr.doc_id);
        out.push((score, idx_val));
    }
    Ok(out)
}

pub fn fetch_hits_df<P: AsRef<Path>>(
    csv_path: P,
    hits: &[(f32, u64)],
) -> anyhow::Result<DataFrame> {
    if hits.is_empty() {
        return Ok(DataFrame::empty());
    }

    // ── 1. extract idx values and create rank mapping ─────────────────
    let ids: Vec<u64> = hits.iter().map(|&(_, idx)| idx).collect();

    // Create rank mapping for preserving search order
    let id_to_rank: std::collections::HashMap<u64, u32> = ids
        .iter()
        .enumerate()
        .map(|(pos, &id)| (id, pos as u32))
        .collect();

    // ── 2. lazy scan and filter CSV ────────────────────────────────────
    let df = load_csv(csv_path)?
        .lazy()
        .filter(col("idx").is_in(lit(Series::new("filter_ids".into(), &ids)), false))
        .with_column(
            col("idx")
                .map(
                    move |s| {
                        let ranks: UInt32Chunked = s
                            .u64()?
                            .into_iter()
                            .map(|opt_id| opt_id.and_then(|id| id_to_rank.get(&id).copied()))
                            .collect();
                        Ok(Some(ranks.into_series().into()))
                    },
                    GetOutput::from_type(DataType::UInt32),
                )
                .alias("__search_rank"),
        )
        .sort_by_exprs([col("__search_rank")], SortMultipleOptions::default())
        .collect()?;

    Ok(df)
}

pub fn fetch_hits_with_scores_df<P: AsRef<Path>>(
    csv_path: P,
    hits: &[(f32, u64)],
) -> anyhow::Result<DataFrame> {
    if hits.is_empty() {
        return Ok(DataFrame::empty());
    }

    // ── 1. create lookup maps ─────────────────────────────────────────
    let ids: Vec<u64> = hits.iter().map(|&(_, idx)| idx).collect();
    let id_to_score: std::collections::HashMap<u64, f32> =
        hits.iter().map(|&(score, idx)| (idx, score)).collect();
    let id_to_rank: std::collections::HashMap<u64, u32> = ids
        .iter()
        .enumerate()
        .map(|(pos, &id)| (id, pos as u32))
        .collect();

    // ── 2. lazy scan, filter, and enrich with search metadata ──────────
    let df = load_csv(csv_path)?
        .lazy()
        .filter(col("idx").is_in(lit(Series::new("filter_ids".into(), &ids)), false))
        .with_columns([
            // Add search score
            col("idx")
                .map(
                    move |s| {
                        let scores: Float32Chunked = s
                            .u64()?
                            .into_iter()
                            .map(|opt_id| opt_id.and_then(|id| id_to_score.get(&id).copied()))
                            .collect();
                        Ok(Some(scores.into_series().into()))
                    },
                    GetOutput::from_type(DataType::Float32),
                )
                .alias("search_score"),
            // Add search rank for sorting
            col("idx")
                .map(
                    move |s| {
                        let ranks: UInt32Chunked = s
                            .u64()?
                            .into_iter()
                            .map(|opt_id| opt_id.and_then(|id| id_to_rank.get(&id).copied()))
                            .collect();
                        Ok(Some(ranks.into_series().into()))
                    },
                    GetOutput::from_type(DataType::UInt32),
                )
                .alias("__search_rank"),
        ])
        .sort_by_exprs([col("__search_rank")], SortMultipleOptions::default())
        .collect()?;

    Ok(df)
}
