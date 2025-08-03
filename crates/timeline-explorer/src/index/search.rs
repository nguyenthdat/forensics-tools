use std::path::Path;
use tantivy::{Index, collector::TopDocs, query::QueryParser, schema::FieldType};

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
    let hits = searcher.search(&query, &TopDocs::with_limit(limit))?; // returns (Score, DocAddress) [oai_citation:2‡docs.rs](https://docs.rs/tantivy/)

    // ── map hits to (score, idx) using the fast-field ────────
    let mut out = Vec::with_capacity(hits.len());
    for (score, addr) in hits {
        let seg_reader = searcher.segment_reader(addr.segment_ord);
        let reader = seg_reader.fast_fields().u64("idx")?; // FAST lookup
        let idx_val = reader.values.get_val(addr.doc_id);
        out.push((score, idx_val));
    }
    Ok(out)
}
