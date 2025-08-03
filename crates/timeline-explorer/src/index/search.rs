//! Tantivy search helpers – keep the API minimal for now but fast enough for egui's
//! "type‑ahead" query bar.
//
//  Typical usage:
//
//  ```rust
//  let searcher = TantivySearch::new(index.clone())?;
//  let hits = searcher.search_row_ids("svchost AND ext:.lnk", 500)?;
//  let df_slice = searcher.search_dataframe(&df, "svchost", 1_000)?;
//  ```

use anyhow::{Context, Result, anyhow};
use polars::prelude::*;
use std::collections::HashMap;
use tantivy::{
    Index, IndexReader, Score, TantivyDocument,
    collector::TopDocs,
    query::QueryParser,
    schema::{Field, FieldType, Value},
};

/// Search results with metadata
#[derive(Debug)]
pub struct SearchResults {
    pub row_ids: Vec<u32>,
    pub scores: Vec<Score>,
    pub total_hits: usize,
}

/// Small wrapper that owns a `tantivy::IndexReader` and knows which field is `row_id`.
pub struct TantivySearch {
    index: Index,
    reader: IndexReader,
    row_id_field: Field,
    /// All schema fields that are `TEXT` so the default query parser can use them.
    text_fields: Vec<Field>,
    /// Field name to Field mapping for advanced queries
    field_map: HashMap<String, Field>,
}

impl TantivySearch {
    /// Build from an already‑created `Index`.
    pub fn new(index: Index) -> Result<Self> {
        let schema = index.schema();

        // 1) find the primary‑key field
        let row_id_field = schema
            .get_field("row_id")
            .map_err(|_| anyhow!("Index schema does not contain 'row_id' field"))?;

        // 2) harvest all TEXT fields for free‑text searches
        let mut text_fields = Vec::new();
        let mut field_map = HashMap::new();

        for (field, entry) in schema.fields() {
            let field_name = entry.name().to_string();
            field_map.insert(field_name, field);

            match entry.field_type() {
                FieldType::Str(_) => {
                    text_fields.push(field);
                }
                _ => {}
            }
        }

        let reader = index
            .reader()
            .with_context(|| "Failed to open IndexReader")?;

        Ok(Self {
            index,
            reader,
            row_id_field,
            text_fields,
            field_map,
        })
    }

    /// Get field by name for advanced queries
    pub fn get_field(&self, name: &str) -> Option<Field> {
        self.field_map.get(name).copied()
    }

    /// List all available field names
    pub fn list_fields(&self) -> Vec<String> {
        self.field_map.keys().cloned().collect()
    }

    /// Enhanced search with detailed results
    pub fn search_detailed(&self, query_str: &str, limit: usize) -> Result<SearchResults> {
        // Validate inputs
        if query_str.trim().is_empty() {
            return Ok(SearchResults {
                row_ids: Vec::new(),
                scores: Vec::new(),
                total_hits: 0,
            });
        }

        if limit == 0 {
            return Ok(SearchResults {
                row_ids: Vec::new(),
                scores: Vec::new(),
                total_hits: 0,
            });
        }

        // Ensure we have the newest segment data
        self.reader.reload()?;
        let searcher = self.reader.searcher();

        // Handle empty text fields case
        if self.text_fields.is_empty() {
            log::warn!("No text fields available for search");
            return Ok(SearchResults {
                row_ids: Vec::new(),
                scores: Vec::new(),
                total_hits: 0,
            });
        }

        let parser = QueryParser::for_index(&self.index, self.text_fields.clone());
        let query = parser
            .parse_query(query_str)
            .with_context(|| format!("Failed to parse query: `{}`", query_str))?;

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit))
            .with_context(|| format!("Search failed for query: `{}`", query_str))?;

        let mut row_ids = Vec::with_capacity(top_docs.len());
        let mut scores = Vec::with_capacity(top_docs.len());

        for (score, addr) in top_docs {
            let doc: TantivyDocument = searcher
                .doc(addr)
                .with_context(|| format!("Failed to retrieve document at address: {:?}", addr))?;

            if let Some(id_val) = doc.get_first(self.row_id_field).and_then(|v| v.as_i64()) {
                row_ids.push(id_val as u32);
                scores.push(score);
            } else {
                log::warn!("Document missing row_id field: {:?}", doc);
            }
        }

        Ok(SearchResults {
            total_hits: row_ids.len(),
            row_ids,
            scores,
        })
    }

    /// Convenience: run `query_str`, gather up to `limit` row_ids.
    pub fn search_row_ids(&self, query_str: &str, limit: usize) -> Result<Vec<u32>> {
        Ok(self.search_detailed(query_str, limit)?.row_ids)
    }

    /// Return a **Polars DataFrame slice** that corresponds to the query hits.
    ///
    /// The returned DataFrame borrows column chunks from the original one
    /// (via `DataFrame::take`) so it's cheap even for thousands of rows.
    pub fn search_dataframe(
        &self,
        df: &DataFrame,
        query_str: &str,
        limit: usize,
    ) -> Result<DataFrame> {
        let results = self.search_detailed(query_str, limit)?;
        if results.row_ids.is_empty() {
            // Return an empty slice with the same schema
            return Ok(df.head(Some(0)));
        }

        // Validate row_ids are within bounds
        let max_row = df.height() as u32;
        let valid_ids: Vec<u32> = results
            .row_ids
            .into_iter()
            .filter(|&id| id < max_row)
            .collect();

        if valid_ids.is_empty() {
            log::warn!("No valid row IDs found within DataFrame bounds");
            return Ok(df.head(Some(0)));
        }

        let id_series = UInt32Chunked::from_vec("row_indices".into(), valid_ids);
        df.take(&id_series)
            .with_context(|| "Failed to slice DataFrame with search results")
    }

    /// Search with pagination support
    pub fn search_paginated(
        &self,
        df: &DataFrame,
        query_str: &str,
        page: usize,
        page_size: usize,
    ) -> Result<(DataFrame, usize)> {
        let total_limit = (page + 1) * page_size;
        let results = self.search_detailed(query_str, total_limit)?;

        let start_idx = page * page_size;
        let end_idx = std::cmp::min(start_idx + page_size, results.row_ids.len());

        if start_idx >= results.row_ids.len() {
            return Ok((df.head(Some(0)), results.total_hits));
        }

        let page_ids = &results.row_ids[start_idx..end_idx];
        if page_ids.is_empty() {
            return Ok((df.head(Some(0)), results.total_hits));
        }

        let id_series = UInt32Chunked::from_vec("row_indices".into(), page_ids.to_vec());
        let page_df = df
            .take(&id_series)
            .with_context(|| "Failed to slice DataFrame for pagination")?;

        Ok((page_df, results.total_hits))
    }

    /// Count total matches without retrieving documents
    pub fn count_matches(&self, query_str: &str) -> Result<usize> {
        if query_str.trim().is_empty() {
            return Ok(0);
        }

        self.reader.reload()?;
        let searcher = self.reader.searcher();

        if self.text_fields.is_empty() {
            return Ok(0);
        }

        let parser = QueryParser::for_index(&self.index, self.text_fields.clone());
        let query = parser
            .parse_query(query_str)
            .with_context(|| format!("Failed to parse query: `{}`", query_str))?;

        let count = searcher.search(&query, &tantivy::collector::Count)?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::csv_loader::load_csv, index::writer::create_index_from_df};

    /// Quick smoke‑test: index a tiny DataFrame and search.
    #[test]
    fn test_search_slice() -> Result<()> {
        // Build a sample DataFrame
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{}/tests/data/mft.csv", manifest_dir);

        // Load the CSV file
        let df = load_csv(path).expect("Failed to load CSV file");

        let index = create_index_from_df(&df)?;
        let engine = TantivySearch::new(index)?;

        // search for text present in row 1
        let hits = engine.search_row_ids("svchost", 10)?;
        println!("Hits: {:?}", hits);
        // assert_eq!(hits, vec![1]);

        let slice = engine.search_dataframe(&df, "svchost", 10)?;
        // assert_eq!(
        //     slice.column("name")?.get(0)?,
        //     AnyValue::String("svchost.exe")
        // );
        println!("Slice: {:?}", slice);
        Ok(())
    }

    #[test]
    fn test_search_detailed() -> Result<()> {
        let df = df![
            "name" => &["test1.txt", "test2.exe", "other.log"],
            "content" => &["hello world", "foo bar", "baz qux"]
        ]?;

        let index = create_index_from_df(&df)?;
        let engine = TantivySearch::new(index)?;

        let results = engine.search_detailed("hello", 10)?;
        assert_eq!(results.row_ids.len(), 1);
        assert_eq!(results.row_ids[0], 0);
        assert!(results.scores[0] > 0.0);

        Ok(())
    }

    #[test]
    fn test_empty_query() -> Result<()> {
        let df = df![
            "name" => &["test.txt"],
            "content" => &["hello"]
        ]?;

        let index = create_index_from_df(&df)?;
        let engine = TantivySearch::new(index)?;

        let results = engine.search_detailed("", 10)?;
        assert_eq!(results.row_ids.len(), 0);

        let results = engine.search_detailed("   ", 10)?;
        assert_eq!(results.row_ids.len(), 0);

        Ok(())
    }

    #[test]
    fn test_pagination() -> Result<()> {
        let df = df![
            "name" => &["file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"],
            "content" => &["test", "test", "test", "test", "test"]
        ]?;

        let index = create_index_from_df(&df)?;
        let engine = TantivySearch::new(index)?;

        let (page1, total) = engine.search_paginated(&df, "test", 0, 2)?;
        assert_eq!(page1.height(), 2);
        assert_eq!(total, 5);

        let (page2, _) = engine.search_paginated(&df, "test", 1, 2)?;
        assert_eq!(page2.height(), 2);

        Ok(())
    }
}
