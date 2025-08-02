use polars::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Load CSV file using Polars DataFrame
pub fn load_csv<P: AsRef<Path>>(path: P) -> anyhow::Result<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(path.as_ref().to_path_buf()))?
        .finish()?;

    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_csv() {
        let path =
            "/Users/datnguyen/Projects/forensics-tools/crates/timeline-explorer/tests/data/mft.csv"; // Replace with your test CSV file path
        let df = load_csv(path).expect("Failed to load CSV file");
        assert!(!df.is_empty());
        println!("DataFrame loaded successfully with {} rows", df.height());
    }
}
