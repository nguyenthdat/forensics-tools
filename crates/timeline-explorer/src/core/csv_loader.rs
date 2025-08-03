use polars::prelude::*;
use std::path::Path;

/// Load CSV file using Polars DataFrame
pub fn load_csv<P: AsRef<Path>>(path: P) -> anyhow::Result<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(path.as_ref().to_path_buf()))?
        .finish()?;

    let df_with_id = df.lazy().with_row_index("id", Some(1)).collect()?;

    Ok(df_with_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    // First filename: ["Signature", "EntryId", "Sequence", "BaseEntryId", "BaseEntrySequence", "HardLinkCount", "Flags", "UsedEntrySize", "TotalEntrySize", "FileSize", "IsADirectory", "IsDeleted", "HasAlternateDataStreams", "StandardInfoFlags", "StandardInfoLastModified", "StandardInfoLastAccess", "StandardInfoCreated", "FileNameFlags", "FileNameLastModified", "FileNameLastAccess", "FileNameCreated", "FullPath"]

    #[test]
    fn test_load_csv() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{}/tests/data/mft.csv", manifest_dir); // Adjust the

        let df = load_csv(path).expect("Failed to load CSV file");
        assert!(!df.is_empty());
        println!("DataFrame loaded successfully with {} rows", df.height());

        let column = df
            .column_iter()
            .map(|s| s.name().to_string())
            .collect::<Vec<_>>();

        assert!(!column.is_empty());
        println!("First filename: {:?}", column);
    }
}
