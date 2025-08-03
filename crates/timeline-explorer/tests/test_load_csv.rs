use timeline_explorer::core::csv_loader::load_csv;

#[test]
fn test_load_csv_data_and_build_index() -> anyhow::Result<()> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!("{}/tests/data/mft.csv", manifest_dir);

    // Load the CSV file
    let df = load_csv(path).expect("Failed to load CSV file");
    assert!(!df.is_empty());
    println!("DataFrame loaded successfully with {} rows", df.height());

    // Check if the DataFrame has the expected columns
    let column_names: Vec<_> = df.get_column_names().to_vec();
    assert!(!column_names.is_empty());
    println!("Column names: {:?}", column_names);

    // build the index
    let index = timeline_explorer::index::writer::create_index_from_df(&df)?;
    println!(
        "Index created successfully with {:?} fields",
        index.fields_metadata()?
    );

    // Verify the index creation
    // search for a specific field in the index
    Ok(())
}
