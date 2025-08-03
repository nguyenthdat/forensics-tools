use timeline_explorer::{
    core::csv_loader::load_csv,
    index::{
        search::{fetch_hits_df, search_ids},
        writer::index_csv,
    },
};

#[test]
fn test_load_csv_data_and_build_index() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!("{}/tests/data/mft.csv", manifest_dir);
    let temp_dir = tempfile::tempdir()?;

    // Load the CSV file
    let df = load_csv(&path).expect("Failed to load CSV file");
    assert!(!df.is_empty());
    println!("DataFrame loaded successfully with {} rows", df.height());

    // Check if the DataFrame has the expected columns
    let column_names: Vec<_> = df.get_column_names().to_vec();
    assert!(!column_names.is_empty());
    println!("Column names: {:?}", column_names);

    // build the index
    let index = index_csv(
        &path,
        &temp_dir.path().to_string_lossy().to_string(),
        10,
        512,
    )?;
    println!(
        "Index created successfully with {:?} fields",
        index.fields_metadata()?
    );

    // Verify the index creation
    // search for a specific field in the index
    Ok(())
}

#[test]
fn test_index_search() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!("{}/tests/data/mft.csv", manifest_dir);
    let temp_dir = tempfile::tempdir()?;

    // Load the CSV file and build the index
    index_csv(
        &path,
        &temp_dir.path().to_string_lossy().to_string(),
        10,
        512,
    )?;

    // Perform a search on the indexed data
    let results = search_ids(&temp_dir.path(), "volume", 10_000)?;

    assert!(!results.is_empty());
    println!("Search results: {:?}", results);

    let df = fetch_hits_df(&path, &results).expect("Failed to fetch hits DataFrame");
    assert!(!df.is_empty());
    println!("Fetched hits DataFrame with {} rows", df);

    Ok(())
}
