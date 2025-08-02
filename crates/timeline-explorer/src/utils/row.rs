use polars::prelude::*;

pub fn add_row_id(mut df: DataFrame) -> PolarsResult<DataFrame> {
    let row_count = df.height() as u32;
    // create UInt32 Series [0, 1, 2, …]
    let ids = UInt32Chunked::from_iter_values("row_id".into(), 0..row_count);
    df.with_column(ids.into_series()).cloned() // appends as last column
}
