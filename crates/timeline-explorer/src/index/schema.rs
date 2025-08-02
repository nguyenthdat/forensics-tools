use polars::{
    frame::DataFrame,
    prelude::{DataType, SchemaExt},
};
use rayon::prelude::*;
use tantivy::Document;
use tantivy::{Index, schema::*};

pub fn build_schema(df: &DataFrame) -> (Schema, Field) {
    let mut builder = SchemaBuilder::default();
    // primary key
    let row_id = builder.add_u64_field("row_id", FAST | STORED);
    // loop over DataFrame schema to add fields
    for field in df.schema().iter_fields() {
        let name = field.name();
        let dtype = field.dtype();
        match dtype {
            DataType::String => {
                builder.add_text_field(name, TEXT | STORED);
            }
            DataType::Int64 | DataType::UInt64 => {
                builder.add_i64_field(name, FAST | STORED);
            }
            DataType::Date | DataType::Datetime(_, _) => {
                builder.add_date_field(name, FAST | STORED);
            }
            _ => { /* skip or custom logic */ }
        }
    }
    (builder.build(), row_id)
}
