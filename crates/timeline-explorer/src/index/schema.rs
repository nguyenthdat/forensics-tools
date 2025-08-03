use polars::frame::DataFrame;
use polars::prelude::*;
use std::collections::HashMap;
use tantivy::schema::{
    FAST, INDEXED, JsonObjectOptions, STORED, SchemaBuilder, TEXT, TextFieldIndexing,
};

/// Build a Tantivy schema from a Polars DataFrame and
/// return the schema plus a map <column-name → Field>
/// so writer code can add documents quickly.
pub fn build_schema_from_df(
    df: &DataFrame,
) -> (
    tantivy::schema::Schema,
    HashMap<String, tantivy::schema::Field>,
) {
    let mut builder = SchemaBuilder::default();

    // Primary key that links Tantivy hits ↔ Polars rows
    let row_id = builder.add_u64_field("row_id", FAST | STORED | INDEXED);

    // For every Polars column add the closest Tantivy field type
    let mut field_map = HashMap::new();
    for field in df.schema().iter_fields() {
        let col_name = field.name().to_string();
        let dtype = field.dtype();

        let field = match dtype {
            DataType::String => builder.add_text_field(&col_name, TEXT | STORED),
            DataType::Int64 | DataType::Int32 => builder.add_i64_field(&col_name, FAST | STORED),
            DataType::UInt64 | DataType::UInt32 => builder.add_u64_field(&col_name, FAST | STORED),
            DataType::Float64 | DataType::Float32 => {
                builder.add_f64_field(&col_name, FAST | STORED)
            }
            DataType::Boolean => builder.add_bool_field(&col_name, STORED),
            DataType::Date | DataType::Datetime(_, _) => {
                builder.add_date_field(&col_name, FAST | STORED)
            }
            // Anything exotic goes into a big JSON field so it’s at least searchable.
            _ => builder.add_json_field(
                &col_name,
                JsonObjectOptions::default()
                    .set_stored()
                    .set_indexing_options(TextFieldIndexing::default()),
            ),
        };
        field_map.insert(col_name.clone(), field);
    }

    field_map.insert("row_id".into(), row_id);
    (builder.build(), field_map)
}
