use csv::ReaderBuilder;
use std::path::Path;
use tantivy::schema::*;

/// Inspect the first `sample_rows` records to guess each column’s type.
/// Falls back to TEXT for mixed/unrecognised data.
pub fn infer_schema<P: AsRef<Path>>(
    csv_path: P,
    sample_rows: usize,
) -> anyhow::Result<(Schema, Vec<Field>)> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(&csv_path)?;
    let headers = rdr.headers()?.clone();

    #[derive(Clone)]
    enum Guess {
        I64,
        F64,
        Bool,
        Date,
        Text,
    }
    let mut guesses = vec![Guess::I64; headers.len()];

    for result in rdr.records().take(sample_rows) {
        let rec = result?;
        for (idx, value) in rec.iter().enumerate() {
            guesses[idx] = match (&guesses[idx], value) {
                (_, "") => Guess::Text, // empty → treat as text so it’s stored
                (Guess::I64, v) if v.parse::<i64>().is_ok() => Guess::I64,
                (Guess::I64, v) | (Guess::F64, v) if v.parse::<f64>().is_ok() => Guess::F64,
                (Guess::I64 | Guess::F64 | Guess::Bool, v)
                    if matches!(v.to_ascii_lowercase().as_str(), "true" | "false") =>
                {
                    Guess::Bool
                }
                // crude ISO-8601 check (needs chrono if you want more)
                (Guess::I64 | Guess::F64 | Guess::Bool | Guess::Date, v)
                    if chrono::DateTime::parse_from_rfc3339(v).is_ok() =>
                {
                    Guess::Date
                }
                _ => Guess::Text,
            };
        }
    }

    // Build Tantivy schema from guesses
    let mut builder = Schema::builder();
    builder.add_u64_field("idx", INDEXED | STORED | FAST);
    let mut fields = Vec::with_capacity(headers.len());

    for (name, guess) in headers.iter().zip(&guesses) {
        let kind = match guess {
            Guess::I64 => builder.add_i64_field(name, INDEXED | STORED | FAST),
            Guess::F64 => builder.add_f64_field(name, INDEXED | STORED | FAST),
            Guess::Bool => builder.add_bool_field(name, INDEXED | STORED | FAST),
            Guess::Date => builder.add_date_field(name, INDEXED | STORED | FAST),
            Guess::Text => builder.add_text_field(name, TEXT | STORED),
        };
        fields.push(kind);
    }

    Ok((builder.build(), fields))
}
