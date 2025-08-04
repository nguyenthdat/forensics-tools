use csv::ReaderBuilder;
use std::path::Path;
use tantivy::schema::*;

/// Inspect the first `sample_rows` records to guess each column's type.
/// Falls back to TEXT for mixed/unrecognised data.
pub fn infer_schema_from_csv<P: AsRef<Path>>(
    csv_path: P,
    sample_rows: usize,
) -> anyhow::Result<(Schema, Vec<Field>)> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .buffer_capacity(8 * 1024)
        .from_path(&csv_path)?;
    let headers = rdr.headers()?.clone();

    #[derive(Clone, Copy, PartialEq)]
    enum Guess {
        I64,
        F64,
        Bool,
        Date,
        Text,
    }

    let mut guesses = vec![Guess::I64; headers.len()];
    let mut records_processed = 0;

    for result in rdr.records() {
        if records_processed >= sample_rows {
            break;
        }

        let rec = result?;
        for (idx, value) in rec.iter().enumerate() {
            if idx >= guesses.len() {
                break; // Safety check
            }

            // Skip if already determined to be text (most permissive type)
            if guesses[idx] == Guess::Text {
                continue;
            }

            guesses[idx] = match (&guesses[idx], value) {
                (_, "") => continue, // Skip empty values instead of forcing to text
                (Guess::I64, v) => {
                    if fast_parse_i64(v).is_some() {
                        Guess::I64
                    } else if fast_parse_f64(v).is_some() {
                        Guess::F64
                    } else if is_bool_fast(v) {
                        Guess::Bool
                    } else if is_iso8601_fast(v) {
                        Guess::Date
                    } else {
                        Guess::Text
                    }
                }
                (Guess::F64, v) => {
                    if fast_parse_f64(v).is_some() {
                        Guess::F64
                    } else if is_bool_fast(v) {
                        Guess::Bool
                    } else if is_iso8601_fast(v) {
                        Guess::Date
                    } else {
                        Guess::Text
                    }
                }
                (Guess::Bool, v) => {
                    if is_bool_fast(v) {
                        Guess::Bool
                    } else if is_iso8601_fast(v) {
                        Guess::Date
                    } else {
                        Guess::Text
                    }
                }
                (Guess::Date, v) => {
                    if is_iso8601_fast(v) {
                        Guess::Date
                    } else {
                        Guess::Text
                    }
                }
                (Guess::Text, _) => Guess::Text,
            };
        }
        records_processed += 1;
    }

    // Build Tantivy schema from guesses
    let mut builder = Schema::builder();
    builder.add_u64_field("row_id", INDEXED | STORED | FAST);
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

// Fast parsers that avoid allocations
#[inline]
fn fast_parse_i64(s: &str) -> Option<i64> {
    s.parse().ok()
}

#[inline]
fn fast_parse_f64(s: &str) -> Option<f64> {
    s.parse().ok()
}

#[inline]
fn is_bool_fast(s: &str) -> bool {
    matches!(s, "true" | "false" | "True" | "False" | "TRUE" | "FALSE")
}

#[inline]
fn is_iso8601_fast(s: &str) -> bool {
    // Quick heuristic check before expensive parsing
    if s.len() < 10 || s.len() > 35 {
        return false;
    }

    // Look for basic ISO8601 patterns
    if s.contains('T') || s.contains('-') {
        chrono::DateTime::parse_from_rfc3339(s).is_ok()
    } else {
        false
    }
}
