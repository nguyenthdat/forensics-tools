use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FileType {
    Zip,
    Csv,
    Json,
    Parquet,
    Mft,
    Unknown,
}

pub fn detect_file_type(path: &str) -> FileType {
    if path.ends_with(".zip") {
        FileType::Zip
    } else if path.ends_with(".csv") {
        FileType::Csv
    } else if path.ends_with(".json") {
        FileType::Json
    } else if path.ends_with(".parquet") {
        FileType::Parquet
    } else if path == "$MFT" || path.ends_with(".mft") {
        FileType::Mft
    } else {
        FileType::Unknown
    }
}
