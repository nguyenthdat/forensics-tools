use std::path::Path;

use ustr::Ustr;

pub fn display_name(path: &str) -> Ustr {
    Path::new(path)
        .file_name()
        .map(|s| Ustr::from(&s.to_string_lossy()))
        .unwrap_or_else(|| Ustr::from(path))
}
