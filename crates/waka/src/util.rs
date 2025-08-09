use std::path::Path;
use ustr::Ustr;

pub fn display_name(path: &str) -> Ustr {
    Path::new(path)
        .file_name()
        .map(|s| Ustr::from(&s.to_string_lossy()))
        .unwrap_or_else(|| Ustr::from(path))
}

pub fn norm<'a>(s: &'a str, casei: bool) -> std::borrow::Cow<'a, str> {
    if casei {
        std::borrow::Cow::Owned(s.to_ascii_lowercase())
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}
