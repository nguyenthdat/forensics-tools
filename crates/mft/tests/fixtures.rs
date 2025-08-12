#![allow(dead_code)]
use std::{path::PathBuf, sync::Once};

static LOGGER_INIT: Once = Once::new();

// Rust runs the tests concurrently, so unless we synchronize logging access
// it will crash when attempting to run `cargo test` with some logging facilities.
pub fn ensure_env_logger_initialized() {
    LOGGER_INIT.call_once(env_logger::init);
}

pub fn samples_dir() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");

    PathBuf::from(manifest_dir)
        .join("samples")
        .canonicalize()
        .unwrap()
}

pub fn mft_sample() -> PathBuf {
    mft_sample_name("MFT")
}

pub fn mft_sample_name(filename: &str) -> PathBuf {
    samples_dir().join(filename)
}
