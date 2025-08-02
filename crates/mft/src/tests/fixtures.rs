use std::path::PathBuf;

pub fn mft_sample() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir).join("samples").join("MFT")
}
