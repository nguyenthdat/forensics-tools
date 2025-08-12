mod fixtures;

use std::{
    fs,
    fs::File,
    io::{Read, Write},
    process::Command,
};

use assert_cmd::prelude::*;
use fixtures::*;
use tempfile::tempdir;

#[test]
fn it_respects_directory_output() {
    let d = tempdir().unwrap();
    let f = d.as_ref().join("test.out");

    let sample = mft_sample();

    let mut cmd = Command::cargo_bin("mft_cmd").expect("failed to find binary");
    cmd.args(&["-f", &f.to_string_lossy(), sample.to_str().unwrap()]);

    assert!(
        cmd.output().unwrap().stdout.is_empty(),
        "Expected output to be printed to file, but was printed to stdout"
    );

    let mut expected = vec![];

    File::open(&f).unwrap().read_to_end(&mut expected).unwrap();
    assert!(
        !expected.is_empty(),
        "Expected output to be printed to file"
    )
}

#[test]
fn test_it_refuses_to_overwrite_directory() {
    let d = tempdir().unwrap();

    let sample = mft_sample();
    let mut cmd = Command::cargo_bin("mft_cmd").expect("failed to find binary");
    cmd.args(&["-f", &d.path().to_string_lossy(), sample.to_str().unwrap()]);

    cmd.assert().failure().code(1);
}

#[test]
fn test_non_mft_file_is_error() {
    let d = tempdir().unwrap();

    let f = d.as_ref().join("test.out");

    let mut file = File::create(&f).unwrap();
    file.write_all(b"I'm a file!").unwrap();

    let mut cmd = Command::cargo_bin("mft_cmd").expect("failed to find binary");
    cmd.args(&[f.to_str().unwrap()]);

    cmd.assert().failure().code(1);
}

#[test]
fn test_it_exports_resident_streams() {
    let d = tempdir().unwrap();

    let sample = mft_sample();
    let mut cmd = Command::cargo_bin("mft_cmd").expect("failed to find binary");
    cmd.args(&[
        "-e",
        &d.path().to_string_lossy().to_string(),
        &sample.to_string_lossy().to_string(),
    ]);

    cmd.assert().success();

    assert_eq!(fs::read_dir(d.path()).unwrap().count(), 2142)
}
