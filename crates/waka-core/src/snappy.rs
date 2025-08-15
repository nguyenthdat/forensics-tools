#![allow(clippy::cast_precision_loss)]

use std::{
    fs,
    io::{self, BufRead, Read, Write, stdin},
};

use anyhow::anyhow;
use gzp::{ZWriter, par::compress::ParCompressBuilder, snap::Snap};
use serde::Deserialize;

use crate::{config, util};

#[derive(Deserialize)]
struct Args {
    arg_input:      Option<String>,
    flag_output:    Option<String>,
    cmd_compress:   bool,
    cmd_decompress: bool,
    cmd_check:      bool,
    cmd_validate:   bool,
    flag_jobs:      Option<usize>,
    flag_quiet:     bool,
}

pub fn run(argv: &[&str]) -> anyhow::Result<()> {
    let args: Args = util::get_args("", argv)?;

    let input_bytes;

    let input_reader: Box<dyn BufRead> = if let Some(uri) = &args.arg_input {
        let path = uri.to_string();

        let file = fs::File::open(path)?;
        input_bytes = file.metadata()?.len();
        Box::new(io::BufReader::with_capacity(
            config::DEFAULT_RDR_BUFFER_CAPACITY,
            file,
        ))
    } else {
        input_bytes = 0;
        Box::new(io::BufReader::new(stdin().lock()))
    };

    let output_writer: Box<dyn Write + Send + 'static> = match &args.flag_output {
        Some(output_path) => Box::new(io::BufWriter::with_capacity(
            config::DEFAULT_WTR_BUFFER_CAPACITY,
            fs::File::create(output_path)?,
        )),
        None => Box::new(io::BufWriter::with_capacity(
            config::DEFAULT_WTR_BUFFER_CAPACITY,
            io::stdout(),
        )),
    };

    if args.cmd_compress {
        let mut jobs = util::njobs(args.flag_jobs);
        if jobs > 1 {
            jobs -= 1; // save one thread for other tasks
        }

        compress(input_reader, output_writer, jobs, gzp::BUFSIZE * 2)?;
        let compressed_bytes = if let Some(path) = &args.flag_output {
            fs::metadata(path)?.len()
        } else {
            0
        };
        if !args.flag_quiet && compressed_bytes > 0 {
            let compression_ratio = input_bytes as f64 / compressed_bytes as f64;
            tracing::info!(
                "Compression successful. Compressed bytes: {}, Decompressed bytes: {}, \
                 Compression ratio: {:.3}:1, Space savings: {} - {:.2}%",
                compressed_bytes,
                input_bytes,
                compression_ratio,
                input_bytes
                    .checked_sub(compressed_bytes)
                    .unwrap_or_default(),
                (1.0 - (compressed_bytes as f64 / input_bytes as f64)) * 100.0
            );
        }
    } else if args.cmd_decompress {
        let decompressed_bytes = decompress(input_reader, output_writer)?;
        if !args.flag_quiet {
            let compression_ratio = decompressed_bytes as f64 / input_bytes as f64;
            tracing::info!(
                "Decompression successful. Compressed bytes: {}, Decompressed bytes: {}, \
                 Compression ratio: {:.3}:1",
                input_bytes,
                decompressed_bytes,
                compression_ratio,
            );
        }
    } else if args.cmd_validate {
        if args.arg_input.is_none() {
            return Err(anyhow!(
                "stdin is not supported by the snappy validate subcommand."
            ));
        }
        let Ok(decompressed_bytes) = validate(input_reader) else {
            return Err(anyhow!("Not a valid snappy file."));
        };
        if !args.flag_quiet {
            let compression_ratio = decompressed_bytes as f64 / input_bytes as f64;
            tracing::info!(
                "Valid snappy file. Compressed bytes: {}, Decompressed bytes: {}, Compression \
                 ratio: {:.3}:1, Space savings: {} - {:.2}%",
                input_bytes,
                decompressed_bytes,
                compression_ratio,
                decompressed_bytes
                    .checked_sub(input_bytes)
                    .unwrap_or_default(),
                (1.0 - (input_bytes as f64 / decompressed_bytes as f64)) * 100.0
            );
        }
    } else if args.cmd_check {
        let check_ok = check(input_reader);
        if args.flag_quiet {
            if check_ok {
                return Ok(());
            }
            return Err(anyhow!("Not a snappy file."));
        } else if check_ok {
            tracing::info!("Snappy file.");
        } else {
            return Err(anyhow!("Not a snappy file."));
        }
    }

    Ok(())
}

// multithreaded streaming snappy compression
pub fn compress<R: Read, W: Write + Send + 'static>(
    mut src: R,
    dst: W,
    jobs: usize,
    buf_size: usize,
) -> anyhow::Result<()> {
    let mut writer = ParCompressBuilder::<Snap>::new()
        .num_threads(jobs)?
        // the buffer size must be at least gzp::DICT_SIZE
        .buffer_size(if buf_size < gzp::DICT_SIZE {
            gzp::DICT_SIZE
        } else {
            buf_size
        })?
        .pin_threads(Some(0))
        .from_writer(dst);
    io::copy(&mut src, &mut writer)?;
    writer.finish()?;

    Ok(())
}

// single-threaded streaming snappy decompression
fn decompress<R: Read, W: Write>(src: R, mut dst: W) -> anyhow::Result<u64> {
    let mut src = snap::read::FrameDecoder::new(src);
    let decompressed_bytes = io::copy(&mut src, &mut dst)?;

    Ok(decompressed_bytes)
}

// quickly check if a file is a snappy file
// note that the fn only reads the first 50 bytes of the file
// and does not check the entire file for validity
fn check<R: Read>(src: R) -> bool {
    let src = snap::read::FrameDecoder::new(src);

    // read the first 50 or less bytes of a file. The snap decoder will return an error
    // if the file does not start with a valid snappy header
    let mut buffer = Vec::with_capacity(50);
    src.take(50).read_to_end(&mut buffer).is_ok()
}

// validate an entire snappy file by decompressing it to sink (i.e. /dev/null). This is useful for
// checking if a snappy file is corrupted.
// Note that this is more expensive than check() as it has to decompress the entire file.
fn validate<R: Read>(src: R) -> anyhow::Result<u64> {
    let mut src = snap::read::FrameDecoder::new(src);
    let mut sink = io::sink();
    match io::copy(&mut src, &mut sink) {
        Ok(decompressed_bytes) => Ok(decompressed_bytes),
        Err(err) => Err(anyhow!("Error validating snappy file: {err:?}")),
    }
}
