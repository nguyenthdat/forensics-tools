use std::{
    fs, io, ops,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use csv_index::RandomAccessSimple;

use crate::{
    config::{Config, DEFAULT_WTR_BUFFER_CAPACITY},
    util,
};

/// Indexed composes a CSV reader with a simple random access index.
pub struct Indexed<R, I> {
    csv_rdr: csv::Reader<R>,
    idx:     RandomAccessSimple<I>,
}

impl<R, I> ops::Deref for Indexed<R, I> {
    type Target = csv::Reader<R>;

    fn deref(&self) -> &csv::Reader<R> {
        &self.csv_rdr
    }
}

impl<R, I> ops::DerefMut for Indexed<R, I> {
    fn deref_mut(&mut self) -> &mut csv::Reader<R> {
        &mut self.csv_rdr
    }
}

impl<R: io::Read + io::Seek, I: io::Read + io::Seek> Indexed<R, I> {
    /// Opens an index.
    pub fn open(csv_rdr: csv::Reader<R>, idx_rdr: I) -> anyhow::Result<Indexed<R, I>> {
        Ok(Indexed {
            csv_rdr,
            idx: RandomAccessSimple::open(idx_rdr)?,
        })
    }

    /// Return the number of records (not including the header record) in this
    /// index.
    #[inline]
    pub fn count(&self) -> u64 {
        if self.csv_rdr.has_headers() && !self.idx.is_empty() {
            self.idx.len() - 1
        } else {
            self.idx.len()
        }
    }

    /// Seek to the starting position of record `i`.
    #[inline]
    pub fn seek(&mut self, mut i: u64) -> anyhow::Result<()> {
        if i >= self.count() {
            let msg = format!(
                "invalid record index {} (there are {} records)",
                i,
                self.count()
            );
            return Err(anyhow!(msg));
        }
        if self.csv_rdr.has_headers() {
            i += 1;
        }
        let pos = self.idx.get(i)?;
        self.csv_rdr.seek(pos)?;
        Ok(())
    }
}

pub struct Args {
    arg_input:   String,
    flag_output: Option<String>,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    if args.arg_input.to_lowercase().ends_with(".sz") {
        return Err(anyhow!("Cannot index a snappy file."));
    }

    let pidx = match args.flag_output {
        None => util::idx_path(Path::new(&args.arg_input)),
        Some(p) => PathBuf::from(&p),
    };

    let rconfig = Config::new(Some(args.arg_input).as_ref());
    let mut rdr = rconfig.reader_file()?;
    let mut wtr =
        io::BufWriter::with_capacity(DEFAULT_WTR_BUFFER_CAPACITY, fs::File::create(pidx)?);
    RandomAccessSimple::create(&mut rdr, &mut wtr)?;
    io::Write::flush(&mut wtr)?;

    Ok(())
}
