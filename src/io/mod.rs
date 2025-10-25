#![allow(missing_docs)]

//! I/O backends used by the reader and writer implementations.

use std::io::{BufReader, BufWriter, Read, Seek, Write};

use crate::error::{Error, Result};

#[cfg(feature = "mmap")]
mod mmap;
mod streaming;

#[cfg(feature = "mmap")]
pub use mmap::MemoryMap;
pub use streaming::{BufferedReader, BufferedWriter};

/// Trait alias for objects that implement `Read + Seek`.
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// Trait alias for objects that implement `Write + Seek`.
pub trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

/// Default buffered reader backend.
pub struct ReaderBackend<R: ReadSeek> {
    inner: BufReader<R>,
}

impl<R: ReadSeek> ReaderBackend<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: BufReader::new(inner),
        }
    }

    pub fn get_mut(&mut self) -> &mut BufReader<R> {
        &mut self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner.into_inner()
    }
}

/// Default buffered writer backend.
pub struct WriterBackend<W: WriteSeek> {
    inner: BufWriter<W>,
}

impl<W: WriteSeek> WriterBackend<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: BufWriter::new(inner),
        }
    }

    pub fn get_mut(&mut self) -> &mut BufWriter<W> {
        &mut self.inner
    }

    pub fn into_inner(self) -> Result<W> {
        match self.inner.into_inner() {
            Ok(writer) => Ok(writer),
            Err(err) => Err(Error::Io(err.into_error())),
        }
    }
}
