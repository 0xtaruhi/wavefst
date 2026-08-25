//! I/O backends used by the reader and writer implementations.

use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};

use crate::error::{Error, Result};

#[cfg(feature = "mmap")]
mod mmap;

#[cfg(feature = "mmap")]
pub use mmap::MemoryMap;

/// Convenience alias for a buffered reader usable by [`crate::ReaderBuilder`].
pub type BufferedReader<R> = BufReader<R>;
/// Convenience alias for a buffered writer usable by [`crate::WriterBuilder`].
pub type BufferedWriter<W> = BufWriter<W>;

/// Trait alias for objects that implement `Read + Seek`.
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// Trait alias for objects that implement `Write + Seek`.
pub trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

/// Input retained by a reader. Wrapped FST files are decoded into memory while the original
/// source is kept so [`ReaderBackend::into_inner`] remains lossless.
pub enum ReaderInput<R> {
    /// Reads directly from the caller-provided source.
    Direct(R),
    /// Reads from a decoded wrapper while retaining the original source for [`ReaderBackend::into_inner`].
    Wrapped {
        /// Original wrapped source returned by [`ReaderBackend::into_inner`].
        original: R,
        /// In-memory decoded FST stream used while parsing.
        decoded: Cursor<Vec<u8>>,
    },
}

impl<R: Read + Seek> Read for ReaderInput<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Direct(inner) => inner.read(buf),
            Self::Wrapped { decoded, .. } => decoded.read(buf),
        }
    }
}

impl<R: Read + Seek> Seek for ReaderInput<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Direct(inner) => inner.seek(pos),
            Self::Wrapped { decoded, .. } => decoded.seek(pos),
        }
    }
}

/// Default buffered reader backend.
pub struct ReaderBackend<R: ReadSeek> {
    inner: BufReader<ReaderInput<R>>,
}

impl<R: ReadSeek> ReaderBackend<R> {
    /// Wraps a direct input in the default buffered backend.
    pub fn new(inner: R) -> Self {
        Self {
            inner: BufReader::new(ReaderInput::Direct(inner)),
        }
    }

    pub(crate) fn wrapped(original: R, decoded: Vec<u8>) -> Self {
        Self {
            inner: BufReader::new(ReaderInput::Wrapped {
                original,
                decoded: Cursor::new(decoded),
            }),
        }
    }

    /// Returns mutable access to the buffered input.
    pub fn get_mut(&mut self) -> &mut BufReader<ReaderInput<R>> {
        &mut self.inner
    }

    /// Consumes the backend and returns the original input object.
    pub fn into_inner(self) -> R {
        match self.inner.into_inner() {
            ReaderInput::Direct(inner) => inner,
            ReaderInput::Wrapped { original, .. } => original,
        }
    }
}

/// Default buffered writer backend.
pub struct WriterBackend<W: WriteSeek> {
    inner: BufWriter<W>,
}

impl<W: WriteSeek> WriterBackend<W> {
    /// Wraps an output in the default buffered backend.
    pub fn new(inner: W) -> Self {
        Self {
            inner: BufWriter::new(inner),
        }
    }

    /// Returns mutable access to the buffered output.
    pub fn get_mut(&mut self) -> &mut BufWriter<W> {
        &mut self.inner
    }

    /// Flushes the buffer and returns the underlying output object.
    pub fn into_inner(self) -> Result<W> {
        match self.inner.into_inner() {
            Ok(writer) => Ok(writer),
            Err(err) => Err(Error::Io(err.into_error())),
        }
    }
}
