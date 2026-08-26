//! I/O backends used by the reader and writer implementations.

#[cfg(feature = "writer")]
use std::io::BufWriter;
#[cfg(any(feature = "reader", feature = "writer"))]
use std::io::Seek;
#[cfg(feature = "writer")]
use std::io::Write;
#[cfg(feature = "reader")]
use std::io::{self, BufReader, Cursor, Read, SeekFrom};

#[cfg(feature = "writer")]
use crate::error::{Error, Result};

#[cfg(feature = "mmap")]
mod mmap;

#[cfg(feature = "mmap")]
pub use mmap::MemoryMap;

/// Convenience alias for a buffered reader usable by [`crate::ReaderBuilder`].
#[cfg(feature = "reader")]
pub type BufferedReader<R> = BufReader<R>;
/// Convenience alias for a buffered writer usable by [`crate::WriterBuilder`].
#[cfg(feature = "writer")]
pub type BufferedWriter<W> = BufWriter<W>;

#[cfg(feature = "reader")]
const READER_BUFFER_CAPACITY: usize = 8 * 1024;

/// Buffered input that keeps its logical position and satisfies seeks from buffered data when
/// possible. FST blocks contain backward trailers and indexes, so this avoids needless kernel
/// seeks on small and medium traces while retaining ordinary streaming behavior for large files.
#[cfg(feature = "reader")]
pub struct BufferedSeekReader<R> {
    inner: BufReader<R>,
    position: u64,
}

#[cfg(feature = "reader")]
impl<R: Read> BufferedSeekReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner: BufReader::with_capacity(READER_BUFFER_CAPACITY, inner),
            position: 0,
        }
    }

    fn into_inner(self) -> R {
        self.inner.into_inner()
    }
}

#[cfg(feature = "reader")]
impl<R: Read> Read for BufferedSeekReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buffer)?;
        self.position = self
            .position
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("buffered reader position overflow"))?;
        Ok(count)
    }
}

#[cfg(feature = "reader")]
impl<R: Read + Seek> Seek for BufferedSeekReader<R> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let absolute = match target {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(delta) => {
                let position = i128::from(self.position) + i128::from(delta);
                u64::try_from(position).ok()
            }
            SeekFrom::End(delta) => {
                let position = self.inner.seek(SeekFrom::End(delta))?;
                self.position = position;
                return Ok(position);
            }
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid seek target"))?;

        let delta = i128::from(absolute) - i128::from(self.position);
        if let Ok(delta) = i64::try_from(delta) {
            self.inner.seek_relative(delta)?;
        } else {
            self.inner.seek(SeekFrom::Start(absolute))?;
        }
        self.position = absolute;
        Ok(absolute)
    }
}

/// Trait alias for objects that implement `Read + Seek`.
#[cfg(feature = "reader")]
pub trait ReadSeek: Read + Seek {}
#[cfg(feature = "reader")]
impl<T: Read + Seek> ReadSeek for T {}

/// Trait alias for objects that implement `Write + Seek`.
#[cfg(feature = "writer")]
pub trait WriteSeek: Write + Seek {}
#[cfg(feature = "writer")]
impl<T: Write + Seek> WriteSeek for T {}

/// Input retained by a reader. Wrapped FST files are decoded into memory while the original
/// source is kept so [`ReaderBackend::into_inner`] remains lossless.
#[cfg(feature = "reader")]
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

#[cfg(feature = "reader")]
impl<R: Read + Seek> Read for ReaderInput<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Direct(inner) => inner.read(buf),
            Self::Wrapped { decoded, .. } => decoded.read(buf),
        }
    }
}

#[cfg(feature = "reader")]
impl<R: Read + Seek> Seek for ReaderInput<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Direct(inner) => inner.seek(pos),
            Self::Wrapped { decoded, .. } => decoded.seek(pos),
        }
    }
}

/// Default buffered reader backend.
#[cfg(feature = "reader")]
pub struct ReaderBackend<R: ReadSeek> {
    inner: BufferedSeekReader<ReaderInput<R>>,
}

#[cfg(feature = "reader")]
impl<R: ReadSeek> ReaderBackend<R> {
    /// Wraps a direct input in the default buffered backend.
    pub fn new(inner: R) -> Self {
        Self {
            inner: BufferedSeekReader::new(ReaderInput::Direct(inner)),
        }
    }

    pub(crate) fn wrapped(original: R, decoded: Vec<u8>) -> Self {
        Self {
            inner: BufferedSeekReader::new(ReaderInput::Wrapped {
                original,
                decoded: Cursor::new(decoded),
            }),
        }
    }

    /// Returns mutable access to the buffered input.
    pub fn get_mut(&mut self) -> &mut BufferedSeekReader<ReaderInput<R>> {
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
#[cfg(feature = "writer")]
pub struct WriterBackend<W: WriteSeek> {
    inner: BufWriter<W>,
}

#[cfg(feature = "writer")]
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
