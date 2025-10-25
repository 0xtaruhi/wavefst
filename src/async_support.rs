//! Async helpers that wrap the synchronous reader and writer APIs.

use std::io::{Cursor, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::error::Result;
use crate::reader::{FstReader, ReaderBuilder, ReaderOptions};
use crate::writer::{ChainCompression, FstWriter, TimeCompression, WriterBuilder, WriterOptions};

/// Reader that loads an async source into memory and exposes the synchronous [`FstReader`] API.
pub struct AsyncReader {
    inner: FstReader<Cursor<Vec<u8>>>,
}

impl AsyncReader {
    /// Loads the entire async reader into memory and constructs an [`FstReader`] with default options.
    pub async fn from_reader<R>(source: R) -> Result<Self>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        Self::from_reader_with_options(source, ReaderOptions::default()).await
    }

    /// Loads the async reader using the supplied [`ReaderOptions`].
    pub async fn from_reader_with_options<R>(mut source: R, options: ReaderOptions) -> Result<Self>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        source.seek(SeekFrom::Start(0)).await?;
        let mut buffer = Vec::new();
        source.read_to_end(&mut buffer).await?;
        let cursor = Cursor::new(buffer);
        let reader = ReaderBuilder::new(cursor).options(options).build()?;
        Ok(Self { inner: reader })
    }

    /// Opens a file via `tokio::fs::File` and constructs a reader with default options.
    pub async fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path).await?;
        Self::from_reader(file).await
    }

    /// Opens a file via `tokio::fs::File` with explicit reader options.
    pub async fn from_file_with_options(
        path: impl AsRef<Path>,
        options: ReaderOptions,
    ) -> Result<Self> {
        let file = File::open(path).await?;
        Self::from_reader_with_options(file, options).await
    }

    /// Returns a shared reference to the underlying synchronous reader.
    pub fn reader(&self) -> &FstReader<Cursor<Vec<u8>>> {
        &self.inner
    }

    /// Returns a mutable reference to the underlying synchronous reader.
    pub fn reader_mut(&mut self) -> &mut FstReader<Cursor<Vec<u8>>> {
        &mut self.inner
    }

    /// Consumes the async wrapper, yielding the synchronous reader.
    pub fn into_reader(self) -> FstReader<Cursor<Vec<u8>>> {
        self.inner
    }
}

impl Deref for AsyncReader {
    type Target = FstReader<Cursor<Vec<u8>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for AsyncReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Builder for [`AsyncWriter`].
pub struct AsyncWriterBuilder<W> {
    sink: W,
    options: WriterOptions,
}

impl<W> AsyncWriterBuilder<W> {
    /// Creates a builder for the given async sink.
    pub fn new(sink: W) -> Self {
        Self {
            sink,
            options: WriterOptions::default(),
        }
    }

    /// Overrides writer options wholesale.
    pub fn options(mut self, options: WriterOptions) -> Self {
        self.options = options;
        self
    }

    /// Selects the per-chain compression.
    pub fn chain_compression(mut self, compression: ChainCompression) -> Self {
        self.options.chain_compression = compression;
        self
    }

    /// Selects the time-table compression.
    pub fn time_compression(mut self, compression: TimeCompression) -> Self {
        self.options.time_compression = compression;
        self
    }

    /// Toggles the outer gzip wrapper.
    pub fn wrap_with_zlib(mut self, wrap: bool) -> Self {
        self.options.wrap_zlib = wrap;
        self
    }

    /// Sets the header timescale exponent.
    pub fn timescale_exponent(mut self, exponent: i8) -> Self {
        self.options.timescale_exponent = exponent;
        self
    }

    /// Sets an optional compression level hint.
    pub fn compression_level(mut self, level: Option<u32>) -> Self {
        self.options.compression_level = level;
        self
    }

    /// Builds the async writer, validating options before returning the instance.
    pub fn build(self) -> Result<AsyncWriter<W>>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let inner = WriterBuilder::new(Cursor::new(Vec::new()))
            .options(self.options.clone())
            .build()?;
        Ok(AsyncWriter {
            sink: self.sink,
            inner,
        })
    }
}

/// Writer that buffers into memory and flushes to an async sink on [`finish`](AsyncWriter::finish).
pub struct AsyncWriter<W> {
    sink: W,
    inner: FstWriter<Cursor<Vec<u8>>>,
}

impl<W> AsyncWriter<W> {
    /// Creates a builder for the supplied async sink.
    pub fn builder(sink: W) -> AsyncWriterBuilder<W> {
        AsyncWriterBuilder::new(sink)
    }
}

impl<W> AsyncWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    /// Flushes buffered data to the async sink, returning the sink on completion.
    pub async fn finish(self) -> Result<W> {
        let AsyncWriter { sink, inner } = self;
        let cursor = inner.finish()?;
        let mut sink = sink;
        let payload = cursor.into_inner();
        if !payload.is_empty() {
            sink.write_all(&payload).await?;
        }
        sink.flush().await?;
        Ok(sink)
    }
}

impl<W> Deref for AsyncWriter<W> {
    type Target = FstWriter<Cursor<Vec<u8>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<W> DerefMut for AsyncWriter<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Convenience wrapper to load an entire file into memory using async I/O and return the raw buffer.
pub async fn read_all<R>(mut source: R) -> Result<Vec<u8>>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    source.seek(SeekFrom::Start(0)).await?;
    let mut buffer = Vec::new();
    source.read_to_end(&mut buffer).await?;
    Ok(buffer)
}
