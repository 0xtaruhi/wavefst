use std::io::{Cursor, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::error::Result;
use crate::reader::{FstReader, ReaderBuilder, ReaderOptions};

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
        let reader = ReaderBuilder::new(Cursor::new(buffer))
            .options(options)
            .build()?;
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

/// Loads an entire async source into memory and returns the raw bytes.
pub async fn read_all<R>(mut source: R) -> Result<Vec<u8>>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    source.seek(SeekFrom::Start(0)).await?;
    let mut buffer = Vec::new();
    source.read_to_end(&mut buffer).await?;
    Ok(buffer)
}
