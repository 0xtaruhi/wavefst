use std::io::Cursor;
use std::ops::{Deref, DerefMut};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::block::HierarchyCompression;
use crate::error::Result;
use crate::types::CodecParallelism;
use crate::writer::{
    ChainCompression, DynamicAliasEncoding, FstWriter, TimeCompression, WriterBuilder,
    WriterOptions,
};

/// Builder for [`AsyncWriter`].
#[must_use]
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

    /// Selects the runtime policy for independent chain compression jobs.
    pub fn codec_parallelism(mut self, parallelism: CodecParallelism) -> Self {
        self.options.codec_parallelism = parallelism;
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

    /// Selects compression for the hierarchy declaration block.
    pub fn hierarchy_compression(mut self, compression: HierarchyCompression) -> Self {
        self.options.hierarchy_compression = compression;
        self
    }

    /// Chooses the legacy or current dynamic-alias index representation.
    pub fn dynamic_alias_encoding(mut self, encoding: DynamicAliasEncoding) -> Self {
        self.options.dynamic_alias_encoding = encoding;
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

    /// Bounds memory used by the synchronous block builder inside this async wrapper.
    pub fn block_change_limit(mut self, limit: usize) -> Self {
        self.options.block_change_limit = limit;
        self
    }

    /// Sets the approximate queued payload byte limit for each VC block.
    pub fn block_size_limit(mut self, bytes: usize) -> Self {
        self.options.block_size_limit = bytes;
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
