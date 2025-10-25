#![allow(missing_docs)]

//! Compression backends used by value change and hierarchy blocks.

use crate::error::Result;

/// Trait implemented by compression algorithms used when writing FST data.
pub trait Compressor {
    fn compress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;
    fn flush(&mut self, _output: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

/// Trait implemented by decompression algorithms used when reading FST data.
pub trait Decompressor {
    fn decompress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;
}

/// No-op compressor used when compression is disabled.
#[derive(Debug, Default)]
pub struct NullCompressor;

impl Compressor for NullCompressor {
    fn compress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(input);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NullDecompressor;

impl Decompressor for NullDecompressor {
    fn decompress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(input);
        Ok(())
    }
}

#[cfg(feature = "gzip")]
mod gzip {
    use super::{Compressor, Decompressor};
    use crate::error::Result;
    use flate2::Compression;
    use flate2::bufread::{ZlibDecoder, ZlibEncoder};
    use std::io::{Cursor, Read};

    #[derive(Debug)]
    pub struct ZlibCompressor {
        level: Compression,
    }

    impl Default for ZlibCompressor {
        fn default() -> Self {
            Self {
                level: Compression::new(4),
            }
        }
    }

    impl Compressor for ZlibCompressor {
        fn compress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
            let mut encoder = ZlibEncoder::new(Cursor::new(input), self.level);
            encoder.read_to_end(output)?;
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    pub struct ZlibDecompressor;

    impl Decompressor for ZlibDecompressor {
        fn decompress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
            let mut decoder = ZlibDecoder::new(Cursor::new(input));
            decoder.read_to_end(output)?;
            Ok(())
        }
    }

    pub use {ZlibCompressor as CompressorImpl, ZlibDecompressor as DecompressorImpl};
}

#[cfg(feature = "gzip")]
pub use gzip::{CompressorImpl as ZlibCompressor, DecompressorImpl as ZlibDecompressor};

#[cfg(feature = "lz4")]
mod lz4 {
    use super::{Compressor, Decompressor};
    use crate::error::{Error, Result};
    use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};

    #[derive(Debug, Default)]
    pub struct Lz4Compressor;

    impl Compressor for Lz4Compressor {
        fn compress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
            let compressed = compress_prepend_size(input);
            output.extend_from_slice(&compressed);
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    pub struct Lz4Decompressor;

    impl Decompressor for Lz4Decompressor {
        fn decompress(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
            let decompressed =
                decompress_size_prepended(input).map_err(|err| Error::decode(err.to_string()))?;
            output.extend_from_slice(&decompressed);
            Ok(())
        }
    }

    pub use {Lz4Compressor as CompressorImpl, Lz4Decompressor as DecompressorImpl};
}

#[cfg(feature = "lz4")]
pub use lz4::{CompressorImpl as Lz4Compressor, DecompressorImpl as Lz4Decompressor};
