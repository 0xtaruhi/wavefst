use std::io::{Read, Write};

#[cfg(feature = "gzip")]
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};

use crate::encoding::{decode_varint, encode_varint};
use crate::error::{Error, Result};
use crate::util::read_u64_be;

/// Describes the layout of a single signal as recorded in the geometry block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeomEntry {
    /// Fixed-width bit-vector stored using `len` bytes.
    Fixed(u32),
    /// IEEE-754 double precision real value (signalled by zero in the block stream).
    Real,
    /// Variable-length payload (strings, enums, etc.).
    Variable,
}

impl GeomEntry {
    fn from_raw(value: u64) -> Result<Self> {
        match value {
            0 => Ok(GeomEntry::Real),
            0xFFFF_FFFF => Ok(GeomEntry::Variable),
            len => {
                let len32 = u32::try_from(len)
                    .map_err(|_| Error::invalid("geometry entry length exceeds u32 range"))?;
                if len32 == 0 {
                    return Err(Error::invalid(
                        "geometry fixed entry may not encode zero-length payload",
                    ));
                }
                Ok(GeomEntry::Fixed(len32))
            }
        }
    }

    fn to_raw(&self) -> u64 {
        match self {
            GeomEntry::Fixed(len) => *len as u64,
            GeomEntry::Real => 0,
            GeomEntry::Variable => 0xFFFF_FFFF,
        }
    }
}

/// Aggregated geometry information for the file.
#[derive(Debug, Clone, Default)]
pub struct GeomInfo {
    /// Highest signal handle defined by the geometry payload.
    pub max_handle: u64,
    /// Per-handle geometry entries (1-based in the FST file, 0-based in this vector).
    pub entries: Vec<GeomEntry>,
}

impl GeomInfo {
    /// Constructs geometry information from a set of coalesced runs. Each run adds `count`
    /// consecutive handles sharing the same geometry entry.
    pub fn from_runs<I>(runs: I) -> Result<Self>
    where
        I: IntoIterator<Item = (u32, GeomEntry)>,
    {
        let mut entries = Vec::new();
        for (count, entry) in runs {
            if count == 0 {
                return Err(Error::invalid("geometry run length may not be zero"));
            }
            let count_usize = usize::try_from(count)
                .map_err(|_| Error::invalid("geometry run length exceeds usize"))?;
            entries.resize(entries.len() + count_usize, entry);
        }
        Ok(Self {
            max_handle: entries.len() as u64,
            entries,
        })
    }

    /// Returns the recorded entry for the provided 1-based handle, if available.
    pub fn entry(&self, handle: u32) -> Option<&GeomEntry> {
        if handle == 0 {
            return None;
        }
        self.entries.get(handle as usize - 1)
    }

    /// Returns an iterator over `(handle, entry)` pairs for all recorded handles.
    pub fn handles(&self) -> impl Iterator<Item = (u32, &GeomEntry)> {
        self.entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| (idx as u32 + 1, entry))
    }

    /// Decodes a geometry section from the provided reader. The `section_length` must be the raw
    /// value stored in the file (including the 8-byte length word itself).
    pub fn decode_block<R: Read>(reader: &mut R, section_length: u64) -> Result<Self> {
        if section_length < 24 {
            return Err(Error::invalid(
                "geometry section shorter than required metadata",
            ));
        }

        let payload_len = section_length
            .checked_sub(8)
            .ok_or_else(|| Error::invalid("geometry section length underflow"))?;
        if payload_len < 16 {
            return Err(Error::invalid(
                "geometry payload shorter than metadata fields",
            ));
        }

        let uncompressed_len = read_u64_be(reader)?;
        let max_handle = read_u64_be(reader)?;
        let compressed_len = payload_len
            .checked_sub(16)
            .ok_or_else(|| Error::invalid("geometry compressed length underflow"))?;
        let compressed_len_usize = usize::try_from(compressed_len)
            .map_err(|_| Error::invalid("geometry payload longer than addressable memory"))?;

        let mut payload = vec![0u8; compressed_len_usize];
        reader.read_exact(&mut payload)?;

        let expected_uncompressed = usize::try_from(uncompressed_len)
            .map_err(|_| Error::invalid("geometry data too big"))?;

        let raw = if compressed_len == uncompressed_len {
            if payload.len() != expected_uncompressed {
                return Err(Error::decode(
                    "geometry uncompressed length mismatch with payload",
                ));
            }
            payload
        } else {
            #[cfg(feature = "gzip")]
            {
                let mut decoder = ZlibDecoder::new(&payload[..]);
                let mut decoded = Vec::with_capacity(expected_uncompressed);
                decoder.read_to_end(&mut decoded)?;
                if decoded.len() != expected_uncompressed {
                    return Err(Error::decode(
                        "geometry decompression length mismatch with header",
                    ));
                }
                decoded
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(Error::unsupported(
                    "geometry block requires zlib decompression; recompile with the `gzip` feature",
                ));
            }
        };

        let max_handle_usize = usize::try_from(max_handle)
            .map_err(|_| Error::invalid("geometry max handle exceeds usize"))?;

        let mut entries = Vec::with_capacity(max_handle_usize);
        let mut slice = raw.as_slice();
        for _ in 0..max_handle_usize {
            let value = decode_varint(&mut slice)?;
            let entry = GeomEntry::from_raw(value)?;
            entries.push(entry);
        }

        if !slice.is_empty() {
            return Err(Error::decode("geometry payload contains trailing data"));
        }

        Ok(Self {
            max_handle,
            entries,
        })
    }

    /// Encodes the geometry information into an FST block payload. When `compress` is `true`, the
    /// encoder attempts zlib compression and falls back to the raw stream if compression is
    /// ineffective.
    pub fn encode_block(&self, compress: bool) -> Result<EncodedGeometry> {
        let mut raw = Vec::with_capacity(self.entries.len() * 2);
        for entry in &self.entries {
            encode_varint(entry.to_raw(), &mut raw);
        }

        let uncompressed_len = raw.len() as u64;
        let (data, used_compression) = if compress {
            #[cfg(feature = "gzip")]
            {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(4));
                encoder.write_all(&raw)?;
                let compressed = encoder.finish()?;
                if compressed.len() < raw.len() {
                    (compressed, true)
                } else {
                    (raw, false)
                }
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(Error::unsupported(
                    "geometry compression requires the `gzip` feature",
                ));
            }
        } else {
            (raw, false)
        };

        let section_length = data.len() as u64 + 24;

        Ok(EncodedGeometry {
            section_length,
            uncompressed_len,
            max_handle: self.max_handle,
            data,
            compressed: used_compression,
        })
    }
}

/// Prepared geometry payload ready to be written into an FST stream.
#[derive(Debug, Clone)]
pub struct EncodedGeometry {
    /// Value to be emitted in the `section_length` field (includes the 8-byte length word).
    pub section_length: u64,
    /// Length of the uncompressed varint table.
    pub uncompressed_len: u64,
    /// Highest handle described by this geometry block.
    pub max_handle: u64,
    /// Compressed-or-raw payload bytes.
    pub data: Vec<u8>,
    /// Indicates whether `data` was produced by compression.
    pub compressed: bool,
}

impl EncodedGeometry {
    /// Writes the encoded geometry payload (excluding block type) to the provided writer.
    pub fn write_to<W: Write + ?Sized>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.section_length.to_be_bytes())?;
        writer.write_all(&self.uncompressed_len.to_be_bytes())?;
        writer.write_all(&self.max_handle.to_be_bytes())?;
        writer.write_all(&self.data)?;
        Ok(())
    }
}
