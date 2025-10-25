#[cfg(feature = "fastlz")]
use fastlz_sys::fastlz_compress;
#[cfg(feature = "gzip")]
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
#[cfg(feature = "lz4")]
use lz4_flex::block::compress as lz4_compress;
#[cfg(feature = "fastlz")]
use std::ffi::c_void;
#[cfg(feature = "gzip")]
use std::io::{Read, Write};

use super::time::TimeSection;
use crate::encoding::{decode_varint_with_len, encode_varint};
use crate::error::{Error, Result};
use crate::types::PackType;

/// Associates a compression marker byte with a semantic [`PackType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackMarker {
    /// Resolved compression type encoded by the marker byte.
    pub pack_type: PackType,
    /// Raw marker byte stored in the value-change section.
    pub marker: u8,
}

impl PackMarker {
    /// Constructs a marker from a raw byte, returning `None` when unknown.
    pub fn new(marker: u8) -> Option<Self> {
        PackType::from_marker(marker).map(|pack_type| Self { pack_type, marker })
    }
}

/// Minimal metadata extracted from a value change block.
#[derive(Debug, Clone)]
pub struct VcBlock {
    /// First timestamp covered by the block.
    pub begin_time: u64,
    /// Last timestamp covered by the block.
    pub end_time: u64,
    /// Reader memory budget hint stored in the block header.
    pub required_memory: u64,
    /// Length of the uncompressed frame payload emitted at the start of the block.
    pub frame_uncompressed_len: u64,
    /// Length of the frame data after compression.
    pub frame_compressed_len: u64,
    /// Highest handle restored by the frame preamble.
    pub frame_max_handle: u64,
    /// Highest handle referenced by the change chains.
    pub vc_max_handle: u64,
    /// Compression marker describing chain payload encoding.
    pub pack_marker: PackMarker,
    /// Length of the trailing index table (used to locate per-handle chains).
    pub index_length: u64,
}

impl VcBlock {
    /// Returns the compression kind used in the block.
    pub fn pack_type(&self) -> PackType {
        self.pack_marker.pack_type
    }
}

/// Decompressed frame preamble restoring initial handle values.
#[derive(Debug, Clone)]
pub struct FrameSection {
    /// Raw frame bytes (one entry per handle).
    pub data: Vec<u8>,
    /// Highest handle covered by the frame.
    pub max_handle: u64,
}

impl FrameSection {
    /// Decodes the frame payload given the compressed bytes and expected lengths.
    pub fn decode(
        uncompressed_len: u64,
        compressed_len: u64,
        bytes: Vec<u8>,
        max_handle: u64,
    ) -> Result<Self> {
        let expected_uncompressed = usize::try_from(uncompressed_len)
            .map_err(|_| Error::invalid("frame data exceeds addressable memory"))?;
        let raw = if compressed_len == uncompressed_len {
            if bytes.len() != expected_uncompressed {
                return Err(Error::decode("frame payload length mismatch"));
            }
            bytes
        } else {
            #[cfg(feature = "gzip")]
            {
                let mut decoder = ZlibDecoder::new(&bytes[..]);
                let mut decoded = Vec::with_capacity(expected_uncompressed);
                decoder.read_to_end(&mut decoded)?;
                if decoded.len() != expected_uncompressed {
                    return Err(Error::decode("frame decompression length mismatch"));
                }
                decoded
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(Error::unsupported(
                    "frame preamble requires zlib support; enable the `gzip` feature",
                ));
            }
        };

        Ok(Self {
            data: raw,
            max_handle,
        })
    }

    /// Returns the raw frame bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

/// Expanded time-table derived from the compressed time section.
#[derive(Debug, Clone)]
pub struct TimeTable {
    /// Raw delta values decoded from the time section.
    pub deltas: Vec<u64>,
    /// Absolute timestamps reconstructed from the deltas.
    pub timestamps: Vec<u64>,
}

impl TimeTable {
    /// Decodes the time section payload into cumulative timestamps.
    pub fn decode(section: &TimeSection, bytes: Vec<u8>) -> Result<Self> {
        let expected = usize::try_from(section.uncompressed_len)
            .map_err(|_| Error::invalid("time table too large"))?;
        let raw = if section.compressed_len == section.uncompressed_len {
            if bytes.len() != expected {
                return Err(Error::decode("time section length mismatch"));
            }
            bytes
        } else {
            #[cfg(feature = "gzip")]
            {
                let mut decoder = ZlibDecoder::new(&bytes[..]);
                let mut decoded = Vec::with_capacity(expected);
                decoder.read_to_end(&mut decoded)?;
                if decoded.len() != expected {
                    return Err(Error::decode("time section decompression mismatch"));
                }
                decoded
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(Error::unsupported(
                    "time section requires zlib support; enable the `gzip` feature",
                ));
            }
        };

        let mut deltas = Vec::with_capacity(section.item_count as usize);
        let mut offset = 0usize;
        while offset < raw.len() && deltas.len() < section.item_count as usize {
            let (value, consumed) = decode_varint_with_len(&raw[offset..])?;
            deltas.push(value);
            offset += consumed;
        }

        if deltas.len() != section.item_count as usize {
            return Err(Error::decode("time section item count mismatch"));
        }

        let mut timestamps = Vec::with_capacity(deltas.len());
        let mut acc = 0u64;
        for delta in &deltas {
            acc = acc
                .checked_add(*delta)
                .ok_or_else(|| Error::decode("time delta accumulation overflow"))?;
            timestamps.push(acc);
        }

        Ok(Self { deltas, timestamps })
    }
}

/// Encoded frame payload along with metadata required by the block header.
#[derive(Debug, Clone)]
pub struct FrameEncoding {
    /// Serialized frame payload written into the block.
    pub payload: Vec<u8>,
    /// Length of the uncompressed frame data.
    pub uncompressed_len: u64,
    /// Length of the payload as written (compressed or raw).
    pub compressed_len: u64,
}

/// Compresses (when beneficial) the frame section that precedes the chain payloads.
pub fn encode_frame_section(
    frame_raw: Vec<u8>,
    compression_level: Option<u32>,
) -> Result<FrameEncoding> {
    let uncompressed_len = u64::try_from(frame_raw.len())
        .map_err(|_| Error::invalid("frame payload exceeds supported length"))?;
    if frame_raw.is_empty() {
        return Ok(FrameEncoding {
            payload: Vec::new(),
            uncompressed_len,
            compressed_len: 0,
        });
    }

    #[cfg(feature = "gzip")]
    {
        let compressed = zlib_compress(&frame_raw, compression_level)?;
        if compressed.len() < frame_raw.len() {
            let compressed_len = u64::try_from(compressed.len())
                .map_err(|_| Error::invalid("compressed frame payload too large"))?;
            return Ok(FrameEncoding {
                payload: compressed,
                uncompressed_len,
                compressed_len,
            });
        }
    }
    #[cfg(not(feature = "gzip"))]
    {
        let _ = compression_level;
    }

    Ok(FrameEncoding {
        payload: frame_raw,
        uncompressed_len,
        compressed_len: uncompressed_len,
    })
}

/// Encoded representation of the time delta section.
#[derive(Debug, Clone)]
pub struct TimeEncoding {
    /// Serialized bytes stored in the block.
    pub payload: Vec<u8>,
    /// Length of the uncompressed sequence.
    pub uncompressed_len: u64,
    /// Length after compression (0 when empty).
    pub compressed_len: u64,
    /// Number of time entries stored in the payload.
    pub item_count: u64,
}

/// Compresses the time table when requested, returning the serialized payload.
pub fn encode_time_section(
    time_raw: Vec<u8>,
    item_count: u64,
    compress: bool,
    compression_level: Option<u32>,
) -> Result<TimeEncoding> {
    let uncompressed_len = u64::try_from(time_raw.len())
        .map_err(|_| Error::invalid("time section exceeds supported length"))?;
    if time_raw.is_empty() {
        return Ok(TimeEncoding {
            payload: Vec::new(),
            uncompressed_len,
            compressed_len: 0,
            item_count,
        });
    }

    if compress {
        #[cfg(feature = "gzip")]
        {
            let compressed = zlib_compress(&time_raw, compression_level)?;
            if compressed.len() < time_raw.len() {
                let compressed_len = u64::try_from(compressed.len())
                    .map_err(|_| Error::invalid("compressed time section too large"))?;
                return Ok(TimeEncoding {
                    payload: compressed,
                    uncompressed_len,
                    compressed_len,
                    item_count,
                });
            }
        }
        #[cfg(not(feature = "gzip"))]
        {
            let _ = compression_level;
            return Err(Error::unsupported(
                "time section compression requires the `gzip` feature",
            ));
        }
    }

    Ok(TimeEncoding {
        payload: time_raw,
        uncompressed_len,
        compressed_len: uncompressed_len,
        item_count,
    })
}

/// Encodes an individual chain payload according to the selected compression marker.
pub fn encode_chain_payload(
    pack_type: PackType,
    data: Vec<u8>,
    compression_level: Option<u32>,
) -> Result<(u64, Vec<u8>)> {
    let raw_len = u64::try_from(data.len())
        .map_err(|_| Error::invalid("chain payload exceeds supported length"))?;
    if data.is_empty() {
        return Ok((0, data));
    }

    match pack_type {
        PackType::None => Ok((0, data)),
        PackType::Zlib => {
            #[cfg(not(feature = "gzip"))]
            {
                let _ = compression_level;
                Err(Error::unsupported(
                    "zlib compression requires the `gzip` feature",
                ))
            }
            #[cfg(feature = "gzip")]
            {
                let compressed = zlib_compress(&data, compression_level)?;
                if compressed.len() < data.len() {
                    return Ok((raw_len, compressed));
                }
                Ok((0, data))
            }
        }
        PackType::Lz4 => {
            #[cfg(not(feature = "lz4"))]
            {
                Err(Error::unsupported(
                    "lz4 compression requires the `lz4` feature",
                ))
            }
            #[cfg(feature = "lz4")]
            {
                let compressed = lz4_compress(&data);
                if compressed.len() < data.len() {
                    return Ok((raw_len, compressed));
                }
                Ok((0, data))
            }
        }
        PackType::FastLz => {
            #[cfg(not(feature = "fastlz"))]
            {
                Err(Error::unsupported(
                    "fastlz compression requires the `fastlz` feature",
                ))
            }
            #[cfg(feature = "fastlz")]
            {
                let compressed = fastlz_compress_bytes(&data)?;
                if compressed.len() < data.len() {
                    return Ok((raw_len, compressed));
                }
                Ok((0, data))
            }
        }
    }
}

/// Entry describing the chain index layout for a single handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainIndexEntry {
    /// Handle has no chain and is not an alias.
    Empty,
    /// Handle owns a chain located at the provided offset.
    Data {
        /// Byte offset (relative to the start of the chain buffer) where the chain begins.
        offset: u64,
    },
    /// Handle is an alias of another handle (`target` is 1-based).
    Alias {
        /// Canonical handle whose chain should be reused for this alias record.
        target: u32,
    },
}

/// Serializes the chain index table for a value change block.
pub fn encode_chain_index(entries: &[ChainIndexEntry]) -> Result<Vec<u8>> {
    const PACK_MARKER_PREFIX: u64 = 1;

    let mut index_bytes = Vec::new();
    let mut empty_run = 0usize;
    let mut last_offset = 0u64;
    let mut seen_data = false;

    for entry in entries {
        match entry {
            ChainIndexEntry::Empty => {
                empty_run += 1;
            }
            ChainIndexEntry::Data { offset } => {
                if empty_run > 0 {
                    encode_varint((empty_run as u64) << 1, &mut index_bytes);
                    empty_run = 0;
                }
                let absolute = PACK_MARKER_PREFIX
                    .checked_add(*offset)
                    .ok_or_else(|| Error::invalid("chain offset overflowed pack marker base"))?;
                let delta = if seen_data {
                    absolute
                        .checked_sub(last_offset)
                        .ok_or_else(|| Error::invalid("chain offsets must be non-decreasing"))?
                } else {
                    absolute
                };
                encode_varint((delta << 1) | 1, &mut index_bytes);
                last_offset = absolute;
                seen_data = true;
            }
            ChainIndexEntry::Alias { target } => {
                if *target == 0 {
                    return Err(Error::invalid("alias handle must be greater than zero"));
                }
                if empty_run > 0 {
                    encode_varint((empty_run as u64) << 1, &mut index_bytes);
                    empty_run = 0;
                }
                encode_varint(0, &mut index_bytes);
                encode_varint(u64::from(*target), &mut index_bytes);
            }
        }
    }

    if empty_run > 0 {
        encode_varint((empty_run as u64) << 1, &mut index_bytes);
    }

    Ok(index_bytes)
}

#[cfg(feature = "gzip")]
fn zlib_compress(input: &[u8], level: Option<u32>) -> Result<Vec<u8>> {
    let lvl = level.map(|v| v.min(9)).unwrap_or(6);
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(lvl));
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

#[cfg(feature = "fastlz")]
fn fastlz_compress_bytes(input: &[u8]) -> Result<Vec<u8>> {
    let input_len = i32::try_from(input.len())
        .map_err(|_| Error::invalid("fastlz input length exceeds i32 range"))?;
    let scratch = input
        .len()
        .checked_add(input.len() / 20)
        .and_then(|v| v.checked_add(66))
        .ok_or_else(|| Error::invalid("fastlz scratch size overflow"))?;
    let capacity = scratch.max(66);
    let mut output = vec![0u8; capacity];
    let written = unsafe {
        fastlz_compress(
            input.as_ptr() as *const c_void,
            input_len,
            output.as_mut_ptr() as *mut c_void,
        )
    };
    if written <= 0 {
        return Err(Error::unsupported("fastlz compression failed"));
    }
    let written_usize = usize::try_from(written)
        .map_err(|_| Error::invalid("fastlz compressed length exceeds addressable memory"))?;
    if written_usize > output.len() {
        return Err(Error::invalid("fastlz compression overflow"));
    }
    output.truncate(written_usize);
    Ok(output)
}
