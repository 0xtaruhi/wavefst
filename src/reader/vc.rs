use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "fastlz")]
use fastlz_sys::fastlz_decompress;
#[cfg(feature = "gzip")]
use flate2::read::ZlibDecoder;
#[cfg(feature = "lz4")]
use lz4_flex::block::decompress as lz4_decompress;

use crate::block::{FrameSection, PackMarker, TimeSection, TimeTable, VcBlock};
use crate::encoding::{decode_svarint, decode_varint_with_len};
use crate::error::{Error, Result};
use crate::types::{BlockType, FstByteOrder, PackType};
use crate::util::{read_u64_be, read_varint_from_reader};

/// Fully decoded metadata and payload slices extracted from a value-change block.
#[derive(Debug, Clone)]
pub struct VcBlockMeta {
    /// Fixed metadata from the value-change block header.
    pub header: VcBlock,
    /// Decoded frame section containing values at the block boundary.
    pub frame: FrameSection,
    /// Original stored/raw chain payload arena.
    pub chain_buffer: Vec<u8>,
    /// Arena containing chains that required decompression.
    pub decoded_chain_buffer: Vec<u8>,
    /// Per-handle resolved change chains; index zero corresponds to handle one.
    pub chains: Vec<Option<ChainData>>,
    /// Metadata describing the encoded time table.
    pub time_section: TimeSection,
    /// Fully decoded timestamp table.
    pub time_table: TimeTable,
    /// Per-handle chain locations and dynamic aliases.
    pub index: ChainIndex,
    /// Byte order used to decode real-valued changes.
    pub double_byte_order: FstByteOrder,
}

/// Resolved per-handle chain metadata extracted from the block index.
#[derive(Debug, Clone, Default)]
pub struct ChainIndex {
    /// Per-handle slots; index zero corresponds to handle one.
    pub slots: Vec<Option<ChainSlot>>,
}

/// Offset/length pair describing where compressed chain data resides for a handle.
#[derive(Debug, Clone, Copy)]
pub struct ChainSlot {
    /// Byte offset of the stored chain within the chain section.
    pub offset: u64,
    /// Stored chain length in bytes.
    pub length: u32,
    /// Canonical handle when this slot is a dynamic alias.
    pub alias_of: Option<u32>,
}

/// In-memory representation of a handle's change stream.
#[derive(Debug, Clone)]
pub struct ChainData {
    /// One-based signal handle represented by this chain.
    pub handle: u32,
    /// Encoded chain length before decompression.
    pub stored_len: u32,
    /// Location of the decoded payload in one of the block arenas.
    pub payload: ChainPayload,
    /// Canonical handle when this chain is a dynamic alias.
    pub alias_of: Option<u32>,
}

/// Slice containing uncompressed chain bytes.
#[derive(Debug, Clone)]
pub enum ChainPayload {
    /// Bytes can be borrowed directly from [`VcBlockMeta::chain_buffer`].
    Borrowed {
        /// Byte range inside the stored/raw chain arena.
        range: Range<usize>,
    },
    /// Bytes reside in [`VcBlockMeta::decoded_chain_buffer`].
    Decoded {
        /// Byte range inside the decoded chain arena.
        range: Range<usize>,
    },
}

impl ChainPayload {
    /// Returns a view of the payload as a byte slice, borrowing when possible.
    pub fn as_slice<'a>(&self, backing: &'a [u8], decoded_backing: &'a [u8]) -> &'a [u8] {
        match self {
            ChainPayload::Borrowed { range } => &backing[range.clone()],
            ChainPayload::Decoded { range } => &decoded_backing[range.clone()],
        }
    }

    /// Returns the payload length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            ChainPayload::Borrowed { range } | ChainPayload::Decoded { range } => {
                range.end - range.start
            }
        }
    }

    /// Returns `true` when the payload range is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub(crate) fn parse_vc_block<R: Read + Seek>(
    reader: &mut R,
    block_type: BlockType,
    section_start: u64,
    payload_len: u64,
    max_block_bytes: u64,
    max_handles: u64,
    double_byte_order: FstByteOrder,
) -> Result<VcBlockMeta> {
    if payload_len < 61 {
        return Err(Error::invalid(
            "value-change payload shorter than required fields",
        ));
    }
    let block_end = section_start
        .checked_add(payload_len)
        .ok_or_else(|| Error::invalid("value-change block exceeds file bounds"))?;
    let begin_time = read_u64_be(reader)?;
    let end_time = read_u64_be(reader)?;
    let required_memory = read_u64_be(reader)?;
    let (frame_uncompressed_len, _) = read_varint_from_reader(reader)?;
    let (frame_compressed_len, _) = read_varint_from_reader(reader)?;
    let (frame_max_handle, _) = read_varint_from_reader(reader)?;

    if required_memory > max_block_bytes || frame_uncompressed_len > max_block_bytes {
        return Err(Error::invalid(
            "decoded value-change data exceeds configured block limit",
        ));
    }

    let frame_compressed_len_usize = usize::try_from(frame_compressed_len)
        .map_err(|_| Error::invalid("frame payload exceeds addressable memory"))?;
    let frame_start = reader.stream_position()?;
    let latest_frame_end = block_end
        .checked_sub(34)
        .ok_or_else(|| Error::invalid("value-change block trailer underflow"))?;
    let frame_end = frame_start
        .checked_add(frame_compressed_len)
        .ok_or_else(|| Error::invalid("frame payload offset overflow"))?;
    if frame_end > latest_frame_end {
        return Err(Error::invalid("frame payload exceeds value-change block"));
    }
    let mut frame_bytes = vec![0u8; frame_compressed_len_usize];
    if frame_compressed_len > 0 {
        reader.read_exact(&mut frame_bytes)?;
    }
    let frame = FrameSection::decode(
        frame_uncompressed_len,
        frame_compressed_len,
        frame_bytes,
        frame_max_handle,
    )?;

    let (vc_max_handle, _) = read_varint_from_reader(reader)?;
    if vc_max_handle > max_handles || vc_max_handle > u64::from(u32::MAX) {
        return Err(Error::invalid(format!(
            "value-change block declares {vc_max_handle} handles, above supported/configured limit {}",
            max_handles.min(u64::from(u32::MAX))
        )));
    }
    let vc_max_handle_usize = usize::try_from(vc_max_handle)
        .map_err(|_| Error::invalid("value-change handle count exceeds addressable memory"))?;

    let mut pack = [0u8; 1];
    reader.read_exact(&mut pack)?;
    let pack_marker = PackMarker::new(pack[0])
        .ok_or_else(|| Error::decode(format!("unknown pack marker {:02x}", pack[0])))?;

    let chain_start = reader.stream_position()?;

    let time_trailer_start = block_end
        .checked_sub(24)
        .ok_or_else(|| Error::invalid("value-change trailer underflow"))?;
    reader.seek(SeekFrom::Start(time_trailer_start))?;
    let time_uncompressed_len = u64::from_be_bytes(crate::util::read_array::<8, _>(reader)?);
    let time_compressed_len = u64::from_be_bytes(crate::util::read_array::<8, _>(reader)?);
    let time_item_count = u64::from_be_bytes(crate::util::read_array::<8, _>(reader)?);
    if time_uncompressed_len > max_block_bytes || time_compressed_len > max_block_bytes {
        return Err(Error::invalid("time table exceeds configured block limit"));
    }

    let time_section = TimeSection {
        uncompressed_len: time_uncompressed_len,
        compressed_len: time_compressed_len,
        item_count: time_item_count,
    };

    let time_data_len = time_section.compressed_len;
    let time_data_start = time_trailer_start
        .checked_sub(time_data_len)
        .ok_or_else(|| Error::invalid("invalid time section lengths"))?;

    let index_length_pos = time_data_start
        .checked_sub(8)
        .ok_or_else(|| Error::invalid("missing index length trailer"))?;
    reader.seek(SeekFrom::Start(index_length_pos))?;
    let index_length = u64::from_be_bytes(crate::util::read_array::<8, _>(reader)?);
    if index_length > max_block_bytes {
        return Err(Error::invalid("chain index exceeds configured block limit"));
    }

    let index_start = index_length_pos
        .checked_sub(index_length)
        .ok_or_else(|| Error::invalid("index length exceeds block bounds"))?;

    let chain_end = index_start;
    if chain_end < chain_start {
        return Err(Error::invalid("chain index overlaps value-change header"));
    }

    let header = VcBlock {
        begin_time,
        end_time,
        required_memory,
        frame_uncompressed_len,
        frame_compressed_len,
        frame_max_handle,
        vc_max_handle,
        pack_marker,
        index_length,
    };

    let index = decode_chain_index(
        reader,
        block_type,
        index_start,
        index_length,
        vc_max_handle_usize,
        chain_start,
        chain_end,
    )?;

    reader.seek(SeekFrom::Start(time_data_start))?;
    let time_data_len_usize = usize::try_from(time_data_len)
        .map_err(|_| Error::invalid("time section exceeds addressable memory"))?;
    let mut time_bytes = vec![0u8; time_data_len_usize];
    if time_data_len_usize > 0 {
        reader.read_exact(&mut time_bytes)?;
    }
    let time_table = TimeTable::decode(&time_section, time_bytes)?;

    let chain_span = chain_end
        .checked_sub(chain_start)
        .ok_or_else(|| Error::invalid("negative chain range"))?;
    let chain_len = usize::try_from(chain_span)
        .map_err(|_| Error::invalid("chain buffer exceeds addressable memory"))?;
    reader.seek(SeekFrom::Start(chain_start))?;
    let mut chain_buffer = vec![0u8; chain_len];
    if chain_len > 0 {
        reader.read_exact(&mut chain_buffer)?;
    }

    reader.seek(SeekFrom::Start(block_end))?;

    let (chains, decoded_chain_buffer) = build_chains(
        &chain_buffer,
        chain_start,
        &index,
        header.pack_marker.pack_type,
    )?;

    Ok(VcBlockMeta {
        header,
        frame,
        chain_buffer,
        decoded_chain_buffer,
        chains,
        time_section,
        time_table,
        index,
        double_byte_order,
    })
}

fn build_chains(
    buffer: &[u8],
    chain_start: u64,
    index: &ChainIndex,
    pack_type: PackType,
) -> Result<(Vec<Option<ChainData>>, Vec<u8>)> {
    struct ChainJob<'a> {
        handle_index: usize,
        alias_of: Option<u32>,
        stored_len: u64,
        compressed: &'a [u8],
    }

    struct ChainJobResult {
        handle_index: usize,
        alias_of: Option<u32>,
        stored_len: u32,
        payload: Vec<u8>,
    }

    let mut chains = vec![None; index.slots.len()];
    let mut jobs = Vec::new();

    for (handle_index, slot_opt) in index.slots.iter().enumerate() {
        let Some(slot) = slot_opt else {
            continue;
        };
        if slot.alias_of.is_some() {
            continue;
        }

        let rel_offset = slot
            .offset
            .checked_sub(chain_start)
            .ok_or_else(|| Error::decode("chain slot precedes chain buffer"))?;
        let rel_offset = usize::try_from(rel_offset)
            .map_err(|_| Error::decode("chain offset exceeds addressable memory"))?;
        let length = usize::try_from(slot.length)
            .map_err(|_| Error::decode("chain length exceeds addressable memory"))?;
        let end = rel_offset
            .checked_add(length)
            .ok_or_else(|| Error::decode("chain slot length overflow"))?;
        if end > buffer.len() {
            return Err(Error::decode("chain slot exceeds buffer bounds"));
        }
        let slice = &buffer[rel_offset..end];
        let (stored_len, consumed) = decode_varint_with_len(slice)?;
        if consumed > slice.len() {
            return Err(Error::decode("chain stored length prefix out of bounds"));
        }
        let payload_bytes = &slice[consumed..];

        if stored_len == 0 {
            let range_start = rel_offset + consumed;
            let range_end = rel_offset + length;
            chains[handle_index] = Some(ChainData {
                handle: (handle_index + 1) as u32,
                stored_len: (length - consumed) as u32,
                payload: ChainPayload::Borrowed {
                    range: range_start..range_end,
                },
                alias_of: slot.alias_of,
            });
        } else {
            jobs.push(ChainJob {
                handle_index,
                alias_of: slot.alias_of,
                stored_len,
                compressed: payload_bytes,
            });
        }
    }

    let decompress = |job: ChainJob<'_>| -> Result<ChainJobResult> {
        let expected = usize::try_from(job.stored_len)
            .map_err(|_| Error::decode("chain stored length exceeds addressable memory"))?;
        let data = decompress_chain_payload(pack_type, job.compressed, expected)?;
        let stored_len = u32::try_from(job.stored_len)
            .map_err(|_| Error::decode("chain stored length exceeds u32 range"))?;
        Ok(ChainJobResult {
            handle_index: job.handle_index,
            alias_of: job.alias_of,
            stored_len,
            payload: data,
        })
    };

    #[cfg(feature = "parallel")]
    let results: Vec<ChainJobResult> = {
        let decoded_bytes: u64 = jobs.iter().map(|job| job.stored_len).sum();
        if jobs.len() < 32 || decoded_bytes < 64 * 1024 {
            jobs.into_iter().map(decompress).collect::<Result<_>>()?
        } else {
            jobs.into_par_iter()
                .map(decompress)
                .collect::<Result<Vec<_>>>()?
        }
    };

    #[cfg(not(feature = "parallel"))]
    let results: Vec<ChainJobResult> = jobs.into_iter().map(decompress).collect::<Result<_>>()?;

    let decoded_capacity = results.iter().map(|result| result.payload.len()).sum();
    let mut decoded_arena = Vec::with_capacity(decoded_capacity);
    let mut decoded_meta = Vec::with_capacity(results.len());
    for result in results {
        let start = decoded_arena.len();
        decoded_arena.extend_from_slice(&result.payload);
        let end = decoded_arena.len();
        decoded_meta.push((
            result.handle_index,
            result.alias_of,
            result.stored_len,
            start..end,
        ));
    }
    for (handle_index, alias_of, stored_len, range) in decoded_meta {
        chains[handle_index] = Some(ChainData {
            handle: (handle_index + 1) as u32,
            stored_len,
            payload: ChainPayload::Decoded { range },
            alias_of,
        });
    }

    for (handle_index, slot_opt) in index.slots.iter().enumerate() {
        let Some(slot) = slot_opt else {
            continue;
        };
        let Some(target) = slot.alias_of else {
            continue;
        };
        let target_index = (target - 1) as usize;
        let canonical = chains
            .get(target_index)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::decode("dynamic alias target has no canonical chain"))?;
        chains[handle_index] = Some(ChainData {
            handle: (handle_index + 1) as u32,
            stored_len: canonical.stored_len,
            payload: canonical.payload.clone(),
            alias_of: Some(target),
        });
    }

    Ok((chains, decoded_arena))
}

fn decompress_chain_payload(
    pack_type: PackType,
    input: &[u8],
    expected_len: usize,
) -> Result<Vec<u8>> {
    match pack_type {
        PackType::None => {
            if input.len() != expected_len {
                return Err(Error::decode("chain length mismatch"));
            }
            Ok(input.to_vec())
        }
        PackType::Zlib => {
            #[cfg(feature = "gzip")]
            {
                let decoder = ZlibDecoder::new(input);
                let mut out = Vec::with_capacity(expected_len.min(16 * 1024 * 1024));
                let limit = u64::try_from(expected_len)
                    .unwrap_or(u64::MAX)
                    .saturating_add(1);
                decoder.take(limit).read_to_end(&mut out)?;
                if out.len() != expected_len {
                    return Err(Error::decode("chain zlib length mismatch"));
                }
                Ok(out)
            }
            #[cfg(not(feature = "gzip"))]
            {
                let _ = (input, expected_len);
                Err(Error::unsupported(
                    "chain zlib decompression requires the `gzip` feature",
                ))
            }
        }
        PackType::Lz4 => {
            #[cfg(feature = "lz4")]
            {
                let out = lz4_decompress(input, expected_len)
                    .map_err(|e| Error::decode(e.to_string()))?;
                if out.len() != expected_len {
                    return Err(Error::decode("chain lz4 length mismatch"));
                }
                Ok(out)
            }
            #[cfg(not(feature = "lz4"))]
            {
                let _ = (input, expected_len);
                Err(Error::unsupported(
                    "chain lz4 decompression requires the `lz4` feature",
                ))
            }
        }
        PackType::FastLz => {
            #[cfg(feature = "fastlz")]
            {
                let input_len = i32::try_from(input.len())
                    .map_err(|_| Error::decode("fastlz chain length exceeds i32 range"))?;
                let expected_len_i32 = i32::try_from(expected_len)
                    .map_err(|_| Error::decode("fastlz output length exceeds i32 range"))?;
                let mut out = vec![0u8; expected_len];
                // SAFETY: the input and output pointers remain valid for the exact lengths passed
                // to FastLZ. The decoder is bounded by `expected_len_i32`, and its return value is
                // validated below before the initialized output is exposed.
                let written = unsafe {
                    fastlz_decompress(
                        input.as_ptr().cast(),
                        input_len,
                        out.as_mut_ptr().cast(),
                        expected_len_i32,
                    )
                };
                if written <= 0 {
                    return Err(Error::decode("fastlz decompression failed"));
                }
                let written_usize = usize::try_from(written).map_err(|_| {
                    Error::decode("fastlz decoded length exceeds addressable memory")
                })?;
                if written_usize != expected_len {
                    return Err(Error::decode("chain fastlz length mismatch"));
                }
                Ok(out)
            }
            #[cfg(not(feature = "fastlz"))]
            {
                let _ = (input, expected_len);
                Err(Error::unsupported(
                    "chain fastlz decompression requires the `fastlz` feature",
                ))
            }
        }
    }
}

fn decode_chain_index<R: Read + Seek>(
    reader: &mut R,
    block_type: BlockType,
    index_start: u64,
    index_length: u64,
    max_handle_hint: usize,
    chain_start: u64,
    chain_end: u64,
) -> Result<ChainIndex> {
    reader.seek(SeekFrom::Start(index_start))?;
    let index_len_usize = usize::try_from(index_length)
        .map_err(|_| Error::invalid("index length exceeds addressable memory"))?;
    let mut bytes = vec![0u8; index_len_usize];
    reader.read_exact(&mut bytes)?;

    #[derive(Debug, Clone)]
    enum EntryTmp {
        Empty,
        Data { offset: u64 },
        Alias { target: usize },
    }

    let mut entries: Vec<EntryTmp> = Vec::with_capacity(max_handle_hint);
    let mut has_payload: Vec<bool> = Vec::with_capacity(max_handle_hint);
    let mut slice = bytes.as_slice();
    let mut last_offset = 0u64;
    let mut last_alias_target: Option<usize> = None;

    while !slice.is_empty() {
        if block_type == BlockType::VcDataDynAlias2 && (slice[0] & 0x01) != 0 {
            let mut tmp = slice;
            let raw = decode_svarint(&mut tmp)?;
            let shval = raw >> 1;
            slice = tmp;

            if shval > 0 {
                if entries.len() >= max_handle_hint {
                    return Err(Error::decode("chain index contains too many handles"));
                }
                last_offset = last_offset
                    .checked_add(shval as u64)
                    .ok_or_else(|| Error::decode("chain index overflow"))?;
                entries.push(EntryTmp::Data {
                    offset: last_offset,
                });
                has_payload.push(true);
                last_alias_target = None;
            } else if shval < 0 {
                if entries.len() >= max_handle_hint {
                    return Err(Error::decode("chain index contains too many handles"));
                }
                let target = ((-shval) as u64)
                    .checked_sub(1)
                    .ok_or_else(|| Error::decode("invalid alias target"))?;
                let target = usize::try_from(target)
                    .map_err(|_| Error::decode("alias target exceeds addressable range"))?;
                entries.push(EntryTmp::Alias { target });
                has_payload.push(false);
                last_alias_target = Some(target);
            } else if let Some(target) = last_alias_target {
                if entries.len() >= max_handle_hint {
                    return Err(Error::decode("chain index contains too many handles"));
                }
                entries.push(EntryTmp::Alias { target });
                has_payload.push(false);
            } else {
                if entries.len() >= max_handle_hint {
                    return Err(Error::decode("chain index contains too many handles"));
                }
                entries.push(EntryTmp::Empty);
                has_payload.push(false);
            }
            continue;
        }

        let (value, consumed) = decode_varint_with_len(slice)?;
        slice = &slice[consumed..];

        if value == 0 {
            let (alias, alias_consumed) = decode_varint_with_len(slice)?;
            slice = &slice[alias_consumed..];
            if alias == 0 {
                if entries.len() >= max_handle_hint {
                    return Err(Error::decode("chain index contains too many handles"));
                }
                entries.push(EntryTmp::Empty);
                has_payload.push(false);
                last_alias_target = None;
            } else {
                if entries.len() >= max_handle_hint {
                    return Err(Error::decode("chain index contains too many handles"));
                }
                let target = alias
                    .checked_sub(1)
                    .ok_or_else(|| Error::decode("invalid alias handle"))?;
                let target = usize::try_from(target)
                    .map_err(|_| Error::decode("alias target exceeds addressable range"))?;
                entries.push(EntryTmp::Alias { target });
                has_payload.push(false);
                last_alias_target = Some(target);
            }
            continue;
        }

        if (value & 1) == 0 {
            let repeat = usize::try_from(value >> 1)
                .map_err(|_| Error::decode("empty chain run exceeds addressable range"))?;
            let remaining = max_handle_hint.saturating_sub(entries.len());
            if repeat == 0 || repeat > remaining {
                return Err(Error::decode("invalid empty run in chain index"));
            }
            entries.extend((0..repeat).map(|_| EntryTmp::Empty));
            has_payload.resize(has_payload.len() + repeat, false);
            continue;
        }

        if entries.len() >= max_handle_hint {
            return Err(Error::decode("chain index contains too many handles"));
        }
        let delta = value >> 1;
        last_offset = last_offset
            .checked_add(delta)
            .ok_or_else(|| Error::decode("chain index overflow"))?;
        entries.push(EntryTmp::Data {
            offset: last_offset,
        });
        has_payload.push(true);
        last_alias_target = None;
    }

    if entries.len() != max_handle_hint {
        return Err(Error::decode(format!(
            "chain index describes {} handles, expected {max_handle_hint}",
            entries.len()
        )));
    }

    let total_chain_len = chain_end
        .checked_sub(chain_start)
        .ok_or_else(|| Error::invalid("negative chain range"))?;

    let mut offsets = Vec::<Option<u64>>::with_capacity(entries.len());
    let mut lengths = Vec::<Option<u32>>::with_capacity(entries.len());
    let mut alias_targets = Vec::<Option<usize>>::with_capacity(entries.len());

    for entry in &entries {
        match entry {
            EntryTmp::Empty => {
                offsets.push(None);
                lengths.push(None);
                alias_targets.push(None);
            }
            EntryTmp::Data { offset } => {
                offsets.push(Some(*offset));
                lengths.push(None);
                alias_targets.push(None);
            }
            EntryTmp::Alias { target } => {
                offsets.push(None);
                lengths.push(None);
                alias_targets.push(Some(*target));
            }
        }
    }

    const PACK_MARKER_PREFIX: u64 = 1;
    for off in offsets.iter_mut().flatten() {
        if *off < PACK_MARKER_PREFIX {
            return Err(Error::decode("chain offset precedes pack marker"));
        }
        *off -= PACK_MARKER_PREFIX;
        if *off > total_chain_len {
            return Err(Error::decode("chain offset exceeds chain payload bounds"));
        }
    }

    let mut prev_data_idx: Option<usize> = None;
    for idx in 0..offsets.len() {
        if let Some(off) = offsets[idx] {
            if let Some(prev) = prev_data_idx
                && let Some(prev_off) = offsets[prev]
            {
                let span = off
                    .checked_sub(prev_off)
                    .ok_or_else(|| Error::decode("chain offsets are not monotonic"))?;
                lengths[prev] = Some(
                    u32::try_from(span)
                        .map_err(|_| Error::decode("chain payload exceeds u32 range"))?,
                );
            }
            prev_data_idx = Some(idx);
        }
    }
    if let Some(last_idx) = prev_data_idx
        && let Some(last_off) = offsets[last_idx]
    {
        let span = total_chain_len
            .checked_sub(last_off)
            .ok_or_else(|| Error::decode("chain offset exceeds payload bounds"))?;
        lengths[last_idx] = Some(
            u32::try_from(span).map_err(|_| Error::decode("chain payload exceeds u32 range"))?,
        );
    }

    fn resolve(
        idx: usize,
        offsets: &mut [Option<u64>],
        lengths: &mut [Option<u32>],
        alias_targets: &[Option<usize>],
        visiting: &mut [bool],
    ) -> Option<(u64, u32)> {
        if let (Some(off), Some(len)) = (offsets[idx], lengths[idx]) {
            return Some((off, len));
        }
        if visiting[idx] {
            return None;
        }
        visiting[idx] = true;
        if let Some(target) = alias_targets[idx]
            && target < offsets.len()
            && let Some((off, len)) = resolve(target, offsets, lengths, alias_targets, visiting)
        {
            offsets[idx] = Some(off);
            lengths[idx] = Some(len);
            visiting[idx] = false;
            return Some((off, len));
        }
        visiting[idx] = false;
        None
    }

    let mut visiting = vec![false; offsets.len()];
    for idx in 0..offsets.len() {
        if offsets[idx].is_none() {
            let _ = resolve(
                idx,
                &mut offsets,
                &mut lengths,
                &alias_targets,
                &mut visiting,
            );
        }
    }

    #[derive(Clone, Copy)]
    enum CanonicalResolution {
        Unknown,
        Missing,
        Found(usize),
    }

    fn resolve_canonical(
        idx: usize,
        alias_targets: &[Option<usize>],
        has_payload: &[bool],
        memo: &mut [CanonicalResolution],
        visiting: &mut [bool],
    ) -> Option<usize> {
        match memo[idx] {
            CanonicalResolution::Found(canonical) => return Some(canonical),
            CanonicalResolution::Missing => return None,
            CanonicalResolution::Unknown => {}
        }
        if visiting[idx] {
            memo[idx] = CanonicalResolution::Missing;
            return None;
        }
        visiting[idx] = true;
        let result = if has_payload[idx] {
            Some(idx)
        } else if let Some(target) = alias_targets[idx] {
            if target < alias_targets.len() {
                resolve_canonical(target, alias_targets, has_payload, memo, visiting)
            } else {
                None
            }
        } else {
            None
        };
        visiting[idx] = false;
        memo[idx] = result.map_or(CanonicalResolution::Missing, CanonicalResolution::Found);
        result
    }

    let mut canonical_memo = vec![CanonicalResolution::Unknown; alias_targets.len()];
    let mut canonical_visiting = vec![false; alias_targets.len()];
    let mut canonical = Vec::with_capacity(alias_targets.len());
    for idx in 0..alias_targets.len() {
        let resolved = resolve_canonical(
            idx,
            &alias_targets,
            &has_payload,
            &mut canonical_memo,
            &mut canonical_visiting,
        );
        canonical.push(resolved);
    }

    let mut slots = Vec::with_capacity(offsets.len());
    for idx in 0..offsets.len() {
        match (offsets[idx], lengths[idx]) {
            (Some(off), Some(len)) => {
                let absolute = chain_start
                    .checked_add(off)
                    .ok_or_else(|| Error::invalid("chain offset overflow"))?;
                let alias_handle = if has_payload[idx] {
                    None
                } else if let Some(canon_idx) = canonical[idx] {
                    Some(
                        u32::try_from(canon_idx + 1)
                            .map_err(|_| Error::invalid("alias target exceeds u32 range"))?,
                    )
                } else {
                    None
                };
                slots.push(Some(ChainSlot {
                    offset: absolute,
                    length: len,
                    alias_of: alias_handle,
                }));
            }
            _ => slots.push(None),
        }
    }

    Ok(ChainIndex { slots })
}
