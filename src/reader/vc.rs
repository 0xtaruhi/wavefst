use std::borrow::Cow;
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
use crate::types::{BlockType, PackType};
use crate::util::{read_u64_be, read_varint_from_reader};

/// Fully decoded metadata and payload slices extracted from a value-change block.
#[derive(Debug, Clone)]
pub struct VcBlockMeta {
    pub header: VcBlock,
    pub frame: FrameSection,
    pub chain_buffer: Vec<u8>,
    pub chains: Vec<Option<ChainData>>,
    pub time_section: TimeSection,
    pub time_table: TimeTable,
    pub index: ChainIndex,
}

/// Resolved per-handle chain metadata extracted from the block index.
#[derive(Debug, Clone, Default)]
pub struct ChainIndex {
    pub slots: Vec<Option<ChainSlot>>,
}

/// Offset/length pair describing where compressed chain data resides for a handle.
#[derive(Debug, Clone, Copy)]
pub struct ChainSlot {
    pub offset: u64,
    pub length: u32,
    pub alias_of: Option<u32>,
}

/// In-memory representation of a handle's change stream.
#[derive(Debug, Clone)]
pub struct ChainData {
    pub handle: u32,
    pub stored_len: u32,
    pub payload: ChainPayload,
    pub alias_of: Option<u32>,
}

/// Borrowed or owned slice containing uncompressed chain bytes.
#[derive(Debug, Clone)]
pub enum ChainPayload {
    Borrowed { range: Range<usize> },
    Owned(Vec<u8>),
}

impl ChainPayload {
    /// Returns a view of the payload as a byte slice, borrowing when possible.
    pub fn as_slice<'a>(&'a self, backing: &'a [u8]) -> Cow<'a, [u8]> {
        match self {
            ChainPayload::Borrowed { range } => Cow::Borrowed(&backing[range.clone()]),
            ChainPayload::Owned(data) => Cow::Borrowed(data),
        }
    }

    pub fn len(&self, _backing: &[u8]) -> usize {
        match self {
            ChainPayload::Borrowed { range } => range.end - range.start,
            ChainPayload::Owned(data) => data.len(),
        }
    }
}

pub fn parse_vc_block<R: Read + Seek>(
    reader: &mut R,
    block_type: BlockType,
    section_start: u64,
    payload_len: u64,
) -> Result<VcBlockMeta> {
    let begin_time = read_u64_be(reader)?;
    let end_time = read_u64_be(reader)?;
    let required_memory = read_u64_be(reader)?;
    let (frame_uncompressed_len, _) = read_varint_from_reader(reader)?;
    let (frame_compressed_len, _) = read_varint_from_reader(reader)?;
    let (frame_max_handle, _) = read_varint_from_reader(reader)?;

    let mut frame_bytes = vec![0u8; frame_compressed_len as usize];
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

    let mut pack = [0u8; 1];
    reader.read_exact(&mut pack)?;
    let pack_marker = PackMarker::new(pack[0])
        .ok_or_else(|| Error::decode(format!("unknown pack marker {:02x}", pack[0])))?;

    let chain_start = reader.stream_position()?;
    let block_end = section_start
        .checked_add(payload_len)
        .ok_or_else(|| Error::invalid("value-change block exceeds file bounds"))?;

    if payload_len < 32 {
        return Err(Error::invalid(
            "value-change payload shorter than required trailer",
        ));
    }

    let time_trailer_start = block_end
        .checked_sub(24)
        .ok_or_else(|| Error::invalid("value-change trailer underflow"))?;
    reader.seek(SeekFrom::Start(time_trailer_start))?;
    let time_uncompressed_len = u64::from_be_bytes(crate::util::read_array::<8, _>(reader)?);
    let time_compressed_len = u64::from_be_bytes(crate::util::read_array::<8, _>(reader)?);
    let time_item_count = u64::from_be_bytes(crate::util::read_array::<8, _>(reader)?);

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

    let index_start = index_length_pos
        .checked_sub(index_length)
        .ok_or_else(|| Error::invalid("index length exceeds block bounds"))?;

    let chain_end = index_start;

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
        vc_max_handle as usize,
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

    let chains = build_chains(
        &chain_buffer,
        chain_start,
        &index,
        header.pack_marker.pack_type,
    )?;

    Ok(VcBlockMeta {
        header,
        frame,
        chain_buffer,
        chains,
        time_section,
        time_table,
        index,
    })
}

fn build_chains(
    buffer: &[u8],
    chain_start: u64,
    index: &ChainIndex,
    pack_type: PackType,
) -> Result<Vec<Option<ChainData>>> {
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

        let rel_offset = (slot.offset - chain_start) as usize;
        let length = slot.length as usize;
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
                handle: handle_index as u32,
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
        if jobs.len() <= 1 {
            jobs.into_iter().map(decompress).collect::<Result<_>>()?
        } else {
            jobs.into_par_iter()
                .map(decompress)
                .collect::<Result<Vec<_>>>()?
        }
    };

    #[cfg(not(feature = "parallel"))]
    let results: Vec<ChainJobResult> = jobs.into_iter().map(decompress).collect::<Result<_>>()?;

    for result in results {
        chains[result.handle_index] = Some(ChainData {
            handle: result.handle_index as u32,
            stored_len: result.stored_len,
            payload: ChainPayload::Owned(result.payload),
            alias_of: result.alias_of,
        });
    }

    Ok(chains)
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
                let mut decoder = ZlibDecoder::new(input);
                let mut out = Vec::with_capacity(expected_len);
                decoder.read_to_end(&mut out)?;
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
                let written = unsafe {
                    fastlz_decompress(
                        input.as_ptr() as *const _,
                        input_len,
                        out.as_mut_ptr() as *mut _,
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

    let mut entries: Vec<EntryTmp> = Vec::with_capacity(max_handle_hint + 1);
    let mut has_payload: Vec<bool> = Vec::with_capacity(max_handle_hint + 1);
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
                last_offset = last_offset
                    .checked_add(shval as u64)
                    .ok_or_else(|| Error::decode("chain index overflow"))?;
                entries.push(EntryTmp::Data {
                    offset: last_offset,
                });
                has_payload.push(true);
                last_alias_target = None;
            } else if shval < 0 {
                let target = ((-shval) as u64)
                    .checked_sub(1)
                    .ok_or_else(|| Error::decode("invalid alias target"))?
                    as usize;
                entries.push(EntryTmp::Alias { target });
                has_payload.push(false);
                last_alias_target = Some(target);
            } else if let Some(target) = last_alias_target {
                entries.push(EntryTmp::Alias { target });
                has_payload.push(false);
            } else {
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
                entries.push(EntryTmp::Empty);
                has_payload.push(false);
                last_alias_target = None;
            } else {
                let target = alias
                    .checked_sub(1)
                    .ok_or_else(|| Error::decode("invalid alias handle"))?
                    as usize;
                entries.push(EntryTmp::Alias { target });
                has_payload.push(false);
                last_alias_target = Some(target);
            }
            continue;
        }

        if (value & 1) == 0 {
            let repeat = (value >> 1) as usize;
            for _ in 0..repeat {
                entries.push(EntryTmp::Empty);
                has_payload.push(false);
            }
            continue;
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
    }

    let mut prev_data_idx: Option<usize> = None;
    for idx in 0..offsets.len() {
        if let Some(off) = offsets[idx] {
            if let Some(prev) = prev_data_idx
                && let Some(prev_off) = offsets[prev]
            {
                lengths[prev] = Some((off - prev_off) as u32);
            }
            prev_data_idx = Some(idx);
        }
    }
    if let Some(last_idx) = prev_data_idx
        && let Some(last_off) = offsets[last_idx]
    {
        lengths[last_idx] = Some((total_chain_len - last_off) as u32);
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

    fn resolve_canonical(
        idx: usize,
        alias_targets: &[Option<usize>],
        has_payload: &[bool],
        memo: &mut [Option<Option<usize>>],
        visiting: &mut [bool],
    ) -> Option<usize> {
        if let Some(cached) = memo[idx] {
            return cached;
        }
        if visiting[idx] {
            memo[idx] = Some(None);
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
        memo[idx] = Some(result);
        result
    }

    let mut canonical_memo = vec![None; alias_targets.len()];
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

    if slots.len() < max_handle_hint + 1 {
        slots.resize(max_handle_hint + 1, None);
    }

    Ok(ChainIndex { slots })
}
