use std::io::{Read, Seek, SeekFrom};
use std::ops::{Range, RangeInclusive};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "fastlz")]
use fastlz_sys::fastlz_decompress;
#[cfg(feature = "gzip")]
use libdeflater::Decompressor as LibdeflateDecompressor;
#[cfg(feature = "lz4")]
use lz4_flex::block::decompress as lz4_decompress;

use crate::block::{FrameSection, PackMarker, TimeSection, TimeTable, VcBlock};
#[cfg(feature = "gzip")]
use crate::compression::{with_decompressor, zlib_decompress_into_with, zlib_decompress_with};
use crate::encoding::{decode_svarint_with_len, decode_varint_with_len};
use crate::error::{Error, Result};
use crate::types::{BlockType, CodecParallelism, FstByteOrder, PackType};
#[cfg(feature = "parallel")]
use crate::util::{codec_partition_len, codec_pool};
use crate::util::{read_u64_be, read_varint_from_reader};

/// Fully decoded metadata and payload slices extracted from a value-change block.
#[derive(Debug, Clone)]
pub struct VcBlockMeta {
    /// Fixed metadata from the value-change block header.
    pub header: VcBlock,
    /// Decoded frame section containing values at the block boundary.
    pub frame: FrameSection,
    /// Stored/raw chain payload arena; compacted to selected chains when filtered.
    pub chain_buffer: Vec<u8>,
    /// Arena containing chains that required decompression.
    pub decoded_chain_buffer: Vec<u8>,
    /// Canonical change chains present in this block.
    pub chains: Vec<ChainData>,
    /// Metadata describing the encoded time table.
    pub time_section: TimeSection,
    /// Fully decoded timestamp table.
    pub time_table: TimeTable,
    /// Per-handle chain locations and dynamic aliases.
    pub index: ChainIndex,
    /// Byte order used to decode real-valued changes.
    pub double_byte_order: FstByteOrder,
    /// Normalized one-based handle filter used to decode this block, if any.
    pub included_handles: Option<Vec<u32>>,
    /// Inclusive timestamp range applied while traversing this block, if any.
    pub time_range: Option<RangeInclusive<u64>>,
}

/// Resolved per-handle chain metadata extracted from the block index.
#[derive(Debug, Clone, Default)]
pub struct ChainIndex {
    dense_slots: Vec<Option<ChainSlot>>,
    sparse_slots: Vec<(u32, ChainSlot)>,
    max_handle: u32,
}

impl ChainIndex {
    fn dense(slots: Vec<Option<ChainSlot>>) -> Self {
        Self {
            max_handle: slots.len() as u32,
            dense_slots: slots,
            sparse_slots: Vec::new(),
        }
    }

    fn sparse(max_handle: usize, mut slots: Vec<(u32, ChainSlot)>) -> Result<Self> {
        slots.sort_unstable_by_key(|(handle, _)| *handle);
        slots.dedup_by_key(|(handle, _)| *handle);
        Ok(Self {
            dense_slots: Vec::new(),
            sparse_slots: slots,
            max_handle: u32::try_from(max_handle)
                .map_err(|_| Error::invalid("chain index handle count exceeds u32 range"))?,
        })
    }

    /// Returns the highest handle represented by the on-disk index.
    #[must_use]
    pub fn max_handle(&self) -> u32 {
        self.max_handle
    }

    /// Looks up a one-based handle without materializing empty slots for sparse selections.
    #[must_use]
    pub fn get(&self, handle: u32) -> Option<&ChainSlot> {
        if handle == 0 || handle > self.max_handle {
            return None;
        }
        if self.sparse_slots.is_empty() {
            return self
                .dense_slots
                .get((handle - 1) as usize)
                .and_then(Option::as_ref);
        }
        self.sparse_slots
            .binary_search_by_key(&handle, |(candidate, _)| *candidate)
            .ok()
            .map(|index| &self.sparse_slots[index].1)
    }

    /// Iterates non-empty slots as `(one_based_handle, slot)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &ChainSlot)> {
        self.dense_slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| slot.as_ref().map(|slot| ((index + 1) as u32, slot)))
            .chain(
                self.sparse_slots
                    .iter()
                    .map(|(handle, slot)| (*handle, slot)),
            )
    }
}

/// Offset/length pair describing where compressed chain data resides for a handle.
#[derive(Debug, Clone, Copy)]
pub struct ChainSlot {
    /// Byte offset of the stored chain within the chain section.
    pub offset: u64,
    /// Stored chain length in bytes.
    pub length: u32,
    alias_handle: u32,
}

impl ChainSlot {
    /// Returns the canonical handle when this slot is a dynamic alias.
    #[must_use]
    pub fn alias_of(&self) -> Option<u32> {
        (self.alias_handle != 0).then_some(self.alias_handle)
    }
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

pub(crate) struct VcParseOptions<'a> {
    pub max_block_bytes: u64,
    pub max_handles: u64,
    pub double_byte_order: FstByteOrder,
    pub codec_parallelism: CodecParallelism,
    pub included_handles: Option<&'a [u32]>,
    pub time_range: Option<RangeInclusive<u64>>,
}

struct ChainStorage {
    stored: Vec<u8>,
    chains: Vec<ChainData>,
    decoded: Vec<u8>,
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
    options: VcParseOptions<'_>,
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

    if required_memory > options.max_block_bytes || frame_uncompressed_len > options.max_block_bytes
    {
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
    if vc_max_handle > options.max_handles || vc_max_handle > u64::from(u32::MAX) {
        return Err(Error::invalid(format!(
            "value-change block declares {vc_max_handle} handles, above supported/configured limit {}",
            options.max_handles.min(u64::from(u32::MAX))
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
    if time_uncompressed_len > options.max_block_bytes
        || time_compressed_len > options.max_block_bytes
    {
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
    if index_length > options.max_block_bytes {
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
        ChainIndexLayout {
            index_start,
            index_length,
            chain_start,
            chain_end,
            max_handle: vc_max_handle_usize,
        },
        options.included_handles,
    )?;

    reader.seek(SeekFrom::Start(time_data_start))?;
    let time_data_len_usize = usize::try_from(time_data_len)
        .map_err(|_| Error::invalid("time section exceeds addressable memory"))?;
    let mut time_bytes = vec![0u8; time_data_len_usize];
    if time_data_len_usize > 0 {
        reader.read_exact(&mut time_bytes)?;
    }
    let time_table = TimeTable::decode(&time_section, time_bytes)?;

    let storage = match options.included_handles {
        None => {
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
            let (chains, decoded_chain_buffer) = build_chains(
                &chain_buffer,
                chain_start,
                &index,
                header.pack_marker.pack_type,
                options.codec_parallelism,
            )?;
            ChainStorage {
                stored: chain_buffer,
                chains,
                decoded: decoded_chain_buffer,
            }
        }
        Some(handles) => read_selected_chains(
            reader,
            &index,
            handles,
            header.pack_marker.pack_type,
            options.codec_parallelism,
        )?,
    };

    reader.seek(SeekFrom::Start(block_end))?;

    Ok(VcBlockMeta {
        header,
        frame,
        chain_buffer: storage.stored,
        decoded_chain_buffer: storage.decoded,
        chains: storage.chains,
        time_section,
        time_table,
        index,
        double_byte_order: options.double_byte_order,
        included_handles: options.included_handles.map(<[u32]>::to_vec),
        time_range: options.time_range,
    })
}

fn read_selected_chains<R: Read + Seek>(
    reader: &mut R,
    index: &ChainIndex,
    included_handles: &[u32],
    pack_type: PackType,
    codec_parallelism: CodecParallelism,
) -> Result<ChainStorage> {
    let mut canonical_needed = Vec::with_capacity(included_handles.len());
    for &handle in included_handles {
        let Some(slot) = index.get(handle) else {
            continue;
        };
        let canonical = slot.alias_of().unwrap_or(handle);
        if canonical > index.max_handle() {
            return Err(Error::decode("dynamic alias target exceeds chain index"));
        }
        canonical_needed.push(canonical);
    }
    canonical_needed.sort_unstable();
    canonical_needed.dedup();

    let mut chain_buffer = Vec::new();
    let mut filtered_slots = Vec::with_capacity(canonical_needed.len() + included_handles.len());
    for handle in canonical_needed {
        let slot = index
            .get(handle)
            .ok_or_else(|| Error::decode("selected canonical handle has no chain slot"))?;
        if slot.alias_of().is_some() {
            return Err(Error::decode("resolved canonical chain is still an alias"));
        }
        let length = usize::try_from(slot.length)
            .map_err(|_| Error::decode("chain length exceeds addressable memory"))?;
        let compact_offset = u64::try_from(chain_buffer.len())
            .map_err(|_| Error::decode("selected chain arena exceeds u64 range"))?;
        let end = chain_buffer
            .len()
            .checked_add(length)
            .ok_or_else(|| Error::decode("selected chain arena length overflow"))?;
        chain_buffer.resize(end, 0);
        reader.seek(SeekFrom::Start(slot.offset))?;
        reader.read_exact(&mut chain_buffer[end - length..end])?;
        let compact_slot = ChainSlot {
            offset: compact_offset,
            length: slot.length,
            alias_handle: 0,
        };
        filtered_slots.push((handle, compact_slot));
    }

    for &handle in included_handles {
        let Some(slot) = index.get(handle) else {
            continue;
        };
        let Some(canonical) = slot.alias_of() else {
            continue;
        };
        let canonical_slot = filtered_slots
            .iter()
            .find(|(candidate, _)| *candidate == canonical)
            .map(|(_, slot)| *slot)
            .ok_or_else(|| Error::decode("selected alias target has no loaded chain"))?;
        filtered_slots.push((
            handle,
            ChainSlot {
                offset: canonical_slot.offset,
                length: canonical_slot.length,
                alias_handle: canonical,
            },
        ));
    }

    let filtered_index = ChainIndex::sparse(index.max_handle() as usize, filtered_slots)?;
    let (chains, decoded_chain_buffer) = build_chains(
        &chain_buffer,
        0,
        &filtered_index,
        pack_type,
        codec_parallelism,
    )?;
    Ok(ChainStorage {
        stored: chain_buffer,
        chains,
        decoded: decoded_chain_buffer,
    })
}

fn build_chains(
    buffer: &[u8],
    chain_start: u64,
    index: &ChainIndex,
    pack_type: PackType,
    codec_parallelism: CodecParallelism,
) -> Result<(Vec<ChainData>, Vec<u8>)> {
    #[cfg(not(feature = "parallel"))]
    let _ = codec_parallelism;
    struct ChainJob<'a> {
        handle: u32,
        stored_len: u64,
        compressed: &'a [u8],
    }

    struct ChainJobResult {
        handle: u32,
        stored_len: u32,
        payload: Vec<u8>,
    }

    let mut chains = Vec::new();
    let mut jobs = Vec::new();

    for (handle, slot) in index.iter() {
        if slot.alias_of().is_some() {
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
            chains.push(ChainData {
                handle,
                stored_len: (length - consumed) as u32,
                payload: ChainPayload::Borrowed {
                    range: range_start..range_end,
                },
            });
        } else {
            jobs.push(ChainJob {
                handle,
                stored_len,
                compressed: payload_bytes,
            });
        }
    }

    #[cfg(feature = "parallel")]
    let decoded_bytes = jobs
        .iter()
        .fold(0_u64, |total, job| total.saturating_add(job.stored_len));
    #[cfg(all(feature = "gzip", feature = "parallel"))]
    let direct_zlib = pack_type == PackType::Zlib
        && (codec_parallelism.is_serial() || jobs.len() < 32 || decoded_bytes < 64 * 1024);
    #[cfg(all(feature = "gzip", not(feature = "parallel")))]
    let direct_zlib = pack_type == PackType::Zlib;
    #[cfg(feature = "gzip")]
    if direct_zlib {
        return with_decompressor(|decoder| {
            let decoded_capacity = jobs.iter().try_fold(0_usize, |total, job| {
                let len = usize::try_from(job.stored_len)
                    .map_err(|_| Error::decode("chain stored length exceeds addressable memory"))?;
                total
                    .checked_add(len)
                    .ok_or_else(|| Error::decode("decoded chain arena length overflow"))
            })?;
            let mut decoded_arena = vec![0_u8; decoded_capacity];
            let mut cursor = 0_usize;
            for job in jobs {
                let expected = usize::try_from(job.stored_len)
                    .map_err(|_| Error::decode("chain stored length exceeds addressable memory"))?;
                let end = cursor
                    .checked_add(expected)
                    .ok_or_else(|| Error::decode("decoded chain range overflow"))?;
                zlib_decompress_into_with(
                    decoder,
                    job.compressed,
                    &mut decoded_arena[cursor..end],
                )?;
                let stored_len = u32::try_from(job.stored_len)
                    .map_err(|_| Error::decode("chain stored length exceeds u32 range"))?;
                chains.push(ChainData {
                    handle: job.handle,
                    stored_len,
                    payload: ChainPayload::Decoded { range: cursor..end },
                });
                cursor = end;
            }
            Ok((chains, decoded_arena))
        });
    }

    let finish_job = |job: ChainJob<'_>, data: Vec<u8>| -> Result<ChainJobResult> {
        let stored_len = u32::try_from(job.stored_len)
            .map_err(|_| Error::decode("chain stored length exceeds u32 range"))?;
        Ok(ChainJobResult {
            handle: job.handle,
            stored_len,
            payload: data,
        })
    };
    let decompress = |job: ChainJob<'_>| -> Result<ChainJobResult> {
        let expected = usize::try_from(job.stored_len)
            .map_err(|_| Error::decode("chain stored length exceeds addressable memory"))?;
        let data = decompress_chain_payload(pack_type, job.compressed, expected)?;
        finish_job(job, data)
    };
    #[cfg(all(feature = "gzip", feature = "parallel"))]
    let decompress_native =
        |decoder: &mut LibdeflateDecompressor, job: ChainJob<'_>| -> Result<ChainJobResult> {
            let expected = usize::try_from(job.stored_len)
                .map_err(|_| Error::decode("chain stored length exceeds addressable memory"))?;
            let data = zlib_decompress_with(decoder, job.compressed, expected)?;
            finish_job(job, data)
        };

    #[cfg(feature = "parallel")]
    let results: Vec<ChainJobResult> = {
        if codec_parallelism.is_serial()
            || (pack_type == PackType::Lz4 && matches!(codec_parallelism, CodecParallelism::Auto))
            || jobs.len() < 32
            || decoded_bytes < 64 * 1024
        {
            jobs.into_iter().map(decompress).collect::<Result<_>>()?
        } else if let Some(pool) = codec_pool(codec_parallelism) {
            let partition_len = codec_partition_len(jobs.len(), codec_parallelism);
            #[cfg(feature = "gzip")]
            if pack_type == PackType::Zlib {
                pool.install(|| {
                    jobs.into_par_iter()
                        .with_min_len(partition_len)
                        .map_init(LibdeflateDecompressor::new, decompress_native)
                        .collect::<Result<Vec<_>>>()
                })?
            } else {
                pool.install(|| {
                    jobs.into_par_iter()
                        .with_min_len(partition_len)
                        .map(decompress)
                        .collect::<Result<Vec<_>>>()
                })?
            }
            #[cfg(not(feature = "gzip"))]
            pool.install(|| {
                jobs.into_par_iter()
                    .with_min_len(partition_len)
                    .map(decompress)
                    .collect::<Result<Vec<_>>>()
            })?
        } else {
            jobs.into_iter().map(decompress).collect::<Result<_>>()?
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
        decoded_meta.push((result.handle, result.stored_len, start..end));
    }
    for (handle, stored_len, range) in decoded_meta {
        chains.push(ChainData {
            handle,
            stored_len,
            payload: ChainPayload::Decoded { range },
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
                let mut decoder = LibdeflateDecompressor::new();
                zlib_decompress_with(&mut decoder, input, expected_len)
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

#[derive(Debug, Clone, Copy)]
enum EntryTmp {
    Empty,
    Data { offset: u64, length: u32 },
    Alias { target: usize },
}

struct SelectedIndexScan {
    selected: Vec<(u32, u32)>,
    data_offsets: Vec<(u32, u64)>,
}

struct ChainIndexLayout {
    index_start: u64,
    index_length: u64,
    chain_start: u64,
    chain_end: u64,
    max_handle: usize,
}

fn scan_chain_index<F>(
    bytes: &[u8],
    block_type: BlockType,
    max_handle_hint: usize,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(usize, EntryTmp) -> Result<()>,
{
    let mut slice = bytes;
    let mut handle_index = 0_usize;
    let mut last_offset = 0_u64;
    let mut last_alias_target = None;

    let push = |entry, visit: &mut F, handle_index: &mut usize| -> Result<()> {
        if *handle_index >= max_handle_hint {
            return Err(Error::decode("chain index contains too many handles"));
        }
        visit(*handle_index, entry)?;
        *handle_index += 1;
        Ok(())
    };

    while !slice.is_empty() {
        if block_type == BlockType::VcDataDynAlias2 && (slice[0] & 0x01) != 0 {
            let (raw, consumed) = decode_svarint_with_len(slice)?;
            let shval = raw >> 1;
            slice = &slice[consumed..];
            if shval > 0 {
                last_offset = last_offset
                    .checked_add(shval as u64)
                    .ok_or_else(|| Error::decode("chain index overflow"))?;
                push(
                    EntryTmp::Data {
                        offset: last_offset,
                        length: 0,
                    },
                    &mut visit,
                    &mut handle_index,
                )?;
                last_alias_target = None;
            } else if shval < 0 {
                let target = ((-shval) as u64)
                    .checked_sub(1)
                    .ok_or_else(|| Error::decode("invalid alias target"))?;
                let target = usize::try_from(target)
                    .map_err(|_| Error::decode("alias target exceeds addressable range"))?;
                push(EntryTmp::Alias { target }, &mut visit, &mut handle_index)?;
                last_alias_target = Some(target);
            } else if let Some(target) = last_alias_target {
                push(EntryTmp::Alias { target }, &mut visit, &mut handle_index)?;
            } else {
                push(EntryTmp::Empty, &mut visit, &mut handle_index)?;
            }
            continue;
        }

        let (value, consumed) = decode_varint_with_len(slice)?;
        slice = &slice[consumed..];
        if value == 0 {
            let (alias, alias_consumed) = decode_varint_with_len(slice)?;
            slice = &slice[alias_consumed..];
            if alias == 0 {
                push(EntryTmp::Empty, &mut visit, &mut handle_index)?;
                last_alias_target = None;
            } else {
                let target = usize::try_from(alias - 1)
                    .map_err(|_| Error::decode("alias target exceeds addressable range"))?;
                push(EntryTmp::Alias { target }, &mut visit, &mut handle_index)?;
                last_alias_target = Some(target);
            }
            continue;
        }

        if value & 1 == 0 {
            let repeat = usize::try_from(value >> 1)
                .map_err(|_| Error::decode("empty chain run exceeds addressable range"))?;
            let remaining = max_handle_hint.saturating_sub(handle_index);
            if repeat == 0 || repeat > remaining {
                return Err(Error::decode("invalid empty run in chain index"));
            }
            for _ in 0..repeat {
                visit(handle_index, EntryTmp::Empty)?;
                handle_index += 1;
            }
            continue;
        }

        last_offset = last_offset
            .checked_add(value >> 1)
            .ok_or_else(|| Error::decode("chain index overflow"))?;
        push(
            EntryTmp::Data {
                offset: last_offset,
                length: 0,
            },
            &mut visit,
            &mut handle_index,
        )?;
        last_alias_target = None;
    }

    if handle_index != max_handle_hint {
        return Err(Error::decode(format!(
            "chain index describes {handle_index} handles, expected {max_handle_hint}"
        )));
    }
    Ok(())
}

fn decode_selected_chain_index(
    bytes: &[u8],
    block_type: BlockType,
    max_handle_hint: usize,
    chain_start: u64,
    chain_end: u64,
    included_handles: &[u32],
) -> Result<ChainIndex> {
    const PACK_MARKER_PREFIX: u64 = 1;
    let total_chain_len = chain_end
        .checked_sub(chain_start)
        .ok_or_else(|| Error::invalid("negative chain range"))?;
    let scan = if block_type == BlockType::VcDataDynAlias2 {
        scan_selected_dyn_alias2(bytes, max_handle_hint, total_chain_len, included_handles)?
    } else {
        let mut canonical_by_handle = vec![0_u32; max_handle_hint];
        let mut selected = Vec::with_capacity(included_handles.len());
        let mut selected_cursor = 0_usize;
        let mut data_offsets = Vec::<(u32, u64)>::new();

        scan_chain_index(bytes, block_type, max_handle_hint, |index, entry| {
            let handle = u32::try_from(index + 1)
                .map_err(|_| Error::invalid("chain index handle exceeds u32 range"))?;
            let canonical = match entry {
                EntryTmp::Empty => 0,
                EntryTmp::Data { offset, .. } => {
                    let normalized = offset
                        .checked_sub(PACK_MARKER_PREFIX)
                        .ok_or_else(|| Error::decode("chain offset precedes pack marker"))?;
                    if normalized > total_chain_len {
                        return Err(Error::decode("chain offset exceeds chain payload bounds"));
                    }
                    if data_offsets
                        .last()
                        .is_some_and(|(_, previous)| normalized < *previous)
                    {
                        return Err(Error::decode("chain offsets are not monotonic"));
                    }
                    data_offsets.push((handle, normalized));
                    handle
                }
                EntryTmp::Alias { target } => {
                    if target >= index {
                        return Err(Error::decode("dynamic alias target must precede its alias"));
                    }
                    let canonical = canonical_by_handle[target];
                    if canonical == 0 {
                        return Err(Error::decode("dynamic alias target has no canonical chain"));
                    }
                    canonical
                }
            };
            canonical_by_handle[index] = canonical;
            if included_handles.get(selected_cursor) == Some(&handle) {
                selected.push((handle, canonical));
                selected_cursor += 1;
            }
            Ok(())
        })?;
        SelectedIndexScan {
            selected,
            data_offsets,
        }
    };

    let mut needed = scan
        .selected
        .iter()
        .filter_map(|(_, canonical)| (*canonical != 0).then_some(*canonical))
        .collect::<Vec<_>>();
    needed.sort_unstable();
    needed.dedup();
    let mut canonical_slots = Vec::with_capacity(needed.len());
    for handle in needed {
        let index = scan
            .data_offsets
            .binary_search_by_key(&handle, |(candidate, _)| *candidate)
            .map_err(|_| Error::decode("selected canonical handle has no data chain"))?;
        let offset = scan.data_offsets[index].1;
        let end = scan
            .data_offsets
            .get(index + 1)
            .map_or(total_chain_len, |(_, next_offset)| *next_offset);
        let length = u32::try_from(
            end.checked_sub(offset)
                .ok_or_else(|| Error::decode("chain offsets are not monotonic"))?,
        )
        .map_err(|_| Error::decode("chain payload exceeds u32 range"))?;
        canonical_slots.push((handle, offset, length));
    }
    let mut sparse = Vec::with_capacity(canonical_slots.len() + scan.selected.len());
    for &(handle, offset, length) in &canonical_slots {
        sparse.push((
            handle,
            ChainSlot {
                offset: chain_start
                    .checked_add(offset)
                    .ok_or_else(|| Error::invalid("chain offset overflow"))?,
                length,
                alias_handle: 0,
            },
        ));
    }
    for (handle, canonical) in scan.selected {
        if canonical == 0 || handle == canonical {
            continue;
        }
        let canonical_slot = sparse
            .iter()
            .find(|(candidate, _)| *candidate == canonical)
            .map(|(_, slot)| *slot)
            .ok_or_else(|| Error::decode("selected alias target has no loaded chain"))?;
        sparse.push((
            handle,
            ChainSlot {
                alias_handle: canonical,
                ..canonical_slot
            },
        ));
    }
    ChainIndex::sparse(max_handle_hint, sparse)
}

fn scan_selected_dyn_alias2(
    mut bytes: &[u8],
    max_handle_hint: usize,
    total_chain_len: u64,
    included_handles: &[u32],
) -> Result<SelectedIndexScan> {
    const PACK_MARKER_PREFIX: u64 = 1;
    let mut canonical_by_handle = vec![0_u32; max_handle_hint];
    let mut selected = Vec::with_capacity(included_handles.len());
    let mut data_offsets = Vec::new();
    let mut selected_cursor = 0_usize;
    let mut handle_index = 0_usize;
    let mut last_offset = 0_u64;
    let mut previous_alias_canonical = 0_u32;

    while !bytes.is_empty() {
        if bytes[0] & 1 == 0 {
            let (encoded, consumed) = decode_varint_with_len(bytes)?;
            bytes = &bytes[consumed..];
            let repeat = usize::try_from(encoded >> 1)
                .map_err(|_| Error::decode("empty chain run exceeds addressable range"))?;
            let end = handle_index
                .checked_add(repeat)
                .ok_or_else(|| Error::decode("empty chain run overflows handle count"))?;
            if repeat == 0 || end > max_handle_hint {
                return Err(Error::decode("invalid empty run in chain index"));
            }
            while included_handles
                .get(selected_cursor)
                .is_some_and(|handle| (*handle as usize) <= end)
            {
                selected.push((included_handles[selected_cursor], 0));
                selected_cursor += 1;
            }
            handle_index = end;
            continue;
        }

        if handle_index >= max_handle_hint {
            return Err(Error::decode("chain index contains too many handles"));
        }
        let (raw, consumed) = decode_svarint_with_len(bytes)?;
        bytes = &bytes[consumed..];
        let shifted = raw >> 1;
        let handle = u32::try_from(handle_index + 1)
            .map_err(|_| Error::invalid("chain index handle exceeds u32 range"))?;
        let canonical = if shifted > 0 {
            last_offset = last_offset
                .checked_add(shifted as u64)
                .ok_or_else(|| Error::decode("chain index overflow"))?;
            let normalized = last_offset
                .checked_sub(PACK_MARKER_PREFIX)
                .ok_or_else(|| Error::decode("chain offset precedes pack marker"))?;
            if normalized > total_chain_len {
                return Err(Error::decode("chain offset exceeds chain payload bounds"));
            }
            data_offsets.push((handle, normalized));
            previous_alias_canonical = 0;
            handle
        } else if shifted < 0 {
            let target = u64::try_from(-i128::from(shifted) - 1)
                .map_err(|_| Error::decode("invalid alias target"))?;
            let target = usize::try_from(target)
                .map_err(|_| Error::decode("alias target exceeds addressable range"))?;
            if target >= handle_index {
                return Err(Error::decode("dynamic alias target must precede its alias"));
            }
            let canonical = canonical_by_handle[target];
            if canonical == 0 {
                return Err(Error::decode("dynamic alias target has no canonical chain"));
            }
            previous_alias_canonical = canonical;
            canonical
        } else {
            previous_alias_canonical
        };
        canonical_by_handle[handle_index] = canonical;
        if included_handles.get(selected_cursor) == Some(&handle) {
            selected.push((handle, canonical));
            selected_cursor += 1;
        }
        handle_index += 1;
    }

    if handle_index != max_handle_hint {
        return Err(Error::decode(format!(
            "chain index describes {handle_index} handles, expected {max_handle_hint}"
        )));
    }
    Ok(SelectedIndexScan {
        selected,
        data_offsets,
    })
}

fn decode_chain_index<R: Read + Seek>(
    reader: &mut R,
    block_type: BlockType,
    layout: ChainIndexLayout,
    included_handles: Option<&[u32]>,
) -> Result<ChainIndex> {
    let ChainIndexLayout {
        index_start,
        index_length,
        chain_start,
        chain_end,
        max_handle: max_handle_hint,
    } = layout;
    reader.seek(SeekFrom::Start(index_start))?;
    let index_len_usize = usize::try_from(index_length)
        .map_err(|_| Error::invalid("index length exceeds addressable memory"))?;
    let mut bytes = vec![0u8; index_len_usize];
    reader.read_exact(&mut bytes)?;

    if let Some(handles) = included_handles {
        return decode_selected_chain_index(
            &bytes,
            block_type,
            max_handle_hint,
            chain_start,
            chain_end,
            handles,
        );
    }

    let mut entries: Vec<EntryTmp> = Vec::with_capacity(max_handle_hint);
    scan_chain_index(&bytes, block_type, max_handle_hint, |_, entry| {
        entries.push(entry);
        Ok(())
    })?;

    let total_chain_len = chain_end
        .checked_sub(chain_start)
        .ok_or_else(|| Error::invalid("negative chain range"))?;

    const PACK_MARKER_PREFIX: u64 = 1;
    let has_aliases = entries
        .iter()
        .any(|entry| matches!(entry, EntryTmp::Alias { .. }));
    if !has_aliases {
        let mut slots: Vec<Option<ChainSlot>> = Vec::with_capacity(entries.len());
        let mut previous_data: Option<(usize, u64)> = None;
        for entry in entries {
            let EntryTmp::Data { offset, .. } = entry else {
                slots.push(None);
                continue;
            };
            let offset = offset
                .checked_sub(PACK_MARKER_PREFIX)
                .ok_or_else(|| Error::decode("chain offset precedes pack marker"))?;
            if offset > total_chain_len {
                return Err(Error::decode("chain offset exceeds chain payload bounds"));
            }
            if let Some((previous_idx, previous_offset)) = previous_data {
                let span = offset
                    .checked_sub(previous_offset)
                    .ok_or_else(|| Error::decode("chain offsets are not monotonic"))?;
                slots[previous_idx]
                    .as_mut()
                    .expect("previous data slot must exist")
                    .length = u32::try_from(span)
                    .map_err(|_| Error::decode("chain payload exceeds u32 range"))?;
            }
            let absolute = chain_start
                .checked_add(offset)
                .ok_or_else(|| Error::invalid("chain offset overflow"))?;
            previous_data = Some((slots.len(), offset));
            slots.push(Some(ChainSlot {
                offset: absolute,
                length: 0,
                alias_handle: 0,
            }));
        }
        if let Some((last_idx, last_offset)) = previous_data {
            let span = total_chain_len
                .checked_sub(last_offset)
                .ok_or_else(|| Error::decode("chain offset exceeds payload bounds"))?;
            slots[last_idx]
                .as_mut()
                .expect("last data slot must exist")
                .length = u32::try_from(span)
                .map_err(|_| Error::decode("chain payload exceeds u32 range"))?;
        }
        return Ok(ChainIndex::dense(slots));
    }

    for entry in &mut entries {
        let EntryTmp::Data { offset, .. } = entry else {
            continue;
        };
        if *offset < PACK_MARKER_PREFIX {
            return Err(Error::decode("chain offset precedes pack marker"));
        }
        *offset -= PACK_MARKER_PREFIX;
        if *offset > total_chain_len {
            return Err(Error::decode("chain offset exceeds chain payload bounds"));
        }
    }

    let mut previous_data: Option<(usize, u64)> = None;
    for idx in 0..entries.len() {
        let offset = match entries[idx] {
            EntryTmp::Data { offset, .. } => offset,
            EntryTmp::Empty | EntryTmp::Alias { .. } => continue,
        };
        if let Some((previous_idx, previous_offset)) = previous_data {
            let span = offset
                .checked_sub(previous_offset)
                .ok_or_else(|| Error::decode("chain offsets are not monotonic"))?;
            let length = u32::try_from(span)
                .map_err(|_| Error::decode("chain payload exceeds u32 range"))?;
            if let EntryTmp::Data {
                length: previous_length,
                ..
            } = &mut entries[previous_idx]
            {
                *previous_length = length;
            }
        }
        previous_data = Some((idx, offset));
    }
    if let Some((last_idx, last_offset)) = previous_data {
        let span = total_chain_len
            .checked_sub(last_offset)
            .ok_or_else(|| Error::decode("chain offset exceeds payload bounds"))?;
        let length =
            u32::try_from(span).map_err(|_| Error::decode("chain payload exceeds u32 range"))?;
        if let EntryTmp::Data {
            length: last_length,
            ..
        } = &mut entries[last_idx]
        {
            *last_length = length;
        }
    }

    const CANONICAL_UNKNOWN: usize = usize::MAX;
    const CANONICAL_MISSING: usize = usize::MAX - 1;

    fn resolve_canonical(
        idx: usize,
        entries: &[EntryTmp],
        memo: &mut [usize],
        visiting: &mut [bool],
    ) -> Option<usize> {
        if memo[idx] == CANONICAL_MISSING {
            return None;
        }
        if memo[idx] != CANONICAL_UNKNOWN {
            return Some(memo[idx]);
        }
        if visiting[idx] {
            memo[idx] = CANONICAL_MISSING;
            return None;
        }
        visiting[idx] = true;
        let result = match entries[idx] {
            EntryTmp::Data { .. } => Some(idx),
            EntryTmp::Alias { target } if target < entries.len() => {
                resolve_canonical(target, entries, memo, visiting)
            }
            EntryTmp::Empty | EntryTmp::Alias { .. } => None,
        };
        visiting[idx] = false;
        memo[idx] = result.unwrap_or(CANONICAL_MISSING);
        result
    }

    let mut canonical = Vec::new();
    if has_aliases {
        canonical.resize(entries.len(), CANONICAL_UNKNOWN);
        let mut visiting = vec![false; entries.len()];
        for idx in 0..entries.len() {
            let EntryTmp::Alias { target } = entries[idx] else {
                continue;
            };
            if target < entries.len() && matches!(entries[target], EntryTmp::Data { .. }) {
                canonical[idx] = target;
                continue;
            }
            let _ = resolve_canonical(idx, &entries, &mut canonical, &mut visiting);
        }
    }

    let mut slots = Vec::with_capacity(entries.len());
    for (idx, entry) in entries.iter().enumerate() {
        match *entry {
            EntryTmp::Data { offset, length } => {
                let absolute = chain_start
                    .checked_add(offset)
                    .ok_or_else(|| Error::invalid("chain offset overflow"))?;
                slots.push(Some(ChainSlot {
                    offset: absolute,
                    length,
                    alias_handle: 0,
                }));
            }
            EntryTmp::Alias { .. } => {
                let canonical_idx = canonical[idx];
                if canonical_idx >= entries.len() {
                    slots.push(None);
                    continue;
                }
                let EntryTmp::Data { offset, length } = entries[canonical_idx] else {
                    slots.push(None);
                    continue;
                };
                let absolute = chain_start
                    .checked_add(offset)
                    .ok_or_else(|| Error::invalid("chain offset overflow"))?;
                let alias_of = u32::try_from(canonical_idx + 1)
                    .map_err(|_| Error::invalid("alias target exceeds u32 range"))?;
                slots.push(Some(ChainSlot {
                    offset: absolute,
                    length,
                    alias_handle: alias_of,
                }));
            }
            EntryTmp::Empty => slots.push(None),
        }
    }

    Ok(ChainIndex::dense(slots))
}

#[cfg(test)]
mod tests {
    use super::scan_selected_dyn_alias2;
    use crate::encoding::{encode_svarint, encode_varint};

    #[test]
    fn selected_dyn_alias2_resolves_alias_repeats_and_empty_runs() {
        let mut index = Vec::new();
        for value in [3, -1, 1] {
            encode_svarint(value, &mut index);
        }
        encode_varint(4, &mut index);

        let scan = scan_selected_dyn_alias2(&index, 5, 8, &[2, 3, 4]).unwrap();
        assert_eq!(scan.selected, [(2, 1), (3, 1), (4, 0)]);
        assert_eq!(scan.data_offsets, [(1, 0)]);
    }

    #[test]
    fn selected_dyn_alias2_rejects_forward_aliases() {
        let mut index = Vec::new();
        encode_svarint(-3, &mut index);
        assert!(scan_selected_dyn_alias2(&index, 1, 0, &[1]).is_err());
    }
}
