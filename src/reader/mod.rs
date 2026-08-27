//! High-level streaming reader for FST files.

use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::ops::RangeInclusive;

use crate::block::{BlackoutBlock, GeomInfo, Header, HierarchyBlock};
#[cfg(feature = "gzip")]
use crate::compression::gzip_decompress;
use crate::error::{Error, Result};
use crate::io::{ReadSeek, ReaderBackend, SEEK_READER_BUFFER_CAPACITY};
use crate::types::{BlockType, CodecParallelism};
use crate::util::{read_u64_be, skip_bytes};

mod vc;
pub use vc::{ChainData, ChainIndex, ChainPayload, ChainSlot, VcBlockMeta};
use vc::{VcParseOptions, parse_vc_block};

mod change;
pub use change::{ValueChange, VcBlockChanges, build_changes};

/// Controls how the [`FstReader`] parses data.
#[derive(Debug, Clone)]
pub struct ReaderOptions {
    /// Runtime policy for decompressing independent value-change chains.
    pub codec_parallelism: CodecParallelism,
    /// When `true`, geometry blocks are loaded eagerly as soon as they appear.
    pub eager_geometry: bool,
    /// When `true`, hierarchy blocks are decompressed and parsed.
    pub load_hierarchy: bool,
    /// Maximum accepted uncompressed size for a whole-file gzip wrapper.
    pub max_decompressed_bytes: u64,
    /// Maximum accepted compressed or decoded size for an individual FST block.
    pub max_block_bytes: u64,
    /// Maximum number of canonical signal handles accepted from an input file.
    pub max_handles: u64,
    /// Optional sorted set of one-based handles whose changes should be decoded and emitted.
    /// `None` selects every handle; `Some(Vec::new())` selects none.
    pub included_handles: Option<Vec<u32>>,
    /// Optional inclusive timestamp range. Blocks outside the range are skipped without decoding.
    pub time_range: Option<RangeInclusive<u64>>,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            codec_parallelism: CodecParallelism::Serial,
            eager_geometry: true,
            load_hierarchy: true,
            max_decompressed_bytes: 8 * 1024 * 1024 * 1024,
            max_block_bytes: 4 * 1024 * 1024 * 1024,
            max_handles: 16_777_216,
            included_handles: None,
            time_range: None,
        }
    }
}

/// Builder used to configure and construct a [`FstReader`].
#[must_use]
pub struct ReaderBuilder<R: ReadSeek> {
    source: R,
    options: ReaderOptions,
}

impl<R: ReadSeek> ReaderBuilder<R> {
    /// Creates a new builder for the given source.
    pub fn new(source: R) -> Self {
        Self {
            source,
            options: ReaderOptions::default(),
        }
    }

    /// Overrides reader options wholesale.
    pub fn options(mut self, options: ReaderOptions) -> Self {
        self.options = options;
        self
    }

    /// Selects the runtime policy for independent chain decompression jobs.
    ///
    /// Policies other than [`CodecParallelism::Serial`] require the `parallel` Cargo feature.
    pub fn codec_parallelism(mut self, parallelism: CodecParallelism) -> Self {
        self.options.codec_parallelism = parallelism;
        self
    }

    /// Enables or disables eager geometry parsing.
    pub fn eager_geometry(mut self, value: bool) -> Self {
        self.options.eager_geometry = value;
        self
    }

    /// Enables or disables hierarchy decoding.
    ///
    /// Disabling it reduces fixed open cost when handles are already known and only value changes
    /// are needed. Geometry remains available for decoding values, while [`FstReader::hierarchy`]
    /// returns `None`.
    pub fn load_hierarchy(mut self, value: bool) -> Self {
        self.options.load_hierarchy = value;
        self
    }

    /// Sets the safety limit used before allocating for `FST_BL_ZWRAPPER`.
    pub fn max_decompressed_bytes(mut self, bytes: u64) -> Self {
        self.options.max_decompressed_bytes = bytes;
        self
    }

    /// Sets the safety limit for individual block allocations and decompression.
    pub fn max_block_bytes(mut self, bytes: u64) -> Self {
        self.options.max_block_bytes = bytes;
        self
    }

    /// Sets the maximum canonical signal-handle count accepted by the reader.
    pub fn max_handles(mut self, handles: u64) -> Self {
        self.options.max_handles = handles;
        self
    }

    /// Restricts decoding and emission to the supplied one-based signal handles.
    ///
    /// Handles are sorted and deduplicated while building the reader. Passing an empty iterator
    /// is valid and produces metadata without any value changes.
    pub fn include_handles<I>(mut self, handles: I) -> Self
    where
        I: IntoIterator<Item = u32>,
    {
        self.options.included_handles = Some(handles.into_iter().collect());
        self
    }

    /// Restricts emitted changes to an inclusive timestamp range.
    ///
    /// Value-change blocks that do not overlap the range are skipped without reading their frame,
    /// time table, chain payloads, or index. An empty range is valid and emits no changes.
    pub fn time_range(mut self, range: RangeInclusive<u64>) -> Self {
        self.options.time_range = Some(range);
        self
    }

    /// Consumes the builder, constructing the reader.
    pub fn build(self) -> Result<FstReader<R>> {
        FstReader::with_backend(self.source, self.options)
    }
}

/// Streaming reader for an FST file.
pub struct FstReader<R: ReadSeek> {
    backend: ReaderBackend<R>,
    options: ReaderOptions,
    value_changes_start: u64,
    header: Header,
    geometry: Option<GeomInfo>,
    blackout: Option<BlackoutBlock>,
    hierarchy: Option<HierarchyBlock>,
    current_vc_block: Option<VcBlockMeta>,
}

impl<R: ReadSeek> FstReader<R> {
    fn with_backend(mut source: R, mut options: ReaderOptions) -> Result<Self> {
        #[cfg(not(feature = "parallel"))]
        if !options.codec_parallelism.is_serial() {
            return Err(Error::unsupported(
                "parallel chain decompression requires the `parallel` feature",
            ));
        }
        source.seek(SeekFrom::Start(0))?;
        let mut tag = [0u8; 1];
        source.read_exact(&mut tag)?;
        source.seek(SeekFrom::Start(0))?;

        let mut backend = if tag[0] == BlockType::ZWrapper as u8 {
            let decoded = decode_wrapper(&mut source, options.max_decompressed_bytes)?;
            ReaderBackend::wrapped(source, decoded)
        } else {
            if options.time_range.is_some() {
                ReaderBackend::with_capacity(source, SEEK_READER_BUFFER_CAPACITY)
            } else {
                ReaderBackend::new(source)
            }
        };
        let header = Header::read(backend.get_mut())?;
        if header.max_handle > options.max_handles || header.max_handle > u64::from(u32::MAX) {
            return Err(Error::invalid(format!(
                "header declares {} handles, above supported/configured limit {}",
                header.max_handle,
                options.max_handles.min(u64::from(u32::MAX))
            )));
        }
        normalize_included_handles(&mut options.included_handles, header.max_handle)?;
        let mut reader = Self {
            backend,
            options,
            value_changes_start: 0,
            header,
            geometry: None,
            blackout: None,
            hierarchy: None,
            current_vc_block: None,
        };
        reader.parse_preamble()?;
        reader.value_changes_start = reader.backend.get_mut().stream_position()?;
        Ok(reader)
    }

    /// Creates a new builder for the given source.
    pub fn builder(source: R) -> ReaderBuilder<R> {
        ReaderBuilder::new(source)
    }

    /// Returns the parsed header metadata.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns the reader options used to configure this reader.
    pub fn options(&self) -> &ReaderOptions {
        &self.options
    }

    /// Returns the parsed geometry information, if available.
    pub fn geometry(&self) -> Option<&GeomInfo> {
        self.geometry.as_ref()
    }

    /// Returns blackout schedule data if present.
    pub fn blackout(&self) -> Option<&BlackoutBlock> {
        self.blackout.as_ref()
    }

    /// Returns hierarchy data if present.
    pub fn hierarchy(&self) -> Option<&HierarchyBlock> {
        self.hierarchy.as_ref()
    }

    /// Returns a mutable reference to the underlying reader backend.
    pub fn raw_reader(&mut self) -> &mut ReaderBackend<R> {
        &mut self.backend
    }

    /// Rewinds to the first value-change block while retaining parsed file metadata.
    ///
    /// This is the low-cost foundation for repeated viewport queries. Any current block is
    /// discarded, but geometry, hierarchy, blackout data, and the underlying open source are
    /// reused.
    pub fn rewind_value_changes(&mut self) -> Result<()> {
        self.current_vc_block = None;
        self.backend
            .get_mut()
            .seek(SeekFrom::Start(self.value_changes_start))?;
        Ok(())
    }

    /// Replaces the signal selection and rewinds for a new query.
    ///
    /// Handles are one-based, sorted, and deduplicated. An empty iterator selects no signals.
    pub fn set_included_handles<I>(&mut self, handles: I) -> Result<()>
    where
        I: IntoIterator<Item = u32>,
    {
        self.options.included_handles = Some(handles.into_iter().collect());
        normalize_included_handles(&mut self.options.included_handles, self.header.max_handle)?;
        self.rewind_value_changes()
    }

    /// Selects every signal and rewinds for a new query.
    pub fn include_all_handles(&mut self) -> Result<()> {
        self.options.included_handles = None;
        self.rewind_value_changes()
    }

    /// Replaces the inclusive timestamp filter and rewinds for a new query.
    ///
    /// Passing `None` selects the complete timeline.
    pub fn set_time_range(&mut self, range: Option<RangeInclusive<u64>>) -> Result<()> {
        self.options.time_range = range;
        self.rewind_value_changes()
    }

    /// Consumes the reader, yielding the underlying I/O object.
    pub fn into_inner(self) -> R {
        self.backend.into_inner()
    }

    /// Skips all remaining blocks and positions the source at end-of-file.
    pub fn skip_remaining(&mut self) -> Result<()> {
        self.current_vc_block = None;
        self.backend.get_mut().seek(SeekFrom::End(0))?;
        Ok(())
    }

    /// Returns metadata for the next value-change block, advancing the stream.
    pub fn next_vc_block(&mut self) -> Result<Option<VcBlockMeta>> {
        loop {
            let reader = self.backend.get_mut();
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
                Err(err) => return Err(err.into()),
            }

            let block_type = BlockType::try_from(tag[0])
                .map_err(|_| Error::invalid(format!("unknown block type {:02x}", tag[0])))?;

            match block_type {
                BlockType::VcData | BlockType::VcDataDynAlias | BlockType::VcDataDynAlias2 => {
                    let section_length = read_u64_be(reader)?;
                    let section_start = reader.stream_position()?;
                    let payload_len = payload_length(section_length)?;
                    validate_block_size(section_length, self.options.max_block_bytes)?;
                    let block_end = section_start.checked_add(payload_len).ok_or_else(|| {
                        Error::invalid("value-change payload exceeds file bounds")
                    })?;
                    if let Some(range) = self.options.time_range.as_ref() {
                        if payload_len < 61 {
                            return Err(Error::invalid(
                                "value-change payload shorter than required fields",
                            ));
                        }
                        let begin_time = read_u64_be(reader)?;
                        let end_time = read_u64_be(reader)?;
                        reader.seek(SeekFrom::Start(section_start))?;
                        if !time_ranges_overlap(begin_time, end_time, range) {
                            let source_end = reader.seek(SeekFrom::End(0))?;
                            if block_end > source_end {
                                return Err(Error::invalid(
                                    "value-change payload exceeds file bounds",
                                ));
                            }
                            reader.seek(SeekFrom::Start(block_end))?;
                            continue;
                        }
                    }
                    let meta = parse_vc_block(
                        reader,
                        block_type,
                        section_start,
                        payload_len,
                        VcParseOptions {
                            max_block_bytes: self.options.max_block_bytes,
                            max_handles: self.options.max_handles,
                            double_byte_order: self.header.double_byte_order,
                            codec_parallelism: self.options.codec_parallelism,
                            included_handles: self.options.included_handles.as_deref(),
                            time_range: self.options.time_range.clone(),
                        },
                    )?;
                    reader.seek(SeekFrom::Start(block_end))?;
                    return Ok(Some(meta));
                }
                BlockType::ZWrapper => {
                    return Err(Error::invalid(
                        "z-wrapper must be the first and only outer block",
                    ));
                }
                BlockType::Header => {
                    return Err(Error::invalid("duplicate header block encountered"));
                }
                metadata => {
                    if !self.consume_metadata_block(metadata)? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Parses the next value-change block and returns an iterator over its value changes.
    /// The iterator borrows the reader, so it must be dropped before calling this method again.
    pub fn next_value_changes(&mut self) -> Result<Option<VcBlockChanges<'_>>> {
        self.current_vc_block = None;
        let Some(block) = self.next_vc_block()? else {
            return Ok(None);
        };
        self.current_vc_block = Some(block);
        self.load_metadata_until_next_vc()?;
        let geom = self.geometry.as_ref().ok_or_else(|| {
            Error::invalid("geometry metadata is required before iterating value changes")
        })?;
        let block_ref = self.current_vc_block.as_ref().expect("block just stored");
        block_ref.changes(geom).map(Some)
    }

    fn parse_preamble(&mut self) -> Result<()> {
        loop {
            let reader = self.backend.get_mut();
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            }
            let Ok(block_type) = BlockType::try_from(tag[0]) else {
                return Err(Error::invalid(format!("unknown block type {:02x}", tag[0])));
            };

            match block_type {
                BlockType::VcData | BlockType::VcDataDynAlias | BlockType::VcDataDynAlias2 => {
                    reader.seek(SeekFrom::Current(-1))?;
                    break;
                }
                BlockType::ZWrapper => {
                    return Err(Error::invalid(
                        "z-wrapper must be the first and only outer block",
                    ));
                }
                BlockType::Header => {
                    return Err(Error::invalid("duplicate header block encountered"));
                }
                metadata => {
                    if !self.consume_metadata_block(metadata)? {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn load_metadata_until_next_vc(&mut self) -> Result<()> {
        loop {
            let reader = self.backend.get_mut();
            let position = reader.stream_position()?;
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(()),
                Err(err) => return Err(err.into()),
            }

            let block_type = BlockType::try_from(tag[0])
                .map_err(|_| Error::invalid(format!("unknown block type {:02x}", tag[0])))?;

            match block_type {
                BlockType::VcData | BlockType::VcDataDynAlias | BlockType::VcDataDynAlias2 => {
                    reader.seek(SeekFrom::Start(position))?;
                    return Ok(());
                }
                BlockType::ZWrapper => {
                    return Err(Error::invalid(
                        "z-wrapper must be the first and only outer block",
                    ));
                }
                BlockType::Header => {
                    return Err(Error::invalid("duplicate header block encountered"));
                }
                metadata => {
                    if !self.consume_metadata_block(metadata)? {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Consumes a non-header, non-value-change block. Returns `false` for the libfst terminal
    /// sentinel and `true` for ordinary metadata.
    fn consume_metadata_block(&mut self, block_type: BlockType) -> Result<bool> {
        let reader = self.backend.get_mut();
        match block_type {
            BlockType::Geometry => {
                let section_length = read_u64_be(reader)?;
                validate_block_size(section_length, self.options.max_block_bytes)?;
                let payload_len = payload_length(section_length)?;
                if self.options.eager_geometry || self.geometry.is_none() {
                    self.geometry = Some(Self::read_geometry_block(
                        reader,
                        section_length,
                        self.options.max_block_bytes,
                        self.options.max_handles,
                    )?);
                } else {
                    skip_bytes(reader, payload_len)?;
                }
                Ok(true)
            }
            BlockType::Blackout => {
                let section_length = read_u64_be(reader)?;
                validate_block_size(section_length, self.options.max_block_bytes)?;
                let payload_len = payload_length(section_length)?;
                let payload_len_usize = usize::try_from(payload_len)
                    .map_err(|_| Error::invalid("blackout payload exceeds addressable memory"))?;
                let mut payload = vec![0u8; payload_len_usize];
                reader.read_exact(&mut payload)?;
                self.blackout = Some(BlackoutBlock::decode(&payload)?);
                Ok(true)
            }
            BlockType::Hierarchy | BlockType::HierarchyLz4 | BlockType::HierarchyLz4Duo => {
                if self.options.load_hierarchy {
                    self.hierarchy = Some(Self::read_hierarchy_block(
                        reader,
                        block_type,
                        self.options.max_block_bytes,
                    )?);
                } else {
                    Self::skip_hierarchy_block(reader, self.options.max_block_bytes)?;
                }
                Ok(true)
            }
            BlockType::Skip => {
                // libfst uses this as an in-progress/end sentinel, including a zero-length
                // placeholder in finalized empty traces. Nothing after it is another section.
                reader.seek(SeekFrom::End(0))?;
                Ok(false)
            }
            _ => Err(Error::invalid("expected an FST metadata block")),
        }
    }

    fn read_geometry_block<Rd: Read + Seek>(
        reader: &mut Rd,
        section_length: u64,
        max_block_bytes: u64,
        max_handles: u64,
    ) -> Result<GeomInfo> {
        let uncompressed_len = read_u64_be(reader)?;
        if uncompressed_len > max_block_bytes {
            return Err(Error::invalid(
                "decoded geometry exceeds configured block limit",
            ));
        }
        let max_handle = read_u64_be(reader)?;
        if max_handle > max_handles || max_handle > u64::from(u32::MAX) {
            return Err(Error::invalid(format!(
                "geometry declares {max_handle} handles, above supported/configured limit {}",
                max_handles.min(u64::from(u32::MAX))
            )));
        }
        reader.seek(SeekFrom::Current(-16))?;
        GeomInfo::decode_block(reader, section_length)
    }

    fn read_hierarchy_block<Rd: Read + Seek>(
        reader: &mut Rd,
        block_type: BlockType,
        max_block_bytes: u64,
    ) -> Result<HierarchyBlock> {
        let section_length = read_u64_be(reader)?;
        validate_block_size(section_length, max_block_bytes)?;
        let uncompressed_len = read_u64_be(reader)?;
        if uncompressed_len > max_block_bytes {
            return Err(Error::invalid(
                "decoded hierarchy exceeds configured block limit",
            ));
        }
        reader.seek(SeekFrom::Current(-8))?;
        HierarchyBlock::decode_block(reader, block_type, section_length)
    }

    fn skip_hierarchy_block<Rd: Read + Seek>(reader: &mut Rd, max_block_bytes: u64) -> Result<()> {
        let section_length = read_u64_be(reader)?;
        validate_block_size(section_length, max_block_bytes)?;
        if section_length < 16 {
            return Err(Error::invalid(
                "hierarchy section shorter than required metadata",
            ));
        }
        let uncompressed_len = read_u64_be(reader)?;
        if uncompressed_len > max_block_bytes {
            return Err(Error::invalid(
                "decoded hierarchy exceeds configured block limit",
            ));
        }
        skip_bytes(reader, section_length - 16)
    }
}

fn decode_wrapper<R: Read + Seek>(source: &mut R, max_decoded: u64) -> Result<Vec<u8>> {
    source.seek(SeekFrom::Start(1))?;
    let section_length = read_u64_be(source)?;
    if section_length < 16 {
        return Err(Error::invalid(
            "z-wrapper section is shorter than its header",
        ));
    }
    let uncompressed_len = read_u64_be(source)?;
    if uncompressed_len > max_decoded {
        return Err(Error::invalid(format!(
            "z-wrapper expands to {uncompressed_len} bytes, above configured limit {max_decoded}"
        )));
    }
    let expected = usize::try_from(uncompressed_len)
        .map_err(|_| Error::invalid("z-wrapper output exceeds addressable memory"))?;
    let compressed_len = section_length - 16;
    let compressed_len = usize::try_from(compressed_len)
        .map_err(|_| Error::invalid("z-wrapper payload exceeds addressable memory"))?;
    let mut compressed = vec![0u8; compressed_len];
    source.read_exact(&mut compressed)?;

    #[cfg(feature = "gzip")]
    {
        gzip_decompress(&compressed, expected)
    }
    #[cfg(not(feature = "gzip"))]
    {
        let _ = (compressed, expected);
        Err(Error::unsupported(
            "z-wrapper decompression requires the `gzip` feature",
        ))
    }
}
fn payload_length(section_length: u64) -> Result<u64> {
    section_length
        .checked_sub(8)
        .ok_or_else(|| Error::invalid("section length shorter than required header"))
}

fn validate_block_size(section_length: u64, limit: u64) -> Result<()> {
    if section_length > limit {
        return Err(Error::invalid(format!(
            "FST block is {section_length} bytes, above configured limit {limit}"
        )));
    }
    Ok(())
}

#[inline]
fn time_ranges_overlap(begin_time: u64, end_time: u64, range: &RangeInclusive<u64>) -> bool {
    range.start() <= range.end() && end_time >= *range.start() && begin_time <= *range.end()
}

fn normalize_included_handles(handles: &mut Option<Vec<u32>>, max_handle: u64) -> Result<()> {
    let Some(selected) = handles.as_mut() else {
        return Ok(());
    };
    selected.sort_unstable();
    selected.dedup();
    if selected.first() == Some(&0) {
        return Err(Error::invalid(
            "included handles are one-based; handle 0 is invalid",
        ));
    }
    if selected
        .last()
        .is_some_and(|handle| u64::from(*handle) > max_handle)
    {
        return Err(Error::invalid(format!(
            "included handle {} exceeds the file's maximum handle {max_handle}",
            selected.last().expect("checked as present")
        )));
    }
    if max_handle != 0 && u64::try_from(selected.len()).ok() == Some(max_handle) {
        *handles = None;
    }
    Ok(())
}
