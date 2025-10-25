#![allow(missing_docs)]

//! High-level streaming reader for FST files.

use std::io::{ErrorKind, Read, Seek, SeekFrom};

use crate::block::{BlackoutBlock, GeomInfo, Header, HierarchyBlock};
use crate::error::{Error, Result};
use crate::io::{ReadSeek, ReaderBackend};
use crate::types::BlockType;
use crate::util::{read_u64_be, skip_bytes};

mod vc;
use vc::parse_vc_block;
pub use vc::{ChainIndex, ChainSlot, VcBlockMeta};

mod change;
pub use change::{ValueChange, VcBlockChanges, build_changes};

/// Controls how the [`FstReader`] parses data.
#[derive(Debug, Clone)]
pub struct ReaderOptions {
    /// When `true`, geometry blocks are loaded eagerly as soon as they appear.
    pub eager_geometry: bool,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            eager_geometry: true,
        }
    }
}

/// Builder used to configure and construct a [`FstReader`].
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

    /// Enables or disables eager geometry parsing.
    pub fn eager_geometry(mut self, value: bool) -> Self {
        self.options.eager_geometry = value;
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
    header: Header,
    geometry: Option<GeomInfo>,
    blackout: Option<BlackoutBlock>,
    hierarchy: Option<HierarchyBlock>,
    current_vc_block: Option<VcBlockMeta>,
}

impl<R: ReadSeek> FstReader<R> {
    fn with_backend(source: R, options: ReaderOptions) -> Result<Self> {
        let mut backend = ReaderBackend::new(source);
        let header = Header::read(backend.get_mut())?;
        let mut reader = Self {
            backend,
            options,
            header,
            geometry: None,
            blackout: None,
            hierarchy: None,
            current_vc_block: None,
        };
        reader.parse_preamble()?;
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

    /// Consumes the reader, yielding the underlying I/O object.
    pub fn into_inner(self) -> R {
        self.backend.into_inner()
    }

    /// Placeholder for future block iteration support.
    pub fn skip_remaining(&mut self) -> Result<()> {
        Err(Error::unsupported(
            "block iteration not yet implemented in fst-format crate",
        ))
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
                    let meta = parse_vc_block(reader, block_type, section_start, payload_len)?;
                    let block_end = section_start.checked_add(payload_len).ok_or_else(|| {
                        Error::invalid("value-change payload exceeds file bounds")
                    })?;
                    reader.seek(SeekFrom::Start(block_end))?;
                    return Ok(Some(meta));
                }
                BlockType::Geometry => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    if self.options.eager_geometry || self.geometry.is_none() {
                        let geom = Self::read_geometry_block(reader, section_length)?;
                        self.geometry = Some(geom);
                    } else {
                        skip_bytes(reader, payload_len)?;
                    }
                }
                BlockType::Blackout => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
                        Error::invalid("blackout payload exceeds addressable memory")
                    })?;
                    let mut buf = vec![0u8; payload_len_usize];
                    reader.read_exact(&mut buf)?;
                    self.blackout = Some(BlackoutBlock::decode(&buf)?);
                }
                BlockType::Hierarchy | BlockType::HierarchyLz4 | BlockType::HierarchyLz4Duo => {
                    let hier = Self::read_hierarchy_block(reader, block_type)?;
                    self.hierarchy = Some(hier);
                }
                BlockType::Skip => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    skip_bytes(reader, payload_len)?;
                }
                BlockType::ZWrapper => {
                    return Err(Error::unsupported(
                        "zlib wrapper blocks are not yet supported",
                    ));
                }
                BlockType::Header => {
                    return Err(Error::invalid("duplicate header block encountered"));
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
        let time_zero = self.header.time_zero;
        let block_ref = self.current_vc_block.as_ref().expect("block just stored");
        block_ref.changes(geom, time_zero).map(Some)
    }

    fn parse_preamble(&mut self) -> Result<()> {
        let reader = self.backend.get_mut();
        loop {
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            }
            let block_type = match BlockType::try_from(tag[0]) {
                Ok(bt) => bt,
                Err(_) => return Err(Error::invalid(format!("unknown block type {:02x}", tag[0]))),
            };

            match block_type {
                BlockType::Geometry => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    if self.options.eager_geometry || self.geometry.is_none() {
                        let geom = Self::read_geometry_block(reader, section_length)?;
                        self.geometry = Some(geom);
                    } else {
                        skip_bytes(reader, payload_len)?;
                    }
                }
                BlockType::Blackout => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
                        Error::invalid("blackout payload exceeds addressable memory")
                    })?;
                    let mut buf = vec![0u8; payload_len_usize];
                    reader.read_exact(&mut buf)?;
                    self.blackout = Some(BlackoutBlock::decode(&buf)?);
                }
                BlockType::Hierarchy | BlockType::HierarchyLz4 | BlockType::HierarchyLz4Duo => {
                    let hier = Self::read_hierarchy_block(reader, block_type)?;
                    self.hierarchy = Some(hier);
                }
                BlockType::Skip => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    skip_bytes(reader, payload_len)?;
                }
                BlockType::VcData | BlockType::VcDataDynAlias | BlockType::VcDataDynAlias2 => {
                    reader.seek(SeekFrom::Current(-1))?;
                    break;
                }
                BlockType::ZWrapper => {
                    return Err(Error::unsupported(
                        "zlib wrapper blocks are not yet supported",
                    ));
                }
                BlockType::Header => {
                    return Err(Error::invalid("duplicate header block encountered"));
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
                BlockType::Geometry => {
                    let section_length = read_u64_be(reader)?;
                    let geom = Self::read_geometry_block(reader, section_length)?;
                    self.geometry = Some(geom);
                }
                BlockType::Blackout => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
                        Error::invalid("blackout payload exceeds addressable memory")
                    })?;
                    let mut buf = vec![0u8; payload_len_usize];
                    reader.read_exact(&mut buf)?;
                    self.blackout = Some(BlackoutBlock::decode(&buf)?);
                }
                BlockType::Hierarchy | BlockType::HierarchyLz4 | BlockType::HierarchyLz4Duo => {
                    let hier = Self::read_hierarchy_block(reader, block_type)?;
                    self.hierarchy = Some(hier);
                }
                BlockType::Skip => {
                    let section_length = read_u64_be(reader)?;
                    let payload_len = payload_length(section_length)?;
                    skip_bytes(reader, payload_len)?;
                }
                BlockType::ZWrapper => {
                    return Err(Error::unsupported(
                        "zlib wrapper blocks are not yet supported",
                    ));
                }
                BlockType::Header => {
                    return Err(Error::invalid("duplicate header block encountered"));
                }
            }
        }
    }

    fn read_geometry_block<Rd: Read + Seek>(
        reader: &mut Rd,
        section_length: u64,
    ) -> Result<GeomInfo> {
        GeomInfo::decode_block(reader, section_length)
    }

    fn read_hierarchy_block<Rd: Read + Seek>(
        reader: &mut Rd,
        block_type: BlockType,
    ) -> Result<HierarchyBlock> {
        let section_length = read_u64_be(reader)?;
        HierarchyBlock::decode_block(reader, block_type, section_length)
    }
}
fn payload_length(section_length: u64) -> Result<u64> {
    section_length
        .checked_sub(8)
        .ok_or_else(|| Error::invalid("section length shorter than required header"))
}
