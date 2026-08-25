//! Incremental writer producing FST output streams.

use crate::block::{
    AttributeEntry, BlackoutBlock, BlackoutEvent, ChainIndexEntry, GeomEntry, GeomInfo, Header,
    HierarchyBlock, HierarchyCompression, HierarchyItem, ScopeEntry, VarEntry, encode_chain_index,
    encode_chain_index_dyn_alias2, encode_chain_payload, encode_frame_section, encode_time_section,
};
use crate::encoding::encode_varint;
use crate::error::{Error, Result};
use crate::io::{WriteSeek, WriterBackend};
use crate::types::{BlockType, PackType, ScopeType, SignalValue, VarDir, VarType};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Cursor, Seek, SeekFrom, Write};

#[cfg(feature = "gzip")]
use flate2::{Compression, write::GzEncoder};
#[cfg(feature = "parallel")]
use rayon::prelude::*;

/// Options controlling [`FstWriter`] behaviour.
#[derive(Debug, Clone)]
pub struct WriterOptions {
    /// Base-10 exponent describing the timescale to encode inside the header.
    pub timescale_exponent: i8,
    /// Optional compression quality hint (algorithm specific).
    pub compression_level: Option<u32>,
    /// Compression applied to chain payloads inside value-change blocks.
    pub chain_compression: ChainCompression,
    /// Compression applied to the trailing time-table section.
    pub time_compression: TimeCompression,
    /// Wrap the entire file in an outer `FST_BL_ZWRAPPER` gzip envelope.
    pub wrap_zlib: bool,
    /// Maximum queued changes before the current value-change block is flushed.
    pub block_change_limit: usize,
    /// Approximate payload bytes queued before a value-change block is flushed.
    pub block_size_limit: usize,
}

/// Compression choice for the per-handle value-change payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainCompression {
    /// Store chains without compression (`pack marker` = `0`).
    Raw,
    /// Compress each chain using zlib/deflate (`pack marker` = `'Z'`).
    Zlib,
    /// Compress each chain with LZ4 (`pack marker` = `'4'`).
    Lz4,
    /// Compress using FastLZ (`pack marker` = `'F'`).
    FastLz,
}

/// Compression choice for the block-level time delta section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeCompression {
    /// Leave the time table uncompressed.
    Raw,
    /// Compress the time table using zlib.
    Zlib,
}

impl Default for WriterOptions {
    fn default() -> Self {
        let chain_compression = if cfg!(feature = "gzip") {
            ChainCompression::Zlib
        } else {
            ChainCompression::Raw
        };
        let time_compression = if cfg!(feature = "gzip") {
            TimeCompression::Zlib
        } else {
            TimeCompression::Raw
        };
        Self {
            timescale_exponent: -9,
            compression_level: None,
            chain_compression,
            time_compression,
            wrap_zlib: false,
            block_change_limit: 1_000_000,
            block_size_limit: 64 * 1024 * 1024,
        }
    }
}

/// Builder for [`FstWriter`].
pub struct WriterBuilder<W: WriteSeek> {
    sink: W,
    options: WriterOptions,
}

impl<W: WriteSeek> WriterBuilder<W> {
    /// Creates a builder from the provided writable sink.
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

    /// Selects the compression strategy used for per-handle chains.
    pub fn chain_compression(mut self, compression: ChainCompression) -> Self {
        self.options.chain_compression = compression;
        self
    }

    /// Selects the compression strategy used for the block time table.
    pub fn time_compression(mut self, compression: TimeCompression) -> Self {
        self.options.time_compression = compression;
        self
    }

    /// Enables or disables the outer `FST_BL_ZWRAPPER` gzip envelope.
    pub fn wrap_with_zlib(mut self, wrap: bool) -> Self {
        self.options.wrap_zlib = wrap;
        self
    }

    /// Sets the timescale exponent that will be recorded in the header.
    pub fn timescale_exponent(mut self, exponent: i8) -> Self {
        self.options.timescale_exponent = exponent;
        self
    }

    /// Sets an algorithm-specific compression level, clamped to each backend's range.
    pub fn compression_level(mut self, level: Option<u32>) -> Self {
        self.options.compression_level = level;
        self
    }

    /// Bounds memory use by flushing after this many queued value changes.
    pub fn block_change_limit(mut self, limit: usize) -> Self {
        self.options.block_change_limit = limit;
        self
    }

    /// Bounds queued value payload memory and the target size of generated VC blocks.
    pub fn block_size_limit(mut self, bytes: usize) -> Self {
        self.options.block_size_limit = bytes;
        self
    }

    /// Builds the writer, validating options before returning the instance.
    pub fn build(self) -> Result<FstWriter<W>> {
        FstWriter::with_backend(self.sink, self.options)
    }
}

fn validate_options(options: &WriterOptions) -> Result<()> {
    if options.block_change_limit == 0 || options.block_size_limit == 0 {
        return Err(Error::invalid(
            "writer block limits must be greater than zero",
        ));
    }
    match options.chain_compression {
        ChainCompression::Raw => {}
        ChainCompression::Zlib => {
            #[cfg(not(feature = "gzip"))]
            {
                return Err(Error::unsupported(
                    "zlib chain compression requires the `gzip` feature",
                ));
            }
        }
        ChainCompression::Lz4 => {
            #[cfg(not(feature = "lz4"))]
            {
                return Err(Error::unsupported(
                    "lz4 chain compression requires the `lz4` feature",
                ));
            }
        }
        ChainCompression::FastLz => {
            #[cfg(not(feature = "fastlz"))]
            {
                return Err(Error::unsupported(
                    "fastlz chain compression requires the `fastlz` feature",
                ));
            }
        }
    }

    match options.time_compression {
        TimeCompression::Raw => {}
        TimeCompression::Zlib => {
            #[cfg(not(feature = "gzip"))]
            {
                return Err(Error::unsupported(
                    "zlib time compression requires the `gzip` feature",
                ));
            }
        }
    }

    if options.wrap_zlib {
        #[cfg(not(feature = "gzip"))]
        {
            return Err(Error::unsupported(
                "file-level zlib wrapper requires the `gzip` feature",
            ));
        }
    }

    #[cfg(any(feature = "gzip", feature = "lz4"))]
    {
        Ok(())
    }
    #[cfg(not(any(feature = "gzip", feature = "lz4")))]
    {
        Err(Error::unsupported(
            "writing a self-contained FST requires the `gzip` or `lz4` feature for hierarchy data",
        ))
    }
}

/// Streaming writer for FST files.
enum OutputBackend<W: WriteSeek> {
    Direct(WriterBackend<W>),
    Wrapped {
        buffer: WriterBackend<Cursor<Vec<u8>>>,
        sink: W,
    },
}

impl<W: WriteSeek> OutputBackend<W> {
    fn direct(sink: W) -> Self {
        OutputBackend::Direct(WriterBackend::new(sink))
    }

    fn wrapped(sink: W) -> Self {
        let cursor = Cursor::new(Vec::new());
        OutputBackend::Wrapped {
            buffer: WriterBackend::new(cursor),
            sink,
        }
    }

    fn writer_mut(&mut self) -> &mut dyn Write {
        match self {
            OutputBackend::Direct(backend) => backend.get_mut(),
            OutputBackend::Wrapped { buffer, .. } => buffer.get_mut(),
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer_mut().write_all(bytes)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            OutputBackend::Direct(backend) => backend.get_mut().flush()?,
            OutputBackend::Wrapped { buffer, .. } => buffer.get_mut().flush()?,
        }
        Ok(())
    }

    fn with_writer<F>(&mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        f(self.writer_mut())
    }

    fn seek(&mut self, position: SeekFrom) -> Result<u64> {
        Ok(match self {
            OutputBackend::Direct(backend) => backend.get_mut().seek(position)?,
            OutputBackend::Wrapped { buffer, .. } => buffer.get_mut().seek(position)?,
        })
    }

    fn stream_position(&mut self) -> Result<u64> {
        self.seek(SeekFrom::Current(0))
    }

    fn into_inner(self, options: &WriterOptions) -> Result<W> {
        match self {
            OutputBackend::Direct(backend) => Ok(backend.into_inner()?),
            OutputBackend::Wrapped { mut buffer, sink } => {
                buffer.get_mut().flush()?;
                let cursor = buffer.into_inner()?;
                let inner = cursor.into_inner();
                #[cfg(not(feature = "gzip"))]
                {
                    let _ = (inner, options, sink);
                    Err(Error::unsupported(
                        "file-level zlib wrapper requires the `gzip` feature",
                    ))
                }
                #[cfg(feature = "gzip")]
                {
                    let mut encoder = GzEncoder::new(
                        Vec::new(),
                        Compression::new(
                            options.compression_level.map(|lvl| lvl.min(9)).unwrap_or(6),
                        ),
                    );
                    encoder.write_all(&inner)?;
                    let compressed = encoder.finish()?;
                    let uncompressed_len = u64::try_from(inner.len()).map_err(|_| {
                        Error::invalid("z-wrapper uncompressed payload exceeds supported length")
                    })?;

                    let mut payload = Vec::with_capacity(compressed.len() + 8);
                    payload.extend_from_slice(&uncompressed_len.to_be_bytes());
                    payload.extend_from_slice(&compressed);

                    let section_length = u64::try_from(payload.len())
                        .map_err(|_| Error::invalid("z-wrapper section length overflow"))?
                        .checked_add(8)
                        .ok_or_else(|| Error::invalid("z-wrapper section length overflow"))?;

                    let mut outer = WriterBackend::new(sink);
                    {
                        let writer = outer.get_mut();
                        writer.write_all(&[BlockType::ZWrapper as u8])?;
                        writer.write_all(&section_length.to_be_bytes())?;
                        writer.write_all(&payload)?;
                        writer.flush()?;
                    }
                    Ok(outer.into_inner()?)
                }
            }
        }
    }
}

/// Streaming writer for the Fast Signal Trace format.
pub struct FstWriter<W: WriteSeek> {
    output: OutputBackend<W>,
    options: WriterOptions,
    header_written: bool,
    metadata_written: bool,
    frame_state: FrameState,
    scopes: Vec<ScopeEntry>,
    variables: Vec<VarEntry>,
    hierarchy_items: Vec<HierarchyItem>,
    attributes: Vec<AttributeEntry>,
    attribute_depth: usize,
    scope_stack: Vec<usize>,
    geometry: Vec<GeomEntry>,
    next_handle: u32,
    header: Option<Header>,
    pending_chains: Vec<PendingChain>,
    pending_times: Vec<u64>,
    pending_time_data: Vec<u8>,
    pending_change_count: usize,
    pending_bytes: usize,
    vc_blocks_written: u64,
    first_change_time: Option<u64>,
    last_change_time: Option<u64>,
    blackout_events: Vec<BlackoutEvent>,
    blackout_written: bool,
}

impl<W: WriteSeek> FstWriter<W> {
    fn with_backend(sink: W, options: WriterOptions) -> Result<Self> {
        validate_options(&options)?;
        let output = if options.wrap_zlib {
            OutputBackend::wrapped(sink)
        } else {
            OutputBackend::direct(sink)
        };
        Ok(Self {
            output,
            options,
            header_written: false,
            metadata_written: false,
            frame_state: FrameState::default(),
            scopes: Vec::new(),
            variables: Vec::new(),
            hierarchy_items: Vec::new(),
            attributes: Vec::new(),
            attribute_depth: 0,
            scope_stack: Vec::new(),
            geometry: Vec::new(),
            next_handle: 1,
            header: None,
            pending_chains: Vec::new(),
            pending_times: Vec::new(),
            pending_time_data: Vec::new(),
            pending_change_count: 0,
            pending_bytes: 0,
            vc_blocks_written: 0,
            first_change_time: None,
            last_change_time: None,
            blackout_events: Vec::new(),
            blackout_written: false,
        })
    }

    /// Starts building a writer for the given sink.
    pub fn builder(sink: W) -> WriterBuilder<W> {
        WriterBuilder::new(sink)
    }

    /// Writes the FST header and declaration blocks.
    pub fn write_header(&mut self, mut header: Header) -> Result<()> {
        if self.header_written {
            return Err(Error::unsupported("header already written"));
        }
        if !self.scope_stack.is_empty() {
            return Err(Error::invalid(
                "cannot write header while scopes remain open; call `end_scope` first",
            ));
        }
        if self.attribute_depth != 0 {
            return Err(Error::invalid(
                "cannot write header while hierarchy attributes remain open",
            ));
        }

        header.scope_count = u64::try_from(self.scopes.len())
            .map_err(|_| Error::invalid("scope count exceeds u64"))?;
        header.var_count = u64::try_from(self.variables.len())
            .map_err(|_| Error::invalid("variable count exceeds u64"))?;
        header.max_handle = u64::try_from(self.geometry.len())
            .map_err(|_| Error::invalid("handle count exceeds u64"))?;
        header.timescale_exponent = self.options.timescale_exponent;
        header.section_length = 329;

        self.write_header_block(&header)?;
        self.write_geometry_block(cfg!(feature = "gzip"))?;
        self.write_hierarchy_block()?;

        self.pending_chains
            .resize_with(self.geometry.len(), PendingChain::default);

        self.header_written = true;
        self.metadata_written = true;
        self.header = Some(header);
        Ok(())
    }

    /// Starts a new scope and pushes it onto the hierarchy stack.
    pub fn begin_scope(
        &mut self,
        scope_type: ScopeType,
        name: impl Into<String>,
        component: Option<String>,
    ) -> Result<ScopeId> {
        self.ensure_metadata_mutable()?;
        let name = name.into();
        validate_hierarchy_text("scope name", &name)?;
        if let Some(component) = &component {
            validate_hierarchy_text("scope component", component)?;
        }
        let parent = self.scope_stack.last().copied();
        let scope = ScopeEntry {
            scope_type,
            name,
            component,
            parent,
        };
        self.scopes.push(scope);
        let index = self.scopes.len() - 1;
        self.hierarchy_items
            .push(HierarchyItem::ScopeBegin { scope_index: index });
        self.scope_stack.push(index);
        Ok(ScopeId(index))
    }

    /// Closes the most recently opened scope.
    pub fn end_scope(&mut self) -> Result<()> {
        self.ensure_metadata_mutable()?;
        if self.scope_stack.pop().is_none() {
            return Err(Error::invalid("scope stack underflow"));
        }
        self.hierarchy_items.push(HierarchyItem::ScopeEnd);
        Ok(())
    }

    /// Emits a self-contained hierarchy attribute (for example a comment or source stem).
    pub fn add_attribute(
        &mut self,
        attr_type: u8,
        subtype: u8,
        name: impl Into<String>,
        argument: u64,
    ) -> Result<AttributeId> {
        self.push_attribute(attr_type, subtype, name.into(), argument, false)
    }

    /// Begins a hierarchy attribute that will be closed by [`end_attribute`](Self::end_attribute).
    pub fn begin_attribute(
        &mut self,
        attr_type: u8,
        subtype: u8,
        name: impl Into<String>,
        argument: u64,
    ) -> Result<AttributeId> {
        self.push_attribute(attr_type, subtype, name.into(), argument, true)
    }

    /// Closes the most recently begun hierarchy attribute.
    pub fn end_attribute(&mut self) -> Result<()> {
        self.ensure_metadata_mutable()?;
        if self.attribute_depth == 0 {
            return Err(Error::invalid("hierarchy attribute stack underflow"));
        }
        self.attribute_depth -= 1;
        self.hierarchy_items.push(HierarchyItem::AttributeEnd);
        Ok(())
    }

    /// Declares a variable within the currently active scope. Returns the newly allocated handle.
    pub fn add_variable(
        &mut self,
        var_type: VarType,
        direction: VarDir,
        name: impl Into<String>,
        geometry: GeomEntry,
    ) -> Result<u32> {
        self.ensure_metadata_mutable()?;
        validate_geometry(var_type, &geometry)?;
        let name = name.into();
        validate_hierarchy_text("variable name", &name)?;
        let scope = self
            .scope_stack
            .last()
            .copied()
            .ok_or_else(|| Error::invalid("variables require an active scope"))?;

        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or_else(|| Error::invalid("handle counter overflow"))?;

        let length = match geometry {
            GeomEntry::Fixed(bytes) => Some(bytes),
            GeomEntry::Real => Some(8),
            GeomEntry::Variable => None,
        };

        self.geometry.push(geometry);
        self.variables.push(VarEntry {
            var_type,
            direction,
            name,
            length,
            handle,
            alias_of: None,
            scope: Some(scope),
            is_alias: false,
        });
        let var_index = self.variables.len() - 1;
        self.hierarchy_items.push(HierarchyItem::Var { var_index });
        if let Some(entry) = self.geometry.last() {
            self.frame_state.register_handle(handle, entry);
        }

        Ok(handle)
    }

    /// Declares an alias that reuses an existing handle. FST aliases do not allocate a new handle;
    /// the returned value is therefore exactly `target_handle`.
    pub fn add_alias(
        &mut self,
        var_type: VarType,
        direction: VarDir,
        name: impl Into<String>,
        target_handle: u32,
    ) -> Result<u32> {
        self.ensure_metadata_mutable()?;
        let name = name.into();
        validate_hierarchy_text("alias name", &name)?;
        if target_handle == 0 || target_handle >= self.next_handle {
            return Err(Error::invalid(format!(
                "alias target handle {target_handle} is out of range (max {})",
                self.next_handle - 1
            )));
        }

        let scope = self
            .scope_stack
            .last()
            .copied()
            .ok_or_else(|| Error::invalid("aliases require an active scope"))?;

        let target_index = (target_handle - 1) as usize;
        let geometry = self.geometry.get(target_index).cloned().ok_or_else(|| {
            Error::invalid(format!(
                "no geometry recorded for target handle {target_handle}"
            ))
        })?;
        validate_geometry(var_type, &geometry)?;

        self.variables.push(VarEntry {
            var_type,
            direction,
            name,
            length: match geometry {
                GeomEntry::Fixed(bytes) => Some(bytes),
                GeomEntry::Real => Some(8),
                GeomEntry::Variable => None,
            },
            handle: target_handle,
            alias_of: Some(target_handle),
            scope: Some(scope),
            is_alias: true,
        });
        let var_index = self.variables.len() - 1;
        self.hierarchy_items.push(HierarchyItem::Var { var_index });

        Ok(target_handle)
    }

    /// Records a value change that will be emitted in the next value-change block.
    pub fn emit_change(
        &mut self,
        timestamp: u64,
        handle: u32,
        value: SignalValue<'_>,
    ) -> Result<()> {
        if !self.header_written {
            return Err(Error::invalid(
                "value changes cannot be emitted before the header is written",
            ));
        }
        if handle == 0 || handle >= self.next_handle {
            return Err(Error::invalid(format!(
                "handle {handle} is out of range (max {})",
                self.next_handle - 1
            )));
        }

        if let Some(previous) = self.last_change_time
            && timestamp < previous
        {
            return Err(Error::invalid(format!(
                "timestamps must be monotonic: {timestamp} follows {previous}"
            )));
        }

        let geom_index = (handle - 1) as usize;
        let geom_entry = self
            .geometry
            .get(geom_index)
            .ok_or_else(|| Error::invalid(format!("no geometry recorded for handle {handle}")))?;
        let owned_value = Self::convert_value(value, geom_entry)?;
        let value_size = owned_value.memory_size();

        if self.pending_change_count != 0
            && self.pending_bytes.saturating_add(value_size) > self.options.block_size_limit
        {
            self.flush_value_changes()?;
        }

        let time_index = if self.pending_times.last().copied() == Some(timestamp) {
            self.pending_times.len() - 1
        } else {
            let delta = match self.pending_times.last().copied() {
                Some(previous) => timestamp
                    .checked_sub(previous)
                    .ok_or_else(|| Error::invalid("timestamps must be non-decreasing"))?,
                None => timestamp,
            };
            encode_varint(delta, &mut self.pending_time_data);
            self.pending_times.push(timestamp);
            self.pending_times.len() - 1
        };

        let chain = self
            .pending_chains
            .get_mut(geom_index)
            .ok_or_else(|| Error::invalid(format!("no pending chain for handle {handle}")))?;
        let delta = match chain.last_time_index {
            Some(previous) => time_index
                .checked_sub(previous)
                .ok_or_else(|| Error::invalid("time indices must be non-decreasing"))?,
            None => time_index,
        };
        let old_len = chain.data.len();
        encode_owned_value(&owned_value, delta, &mut chain.data)?;
        chain.last_time_index = Some(time_index);
        chain.latest_value = Some(owned_value);
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(chain.data.len().saturating_sub(old_len));
        self.pending_change_count = self.pending_change_count.saturating_add(1);
        self.first_change_time.get_or_insert(timestamp);
        self.last_change_time = Some(timestamp);

        if self.pending_change_count >= self.options.block_change_limit
            || self.pending_bytes >= self.options.block_size_limit
        {
            self.flush_value_changes()?;
        }

        Ok(())
    }

    /// Records a `$dumpon`/`$dumpoff` transition for the blackout block.
    pub fn emit_dump_active(&mut self, timestamp: u64, is_on: bool) -> Result<()> {
        if !self.header_written {
            return Err(Error::invalid(
                "dump activity cannot be emitted before the header is written",
            ));
        }
        if self.blackout_written {
            return Err(Error::invalid("blackout block has already been finalized"));
        }
        if let Some(previous) = self.last_change_time
            && timestamp < previous
        {
            return Err(Error::invalid(format!(
                "timestamps must be monotonic: {timestamp} follows {previous}"
            )));
        }
        self.blackout_events.push(BlackoutEvent {
            is_on,
            time: timestamp,
        });
        self.first_change_time.get_or_insert(timestamp);
        self.last_change_time = Some(timestamp);
        Ok(())
    }

    /// Flushes any buffered data to the sink.
    pub fn flush(&mut self) -> Result<()> {
        self.flush_value_changes()?;
        self.patch_header_statistics()?;
        self.output.flush()?;
        Ok(())
    }

    /// Consumes the writer, returning the underlying sink once buffered data has been flushed.
    pub fn finish(mut self) -> Result<W> {
        if !self.header_written {
            return Err(Error::invalid(
                "cannot finish an FST stream before writing its header",
            ));
        }
        self.flush_value_changes()?;
        self.write_blackout_block()?;
        self.patch_header_statistics()?;
        self.output.flush()?;
        self.output.into_inner(&self.options)
    }

    fn ensure_metadata_mutable(&self) -> Result<()> {
        if self.metadata_written {
            Err(Error::unsupported(
                "metadata definitions must occur before writing the header",
            ))
        } else {
            Ok(())
        }
    }

    fn push_attribute(
        &mut self,
        attr_type: u8,
        subtype: u8,
        name: String,
        argument: u64,
        nested: bool,
    ) -> Result<AttributeId> {
        self.ensure_metadata_mutable()?;
        validate_hierarchy_text("attribute name", &name)?;
        let index = self.attributes.len();
        self.attributes.push(AttributeEntry {
            attr_type,
            subtype,
            name,
            argument,
            scope: self.scope_stack.last().copied(),
        });
        self.hierarchy_items.push(HierarchyItem::AttributeBegin {
            attribute_index: index,
        });
        if nested {
            self.attribute_depth = self
                .attribute_depth
                .checked_add(1)
                .ok_or_else(|| Error::invalid("hierarchy attribute depth overflow"))?;
        }
        Ok(AttributeId(index))
    }

    fn convert_value(value: SignalValue<'_>, geom: &GeomEntry) -> Result<OwnedValue> {
        match geom {
            GeomEntry::Fixed(1) => match value {
                SignalValue::Bit(bit) => Ok(OwnedValue::Bit(BitValue::from_char(bit)?)),
                SignalValue::Vector(v) if v.len() == 1 => {
                    let ch = v
                        .chars()
                        .next()
                        .ok_or_else(|| Error::invalid("empty vector value for 1-bit handle"))?;
                    Ok(OwnedValue::Bit(BitValue::from_char(ch)?))
                }
                SignalValue::Bytes(bytes) if bytes.len() == 1 => {
                    let ch = bytes[0] as char;
                    Ok(OwnedValue::Bit(BitValue::from_char(ch)?))
                }
                SignalValue::PackedBits { width: 1, bits } => {
                    let normalized = normalize_packed_bits(1, bits.as_ref())?;
                    let byte = normalized[0];
                    let ch = if (byte & 0x80) != 0 { '1' } else { '0' };
                    Ok(OwnedValue::Bit(BitValue::from_char(ch)?))
                }
                _ => Err(Error::unsupported(
                    "value type is not compatible with single-bit geometry",
                )),
            },
            GeomEntry::Fixed(width) => {
                let width_usize = *width as usize;
                if width_usize == 0 {
                    return Err(Error::invalid("zero-width fixed geometry encountered"));
                }

                match value {
                    SignalValue::Vector(text) => {
                        let bytes = text.as_bytes();
                        if bytes.len() != width_usize {
                            return Err(Error::invalid(format!(
                                "vector value length {} does not match geometry width {}",
                                bytes.len(),
                                width_usize
                            )));
                        }
                        let data = bytes.to_vec();
                        let packed = pack_ascii_bits(&data, *width);
                        Ok(OwnedValue::Vector {
                            width: *width,
                            data,
                            packed,
                        })
                    }
                    SignalValue::Bytes(bytes) => {
                        let owned = bytes.into_owned();
                        if owned.len() != width_usize {
                            return Err(Error::invalid(format!(
                                "byte value length {} does not match geometry width {}",
                                owned.len(),
                                width_usize
                            )));
                        }
                        let packed = pack_ascii_bits(&owned, *width);
                        Ok(OwnedValue::Vector {
                            width: *width,
                            data: owned,
                            packed,
                        })
                    }
                    SignalValue::PackedBits { width: w, bits } => {
                        if w != *width {
                            return Err(Error::invalid(format!(
                                "packed bit vector width {} does not match geometry width {}",
                                w, width
                            )));
                        }
                        let normalized = normalize_packed_bits(*width, bits.as_ref())?;
                        let unpacked = unpack_packed_bits(*width, &normalized)?;
                        Ok(OwnedValue::Vector {
                            width: *width,
                            data: unpacked,
                            packed: Some(normalized),
                        })
                    }
                    _ => Err(Error::unsupported(
                        "value type is not yet supported for fixed-width vectors",
                    )),
                }
            }
            GeomEntry::Real => match value {
                SignalValue::Real(real) => Ok(OwnedValue::Real(real)),
                SignalValue::Bytes(bytes) => {
                    let owned = bytes.into_owned();
                    if owned.len() != 8 {
                        return Err(Error::invalid(format!(
                            "real signal expects 8 bytes, received {}",
                            owned.len()
                        )));
                    }
                    let mut raw = [0u8; 8];
                    raw.copy_from_slice(&owned);
                    let value = if cfg!(target_endian = "little") {
                        f64::from_le_bytes(raw)
                    } else {
                        f64::from_be_bytes(raw)
                    };
                    Ok(OwnedValue::Real(value))
                }
                _ => Err(Error::unsupported(
                    "value type is not compatible with real-valued geometry",
                )),
            },
            GeomEntry::Variable => match value {
                SignalValue::Bytes(bytes) => Ok(OwnedValue::VarLen(bytes.into_owned())),
                SignalValue::Vector(text) => Ok(OwnedValue::VarLen(text.into_owned().into_bytes())),
                SignalValue::Bit(bit) => Ok(OwnedValue::VarLen(vec![bit as u8])),
                _ => Err(Error::unsupported(
                    "value type is not compatible with variable-length geometry",
                )),
            },
        }
    }

    fn flush_value_changes(&mut self) -> Result<()> {
        if self.pending_change_count == 0 {
            return Ok(());
        }
        let (block_type, payload) = self.build_vc_block()?;
        let section_length = (payload.len() as u64)
            .checked_add(8)
            .ok_or_else(|| Error::invalid("value-change block length overflow"))?;
        self.output.write_all(&[block_type as u8])?;
        self.output.write_all(&section_length.to_be_bytes())?;
        self.output.write_all(&payload)?;

        for (handle_index, chain) in self.pending_chains.iter_mut().enumerate() {
            if let Some(value) = chain.latest_value.take() {
                self.frame_state.update((handle_index + 1) as u32, &value)?;
            }
            chain.data.clear();
            chain.last_time_index = None;
        }
        self.pending_times.clear();
        self.pending_time_data.clear();
        self.pending_change_count = 0;
        self.pending_bytes = 0;
        self.vc_blocks_written = self
            .vc_blocks_written
            .checked_add(1)
            .ok_or_else(|| Error::invalid("vc section counter overflow"))?;
        Ok(())
    }

    fn patch_header_statistics(&mut self) -> Result<()> {
        if !self.header_written {
            return Ok(());
        }

        let end_position = self.output.stream_position()?;
        let header = self
            .header
            .as_mut()
            .ok_or_else(|| Error::invalid("header state missing after write"))?;
        if let Some(start) = self.first_change_time {
            header.start_time = start;
        }
        if let Some(end) = self.last_change_time {
            header.end_time = end;
        }
        header.vc_section_count = self.vc_blocks_written;
        header.scope_count = u64::try_from(self.scopes.len())
            .map_err(|_| Error::invalid("scope count exceeds u64"))?;
        header.var_count = u64::try_from(self.variables.len())
            .map_err(|_| Error::invalid("variable count exceeds u64"))?;
        header.max_handle = u64::try_from(self.geometry.len())
            .map_err(|_| Error::invalid("handle count exceeds u64"))?;

        self.output.seek(SeekFrom::Start(9))?;
        self.output.write_all(&header.start_time.to_be_bytes())?;
        self.output.write_all(&header.end_time.to_be_bytes())?;
        self.output.seek(SeekFrom::Start(41))?;
        self.output.write_all(&header.scope_count.to_be_bytes())?;
        self.output.write_all(&header.var_count.to_be_bytes())?;
        self.output.write_all(&header.max_handle.to_be_bytes())?;
        self.output
            .write_all(&header.vc_section_count.to_be_bytes())?;
        self.output.seek(SeekFrom::Start(end_position))?;
        Ok(())
    }

    fn write_blackout_block(&mut self) -> Result<()> {
        if self.blackout_written || self.blackout_events.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::new();
        BlackoutBlock {
            events: self.blackout_events.clone(),
        }
        .encode(&mut payload)?;
        let section_length = u64::try_from(payload.len())
            .map_err(|_| Error::invalid("blackout block exceeds supported length"))?
            .checked_add(8)
            .ok_or_else(|| Error::invalid("blackout section length overflow"))?;
        self.output.write_all(&[BlockType::Blackout as u8])?;
        self.output.write_all(&section_length.to_be_bytes())?;
        self.output.write_all(&payload)?;
        self.blackout_written = true;
        Ok(())
    }

    fn build_vc_block(&self) -> Result<(BlockType, Vec<u8>)> {
        if self.pending_change_count == 0 || self.pending_times.is_empty() {
            return Err(Error::invalid(
                "attempted to build a value-change block with no pending changes",
            ));
        }

        let max_handle = self.next_handle.saturating_sub(1);
        if max_handle == 0 {
            return Err(Error::invalid(
                "no handles defined; unable to encode value-change block",
            ));
        }

        let frame_bytes = self
            .frame_state
            .build_frame_bytes(&self.geometry, max_handle)?;
        let frame_encoding = encode_frame_section(frame_bytes, self.options.compression_level)?;
        let frame_max_handle = if frame_encoding.uncompressed_len > 0 {
            max_handle as u64
        } else {
            0
        };

        let pack_type = self.chain_pack_type();
        let mut required_memory = 0u64;
        let mut chain_buffer = Vec::with_capacity(self.pending_bytes);
        let mut index_entries = Vec::with_capacity(self.pending_chains.len());
        let mut canonical_chains: HashMap<&[u8], u32> = HashMap::new();
        let mut canonical_payloads = Vec::new();
        let mut has_dynamic_aliases = false;

        for (handle_index, chain) in self.pending_chains.iter().enumerate() {
            if chain.data.is_empty() {
                index_entries.push(ChainIndexEntry::Empty);
                continue;
            }

            let raw_len = u64::try_from(chain.data.len())
                .map_err(|_| Error::invalid("chain payload exceeds supported length"))?;
            required_memory = required_memory
                .checked_add(raw_len)
                .ok_or_else(|| Error::invalid("chain memory requirement overflow"))?;

            if let Some(&target) = canonical_chains.get(chain.data.as_slice()) {
                index_entries.push(ChainIndexEntry::Alias { target });
                has_dynamic_aliases = true;
                continue;
            }

            let handle = u32::try_from(handle_index + 1)
                .map_err(|_| Error::invalid("handle exceeds u32 range"))?;
            canonical_chains.insert(chain.data.as_slice(), handle);
            canonical_payloads.push(chain.data.as_slice());
            index_entries.push(ChainIndexEntry::Data { offset: 0 });
        }

        let chain_compression = self.options.chain_compression;
        let compression_level = self.options.compression_level;
        let encode_payload = |data: &&[u8]| -> Result<(u64, Vec<u8>)> {
            let data = *data;
            if chain_compression == ChainCompression::Raw {
                Ok((0, data.to_vec()))
            } else {
                encode_chain_payload(pack_type, data, compression_level)
            }
        };

        #[cfg(feature = "parallel")]
        let encoded_payloads: Vec<(u64, Vec<u8>)> = {
            let canonical_bytes: usize = canonical_payloads.iter().map(|data| data.len()).sum();
            if canonical_payloads.len() >= 32 && canonical_bytes >= 64 * 1024 {
                canonical_payloads
                    .par_iter()
                    .map(encode_payload)
                    .collect::<Result<_>>()?
            } else {
                canonical_payloads
                    .iter()
                    .map(encode_payload)
                    .collect::<Result<_>>()?
            }
        };

        #[cfg(not(feature = "parallel"))]
        let encoded_payloads: Vec<(u64, Vec<u8>)> = canonical_payloads
            .iter()
            .map(encode_payload)
            .collect::<Result<_>>()?;

        let mut encoded_payloads = encoded_payloads.into_iter();
        for entry in &mut index_entries {
            if let ChainIndexEntry::Data { offset } = entry {
                let (stored_len, payload_bytes) = encoded_payloads
                    .next()
                    .ok_or_else(|| Error::invalid("missing encoded chain payload"))?;
                *offset = u64::try_from(chain_buffer.len())
                    .map_err(|_| Error::invalid("chain buffer exceeds u64 range"))?;
                encode_varint(stored_len, &mut chain_buffer);
                chain_buffer.extend_from_slice(&payload_bytes);
            }
        }

        let index_bytes = if has_dynamic_aliases {
            encode_chain_index_dyn_alias2(&index_entries)?
        } else {
            encode_chain_index(&index_entries)?
        };
        let index_length = u64::try_from(index_bytes.len())
            .map_err(|_| Error::invalid("index length exceeds supported range"))?;
        let time_item_count = u64::try_from(self.pending_times.len())
            .map_err(|_| Error::invalid("time series length exceeds supported range"))?;
        let time_encoding = encode_time_section(
            self.pending_time_data.clone(),
            time_item_count,
            matches!(self.options.time_compression, TimeCompression::Zlib),
            self.options.compression_level,
        )?;

        let begin_time = self.pending_times[0];
        let end_time = *self
            .pending_times
            .last()
            .expect("pending times is non-empty");
        let mut payload = Vec::with_capacity(
            frame_encoding.payload.len()
                + chain_buffer.len()
                + index_bytes.len()
                + time_encoding.payload.len()
                + 96,
        );
        payload.extend_from_slice(&begin_time.to_be_bytes());
        payload.extend_from_slice(&end_time.to_be_bytes());
        payload.extend_from_slice(&required_memory.to_be_bytes());
        encode_varint(frame_encoding.uncompressed_len, &mut payload);
        encode_varint(frame_encoding.compressed_len, &mut payload);
        encode_varint(frame_max_handle, &mut payload);
        payload.extend_from_slice(&frame_encoding.payload);
        encode_varint(max_handle as u64, &mut payload);
        payload.push(pack_type.marker());
        payload.extend_from_slice(&chain_buffer);
        payload.extend_from_slice(&index_bytes);
        payload.extend_from_slice(&index_length.to_be_bytes());
        payload.extend_from_slice(&time_encoding.payload);
        payload.extend_from_slice(&time_encoding.uncompressed_len.to_be_bytes());
        payload.extend_from_slice(&time_encoding.compressed_len.to_be_bytes());
        payload.extend_from_slice(&time_encoding.item_count.to_be_bytes());

        let block_type = if has_dynamic_aliases {
            BlockType::VcDataDynAlias2
        } else {
            BlockType::VcData
        };
        Ok((block_type, payload))
    }

    fn write_header_block(&mut self, header: &Header) -> Result<()> {
        self.output.write_all(&[BlockType::Header as u8])?;
        self.output
            .write_all(&header.section_length.to_be_bytes())?;
        self.output.write_all(&header.start_time.to_be_bytes())?;
        self.output.write_all(&header.end_time.to_be_bytes())?;
        self.output.write_all(&std::f64::consts::E.to_ne_bytes())?;
        self.output.write_all(&header.memory_used.to_be_bytes())?;
        self.output.write_all(&header.scope_count.to_be_bytes())?;
        self.output.write_all(&header.var_count.to_be_bytes())?;
        self.output.write_all(&header.max_handle.to_be_bytes())?;
        self.output
            .write_all(&header.vc_section_count.to_be_bytes())?;
        self.output.write_all(&[header.timescale_exponent as u8])?;

        let mut version = [0u8; crate::block::VERSION_FIELD_LEN];
        let mut date = [0u8; crate::block::DATE_FIELD_LEN];
        write_cstring(&mut version, &header.version);
        write_cstring(&mut date, &header.date);
        self.output.write_all(&version)?;
        self.output.write_all(&date)?;

        self.output.write_all(&[header.file_type.into()])?;
        self.output
            .write_all(&(header.time_zero as u64).to_be_bytes())?;
        self.output.flush()?;
        Ok(())
    }

    fn write_geometry_block(&mut self, compress: bool) -> Result<()> {
        let geom = GeomInfo {
            max_handle: self.geometry.len() as u64,
            entries: self.geometry.clone(),
        };
        let encoded = geom.encode_block(compress)?;
        self.output.write_all(&[BlockType::Geometry as u8])?;
        self.output.with_writer(|writer| encoded.write_to(writer))?;
        Ok(())
    }

    fn write_hierarchy_block(&mut self) -> Result<()> {
        let block = HierarchyBlock {
            items: self.hierarchy_items.clone(),
            scopes: self.scopes.clone(),
            variables: self.variables.clone(),
            attributes: self.attributes.clone(),
        };
        #[cfg(feature = "gzip")]
        let compression = HierarchyCompression::Zlib {
            level: self.options.compression_level.unwrap_or(4).min(9),
        };
        #[cfg(all(not(feature = "gzip"), feature = "lz4"))]
        let compression = HierarchyCompression::Lz4;
        #[cfg(not(any(feature = "lz4", feature = "gzip")))]
        let compression = HierarchyCompression::Raw;
        let encoded = block.encode_block(compression)?;
        self.output.write_all(&[encoded.block_type as u8])?;
        self.output.with_writer(|writer| encoded.write_to(writer))?;
        Ok(())
    }

    fn chain_pack_type(&self) -> PackType {
        match self.options.chain_compression {
            // libfst has no raw marker. A 'Z' marker with stored_len=0 chains is the canonical
            // representation for uncompressed chains and needs no zlib decoder.
            ChainCompression::Raw => PackType::Zlib,
            ChainCompression::Zlib => PackType::Zlib,
            ChainCompression::Lz4 => PackType::Lz4,
            ChainCompression::FastLz => PackType::FastLz,
        }
    }
}

fn write_cstring(buf: &mut [u8], value: &str) {
    let bytes = value.as_bytes();
    let len = bytes.len().min(buf.len().saturating_sub(1));
    buf[..len].copy_from_slice(&bytes[..len]);
    buf[len] = 0;
}

fn validate_hierarchy_text(field: &str, value: &str) -> Result<()> {
    if value.as_bytes().contains(&0) {
        return Err(Error::invalid(format!(
            "{field} contains an embedded NUL byte"
        )));
    }
    Ok(())
}

fn validate_geometry(var_type: VarType, geometry: &GeomEntry) -> Result<()> {
    if matches!(geometry, GeomEntry::Fixed(0)) {
        return Err(Error::invalid(
            "fixed-width geometry must be greater than zero",
        ));
    }

    let real_type = matches!(
        var_type,
        VarType::VcdReal | VarType::VcdRealParameter | VarType::VcdRealtime | VarType::SvShortReal
    );
    if real_type != matches!(geometry, GeomEntry::Real) {
        return Err(Error::invalid(format!(
            "variable type {var_type:?} and geometry {geometry:?} disagree about real encoding"
        )));
    }
    if (var_type == VarType::GenString) != matches!(geometry, GeomEntry::Variable) {
        return Err(Error::invalid(format!(
            "variable type {var_type:?} and geometry {geometry:?} disagree about variable-length encoding"
        )));
    }
    Ok(())
}

/// Identifier returned when opening a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScopeId(pub usize);

/// Identifier returned when adding a hierarchy attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttributeId(pub usize);

const SPECIAL_BIT_CHARS: [u8; 8] = *b"xzhuwl-?";

#[derive(Debug, Clone, Copy)]
enum BitValue {
    Zero,
    One,
    Special { index: u8 },
}

impl BitValue {
    fn from_char(ch: char) -> Result<Self> {
        match ch {
            '0' => Ok(BitValue::Zero),
            '1' => Ok(BitValue::One),
            other => {
                let canonical = other.to_ascii_lowercase();
                let idx = SPECIAL_BIT_CHARS
                    .iter()
                    .position(|c| *c as char == canonical)
                    .ok_or_else(|| {
                        Error::unsupported(format!(
                            "bit state '{other}' is not supported for writing"
                        ))
                    })?;
                Ok(BitValue::Special { index: idx as u8 })
            }
        }
    }

    fn encode_marker(self, delta: usize) -> Result<u64> {
        let delta_u64 = u64::try_from(delta)
            .map_err(|_| Error::invalid("time delta exceeds addressable range"))?;
        let marker = match self {
            BitValue::Zero => delta_u64 << 2,
            BitValue::One => (delta_u64 << 2) | 0b10,
            BitValue::Special { index } => (delta_u64 << 4) | (1 | ((index as u64) << 1)),
        };
        Ok(marker)
    }

    fn to_char(self) -> char {
        match self {
            BitValue::Zero => '0',
            BitValue::One => '1',
            BitValue::Special { index } => SPECIAL_BIT_CHARS[index as usize] as char,
        }
    }
}

#[derive(Debug, Default)]
struct PendingChain {
    data: Vec<u8>,
    last_time_index: Option<usize>,
    latest_value: Option<OwnedValue>,
}

#[derive(Debug, Default)]
struct FrameState {
    entries: Vec<Option<FrameValue>>,
}

impl FrameState {
    fn register_handle(&mut self, handle: u32, geom: &GeomEntry) {
        let idx = handle as usize;
        if self.entries.len() < idx {
            self.entries.resize(idx, None);
        }
        match geom {
            GeomEntry::Fixed(1) => {
                // default uninitialised bit left as None; frame builder will emit 'x'.
            }
            GeomEntry::Fixed(len) => {
                let len_usize = *len as usize;
                if let Some(slot) = self.entries.get_mut(idx - 1) {
                    slot.get_or_insert_with(|| FrameValue::Vector(vec![b'x'; len_usize]));
                }
            }
            GeomEntry::Real => {
                if let Some(slot) = self.entries.get_mut(idx - 1) {
                    slot.get_or_insert_with(|| FrameValue::Real(f64::NAN));
                }
            }
            GeomEntry::Variable => {
                // Variable-length signals are not represented in the initial frame.
            }
        };
    }

    fn update(&mut self, handle: u32, value: &OwnedValue) -> Result<()> {
        let idx = handle as usize;
        if self.entries.len() < idx {
            self.entries.resize(idx, None);
        }
        let slot = &mut self.entries[idx - 1];
        match value {
            OwnedValue::Bit(bit) => {
                *slot = Some(FrameValue::Bit(*bit));
            }
            OwnedValue::Vector { data, .. } => {
                *slot = Some(FrameValue::Vector(data.clone()));
            }
            OwnedValue::Real(val) => {
                *slot = Some(FrameValue::Real(*val));
            }
            OwnedValue::VarLen(_) => {
                // Variable-length signals do not participate in the initial frame.
            }
        }
        Ok(())
    }

    fn build_frame_bytes(&self, geometry: &[GeomEntry], max_handle: u32) -> Result<Vec<u8>> {
        if max_handle == 0 {
            return Ok(Vec::new());
        }

        let mut buf = Vec::with_capacity(max_handle as usize);
        for idx in 0..max_handle as usize {
            let geom = geometry.get(idx).ok_or_else(|| {
                Error::invalid(format!("missing geometry entry for handle {}", idx + 1))
            })?;
            match geom {
                GeomEntry::Fixed(1) => {
                    let ch = self
                        .entries
                        .get(idx)
                        .and_then(|opt| opt.as_ref())
                        .map(FrameValue::as_bit_char)
                        .unwrap_or('x');
                    buf.push(ch as u8);
                }
                GeomEntry::Fixed(len) => {
                    let len_usize = *len as usize;
                    let slice = self
                        .entries
                        .get(idx)
                        .and_then(|opt| opt.as_ref())
                        .and_then(FrameValue::as_vector_bytes);
                    if let Some(data) = slice {
                        if data.len() == len_usize {
                            buf.extend_from_slice(data);
                        } else {
                            let fill_start = buf.len();
                            buf.resize(fill_start + len_usize, b'x');
                        }
                    } else {
                        let fill_start = buf.len();
                        buf.resize(fill_start + len_usize, b'x');
                    }
                }
                GeomEntry::Real => {
                    let value = self
                        .entries
                        .get(idx)
                        .and_then(|opt| opt.as_ref())
                        .and_then(FrameValue::as_real)
                        .unwrap_or(f64::NAN);
                    let bytes = if cfg!(target_endian = "little") {
                        value.to_le_bytes()
                    } else {
                        value.to_be_bytes()
                    };
                    buf.extend_from_slice(&bytes);
                }
                GeomEntry::Variable => {
                    // Variable-length signals have no fixed frame contribution.
                }
            }
        }
        Ok(buf)
    }
}

#[derive(Debug, Clone)]
enum FrameValue {
    Bit(BitValue),
    Vector(Vec<u8>),
    Real(f64),
}

impl FrameValue {
    fn as_bit_char(&self) -> char {
        match self {
            FrameValue::Bit(bit) => bit.to_char(),
            FrameValue::Vector(_) => 'x',
            FrameValue::Real(_) => 'x',
        }
    }

    fn as_vector_bytes(&self) -> Option<&[u8]> {
        match self {
            FrameValue::Vector(data) => Some(data.as_slice()),
            FrameValue::Bit(_) | FrameValue::Real(_) => None,
        }
    }

    fn as_real(&self) -> Option<f64> {
        match self {
            FrameValue::Real(value) => Some(*value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum OwnedValue {
    Bit(BitValue),
    Vector {
        width: u32,
        data: Vec<u8>,
        packed: Option<Vec<u8>>,
    },
    Real(f64),
    VarLen(Vec<u8>),
}

impl OwnedValue {
    fn memory_size(&self) -> usize {
        match self {
            Self::Bit(_) | Self::Real(_) => std::mem::size_of::<Self>(),
            Self::Vector { data, packed, .. } => std::mem::size_of::<Self>()
                .saturating_add(data.len())
                .saturating_add(packed.as_ref().map_or(0, Vec::len)),
            Self::VarLen(data) => std::mem::size_of::<Self>().saturating_add(data.len()),
        }
    }
}

fn encode_owned_value(value: &OwnedValue, delta: usize, output: &mut Vec<u8>) -> Result<()> {
    match value {
        OwnedValue::Bit(bit) => {
            encode_varint(bit.encode_marker(delta)?, output);
        }
        OwnedValue::Vector {
            width,
            packed,
            data,
        } => {
            let delta =
                u64::try_from(delta).map_err(|_| Error::invalid("time delta exceeds u64 range"))?;
            if let Some(bits) = packed {
                if bits.len() != packed_len(*width) {
                    return Err(Error::invalid("packed vector payload length mismatch"));
                }
                encode_varint(delta << 1, output);
                output.extend_from_slice(bits);
            } else {
                if data.len() != *width as usize {
                    return Err(Error::invalid(
                        "vector payload length mismatch with geometry",
                    ));
                }
                encode_varint((delta << 1) | 1, output);
                output.extend_from_slice(data);
            }
        }
        OwnedValue::Real(value) => {
            let delta =
                u64::try_from(delta).map_err(|_| Error::invalid("time delta exceeds u64 range"))?;
            encode_varint((delta << 1) | 1, output);
            let bytes = if cfg!(target_endian = "little") {
                value.to_le_bytes()
            } else {
                value.to_be_bytes()
            };
            output.extend_from_slice(&bytes);
        }
        OwnedValue::VarLen(bytes) => {
            let delta =
                u64::try_from(delta).map_err(|_| Error::invalid("time delta exceeds u64 range"))?;
            encode_varint(delta << 1, output);
            let len = u64::try_from(bytes.len())
                .map_err(|_| Error::invalid("variable-length payload exceeds u64 range"))?;
            encode_varint(len, output);
            output.extend_from_slice(bytes);
        }
    }
    Ok(())
}

fn pack_ascii_bits(data: &[u8], width: u32) -> Option<Vec<u8>> {
    let len = packed_len(width);

    #[cfg(feature = "simd")]
    {
        if let Some(result) = crate::simd::pack_ascii_bits(data, width, len) {
            return Some(result);
        }
    }

    pack_ascii_bits_scalar(data, len)
}

fn pack_ascii_bits_scalar(data: &[u8], len: usize) -> Option<Vec<u8>> {
    if data.iter().any(|b| *b != b'0' && *b != b'1') {
        return None;
    }
    let mut out = vec![0u8; len];
    for (idx, byte) in data.iter().enumerate() {
        if *byte == b'1' {
            let byte_index = idx / 8;
            let bit_index = 7 - (idx % 8);
            if let Some(slot) = out.get_mut(byte_index) {
                *slot |= 1 << bit_index;
            }
        }
    }
    Some(out)
}

fn normalize_packed_bits(width: u32, bits: &[u8]) -> Result<Vec<u8>> {
    let len = packed_len(width);
    if bits.len() < len {
        return Err(Error::invalid(
            "packed bit payload shorter than required length",
        ));
    }
    let mut out = bits[..len].to_vec();
    if !width.is_multiple_of(8) {
        let remainder = (width % 8) as u8;
        if let Some(last) = out.last_mut() {
            let mask = (!0u8) << (8 - remainder);
            *last &= mask;
        }
    }
    if bits.len() > len && bits[len..].iter().any(|&b| b != 0) {
        return Err(Error::invalid(
            "packed bit payload longer than required length",
        ));
    }
    Ok(out)
}

fn unpack_packed_bits(width: u32, bits: &[u8]) -> Result<Vec<u8>> {
    let len = packed_len(width);
    if bits.len() < len {
        return Err(Error::invalid(
            "packed bit payload shorter than required length",
        ));
    }
    let width_usize = usize::try_from(width)
        .map_err(|_| Error::invalid("vector width exceeds addressable range"))?;
    let mut out = Vec::with_capacity(width_usize);
    for idx in 0..width_usize {
        let byte = bits[idx / 8];
        let bit_index = 7 - (idx % 8);
        let bit = (byte >> bit_index) & 1;
        out.push(if bit == 1 { b'1' } else { b'0' });
    }
    Ok(out)
}

fn packed_len(width: u32) -> usize {
    let width_usize =
        usize::try_from(width).expect("vector width should fit into platform usize during packing");
    width_usize.div_ceil(8).max(1)
}
