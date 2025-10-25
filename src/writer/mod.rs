//! Incremental writer producing FST output streams.

use crate::block::{
    ChainIndexEntry, GeomEntry, GeomInfo, Header, HierarchyBlock, HierarchyCompression,
    HierarchyItem, ScopeEntry, VarEntry, encode_chain_index, encode_chain_payload,
    encode_frame_section, encode_time_section,
};
use crate::encoding::encode_varint;
use crate::error::{Error, Result};
use crate::io::{WriteSeek, WriterBackend};
use crate::types::{BlockType, PackType, ScopeType, SignalValue, VarDir, VarType};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Cursor, Write};

#[cfg(feature = "gzip")]
use flate2::{Compression, write::GzEncoder};

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

    /// Builds the writer, validating options before returning the instance.
    pub fn build(self) -> Result<FstWriter<W>> {
        FstWriter::with_backend(self.sink, self.options)
    }
}

fn validate_options(options: &WriterOptions) -> Result<()> {
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

    Ok(())
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

    fn into_inner(self, options: &WriterOptions) -> Result<W> {
        match self {
            OutputBackend::Direct(backend) => Ok(backend.into_inner()?),
            OutputBackend::Wrapped { mut buffer, sink } => {
                buffer.get_mut().flush()?;
                let cursor = buffer.into_inner()?;
                let inner = cursor.into_inner();
                #[cfg(not(feature = "gzip"))]
                {
                    let _ = (inner, options);
                    return Err(Error::unsupported(
                        "file-level zlib wrapper requires the `gzip` feature",
                    ));
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
                    let payload_len = u64::try_from(compressed.len()).map_err(|_| {
                        Error::invalid("z-wrapper payload exceeds supported length")
                    })?;
                    let uncompressed_len = u64::try_from(inner.len()).map_err(|_| {
                        Error::invalid("z-wrapper uncompressed payload exceeds supported length")
                    })?;

                    let mut payload = Vec::with_capacity(compressed.len() + 16);
                    payload.extend_from_slice(&uncompressed_len.to_be_bytes());
                    payload.extend_from_slice(&payload_len.to_be_bytes());
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
    scope_stack: Vec<usize>,
    geometry: Vec<GeomEntry>,
    alias_of: Vec<Option<u32>>,
    alias_children: Vec<Vec<u32>>,
    next_handle: u32,
    header: Option<Header>,
    pending_changes: Vec<PendingChange>,
    vc_blocks_written: u64,
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
            scope_stack: Vec::new(),
            geometry: Vec::new(),
            alias_of: Vec::new(),
            alias_children: Vec::new(),
            next_handle: 1,
            header: None,
            pending_changes: Vec::new(),
            vc_blocks_written: 0,
        })
    }

    /// Starts building a writer for the given sink.
    pub fn builder(sink: W) -> WriterBuilder<W> {
        WriterBuilder::new(sink)
    }

    /// Writes the FST header block. This implementation currently emits a minimal header and is
    /// intended as a starting point for further development.
    pub fn write_header(&mut self, mut header: Header) -> Result<()> {
        if self.header_written {
            return Err(Error::unsupported("header already written"));
        }
        if !self.scope_stack.is_empty() {
            return Err(Error::invalid(
                "cannot write header while scopes remain open; call `end_scope` first",
            ));
        }

        header.scope_count = self.scopes.len() as u64;
        header.var_count = self.variables.len() as u64;
        header.max_handle = self.next_handle.saturating_sub(1) as u64;
        header.timescale_exponent = self.options.timescale_exponent;
        header.section_length = 329;

        self.write_header_block(&header)?;
        self.write_geometry_block(false)?;
        self.write_hierarchy_block()?;

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
        let parent = self.scope_stack.last().copied();
        let scope = ScopeEntry {
            scope_type,
            name: name.into(),
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

    /// Declares a variable within the currently active scope. Returns the newly allocated handle.
    pub fn add_variable(
        &mut self,
        var_type: VarType,
        direction: VarDir,
        name: impl Into<String>,
        geometry: GeomEntry,
    ) -> Result<u32> {
        self.ensure_metadata_mutable()?;
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
            GeomEntry::Real | GeomEntry::Variable => None,
        };

        self.geometry.push(geometry);
        self.alias_of.push(None);
        self.alias_children.push(Vec::new());
        self.variables.push(VarEntry {
            var_type,
            direction,
            name: name.into(),
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

    /// Declares an alias that reuses the value stream of an existing handle.
    pub fn add_alias(
        &mut self,
        var_type: VarType,
        direction: VarDir,
        name: impl Into<String>,
        target_handle: u32,
    ) -> Result<u32> {
        self.ensure_metadata_mutable()?;
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

        let canonical = self.resolve_canonical_handle(target_handle)?;
        let target_index = (canonical - 1) as usize;
        let geometry = self.geometry.get(target_index).cloned().ok_or_else(|| {
            Error::invalid(format!(
                "no geometry recorded for canonical handle {canonical}"
            ))
        })?;

        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or_else(|| Error::invalid("handle counter overflow"))?;

        self.geometry.push(geometry.clone());
        self.alias_of.push(Some(canonical));
        self.alias_children.push(Vec::new());

        let canonical_index = (canonical - 1) as usize;
        if let Some(children) = self.alias_children.get_mut(canonical_index) {
            children.push(handle);
        }

        self.variables.push(VarEntry {
            var_type,
            direction,
            name: name.into(),
            length: match geometry {
                GeomEntry::Fixed(bytes) => Some(bytes),
                GeomEntry::Real | GeomEntry::Variable => None,
            },
            handle,
            alias_of: Some(canonical),
            scope: Some(scope),
            is_alias: true,
        });
        let var_index = self.variables.len() - 1;
        self.hierarchy_items.push(HierarchyItem::Var { var_index });

        self.frame_state.register_handle(handle, &geometry);
        self.frame_state.clone_from(canonical, handle)?;

        Ok(handle)
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

        let canonical = self.resolve_canonical_handle(handle)?;
        let geom_index = (canonical - 1) as usize;
        let geom_entry = self.geometry.get(geom_index).ok_or_else(|| {
            Error::invalid(format!(
                "no geometry recorded for canonical handle {canonical}"
            ))
        })?;
        let owned_value = Self::convert_value(value, geom_entry)?;

        self.pending_changes.push(PendingChange {
            timestamp,
            handle: canonical,
            value: owned_value.clone(),
        });
        self.frame_state.update(canonical, &owned_value)?;
        if let Some(children) = self.alias_children.get((canonical - 1) as usize) {
            for &alias in children {
                self.frame_state.update(alias, &owned_value)?;
            }
        }

        Ok(())
    }

    /// Flushes any buffered data to the sink.
    pub fn flush(&mut self) -> Result<()> {
        self.flush_value_changes()?;
        self.output.flush()?;
        Ok(())
    }

    /// Consumes the writer, returning the underlying sink once buffered data has been flushed.
    pub fn finish(mut self) -> Result<W> {
        self.flush()?;
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

    fn resolve_canonical_handle(&self, mut handle: u32) -> Result<u32> {
        if handle == 0 || handle >= self.next_handle {
            return Err(Error::invalid(format!(
                "handle {handle} is out of range (max {})",
                self.next_handle - 1
            )));
        }
        let limit = self.next_handle as usize;
        for _ in 0..limit {
            let alias = self
                .alias_of
                .get((handle - 1) as usize)
                .and_then(|entry| *entry);
            match alias {
                Some(target) if target != handle => {
                    handle = target;
                }
                Some(_) => {
                    return Err(Error::invalid("alias handle cannot refer to itself"));
                }
                None => return Ok(handle),
            }
        }
        Err(Error::invalid("alias resolution cycle detected"))
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
                    let byte = bits
                        .as_ref()
                        .first()
                        .copied()
                        .ok_or_else(|| Error::invalid("packed bit payload is empty"))?;
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
        if self.pending_changes.is_empty() {
            return Ok(());
        }
        let changes = std::mem::take(&mut self.pending_changes);
        let payload = self.build_vc_block(changes)?;
        let section_length = (payload.len() as u64)
            .checked_add(8)
            .ok_or_else(|| Error::invalid("value-change block length overflow"))?;
        self.output.write_all(&[BlockType::VcData as u8])?;
        self.output.write_all(&section_length.to_be_bytes())?;
        self.output.write_all(&payload)?;
        self.vc_blocks_written = self
            .vc_blocks_written
            .checked_add(1)
            .ok_or_else(|| Error::invalid("vc section counter overflow"))?;
        Ok(())
    }

    fn build_vc_block(&mut self, mut changes: Vec<PendingChange>) -> Result<Vec<u8>> {
        if changes.is_empty() {
            return Err(Error::invalid(
                "attempted to build a value-change block with no pending changes",
            ));
        }

        changes.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.handle.cmp(&b.handle)));

        let max_handle = self.next_handle.saturating_sub(1);
        if max_handle == 0 {
            return Err(Error::invalid(
                "no handles defined; unable to encode value-change block",
            ));
        }

        let mut time_points: Vec<u64> = Vec::new();
        for change in &changes {
            if time_points.last().copied() != Some(change.timestamp) {
                time_points.push(change.timestamp);
            }
        }
        if time_points.is_empty() {
            return Err(Error::invalid(
                "value-change block requires at least one timestamp",
            ));
        }

        let mut time_index = HashMap::with_capacity(time_points.len());
        for (idx, ts) in time_points.iter().enumerate() {
            time_index.insert(*ts, idx);
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

        let mut required_memory = frame_encoding.uncompressed_len;

        let mut per_handle: Vec<Vec<(usize, OwnedValue)>> = vec![Vec::new(); max_handle as usize];
        for change in &changes {
            let idx = *time_index
                .get(&change.timestamp)
                .ok_or_else(|| Error::invalid("internal time index map inconsistency"))?;
            per_handle[(change.handle - 1) as usize].push((idx, change.value.clone()));
        }

        let pack_type = self.chain_pack_type();

        let mut chain_buffer = Vec::new();
        let mut chain_offsets: Vec<Option<u64>> = vec![None; max_handle as usize];

        for (handle_idx, events) in per_handle.iter().enumerate() {
            if events.is_empty() {
                continue;
            }

            let mut chain_bytes = Vec::with_capacity(events.len() * 2);
            let mut previous_index: Option<usize> = None;
            for (time_idx_ref, value) in events.iter() {
                let time_idx = *time_idx_ref;
                let delta = match previous_index {
                    Some(prev) => time_idx
                        .checked_sub(prev)
                        .ok_or_else(|| Error::invalid("time indices must be non-decreasing"))?,
                    None => time_idx,
                };
                previous_index = Some(time_idx);
                match value {
                    OwnedValue::Bit(bit) => {
                        let marker = bit.encode_marker(delta)?;
                        encode_varint(marker, &mut chain_bytes);
                    }
                    OwnedValue::Vector {
                        width,
                        packed,
                        data,
                    } => {
                        let delta_u64 = u64::try_from(delta)
                            .map_err(|_| Error::invalid("time delta exceeds u64 range"))?;
                        let width = *width;
                        if let Some(bits) = packed {
                            let expected = packed_len(width);
                            if bits.len() != expected {
                                return Err(Error::invalid(
                                    "packed vector payload length mismatch",
                                ));
                            }
                            let marker = delta_u64 << 1;
                            encode_varint(marker, &mut chain_bytes);
                            chain_bytes.extend_from_slice(bits);
                        } else {
                            if data.len() != width as usize {
                                return Err(Error::invalid(
                                    "vector payload length mismatch with geometry",
                                ));
                            }
                            let marker = (delta_u64 << 1) | 1;
                            encode_varint(marker, &mut chain_bytes);
                            chain_bytes.extend_from_slice(data);
                        }
                    }
                    OwnedValue::Real(value) => {
                        let delta_u64 = u64::try_from(delta)
                            .map_err(|_| Error::invalid("time delta exceeds u64 range"))?;
                        let marker = (delta_u64 << 1) | 1;
                        encode_varint(marker, &mut chain_bytes);
                        let bytes = if cfg!(target_endian = "little") {
                            value.to_le_bytes()
                        } else {
                            value.to_be_bytes()
                        };
                        chain_bytes.extend_from_slice(&bytes);
                    }
                    OwnedValue::VarLen(bytes) => {
                        let delta_u64 = u64::try_from(delta)
                            .map_err(|_| Error::invalid("time delta exceeds u64 range"))?;
                        let marker = delta_u64 << 1;
                        encode_varint(marker, &mut chain_bytes);
                        let len_u64 = u64::try_from(bytes.len()).map_err(|_| {
                            Error::invalid("variable-length payload exceeds u64 range")
                        })?;
                        encode_varint(len_u64, &mut chain_bytes);
                        chain_bytes.extend_from_slice(bytes);
                    }
                }
            }

            let raw_len = u64::try_from(chain_bytes.len())
                .map_err(|_| Error::invalid("chain payload exceeds supported length"))?;
            required_memory = required_memory
                .checked_add(raw_len)
                .ok_or_else(|| Error::invalid("chain memory requirement overflow"))?;

            let (stored_len, payload_bytes) =
                encode_chain_payload(pack_type, chain_bytes, self.options.compression_level)?;

            let offset = chain_buffer.len() as u64;
            encode_varint(stored_len, &mut chain_buffer);
            chain_buffer.extend_from_slice(&payload_bytes);
            chain_offsets[handle_idx] = Some(offset);
        }

        let mut index_entries = Vec::with_capacity(chain_offsets.len());
        for (handle_idx, offset) in chain_offsets.iter().enumerate() {
            if let Some(Some(target)) = self.alias_of.get(handle_idx) {
                index_entries.push(ChainIndexEntry::Alias { target: *target });
            } else if let Some(offset) = offset {
                index_entries.push(ChainIndexEntry::Data { offset: *offset });
            } else {
                index_entries.push(ChainIndexEntry::Empty);
            }
        }

        let index_bytes = encode_chain_index(&index_entries)?;

        let index_length = u64::try_from(index_bytes.len())
            .map_err(|_| Error::invalid("index length exceeds supported range"))?;

        let mut time_data = Vec::with_capacity(time_points.len() * 2);
        let mut prev_time = 0u64;
        for (idx, ts) in time_points.iter().enumerate() {
            let delta = if idx == 0 {
                *ts
            } else {
                ts.checked_sub(prev_time)
                    .ok_or_else(|| Error::invalid("timestamps must be non-decreasing"))?
            };
            prev_time = *ts;
            encode_varint(delta, &mut time_data);
        }

        let time_item_count = u64::try_from(time_points.len())
            .map_err(|_| Error::invalid("time series length exceeds supported range"))?;

        let time_encoding = encode_time_section(
            time_data,
            time_item_count,
            matches!(self.options.time_compression, TimeCompression::Zlib),
            self.options.compression_level,
        )?;

        let begin_time = *time_points.first().unwrap();
        let end_time = *time_points.last().unwrap();

        let mut payload = Vec::new();
        payload.extend_from_slice(&begin_time.to_be_bytes());
        payload.extend_from_slice(&end_time.to_be_bytes());
        payload.extend_from_slice(&required_memory.to_be_bytes());
        encode_varint(frame_encoding.uncompressed_len, &mut payload);
        encode_varint(frame_encoding.compressed_len, &mut payload);
        encode_varint(frame_max_handle, &mut payload);
        payload.extend_from_slice(&frame_encoding.payload);
        encode_varint(max_handle as u64, &mut payload); // vc_max_handle
        payload.push(pack_type.marker());

        payload.extend_from_slice(&chain_buffer);
        payload.extend_from_slice(&index_bytes);
        payload.extend_from_slice(&index_length.to_be_bytes());
        payload.extend_from_slice(&time_encoding.payload);
        payload.extend_from_slice(&time_encoding.uncompressed_len.to_be_bytes());
        payload.extend_from_slice(&time_encoding.compressed_len.to_be_bytes());
        payload.extend_from_slice(&time_encoding.item_count.to_be_bytes());

        Ok(payload)
    }

    fn write_header_block(&mut self, header: &Header) -> Result<()> {
        self.output.write_all(&[BlockType::Header as u8])?;
        self.output
            .write_all(&header.section_length.to_be_bytes())?;
        self.output.write_all(&header.start_time.to_be_bytes())?;
        self.output.write_all(&header.end_time.to_be_bytes())?;
        self.output
            .write_all(&std::f64::consts::E.to_bits().to_be_bytes())?;
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

        self.output.write_all(&[header.file_type])?;
        self.output.write_all(&header.time_zero.to_be_bytes())?;
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
            attributes: Vec::new(),
        };
        let encoded = block.encode_block(HierarchyCompression::Raw)?;
        self.output.write_all(&[encoded.block_type as u8])?;
        self.output.with_writer(|writer| encoded.write_to(writer))?;
        Ok(())
    }

    fn chain_pack_type(&self) -> PackType {
        match self.options.chain_compression {
            ChainCompression::Raw => PackType::None,
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

/// Identifier returned when opening a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScopeId(pub usize);

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

#[derive(Debug, Clone)]
struct PendingChange {
    timestamp: u64,
    handle: u32,
    value: OwnedValue,
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

    fn clone_from(&mut self, source: u32, target: u32) -> Result<()> {
        if source == 0 || target == 0 {
            return Err(Error::invalid("frame handles must be non-zero"));
        }
        let src_idx = (source - 1) as usize;
        let dst_idx = (target - 1) as usize;
        if self.entries.len() <= dst_idx {
            self.entries.resize(dst_idx + 1, None);
        }
        let value = self.entries.get(src_idx).cloned().unwrap_or(None);
        self.entries[dst_idx] = value;
        Ok(())
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
