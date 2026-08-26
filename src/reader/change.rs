use std::borrow::Cow;
use std::collections::VecDeque;
use std::str;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use crate::block::{GeomEntry, GeomInfo};
use crate::encoding::decode_varint_with_len;
use crate::error::{Error, Result};
use crate::reader::vc::{ChainIndex, ChainPayload, VcBlockMeta};
use crate::types::{FstByteOrder, SignalValue};

const FST_RCV_STR: [char; 8] = ['x', 'z', 'h', 'u', 'w', 'l', '-', '?'];
const NO_CURSOR: usize = usize::MAX;

enum AliasMap {
    Dense(Vec<Vec<u32>>),
    Sparse(Vec<(u32, Vec<u32>)>),
}

impl AliasMap {
    fn build(index: &ChainIndex, included: Option<&[u32]>) -> Self {
        const MAX_SPARSE_GROUPS: usize = 1_024;
        let mut sparse = Vec::<(u32, Vec<u32>)>::new();
        let mut dense: Option<Vec<Vec<u32>>> = None;
        for (alias, slot) in index.iter() {
            let Some(canonical) = slot.alias_of() else {
                continue;
            };
            if included.is_some_and(|handles| handles.binary_search(&alias).is_err()) {
                continue;
            }
            if let Some(groups) = dense.as_mut() {
                groups[canonical as usize].push(alias);
                continue;
            }
            match sparse.binary_search_by_key(&canonical, |(handle, _)| *handle) {
                Ok(position) => sparse[position].1.push(alias),
                Err(position) if sparse.len() < MAX_SPARSE_GROUPS => {
                    sparse.insert(position, (canonical, vec![alias]));
                }
                Err(_) => {
                    let mut groups = Vec::new();
                    groups.resize_with(index.max_handle() as usize + 1, Vec::new);
                    for (handle, aliases) in sparse.drain(..) {
                        groups[handle as usize] = aliases;
                    }
                    groups[canonical as usize].push(alias);
                    dense = Some(groups);
                }
            }
        }
        dense.map_or(Self::Sparse(sparse), Self::Dense)
    }

    #[inline]
    fn get(&self, handle: u32) -> &[u32] {
        match self {
            Self::Dense(aliases) => aliases.get(handle as usize).map_or(&[], Vec::as_slice),
            Self::Sparse(aliases) => aliases
                .binary_search_by_key(&handle, |(canonical, _)| *canonical)
                .ok()
                .map_or(&[], |index| aliases[index].1.as_slice()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SignalKind {
    Bit,
    Vector { width: u32 },
    VarLen,
    Real,
}

impl SignalKind {
    fn from_geom(entry: &GeomEntry, handle: u32) -> Result<Self> {
        match entry {
            GeomEntry::Fixed(width) => {
                if *width == 0 {
                    return Err(Error::invalid(format!(
                        "handle {handle} has zero-width fixed geometry"
                    )));
                }
                if *width == 1 {
                    Ok(SignalKind::Bit)
                } else {
                    Ok(SignalKind::Vector { width: *width })
                }
            }
            GeomEntry::Real => Ok(SignalKind::Real),
            GeomEntry::Variable => Ok(SignalKind::VarLen),
        }
    }
}

#[derive(Debug)]
struct ChainCursor<'a> {
    handle: u32,
    emit_canonical: bool,
    kind: SignalKind,
    data: &'a [u8],
    offset: usize,
    current_time_index: usize,
    next_marker: Option<(u64, usize, usize)>,
    double_byte_order: FstByteOrder,
}

impl<'a> ChainCursor<'a> {
    fn new(
        handle: u32,
        emit_canonical: bool,
        kind: SignalKind,
        data: &'a [u8],
        double_byte_order: FstByteOrder,
    ) -> Self {
        Self {
            handle,
            emit_canonical,
            kind,
            data,
            offset: 0,
            current_time_index: 0,
            next_marker: None,
            double_byte_order,
        }
    }

    #[inline]
    fn peek_delta(&mut self) -> Result<Option<usize>> {
        if let Some((_, _, delta)) = self.next_marker {
            return Ok(Some(delta));
        }
        if self.offset >= self.data.len() {
            return Ok(None);
        }
        let slice = &self.data[self.offset..];
        let (marker, consumed) = decode_varint_with_len(slice)?;
        let delta = self.compute_delta(marker);
        self.next_marker = Some((marker, consumed, delta));
        Ok(Some(delta))
    }

    #[inline]
    fn read_value(&mut self, expected_time_index: usize) -> Result<Option<SignalValue<'a>>> {
        if self.offset >= self.data.len() {
            return Ok(None);
        }

        let (marker, consumed, delta) = match self.next_marker.take() {
            Some(cached) => cached,
            None => {
                let slice = &self.data[self.offset..];
                let (marker, consumed) = decode_varint_with_len(slice)?;
                (marker, consumed, self.compute_delta(marker))
            }
        };
        self.offset += consumed;

        self.current_time_index = self
            .current_time_index
            .checked_add(delta)
            .ok_or_else(|| Error::decode("chain time index overflow"))?;

        if self.current_time_index != expected_time_index {
            return Err(Error::decode("chain scheduling mismatch"));
        }

        match self.kind {
            SignalKind::Bit => {
                let ch = if (marker & 1) == 0 {
                    let bit = ((marker >> 1) & 1) as u8;
                    (b'0' + bit) as char
                } else {
                    let idx = ((marker >> 1) & 7) as usize;
                    FST_RCV_STR
                        .get(idx)
                        .copied()
                        .ok_or_else(|| Error::decode("invalid packed bit marker"))?
                };
                Ok(Some(SignalValue::Bit(ch)))
            }
            SignalKind::VarLen => {
                let slice = &self.data[self.offset..];
                let (len, consumed_len) = decode_varint_with_len(slice)?;
                let len_usize = usize::try_from(len)
                    .map_err(|_| Error::decode("variable-length payload exceeds usize"))?;
                self.offset += consumed_len;
                let end = self
                    .offset
                    .checked_add(len_usize)
                    .ok_or_else(|| Error::decode("variable-length payload overflow"))?;
                if end > self.data.len() {
                    return Err(Error::decode(
                        "variable-length payload exceeds chain bounds",
                    ));
                }
                let bytes = &self.data[self.offset..end];
                self.offset = end;
                Ok(Some(SignalValue::Bytes(Cow::Borrowed(bytes))))
            }
            SignalKind::Vector { width } => {
                let width_usize = width as usize;
                if width_usize == 0 {
                    return Err(Error::decode("vector width may not be zero"));
                }

                if (marker & 1) == 0 {
                    let packed_len = width_usize.div_ceil(8).max(1);
                    let end = self
                        .offset
                        .checked_add(packed_len)
                        .ok_or_else(|| Error::decode("packed vector payload overflow"))?;
                    if end > self.data.len() {
                        return Err(Error::decode("packed vector payload exceeds chain bounds"));
                    }
                    let bits = &self.data[self.offset..end];
                    self.offset = end;
                    Ok(Some(SignalValue::PackedBits {
                        width,
                        bits: Cow::Borrowed(bits),
                    }))
                } else {
                    let end = self
                        .offset
                        .checked_add(width_usize)
                        .ok_or_else(|| Error::decode("vector payload overflow"))?;
                    if end > self.data.len() {
                        return Err(Error::decode("vector payload exceeds chain bounds"));
                    }
                    let bytes = &self.data[self.offset..end];
                    self.offset = end;
                    match str::from_utf8(bytes) {
                        Ok(text) => Ok(Some(SignalValue::Vector(Cow::Borrowed(text)))),
                        Err(_) => Ok(Some(SignalValue::Bytes(Cow::Borrowed(bytes)))),
                    }
                }
            }
            SignalKind::Real => {
                if (marker & 1) == 0 {
                    let end = self
                        .offset
                        .checked_add(1)
                        .ok_or_else(|| Error::decode("packed real payload overflow"))?;
                    if end > self.data.len() {
                        return Err(Error::decode("packed real payload exceeds chain bounds"));
                    }
                    let bits = &self.data[self.offset..end];
                    self.offset = end;
                    Ok(Some(SignalValue::PackedBits {
                        width: 8,
                        bits: Cow::Borrowed(bits),
                    }))
                } else {
                    let end = self
                        .offset
                        .checked_add(8)
                        .ok_or_else(|| Error::decode("real payload overflow"))?;
                    if end > self.data.len() {
                        return Err(Error::decode("real payload exceeds chain bounds"));
                    }
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&self.data[self.offset..end]);
                    self.offset = end;
                    let value = self.double_byte_order.decode_f64(buf);
                    Ok(Some(SignalValue::Real(value)))
                }
            }
        }
    }

    #[inline]
    fn read_binary(&mut self, expected_time_index: usize) -> Result<Option<bool>> {
        if self.offset >= self.data.len() {
            return Ok(None);
        }
        let (marker, consumed, delta) = match self.next_marker.take() {
            Some(cached) => cached,
            None => {
                let (marker, consumed) = decode_varint_with_len(&self.data[self.offset..])?;
                (marker, consumed, self.compute_delta(marker))
            }
        };
        self.offset += consumed;
        self.current_time_index = self
            .current_time_index
            .checked_add(delta)
            .ok_or_else(|| Error::decode("chain time index overflow"))?;
        if self.current_time_index != expected_time_index {
            return Err(Error::decode("chain scheduling mismatch"));
        }
        if marker & 1 != 0 {
            return Err(Error::decode(
                "binary fold encountered an extended-state bit value",
            ));
        }
        Ok(Some((marker >> 1) & 1 != 0))
    }

    #[inline]
    fn compute_delta(&self, marker: u64) -> usize {
        match self.kind {
            SignalKind::Bit => {
                let flag = (marker & 1) as usize;
                let shift = 2usize << flag;
                (marker >> shift) as usize
            }
            SignalKind::Vector { .. } | SignalKind::VarLen | SignalKind::Real => {
                (marker >> 1) as usize
            }
        }
    }

    fn skip_before(&mut self, first_time_index: usize) -> Result<()> {
        while let Some(delta) = self.peek_delta()? {
            let next_time_index = self
                .current_time_index
                .checked_add(delta)
                .ok_or_else(|| Error::decode("chain time index overflow"))?;
            if next_time_index >= first_time_index {
                break;
            }
            self.read_value(next_time_index)?
                .ok_or_else(|| Error::decode("scheduled chain ended unexpectedly"))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// One decoded signal transition from a value-change block.
pub struct ValueChange<'a> {
    /// Absolute simulation timestamp.
    pub timestamp: u64,
    /// One-based signal handle.
    pub handle: u32,
    /// Canonical handle when this event was expanded from a dynamic alias.
    pub alias_of: Option<u32>,
    /// Borrowed decoded signal value.
    pub value: SignalValue<'a>,
}

/// Validating iterator and high-throughput traversal API for one value-change block.
pub struct VcBlockChanges<'a> {
    block: &'a VcBlockMeta,
    cursors: Vec<ChainCursor<'a>>,
    schedule_heads: Vec<usize>,
    schedule_next: Vec<usize>,
    pending_aliases: VecDeque<ValueChange<'a>>,
    alias_map: AliasMap,
    time_index: usize,
    end_time_index: usize,
}

impl<'a> VcBlockChanges<'a> {
    /// Builds a traversal from decoded block metadata, geometry, and alias information.
    pub fn new(
        block: &'a VcBlockMeta,
        geom: &'a GeomInfo,
        alias_index: &'a ChainIndex,
    ) -> Result<Self> {
        let time_len = block.time_table.timestamps.len();
        let (start_time_index, end_time_index) = match block.time_range.as_ref() {
            None => (0, time_len),
            Some(range) if range.start() > range.end() => (0, 0),
            Some(range) => (
                block
                    .time_table
                    .timestamps
                    .partition_point(|timestamp| timestamp < range.start()),
                block
                    .time_table
                    .timestamps
                    .partition_point(|timestamp| timestamp <= range.end()),
            ),
        };
        let included_handles = block.included_handles.as_deref();
        let mut cursors = Vec::new();
        for chain in &block.chains {
            let handle = chain.handle;
            let emit_canonical =
                included_handles.is_none_or(|handles| handles.binary_search(&handle).is_ok());
            let geom_entry = geom.entry(handle).ok_or_else(|| {
                Error::invalid(format!("missing geometry entry for handle {handle}"))
            })?;
            let kind = SignalKind::from_geom(geom_entry, handle)?;
            let data = match &chain.payload {
                ChainPayload::Borrowed { range } => &block.chain_buffer[range.clone()],
                ChainPayload::Decoded { range } => &block.decoded_chain_buffer[range.clone()],
            };
            let mut cursor =
                ChainCursor::new(handle, emit_canonical, kind, data, block.double_byte_order);
            if start_time_index != 0 {
                cursor.skip_before(start_time_index)?;
            }
            cursors.push(cursor);
        }

        let mut schedule_heads = vec![NO_CURSOR; end_time_index];
        let mut schedule_next = vec![NO_CURSOR; cursors.len()];

        for (idx, cursor) in cursors.iter_mut().enumerate() {
            if let Some(delta) = cursor.peek_delta()? {
                let next_time_index = cursor
                    .current_time_index
                    .checked_add(delta)
                    .ok_or_else(|| Error::decode("chain time index overflow"))?;
                if next_time_index >= time_len {
                    return Err(Error::decode("initial chain delta exceeds time table"));
                }
                if next_time_index < end_time_index {
                    schedule_next[idx] = schedule_heads[next_time_index];
                    schedule_heads[next_time_index] = idx;
                }
            }
        }

        let alias_map = AliasMap::build(alias_index, included_handles);

        Ok(Self {
            block,
            cursors,
            schedule_heads,
            schedule_next,
            pending_aliases: VecDeque::new(),
            alias_map,
            time_index: start_time_index,
            end_time_index,
        })
    }

    /// Visits every change without the extra `Option<Result<_>>` layer of [`Iterator::next`].
    /// This callback-oriented path is intended for high-throughput waveform processing.
    #[inline]
    pub fn try_for_each<F>(mut self, mut visitor: F) -> Result<()>
    where
        F: FnMut(ValueChange<'a>),
    {
        while self.time_index < self.schedule_heads.len() {
            let cursor_idx = self.schedule_heads[self.time_index];
            if cursor_idx == NO_CURSOR {
                self.time_index += 1;
                continue;
            }
            self.schedule_heads[self.time_index] = self.schedule_next[cursor_idx];
            let timestamp = self.block.time_table.timestamps[self.time_index];
            let cursor = &mut self.cursors[cursor_idx];
            let Some(value) = cursor.read_value(self.time_index)? else {
                continue;
            };
            if let Some(next_delta) = cursor.peek_delta()? {
                let next_time = self
                    .time_index
                    .checked_add(next_delta)
                    .ok_or_else(|| Error::decode("chain delta overflow"))?;
                if next_time >= self.block.time_table.timestamps.len() {
                    return Err(Error::decode("chain delta exceeds time table"));
                }
                if next_time < self.schedule_heads.len() {
                    self.schedule_next[cursor_idx] = self.schedule_heads[next_time];
                    self.schedule_heads[next_time] = cursor_idx;
                }
            }

            let handle = cursor.handle;
            let aliases = self.alias_map.get(handle);
            if cursor.emit_canonical && aliases.is_empty() {
                visitor(ValueChange {
                    timestamp,
                    handle,
                    alias_of: None,
                    value,
                });
                continue;
            }

            if cursor.emit_canonical {
                visitor(ValueChange {
                    timestamp,
                    handle,
                    alias_of: None,
                    value: value.clone(),
                });
            }
            for &alias in aliases {
                visitor(ValueChange {
                    timestamp,
                    handle: alias,
                    alias_of: Some(handle),
                    value: value.clone(),
                });
            }
        }
        Ok(())
    }

    /// Visits timestamp, handle, dynamic-alias target, and value as separate arguments.
    /// This is the lowest-overhead validated scan path for callers that do not need a
    /// [`ValueChange`] aggregate.
    #[inline]
    pub fn try_for_each_parts<F>(self, mut visitor: F) -> Result<()>
    where
        F: FnMut(u64, u32, Option<u32>, SignalValue<'a>),
    {
        self.try_fold_parts((), |(), timestamp, handle, alias_of, value| {
            visitor(timestamp, handle, alias_of, value);
        })
    }

    /// Strictly timestamp-ordered fold over the validated change stream.
    /// Passing the accumulator by value lets scalar reductions remain in registers.
    #[inline]
    pub fn try_fold_parts<T, F>(mut self, mut accumulator: T, mut fold: F) -> Result<T>
    where
        F: FnMut(T, u64, u32, Option<u32>, SignalValue<'a>) -> T,
    {
        while self.time_index < self.schedule_heads.len() {
            let mut cursor_idx =
                std::mem::replace(&mut self.schedule_heads[self.time_index], NO_CURSOR);
            if cursor_idx == NO_CURSOR {
                self.time_index += 1;
                continue;
            }
            let timestamp = self.block.time_table.timestamps[self.time_index];
            while cursor_idx != NO_CURSOR {
                let next_cursor = self.schedule_next[cursor_idx];
                let cursor = &mut self.cursors[cursor_idx];
                let value = cursor
                    .read_value(self.time_index)?
                    .ok_or_else(|| Error::decode("scheduled chain ended unexpectedly"))?;
                let next_delta = cursor.peek_delta()?;
                if let Some(next_delta) = next_delta {
                    let next_time = self
                        .time_index
                        .checked_add(next_delta)
                        .ok_or_else(|| Error::decode("chain delta overflow"))?;
                    if next_time >= self.block.time_table.timestamps.len() {
                        return Err(Error::decode("chain delta exceeds time table"));
                    }
                    if next_time < self.schedule_heads.len() {
                        if next_time == self.time_index {
                            self.schedule_next[cursor_idx] = next_cursor;
                        } else {
                            self.schedule_next[cursor_idx] = self.schedule_heads[next_time];
                            self.schedule_heads[next_time] = cursor_idx;
                        }
                    }
                }

                let handle = cursor.handle;
                let aliases = self.alias_map.get(handle);
                if cursor.emit_canonical && aliases.is_empty() {
                    accumulator = fold(accumulator, timestamp, handle, None, value);
                } else {
                    if cursor.emit_canonical {
                        accumulator = fold(accumulator, timestamp, handle, None, value.clone());
                    }
                    for &alias in aliases {
                        accumulator =
                            fold(accumulator, timestamp, alias, Some(handle), value.clone());
                    }
                }

                cursor_idx = if next_delta == Some(0) {
                    cursor_idx
                } else {
                    next_cursor
                };
            }
            self.time_index += 1;
        }
        Ok(accumulator)
    }

    /// Strictly timestamp-ordered fold specialized for two-state, single-bit traces.
    /// Returns an error for vector/real/variable-width handles or extended bit states.
    #[inline]
    pub fn try_fold_binary<T, F>(mut self, mut accumulator: T, mut fold: F) -> Result<T>
    where
        F: FnMut(T, u64, u32, Option<u32>, bool) -> T,
    {
        if self
            .cursors
            .iter()
            .any(|cursor| !matches!(cursor.kind, SignalKind::Bit))
        {
            return Err(Error::invalid(
                "binary fold requires every canonical handle to be single-bit",
            ));
        }

        while self.time_index < self.schedule_heads.len() {
            let mut cursor_idx =
                std::mem::replace(&mut self.schedule_heads[self.time_index], NO_CURSOR);
            if cursor_idx == NO_CURSOR {
                self.time_index += 1;
                continue;
            }
            let timestamp = self.block.time_table.timestamps[self.time_index];
            while cursor_idx != NO_CURSOR {
                let next_cursor = self.schedule_next[cursor_idx];
                let cursor = &mut self.cursors[cursor_idx];
                let value = cursor
                    .read_binary(self.time_index)?
                    .ok_or_else(|| Error::decode("scheduled chain ended unexpectedly"))?;
                let next_delta = cursor.peek_delta()?;
                if let Some(next_delta) = next_delta {
                    let next_time = self
                        .time_index
                        .checked_add(next_delta)
                        .ok_or_else(|| Error::decode("chain delta overflow"))?;
                    if next_time >= self.block.time_table.timestamps.len() {
                        return Err(Error::decode("chain delta exceeds time table"));
                    }
                    if next_time < self.schedule_heads.len() {
                        if next_time == self.time_index {
                            self.schedule_next[cursor_idx] = next_cursor;
                        } else {
                            self.schedule_next[cursor_idx] = self.schedule_heads[next_time];
                            self.schedule_heads[next_time] = cursor_idx;
                        }
                    }
                }

                let handle = cursor.handle;
                let aliases = self.alias_map.get(handle);
                if cursor.emit_canonical {
                    accumulator = fold(accumulator, timestamp, handle, None, value);
                }
                for &alias in aliases {
                    accumulator = fold(accumulator, timestamp, alias, Some(handle), value);
                }
                cursor_idx = if next_delta == Some(0) {
                    cursor_idx
                } else {
                    next_cursor
                };
            }
            self.time_index += 1;
        }
        Ok(accumulator)
    }

    /// Visits changes handle-by-handle instead of globally sorting them by timestamp.
    /// Per-handle timestamp order is preserved. This cache-friendly path is useful for indexing,
    /// statistics, and transformations that do not require a global event order.
    #[inline]
    pub fn try_for_each_parts_unordered<F>(self, mut visitor: F) -> Result<()>
    where
        F: FnMut(u64, u32, Option<u32>, SignalValue<'a>),
    {
        for mut cursor in self.cursors {
            let handle = cursor.handle;
            let aliases = self.alias_map.get(handle);
            while let Some(delta) = cursor.peek_delta()? {
                let time_index = cursor
                    .current_time_index
                    .checked_add(delta)
                    .ok_or_else(|| Error::decode("chain delta overflow"))?;
                if time_index >= self.block.time_table.timestamps.len() {
                    return Err(Error::decode("chain delta exceeds time table"));
                }
                if time_index >= self.end_time_index {
                    break;
                }
                let timestamp = *self
                    .block
                    .time_table
                    .timestamps
                    .get(time_index)
                    .ok_or_else(|| Error::decode("chain delta exceeds time table"))?;
                let value = cursor
                    .read_value(time_index)?
                    .ok_or_else(|| Error::decode("scheduled chain ended unexpectedly"))?;
                if cursor.emit_canonical && aliases.is_empty() {
                    visitor(timestamp, handle, None, value);
                    continue;
                }
                if cursor.emit_canonical {
                    visitor(timestamp, handle, None, value.clone());
                }
                for &alias in aliases {
                    visitor(timestamp, alias, Some(handle), value.clone());
                }
            }
        }
        Ok(())
    }

    /// Parallel handle-major scan. Callback invocations may occur concurrently and global event
    /// order is unspecified; timestamps remain ordered within each canonical handle.
    #[cfg(feature = "parallel")]
    pub fn try_for_each_parts_parallel<F>(self, visitor: F) -> Result<()>
    where
        F: Fn(u64, u32, Option<u32>, SignalValue<'a>) + Sync + Send,
    {
        let timestamps = &self.block.time_table.timestamps;
        let aliases = &self.alias_map;
        let end_time_index = self.end_time_index;
        self.cursors
            .into_par_iter()
            .try_for_each(|mut cursor| -> Result<()> {
                let handle = cursor.handle;
                let handle_aliases = aliases.get(handle);
                while let Some(delta) = cursor.peek_delta()? {
                    let time_index = cursor
                        .current_time_index
                        .checked_add(delta)
                        .ok_or_else(|| Error::decode("chain delta overflow"))?;
                    if time_index >= timestamps.len() {
                        return Err(Error::decode("chain delta exceeds time table"));
                    }
                    if time_index >= end_time_index {
                        break;
                    }
                    let timestamp = *timestamps
                        .get(time_index)
                        .ok_or_else(|| Error::decode("chain delta exceeds time table"))?;
                    let value = cursor
                        .read_value(time_index)?
                        .ok_or_else(|| Error::decode("scheduled chain ended unexpectedly"))?;
                    if cursor.emit_canonical && handle_aliases.is_empty() {
                        visitor(timestamp, handle, None, value);
                        continue;
                    }
                    if cursor.emit_canonical {
                        visitor(timestamp, handle, None, value.clone());
                    }
                    for &alias in handle_aliases {
                        visitor(timestamp, alias, Some(handle), value.clone());
                    }
                }
                Ok(())
            })
    }

    /// Parallel handle-major fold with one accumulator per Rayon worker and a final reduction.
    /// This avoids synchronisation on every event and is the preferred parallel statistics API.
    #[cfg(feature = "parallel")]
    pub fn try_fold_parts_parallel<T, Init, Fold, Reduce>(
        self,
        init: Init,
        fold: Fold,
        reduce: Reduce,
    ) -> Result<T>
    where
        T: Send,
        Init: Fn() -> T + Sync + Send,
        Fold: Fn(&mut T, u64, u32, Option<u32>, SignalValue<'a>) + Sync + Send,
        Reduce: Fn(T, T) -> T + Sync + Send,
    {
        let timestamps = &self.block.time_table.timestamps;
        let aliases = &self.alias_map;
        let end_time_index = self.end_time_index;
        let init_ref = &init;
        self.cursors
            .into_par_iter()
            .map(|mut cursor| -> Result<T> {
                let mut accumulator = init_ref();
                let handle = cursor.handle;
                let handle_aliases = aliases.get(handle);
                while let Some(delta) = cursor.peek_delta()? {
                    let time_index = cursor
                        .current_time_index
                        .checked_add(delta)
                        .ok_or_else(|| Error::decode("chain delta overflow"))?;
                    if time_index >= timestamps.len() {
                        return Err(Error::decode("chain delta exceeds time table"));
                    }
                    if time_index >= end_time_index {
                        break;
                    }
                    let timestamp = *timestamps
                        .get(time_index)
                        .ok_or_else(|| Error::decode("chain delta exceeds time table"))?;
                    let value = cursor
                        .read_value(time_index)?
                        .ok_or_else(|| Error::decode("scheduled chain ended unexpectedly"))?;
                    if cursor.emit_canonical && handle_aliases.is_empty() {
                        fold(&mut accumulator, timestamp, handle, None, value);
                        continue;
                    }
                    if cursor.emit_canonical {
                        fold(&mut accumulator, timestamp, handle, None, value.clone());
                    }
                    for &alias in handle_aliases {
                        fold(
                            &mut accumulator,
                            timestamp,
                            alias,
                            Some(handle),
                            value.clone(),
                        );
                    }
                }
                Ok(accumulator)
            })
            .try_reduce(init_ref, |left, right| Ok(reduce(left, right)))
    }

    #[inline]
    fn next_canonical(&mut self) -> Result<Option<ValueChange<'a>>> {
        loop {
            if let Some(value) = self.pending_aliases.pop_front() {
                return Ok(Some(value));
            }

            if self.time_index >= self.schedule_heads.len() {
                return Ok(None);
            }

            let cursor_idx = self.schedule_heads[self.time_index];
            if cursor_idx == NO_CURSOR {
                self.time_index += 1;
                continue;
            }
            self.schedule_heads[self.time_index] = self.schedule_next[cursor_idx];

            let timestamp = self.block.time_table.timestamps[self.time_index];

            let cursor = &mut self.cursors[cursor_idx];
            let Some(value) = cursor.read_value(self.time_index)? else {
                continue;
            };

            if let Some(next_delta) = cursor.peek_delta()? {
                let next_time = self
                    .time_index
                    .checked_add(next_delta)
                    .ok_or_else(|| Error::decode("chain delta overflow"))?;
                if next_time >= self.block.time_table.timestamps.len() {
                    return Err(Error::decode("chain delta exceeds time table"));
                }
                if next_time < self.schedule_heads.len() {
                    self.schedule_next[cursor_idx] = self.schedule_heads[next_time];
                    self.schedule_heads[next_time] = cursor_idx;
                }
            }

            let handle = cursor.handle;
            for &alias in self.alias_map.get(handle) {
                self.pending_aliases.push_back(ValueChange {
                    timestamp,
                    handle: alias,
                    alias_of: Some(handle),
                    value: value.clone(),
                });
            }
            if cursor.emit_canonical {
                return Ok(Some(ValueChange {
                    timestamp,
                    handle,
                    alias_of: None,
                    value,
                }));
            }
            if let Some(alias) = self.pending_aliases.pop_front() {
                return Ok(Some(alias));
            }
        }
    }
}

impl<'a> Iterator for VcBlockChanges<'a> {
    type Item = Result<ValueChange<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.next_canonical() {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

/// Builds a timestamp-ordered value-change traversal for a decoded block.
pub fn build_changes<'a>(block: &'a VcBlockMeta, geom: &'a GeomInfo) -> Result<VcBlockChanges<'a>> {
    VcBlockChanges::new(block, geom, &block.index)
}

impl VcBlockMeta {
    /// Builds a timestamp-ordered value-change traversal using the supplied geometry.
    pub fn changes<'a>(&'a self, geom: &'a GeomInfo) -> Result<VcBlockChanges<'a>> {
        build_changes(self, geom)
    }
}
