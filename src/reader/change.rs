use std::borrow::Cow;
use std::collections::VecDeque;
use std::str;

use crate::block::{GeomEntry, GeomInfo};
use crate::encoding::decode_varint_with_len;
use crate::error::{Error, Result};
use crate::reader::vc::{ChainIndex, ChainPayload, VcBlockMeta};
use crate::types::SignalValue;

const FST_RCV_STR: [char; 8] = ['x', 'z', 'h', 'u', 'w', 'l', '-', '?'];

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
    kind: SignalKind,
    data: &'a [u8],
    offset: usize,
    current_time_index: usize,
    next_marker: Option<(u64, usize, usize)>,
}

impl<'a> ChainCursor<'a> {
    fn new(handle: u32, kind: SignalKind, data: &'a [u8]) -> Self {
        Self {
            handle,
            kind,
            data,
            offset: 0,
            current_time_index: 0,
            next_marker: None,
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
                    let value = if cfg!(target_endian = "little") {
                        f64::from_bits(u64::from_le_bytes(buf))
                    } else {
                        f64::from_bits(u64::from_be_bytes(buf))
                    };
                    Ok(Some(SignalValue::Real(value)))
                }
            }
        }
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
}

#[derive(Debug, Clone)]
pub struct ValueChange<'a> {
    pub timestamp: u64,
    pub handle: u32,
    pub alias_of: Option<u32>,
    pub value: SignalValue<'a>,
}

pub struct VcBlockChanges<'a> {
    block: &'a VcBlockMeta,
    cursors: Vec<ChainCursor<'a>>,
    schedule_heads: Vec<Option<usize>>,
    schedule_next: Vec<Option<usize>>,
    pending_aliases: VecDeque<ValueChange<'a>>,
    alias_map: Vec<Vec<u32>>,
    time_index: usize,
}

impl<'a> VcBlockChanges<'a> {
    pub fn new(
        block: &'a VcBlockMeta,
        geom: &'a GeomInfo,
        alias_index: &'a ChainIndex,
    ) -> Result<Self> {
        let mut cursors = Vec::new();
        let mut handle_to_cursor = vec![None; block.chains.len()];

        for (idx, chain_opt) in block.chains.iter().enumerate() {
            let Some(chain) = chain_opt else {
                continue;
            };
            if chain.alias_of.is_some() {
                continue;
            }
            let handle = (idx + 1) as u32;
            let geom_entry = geom.entry(handle).ok_or_else(|| {
                Error::invalid(format!("missing geometry entry for handle {handle}"))
            })?;
            let kind = SignalKind::from_geom(geom_entry, handle)?;
            let data = match &chain.payload {
                ChainPayload::Borrowed { range } => &block.chain_buffer[range.clone()],
                ChainPayload::Owned(buffer) => buffer.as_slice(),
            };
            handle_to_cursor[idx] = Some(cursors.len());
            cursors.push(ChainCursor::new(handle, kind, data));
        }

        let time_len = block.time_table.timestamps.len();
        let mut schedule_heads = vec![None; time_len];
        let mut schedule_next = vec![None; cursors.len()];

        for (idx, cursor) in cursors.iter_mut().enumerate() {
            if let Some(delta) = cursor.peek_delta()? {
                if delta >= time_len {
                    return Err(Error::decode("initial chain delta exceeds time table"));
                }
                schedule_next[idx] = schedule_heads[delta].take();
                schedule_heads[delta] = Some(idx);
            }
        }

        let mut alias_map = vec![Vec::new(); alias_index.slots.len() + 1];
        for (slot_idx, slot_opt) in alias_index.slots.iter().enumerate() {
            let Some(slot) = slot_opt else {
                continue;
            };
            if let Some(canon) = slot.alias_of {
                alias_map[canon as usize].push((slot_idx + 1) as u32);
            }
        }

        Ok(Self {
            block,
            cursors,
            schedule_heads,
            schedule_next,
            pending_aliases: VecDeque::new(),
            alias_map,
            time_index: 0,
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
            let Some(cursor_idx) = self.schedule_heads[self.time_index].take() else {
                self.time_index += 1;
                continue;
            };
            self.schedule_heads[self.time_index] = self.schedule_next[cursor_idx].take();
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
                if next_time >= self.schedule_heads.len() {
                    return Err(Error::decode("chain delta exceeds time table"));
                }
                self.schedule_next[cursor_idx] = self.schedule_heads[next_time].take();
                self.schedule_heads[next_time] = Some(cursor_idx);
            }

            let handle = cursor.handle;
            let aliases = &self.alias_map[handle as usize];
            if aliases.is_empty() {
                visitor(ValueChange {
                    timestamp,
                    handle,
                    alias_of: None,
                    value,
                });
                continue;
            }

            visitor(ValueChange {
                timestamp,
                handle,
                alias_of: None,
                value: value.clone(),
            });
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

    #[inline]
    fn next_canonical(&mut self) -> Result<Option<ValueChange<'a>>> {
        loop {
            if let Some(value) = self.pending_aliases.pop_front() {
                return Ok(Some(value));
            }

            if self.time_index >= self.schedule_heads.len() {
                return Ok(None);
            }

            let Some(cursor_idx) = self.schedule_heads[self.time_index].take() else {
                self.time_index += 1;
                continue;
            };
            self.schedule_heads[self.time_index] = self.schedule_next[cursor_idx].take();

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
                if next_time >= self.schedule_heads.len() {
                    return Err(Error::decode("chain delta exceeds time table"));
                }
                self.schedule_next[cursor_idx] = self.schedule_heads[next_time].take();
                self.schedule_heads[next_time] = Some(cursor_idx);
            }

            let handle = cursor.handle;
            if let Some(aliases) = self.alias_map.get(handle as usize) {
                for &alias in aliases {
                    self.pending_aliases.push_back(ValueChange {
                        timestamp,
                        handle: alias,
                        alias_of: Some(handle),
                        value: value.clone(),
                    });
                }
            }
            return Ok(Some(ValueChange {
                timestamp,
                handle,
                alias_of: None,
                value,
            }));
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

pub fn build_changes<'a>(block: &'a VcBlockMeta, geom: &'a GeomInfo) -> Result<VcBlockChanges<'a>> {
    VcBlockChanges::new(block, geom, &block.index)
}

impl VcBlockMeta {
    pub fn changes<'a>(&'a self, geom: &'a GeomInfo) -> Result<VcBlockChanges<'a>> {
        build_changes(self, geom)
    }
}
