use crate::encoding::{decode_varint, encode_varint};
use crate::error::{Error, Result};

/// Represents a single dump on/off event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlackoutEvent {
    /// `true` when dumping is enabled, `false` when disabled.
    pub is_on: bool,
    /// Absolute simulation time (after accumulated deltas) associated with the event.
    pub time: u64,
}

/// Parsed blackout block.
#[derive(Debug, Clone, Default)]
pub struct BlackoutBlock {
    /// Chronologically ordered blackout events decoded from the block payload.
    pub events: Vec<BlackoutEvent>,
}

impl BlackoutBlock {
    /// Serializes the block to a buffer.
    pub fn encode(&self, out: &mut Vec<u8>) {
        encode_varint(self.events.len() as u64, out);
        let mut prev = 0u64;
        for event in &self.events {
            out.push(if event.is_on { 1 } else { 0 });
            let delta = event.time.saturating_sub(prev);
            encode_varint(delta, out);
            prev = event.time;
        }
    }

    /// Decodes the block from raw bytes.
    pub fn decode(mut data: &[u8]) -> Result<Self> {
        let count = decode_varint(&mut data)?;
        let mut events = Vec::with_capacity(count as usize);
        let mut time = 0u64;
        for _ in 0..count {
            let (flag, rest) = data
                .split_first()
                .ok_or_else(|| Error::decode("unexpected end of blackout data"))?;
            data = rest;
            let delta = decode_varint(&mut data)?;
            time = time
                .checked_add(delta)
                .ok_or_else(|| Error::decode("blackout time overflow"))?;
            events.push(BlackoutEvent {
                is_on: *flag != 0,
                time,
            });
        }
        Ok(Self { events })
    }
}
