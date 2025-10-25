use crate::encoding::varint::{decode_varint, encode_varint};
use crate::error::Result;

/// Encodes a signed integer using ZigZag + varint encoding.
pub fn encode_svarint(value: i64, out: &mut Vec<u8>) -> usize {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zigzag, out)
}

/// Decodes a signed ZigZag/varint integer.
pub fn decode_svarint(input: &mut &[u8]) -> Result<i64> {
    let raw = decode_varint(input)?;
    let magnitude = (raw >> 1) as i64;
    let sign = (raw & 1) as i64;
    Ok(magnitude ^ -sign)
}
