use crate::error::{Error, Result};

/// Maximum number of bytes that a u64 varint can occupy.
pub const VARINT_MAX_LEN: usize = 10;

/// Encodes the given value as an unsigned LEB128 varint and appends it to `out`.
pub fn encode_varint(mut value: u64, out: &mut Vec<u8>) -> usize {
    let start_len = out.len();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
            out.push(byte);
        } else {
            out.push(byte);
            break;
        }
    }
    out.len() - start_len
}

/// Decodes a u64 varint from the provided byte slice, advancing the slice on success.
pub fn decode_varint(input: &mut &[u8]) -> Result<u64> {
    let mut value = 0u64;
    for i in 0..VARINT_MAX_LEN {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(Error::decode(
                "unexpected end of input while decoding varint",
            ));
        };
        *input = rest;
        value |= ((byte & 0x7f) as u64) << (i * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::decode("varint exceeds maximum length"))
}

/// Decodes a u64 varint from the provided byte slice, returning the value and bytes consumed.
pub fn decode_varint_with_len(input: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for i in 0..VARINT_MAX_LEN {
        if i >= input.len() {
            return Err(Error::decode(
                "unexpected end of input while decoding varint",
            ));
        }
        let byte = input[i];
        value |= ((byte & 0x7f) as u64) << (i * 7);
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(Error::decode("varint exceeds maximum length"))
}
