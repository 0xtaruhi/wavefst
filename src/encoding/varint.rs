use crate::error::{Error, Result};

/// Maximum number of bytes that a u64 varint can occupy.
pub const VARINT_MAX_LEN: usize = 10;

/// Encodes the given value as an unsigned LEB128 varint and appends it to `out`.
#[inline]
pub fn encode_varint(mut value: u64, out: &mut Vec<u8>) -> usize {
    let start_len = out.len();
    if value < 0x80 {
        out.push(value as u8);
        return 1;
    }
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
#[inline]
pub fn decode_varint(input: &mut &[u8]) -> Result<u64> {
    let mut value = 0u64;
    for i in 0..VARINT_MAX_LEN {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(Error::decode(
                "unexpected end of input while decoding varint",
            ));
        };
        if i == VARINT_MAX_LEN - 1 && byte > 1 {
            return Err(Error::decode("varint overflows 64-bit capacity"));
        }
        *input = rest;
        value |= ((byte & 0x7f) as u64) << (i * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::decode("varint exceeds maximum length"))
}

/// Decodes a u64 varint from the provided byte slice, returning the value and bytes consumed.
#[inline]
pub fn decode_varint_with_len(input: &[u8]) -> Result<(u64, usize)> {
    if let Some(&byte) = input.first()
        && byte < 0x80
    {
        return Ok((u64::from(byte), 1));
    }
    let mut value = 0u64;
    for i in 0..VARINT_MAX_LEN {
        if i >= input.len() {
            return Err(Error::decode(
                "unexpected end of input while decoding varint",
            ));
        }
        let byte = input[i];
        if i == VARINT_MAX_LEN - 1 && byte > 1 {
            return Err(Error::decode("varint overflows 64-bit capacity"));
        }
        value |= ((byte & 0x7f) as u64) << (i * 7);
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(Error::decode("varint exceeds maximum length"))
}

#[cfg(test)]
mod tests {
    use super::{decode_varint, decode_varint_with_len, encode_varint};

    #[test]
    fn round_trips_boundaries_and_deterministic_samples() {
        let boundaries = [
            0,
            1,
            0x7f,
            0x80,
            0x3fff,
            0x4000,
            u64::from(u32::MAX),
            u64::MAX - 1,
            u64::MAX,
        ];
        for value in
            boundaries
                .into_iter()
                .chain((0..10_000).scan(0x9e37_79b9_7f4a_7c15u64, |state, _| {
                    *state ^= *state << 7;
                    *state ^= *state >> 9;
                    *state ^= *state << 8;
                    Some(*state)
                }))
        {
            let mut encoded = Vec::new();
            let encoded_len = encode_varint(value, &mut encoded);
            assert_eq!(encoded_len, encoded.len());
            assert!(encoded_len <= 10);

            let mut remaining = encoded.as_slice();
            assert_eq!(decode_varint(&mut remaining).unwrap(), value);
            assert!(remaining.is_empty());
            assert_eq!(
                decode_varint_with_len(&encoded).unwrap(),
                (value, encoded_len)
            );
        }
    }

    #[test]
    fn rejects_truncated_and_overflowing_encodings() {
        assert!(decode_varint(&mut &[0x80][..]).is_err());
        assert!(decode_varint_with_len(&[0x80]).is_err());

        let overflow = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        assert!(decode_varint(&mut &overflow[..]).is_err());
        assert!(decode_varint_with_len(&overflow).is_err());
    }
}
