#[cfg(any(feature = "reader", test))]
use crate::error::{Error, Result};

/// Encodes a signed integer using the signed LEB128 representation used by libfst.
pub fn encode_svarint(mut value: i64, out: &mut Vec<u8>) -> usize {
    let start = out.len();
    loop {
        let mut byte = (value as u8) & 0x7f;
        value >>= 7;
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        if !done {
            byte |= 0x80;
        }
        out.push(byte);
        if done {
            break;
        }
    }
    out.len() - start
}

/// Decodes signed LEB128 without mutating the input and returns the encoded length.
#[inline]
#[cfg(any(feature = "reader", test))]
pub(crate) fn decode_svarint_with_len(input: &[u8]) -> Result<(i64, usize)> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    for index in 0..10 {
        let Some(&byte) = input.get(index) else {
            return Err(Error::decode(
                "unexpected end of input while decoding signed varint",
            ));
        };
        if index == 9 {
            if byte & 0x80 != 0 {
                return Err(Error::decode("signed varint exceeds maximum length"));
            }
            if byte != 0 && byte != 0x7f {
                return Err(Error::decode("signed varint overflows i64"));
            }
            value |= u64::from(byte & 1) << 63;
            return Ok((value as i64, 10));
        }

        value |= u64::from(byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            if byte & 0x40 != 0 {
                value |= (!0_u64) << shift;
            }
            return Ok((value as i64, index + 1));
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::{decode_svarint_with_len, encode_svarint};

    fn decode_svarint(input: &mut &[u8]) -> crate::Result<i64> {
        let (value, consumed) = decode_svarint_with_len(input)?;
        *input = &input[consumed..];
        Ok(value)
    }

    #[test]
    fn matches_signed_leb128_vectors() {
        let vectors: &[(i64, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (-1, &[0x7f]),
            (63, &[0x3f]),
            (64, &[0xc0, 0x00]),
            (-64, &[0x40]),
            (-65, &[0xbf, 0x7f]),
        ];
        for &(value, expected) in vectors {
            let mut encoded = Vec::new();
            encode_svarint(value, &mut encoded);
            assert_eq!(encoded, expected);
            let mut input = encoded.as_slice();
            assert_eq!(decode_svarint(&mut input).unwrap(), value);
            assert!(input.is_empty());
        }
    }

    #[test]
    fn round_trips_extremes() {
        for value in [i64::MIN, i64::MAX, i64::from(i32::MIN), i64::from(i32::MAX)] {
            let mut encoded = Vec::new();
            encode_svarint(value, &mut encoded);
            let mut input = encoded.as_slice();
            assert_eq!(decode_svarint(&mut input).unwrap(), value);
        }
    }

    #[test]
    fn round_trips_deterministic_samples() {
        let mut state = 0xd1b5_4a32_d192_ed03u64;
        for _ in 0..10_000 {
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            let value = state.cast_signed();
            let mut encoded = Vec::new();
            encode_svarint(value, &mut encoded);
            let mut input = encoded.as_slice();
            assert_eq!(decode_svarint(&mut input).unwrap(), value);
            assert!(input.is_empty());
        }
    }
}
