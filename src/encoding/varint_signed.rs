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

/// Decodes a signed LEB128 integer.
pub fn decode_svarint(input: &mut &[u8]) -> Result<i64> {
    let mut value = 0i128;
    let mut shift = 0u32;
    for _ in 0..10 {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(Error::decode(
                "unexpected end of input while decoding signed varint",
            ));
        };
        *input = rest;
        value |= i128::from(byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            if byte & 0x40 != 0 {
                value |= (!0i128) << shift;
            }
            return i64::try_from(value).map_err(|_| Error::decode("signed varint overflows i64"));
        }
    }
    Err(Error::decode("signed varint exceeds maximum length"))
}

#[cfg(test)]
mod tests {
    use super::{decode_svarint, encode_svarint};

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
        for value in [i64::MIN, i64::MAX, i32::MIN as i64, i32::MAX as i64] {
            let mut encoded = Vec::new();
            encode_svarint(value, &mut encoded);
            let mut input = encoded.as_slice();
            assert_eq!(decode_svarint(&mut input).unwrap(), value);
        }
    }
}
