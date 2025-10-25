use std::borrow::Cow;
use std::io::{Read, Seek, SeekFrom};

use crate::error::{Error, Result};

/// Reads an exact number of bytes into a fixed-size array.
#[inline]
pub fn read_array<const N: usize, R: Read>(reader: &mut R) -> Result<[u8; N]> {
    let mut buf = [0u8; N];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Reads a big-endian `u64`.
#[inline]
pub fn read_u64_be<R: Read>(reader: &mut R) -> Result<u64> {
    let bytes = read_array::<8, _>(reader)?;
    Ok(u64::from_be_bytes(bytes))
}

/// Reads a big-endian IEEE-754 `f64`.
#[inline]
pub fn read_f64_be<R: Read>(reader: &mut R) -> Result<f64> {
    let bytes = read_array::<8, _>(reader)?;
    Ok(f64::from_bits(u64::from_be_bytes(bytes)))
}

/// Reads a fixed-size, null-terminated UTF-8 string.
pub fn read_cstring<R: Read>(reader: &mut R, len: usize) -> Result<String> {
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    let nul = buf.iter().position(|&b| b == 0).unwrap_or(len);
    let text = String::from_utf8_lossy(&buf[..nul]);
    Ok(match text {
        Cow::Borrowed(s) => s.to_string(),
        Cow::Owned(s) => s,
    })
}

/// Advances the reader by `len` bytes.
#[inline]
pub fn skip_bytes<R: Read + Seek>(reader: &mut R, len: u64) -> Result<()> {
    reader.seek(SeekFrom::Current(len as i64))?;
    Ok(())
}

/// Reads a varint directly from a reader, returning the value and number of bytes consumed.
pub fn read_varint_from_reader<R: Read>(reader: &mut R) -> Result<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0usize;
    let mut buf = [0u8; 1];
    let mut consumed = 0usize;

    loop {
        if shift >= 64 {
            return Err(Error::decode("varint exceeds 64-bit capacity"));
        }
        reader.read_exact(&mut buf)?;
        consumed += 1;
        let byte = buf[0];
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    Ok((value, consumed))
}

/// Ensures the `endian_test` field matches the expected constant.
pub fn validate_endian(endian_test: f64) -> Result<()> {
    const EXPECTED: f64 = std::f64::consts::E;
    let bits = endian_test.to_bits();
    let expected_bits = EXPECTED.to_bits();
    if bits != expected_bits && bits != expected_bits.swap_bytes() {
        return Err(Error::invalid(format!(
            "unexpected endian test marker: {endian_test:?}"
        )));
    }
    Ok(())
}
