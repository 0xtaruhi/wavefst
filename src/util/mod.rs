use std::io::{Read, Seek, SeekFrom};

use crate::error::{Error, Result};

/// Reads an exact number of bytes into a fixed-size array.
#[inline]
pub(crate) fn read_array<const N: usize, R: Read>(reader: &mut R) -> Result<[u8; N]> {
    let mut buf = [0u8; N];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Reads a big-endian `u64`.
#[inline]
pub(crate) fn read_u64_be<R: Read>(reader: &mut R) -> Result<u64> {
    let bytes = read_array::<8, _>(reader)?;
    Ok(u64::from_be_bytes(bytes))
}

/// Reads a fixed-size, null-terminated UTF-8 string.
pub(crate) fn read_cstring<R: Read>(reader: &mut R, len: usize) -> Result<String> {
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    let nul = buf.iter().position(|&b| b == 0).unwrap_or(len);
    Ok(String::from_utf8_lossy(&buf[..nul]).into_owned())
}

/// Advances the reader by `len` bytes.
#[inline]
pub(crate) fn skip_bytes<R: Read + Seek>(reader: &mut R, len: u64) -> Result<()> {
    let offset = i64::try_from(len)
        .map_err(|_| Error::invalid("seek distance exceeds signed 64-bit range"))?;
    reader.seek(SeekFrom::Current(offset))?;
    Ok(())
}

/// Reads a varint directly from a reader, returning the value and number of bytes consumed.
pub(crate) fn read_varint_from_reader<R: Read>(reader: &mut R) -> Result<(u64, usize)> {
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
        if shift == 63 && byte > 1 {
            return Err(Error::decode("varint overflows 64-bit capacity"));
        }
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    Ok((value, consumed))
}
