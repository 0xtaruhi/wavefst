use std::io::{Read, Seek, SeekFrom};
#[cfg(feature = "parallel")]
use std::sync::OnceLock;

use crate::error::{Error, Result};

/// Maximum number of independent codec partitions submitted to Rayon for a single FST block.
/// More partitions increase work-stealing overhead sharply on many-core hosts because FST chains
/// are usually small, independent codec streams.
#[cfg(feature = "parallel")]
pub(crate) const MAX_CODEC_TASKS: usize = 32;

/// Returns a Rayon minimum partition length that caps one block at [`MAX_CODEC_TASKS`] partitions.
#[cfg(feature = "parallel")]
#[inline]
pub(crate) fn codec_partition_len(item_count: usize) -> usize {
    item_count.div_ceil(MAX_CODEC_TASKS).max(1)
}

/// Runs codec work on a bounded, lazily initialized pool. A separate pool prevents a machine's
/// full global Rayon width from being awakened for the many short streams commonly found in one
/// FST block. If pool construction fails, the operation safely falls back to the caller thread.
#[cfg(feature = "parallel")]
pub(crate) fn in_codec_pool<R, F>(operation: F) -> R
where
    R: Send,
    F: FnOnce() -> R + Send,
{
    static CODEC_POOL: OnceLock<Option<rayon::ThreadPool>> = OnceLock::new();
    let pool = CODEC_POOL.get_or_init(|| {
        let available = std::thread::available_parallelism().map_or(1, usize::from);
        rayon::ThreadPoolBuilder::new()
            .num_threads(available.min(MAX_CODEC_TASKS))
            .thread_name(|index| format!("wavefst-codec-{index}"))
            .build()
            .ok()
    });
    match pool {
        Some(pool) => pool.install(operation),
        None => operation(),
    }
}

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
