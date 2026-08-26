#[cfg(feature = "parallel")]
use std::collections::HashMap;
use std::io::Read;
#[cfg(feature = "reader")]
use std::io::{Seek, SeekFrom};
#[cfg(feature = "parallel")]
use std::sync::{Arc, Mutex, OnceLock};

#[cfg(feature = "reader")]
use crate::error::Error;
use crate::error::Result;
#[cfg(feature = "parallel")]
use crate::types::CodecParallelism;

/// Maximum worker count selected by [`CodecParallelism::Auto`]. Explicit thread counts remain
/// under application control.
#[cfg(feature = "parallel")]
pub(crate) const MAX_AUTO_CODEC_THREADS: usize = 32;

#[cfg(feature = "parallel")]
fn codec_thread_count(parallelism: CodecParallelism) -> usize {
    match parallelism {
        CodecParallelism::Serial => 1,
        CodecParallelism::Auto => std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_AUTO_CODEC_THREADS),
        CodecParallelism::Threads(threads) => threads.get(),
    }
}

/// Returns a Rayon minimum partition length matched to the selected codec worker count.
#[cfg(feature = "parallel")]
#[inline]
pub(crate) fn codec_partition_len(item_count: usize, parallelism: CodecParallelism) -> usize {
    item_count
        .div_ceil(codec_thread_count(parallelism).min(item_count).max(1))
        .max(1)
}

/// Returns a lazily initialized private codec pool for the selected width. Failed constructions
/// are cached so callers can fall back to their serial path without repeatedly allocating.
#[cfg(feature = "parallel")]
pub(crate) fn codec_pool(parallelism: CodecParallelism) -> Option<Arc<rayon::ThreadPool>> {
    let threads = codec_thread_count(parallelism);
    static CODEC_POOLS: OnceLock<Mutex<HashMap<usize, Option<Arc<rayon::ThreadPool>>>>> =
        OnceLock::new();
    let pools = CODEC_POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut pools = pools
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(pool) = pools.get(&threads) {
        return pool.clone();
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(move |index| format!("wavefst-codec-{threads}-{index}"))
        .build()
        .ok()
        .map(Arc::new);
    pools.insert(threads, pool.clone());
    pool
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
#[cfg(feature = "reader")]
#[inline]
pub(crate) fn skip_bytes<R: Read + Seek>(reader: &mut R, len: u64) -> Result<()> {
    let offset = i64::try_from(len)
        .map_err(|_| Error::invalid("seek distance exceeds signed 64-bit range"))?;
    reader.seek(SeekFrom::Current(offset))?;
    Ok(())
}

/// Reads a varint directly from a reader, returning the value and number of bytes consumed.
#[cfg(feature = "reader")]
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
