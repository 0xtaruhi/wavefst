use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use memmap2::{Mmap, MmapOptions};

/// Wrapper around a memory mapped file region.
#[cfg(feature = "mmap")]
#[derive(Debug)]
pub struct MemoryMap {
    mmap: Mmap,
    position: usize,
}

impl MemoryMap {
    /// Wraps an existing read-only memory map at position zero.
    pub fn new(mmap: Mmap) -> Self {
        Self { mmap, position: 0 }
    }

    /// Maps a file for read-only access.
    ///
    /// # Safety
    /// The mapped file must not be truncated or modified for the lifetime of the returned map.
    pub unsafe fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: the caller guarantees that the file remains immutable while mapped.
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Ok(Self::new(mmap))
    }

    /// Returns the entire mapped byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap
    }

    /// Returns the mapped file length in bytes.
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Returns `true` when the mapped file contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

impl AsRef<[u8]> for MemoryMap {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Read for MemoryMap {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let remaining = &self.mmap[self.position..];
        let count = remaining.len().min(buffer.len());
        buffer[..count].copy_from_slice(&remaining[..count]);
        self.position += count;
        Ok(count)
    }
}

impl Seek for MemoryMap {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let base = match position {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::End(offset) => self.mmap.len() as i128 + i128::from(offset),
            SeekFrom::Current(offset) => self.position as i128 + i128::from(offset),
        };
        if base < 0 || base > self.mmap.len() as i128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory-map seek is outside the mapped file",
            ));
        }
        self.position = base as usize;
        Ok(self.position as u64)
    }
}
