#![allow(missing_docs)]

#[cfg(feature = "mmap")]
use memmap2::Mmap;

/// Wrapper around a memory mapped file region.
#[cfg(feature = "mmap")]
#[derive(Debug)]
pub struct MemoryMap {
    mmap: Mmap,
}

#[cfg(feature = "mmap")]
impl MemoryMap {
    pub fn new(mmap: Mmap) -> Self {
        Self { mmap }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.mmap
    }
}
