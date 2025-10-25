#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

/// Block-level data structures mapping raw FST sections into typed records.
pub mod block;
/// Compression backends used for value-change and hierarchy payloads.
pub mod compression;
/// Encoding helpers such as variable-length integer codecs.
pub mod encoding;
/// Shared error and result types.
pub mod error;
/// I/O backends (buffered and memory-mapped).
pub mod io;
/// Streaming reader front-end for FST files.
pub mod reader;
/// Enumerations and value abstractions used across the crate.
pub mod types;
/// Miscellaneous helpers consumed by readers and writers.
pub mod util;
/// Streaming writer for constructing FST traces.
pub mod writer;

#[cfg(feature = "async")]
pub mod async_support;
#[cfg(feature = "serde")]
pub mod serde_support;
#[cfg(feature = "simd")]
mod simd;

#[cfg(feature = "async")]
pub use async_support::{AsyncReader, AsyncWriter, AsyncWriterBuilder, read_all as async_read_all};
pub use block::{
    BlackoutBlock, BlackoutEvent, GeomEntry, GeomInfo, Header, HierarchyBlock, ScopeEntry,
    TimeSection, VarEntry, VcBlock,
};
pub use compression::{Compressor, Decompressor, NullCompressor, NullDecompressor};
pub use error::{Error, Result};
pub use reader::{ChainIndex, ChainSlot, FstReader, ReaderBuilder, ReaderOptions, VcBlockMeta};
#[cfg(feature = "serde")]
pub use serde_support::{
    AttributeNode, HierarchySnapshot, OwnedSignalValue, OwnedValueChange, ScopeNode, VariableNode,
    collect_value_changes, snapshot_hierarchy,
};
pub use types::*;
pub use writer::{
    ChainCompression, FstWriter, ScopeId, TimeCompression, WriterBuilder, WriterOptions,
};
