#![cfg_attr(
    all(feature = "reader", feature = "writer"),
    doc = include_str!("../README.md")
)]
#![cfg_attr(
    not(all(feature = "reader", feature = "writer")),
    doc = "Feature-selected wavefst build. Enable `reader`, `writer`, or both; see the package README for the complete API guide."
)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

/// Block-level data structures mapping raw FST sections into typed records.
pub mod block;
#[cfg(feature = "gzip")]
mod compression;
/// Encoding helpers such as variable-length integer codecs.
pub mod encoding;
/// Shared error and result types.
pub mod error;
/// I/O backends (buffered and memory-mapped).
pub mod io;
/// Streaming reader front-end for FST files.
#[cfg(feature = "reader")]
pub mod reader;
/// Enumerations and value abstractions used across the crate.
pub mod types;
mod util;
/// Streaming writer for constructing FST traces.
#[cfg(feature = "writer")]
pub mod writer;

#[cfg(any(feature = "async-read", feature = "async-write"))]
pub mod async_support;
#[cfg(feature = "serde")]
pub mod serde_support;
#[cfg(feature = "simd")]
mod simd;

#[cfg(feature = "async-read")]
pub use async_support::{AsyncReader, read_all as async_read_all};
#[cfg(feature = "async-write")]
pub use async_support::{AsyncWriter, AsyncWriterBuilder};
pub use block::{
    BlackoutBlock, BlackoutEvent, GeomEntry, GeomInfo, Header, HierarchyBlock,
    HierarchyCompression, ScopeEntry, TimeSection, VarEntry, VcBlock,
};
pub use error::{Error, Result};
#[cfg(feature = "reader")]
pub use reader::{
    ChainData, ChainIndex, ChainPayload, ChainSlot, FstReader, ReaderBuilder, ReaderOptions,
    VcBlockMeta,
};
#[cfg(feature = "serde")]
pub use serde_support::{
    AttributeNode, HierarchySnapshot, OwnedSignalValue, OwnedValueChange, ScopeNode, VariableNode,
    collect_value_changes, snapshot_hierarchy,
};
pub use types::*;
#[cfg(feature = "writer")]
pub use writer::{
    AttributeId, ChainCompression, DynamicAliasEncoding, FstWriter, ScopeId,
    SupplementalVariableMetadata, TimeCompression, WriterBuilder, WriterOptions,
};
