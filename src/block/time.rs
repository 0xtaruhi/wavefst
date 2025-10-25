/// Captures metadata about the trailing time section of a value-change block.
#[derive(Debug, Clone, Default)]
pub struct TimeSection {
    /// Number of bytes in the uncompressed time delta stream.
    pub uncompressed_len: u64,
    /// Number of bytes stored on disk after compression.
    pub compressed_len: u64,
    /// Number of time delta entries encoded in the section.
    pub item_count: u64,
}
