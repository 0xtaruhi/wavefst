# Changelog

All notable changes to this project are documented in this file.

## 0.2.0 - 2026-08-26

### Added

- Bidirectional interoperability tests against a pinned upstream libfst revision.
- Complete public enums and hierarchy writers for libfst scope, variable, attribute, alias, and
  supplemental metadata records.
- Configurable hierarchy, chain, time-table, dynamic-alias, and whole-file compression encodings.
- Memory-mapped, async, serde, binary batch, ordered fold, handle-major, and parallel traversal
  APIs.
- Criterion reader/writer benchmarks and CI feature, documentation, packaging, and upstream-oracle
  checks.

### Fixed

- GTKWave/libfst compatibility for empty files, block checkpoints, legacy and compact dynamic
  aliases, binary source-stem attributes, opposite-endian real values, and VCD port geometry.
- Length, handle, offset, allocation, timestamp, and decompression bounds throughout the reader and
  writer.

### Changed

- Reworked value-change decoding around reusable arenas and specialized validated hot paths.
- Bounded parallel chain codecs, fused dense binary batches, adaptive per-chain reservations,
  direct raw-chain assembly, and fast randomized chain deduplication to avoid excessive work
  stealing, copying, hashing, and allocator growth.
- Updated all dependencies to current releases, including `lz4_flex` 0.14, `thiserror` 2, and
  Criterion 0.8.
- Removed unused compression and streaming wrapper modules and reduced the exposed internal API.
