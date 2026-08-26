# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Changed

- Decode serial zlib value chains directly into their final arena, eliminating one allocation and
  copy per compressed chain.
- Inline the common one-byte varint decoder while keeping multi-byte validation in an outlined
  slow path.

## 0.2.1 - 2026-08-26

### Added

- Reproducible, single-core dense-trace performance comparison against the pinned upstream libfst
  implementation, including cross-reader event-count validation and output-size reporting.

### Changed

- Replaced the flate2/zlib-rs gzip and zlib backend with bundled libdeflate across hierarchy,
  geometry, frame, time-table, value-chain, and whole-file wrapper codecs.
- Reused libdeflate compressor/decompressor state and scratch buffers across independent value
  chains and Rayon partitions.

### Fixed

- Added gzip/zlib round-trip coverage at empty, small-chain, zlib-window, and declared-length
  boundaries.

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
  direct raw-chain assembly, fast randomized chain deduplication, libdeflate-backed gzip/zlib,
  reusable codec state and scratch space, and borrowed metadata encoding to avoid excessive work
  stealing, copying, hashing, state clearing, and allocator growth.
- Updated all dependencies to current releases, including `lz4_flex` 0.14, `thiserror` 2, and
  Criterion 0.8.
- Removed unused compression and streaming wrapper modules and reduced the exposed internal API.
