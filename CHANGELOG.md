# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Removed

- Removed the optional `mmap` feature, `memmap2` dependency, and `io::MemoryMap` adapter. Callers
  that choose memory mapping can pass `std::io::Cursor<memmap2::Mmap>` directly to `ReaderBuilder`.

## 0.3.0 - 2026-08-27

### Added

- Added independent `reader` and `writer` Cargo features, plus `async-read` and `async-write`
  front ends with the former `async` feature retained as a compatibility alias.
- Added `CodecParallelism::{Serial, Auto, Threads}` and reader/writer builder controls so thread
  creation and worker counts are explicit runtime choices.
- Added a reproducible 500k-signal selective-access benchmark covering random signal subsets,
  1% time windows, viewport dragging, cold/warm cache states, wavefst 0.2.2, fst-reader 0.17,
  libfst, and Wellen `load_signals`, with wall time, I/O bytes, peak RSS, and CPU-cycle metrics.
- Added reusable reader queries through `set_included_handles`, `include_all_handles`,
  `set_time_range`, and `rewind_value_changes`, retaining parsed metadata across viewports.

### Changed

- Disabled `parallel` and `mmap` in the default feature set. Both remain opt-in capabilities;
  codec execution stays serial even when `parallel` is compiled until a builder selects otherwise.
- Made writer-only AHash and SIMD dependencies conditional, and made mmap, serde, and async
  features imply only the front-end they actually require.
- Replaced the former commercial-use restriction with the standard MIT License and declared the
  package using the SPDX `MIT` identifier.
- Expanded reproducible performance coverage and README results across dense, wide, long, full,
  selective, hierarchy-open, batch, and scalar scenarios against libfst, Wellen, and libfstwriter.
- Reduced reader fixed costs with buffered in-memory backtracking, optional hierarchy loading,
  reusable thread-local libdeflate decoders, compact selected-chain staging, and an alias-free
  chain-index fast path.
- Replaced dense selected-handle indexes and alias maps with compact representations, added a
  validated single-pass dynamic-alias scanner, and tuned random-seek buffering. On the checked-in
  500k-signal workload this makes current wavefst faster than pinned libfst in A–E while using
  about 9–10MiB peak RSS for sparse cases.

## 0.2.2 - 2026-08-26

### Added

- Added `ReaderBuilder::include_handles` and `ReaderBuilder::time_range` for indexed selective
  reads that seek directly to requested chains and skip non-overlapping value-change blocks.
- Added codec, dynamic-alias, time-boundary, traversal-mode, and real-libfst regression coverage
  for selective reads, plus Criterion benchmarks for sparse workloads.

### Changed

- Decode serial zlib value chains directly into their final arena, eliminating one allocation and
  copy per compressed chain.
- Inline the common one-byte varint decoder while keeping multi-byte validation in an outlined
  slow path.
- Preserve the original contiguous full-scan path when no filter is configured or every handle is
  selected.

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
