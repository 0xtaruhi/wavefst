# wavefst

[![Crates.io](https://img.shields.io/crates/v/wavefst.svg)](https://crates.io/crates/wavefst)
[![Documentation](https://img.shields.io/docsrs/wavefst)](https://docs.rs/wavefst)
[![License](https://img.shields.io/crates/l/wavefst.svg)](https://github.com/0xtaruhi/wavefst/blob/main/LICENSE)

> Modern Rust reader and writer for the Fast Signal Trace (FST) waveform format.

`wavefst` is a safe Rust reader and writer for GTKWave's FST format. Its on-disk layouts follow the
reference `libfst` implementation, including gzip/LZ4 hierarchy blocks, signed-LEB128 dynamic
aliases, block checkpoints, static alias handles, and whole-file gzip wrappers.

---

## Highlights

- **libfst-compatible output** – files are accepted by the reference reader and GTKWave, including
  static aliases and optional `FST_BL_ZWRAPPER` output.
- **Bounded streaming** – writers automatically split large change sets into checkpointed VC blocks;
  readers expose configurable decompression and per-block safety limits.
- **Indexed selective reads** – readers can restrict handles and inclusive time ranges, seek directly
  to selected chains, and skip non-overlapping value-change blocks without decompressing them.
- **Selectable compression** – uncompressed chains plus zlib, LZ4, and FastLZ payloads are supported
  behind feature flags. Raw chains still use the standard `'Z'` block marker.
- **Async, SIMD, serde** – optional helpers wrap the synchronous APIs for async I/O, fast ASCII→bit
  packing, and serialisable hierarchy/value-change snapshots.
- **Tooling ready** – Criterion benchmarks, feature-matrix tests, and conformance fixtures are
  included to keep regressions in check.
- **Auditable compatibility** – the [format matrix](COMPATIBILITY.md) records every libfst disk tag
  and a pinned, bidirectional upstream oracle test.

---

## Installation

```bash
cargo add wavefst
```

The default feature set enables libdeflate-backed gzip/zlib (`gzip`), LZ4 (`lz4`), memory mapping
(`mmap`), parallel chain codecs (`parallel`), and the SSE2 packed-bit fast path (`simd`). Disable
them with `--no-default-features` and opt back into the ones you need. The `gzip` feature builds the
bundled libdeflate C library and therefore needs a working C compiler.

---

## Quick Start

### Reading a trace

```rust
use wavefst::{ReaderBuilder, SignalValue};

fn dump_changes(path: &str) -> wavefst::Result<()> {
    let file = std::fs::File::open(path)?;
    let mut reader = ReaderBuilder::new(file).build()?;

    while let Some(mut block) = reader.next_value_changes()? {
        while let Some(event) = block.next() {
            let event = event?;
            println!("t={} handle={} value={:?}", event.timestamp, event.handle, event.value);
        }
    }
    Ok(())
}
```

When only part of a waveform is needed, configure the selection before building the reader:

```rust
use wavefst::ReaderBuilder;

fn read_window(path: &str) -> wavefst::Result<()> {
    let file = std::fs::File::open(path)?;
    let mut reader = ReaderBuilder::new(file)
        .include_handles([1, 7, 42])
        .time_range(1_000..=2_000)
        .build()?;

    while let Some(changes) = reader.next_value_changes()? {
        changes.try_for_each_parts(|timestamp, handle, alias_of, value| {
            println!("{timestamp} {handle} {alias_of:?} {value:?}");
        })?;
    }
    Ok(())
}
```

Handles are one-based. The time range is inclusive. The reader still decodes the compact chain
index and the relevant time tables, but reads and decompresses only selected chain payloads;
value-change blocks wholly outside the requested range are seek-skipped. Omitting both filters uses
the original contiguous full-scan path. If handles are already known and signal names are not
needed, add `.load_hierarchy(false)` to skip hierarchy decompression and parsing while retaining the
geometry required to decode values.

For handle-major analysis that does not require global timestamp ordering, use
`try_for_each_parts_unordered`. With the default `parallel` feature, reductions can avoid shared
atomics and locks through `try_fold_parts_parallel`. Strictly ordered reductions can keep their
accumulator in registers with `try_fold_parts`; two-state bit traces have the specialized
`try_fold_binary` path.

### Writing a trace

```rust
use std::borrow::Cow;
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ScopeType, SignalValue, TimeCompression,
    VarDir, VarType,
};

fn build_example(path: &str) -> wavefst::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = FstWriter::builder(file)
        .chain_compression(ChainCompression::Lz4)
        .time_compression(TimeCompression::Zlib)
        .build()?;

    writer.begin_scope(ScopeType::VcdModule, "tb", None)?;
    let bit = writer.add_variable(VarType::VcdWire, VarDir::Implicit, "bit", GeomEntry::Fixed(1))?;
    let vec = writer.add_variable(VarType::VcdWire, VarDir::Implicit, "vec", GeomEntry::Fixed(8))?;
    writer.end_scope()?;

    let mut header = Header::default();
    header.version = "wavefst-demo".into();
    // Time range, handle counts, and VC section count are backpatched by finish().
    writer.write_header(header)?;

    writer.emit_change(0, bit, SignalValue::Bit('0'))?;
    writer.emit_change(10, bit, SignalValue::Bit('1'))?;
    writer.emit_change(12, vec, SignalValue::Vector(Cow::Borrowed("10101010")))?;
    writer.finish()?;
    Ok(())
}
```

Dense binary simulators can submit one timestamp at a time with
`emit_binary_batch(timestamp, &[(handle, value), ...])`, avoiding per-event type dispatch.

---

## Feature Flags

| Feature    | Default | Description                                                                  |
|------------|:-------:|------------------------------------------------------------------------------|
| `gzip`     | ✅      | Enable libdeflate-backed gzip hierarchy/wrapper and zlib VC compression.     |
| `lz4`      | ✅      | Support LZ4-compressed hierarchy blocks and value-change chains.             |
| `fastlz`   | ⛔️     | Add FastLZ decompression/compression for value-change chains.                |
| `parallel` | ✅      | Use Rayon for large chain compression/decompression jobs.                    |
| `serde`    | ⛔️     | Provide serialisable hierarchy and value-change snapshots (`serde_support`). |
| `mmap`     | ✅      | Provide seekable, explicitly unsafe `io::MemoryMap` input for `ReaderBuilder`.|
| `async`    | ⛔️     | Include buffered async wrappers (`async_support`) built on `tokio`.          |
| `simd`     | ✅      | Use SSE2 to accelerate ASCII vector packing (falls back to scalar elsewhere).|

Disable defaults with `--no-default-features` and enable the subset you need, for example:

```bash
cargo add wavefst --no-default-features --features "gzip parallel"
```

---

## Performance

Criterion benchmarks cover full trace creation and full value-change traversal for raw, zlib, LZ4,
and optional FastLZ configurations:

```bash
cargo bench
```

The cross-tool numbers below were measured on an Intel Xeon Gold 6148 with one process pinned to
CPU 12, Rust 1.98.0, and GCC 15.2.0. They are medians of five samples after warmup; every iteration
opens or creates the file and validates the resulting item count. Lower latency is better. The
comparison pins [Wellen](https://github.com/ekiwi/wellen) 0.25.6,
[libfst](https://github.com/gtkwave/libfst) at `cf74bef`, and
[libfstwriter](https://github.com/gtkwave/libfstwriter) at `6397a1e`. Absolute times depend on CPU,
filesystem cache, compiler, and waveform shape, so the scripts—not these numbers—are authoritative.

### Rust reader comparison: Wellen

These traces use zlib chains and contain deterministic one-bit values. `all` streams every change,
`one` streams only the first signal through each library's native filter, and `open` parses enough
metadata to count signals. Both readers are configured for a single thread.

| Shape | Operation | Items | wavefst | Wellen | wavefst relative |
|-------|-----------|------:|--------:|-------:|-----------------:|
| Dense: 512 × 128 | all | 65,536 | 2.01 ms | 13.77 ms | 6.83× faster |
| Dense: 512 × 128 | one | 128 | 0.112 ms | 0.379 ms | 3.39× faster |
| Dense: 512 × 128 | open | 512 | 0.095 ms | 0.305 ms | 3.23× faster |
| Wide: 8,192 × 256 | all | 2,097,152 | 60.38 ms | 319.26 ms | 5.29× faster |
| Wide: 8,192 × 256 | one | 256 | 2.38 ms | 4.12 ms | 1.73× faster |
| Wide: 8,192 × 256 | open | 8,192 | 1.21 ms | 3.88 ms | 3.21× faster |
| Long: 32 × 65,536 | all | 2,097,152 | 39.01 ms | 179.91 ms | 4.61× faster |
| Long: 32 × 65,536 | one | 65,536 | 2.05 ms | 8.31 ms | 4.06× faster |
| Long: 32 × 65,536 | open | 32 | 0.079 ms | 1.09 ms | 13.81× faster |

The generated files are 29,333 bytes (dense), 683,480 bytes (wide), and 342,899 bytes (long).
Reproduce the table with:

```bash
BENCH_CPU=12 SAMPLES=5 scripts/bench-wellen.sh
```

### Reference reader and writer: libfst

The bidirectional libfst harness uses 512 signals × 128 timestamps, zlib value chains, and both
implementations' output. This exposes format-dependent results instead of benchmarking each reader
only on its preferred layout.

| Operation | Input | wavefst | libfst | Result |
|-----------|-------|--------:|-------:|--------|
| Full read | wavefst FST | 2.29 ms | 2.75 ms | wavefst 1.20× faster |
| Full read | libfst FST | 2.19 ms | 2.67 ms | wavefst 1.22× faster |
| One signal | wavefst FST | 0.059 ms | 0.088 ms | wavefst 1.49× faster |
| One signal | libfst FST | 0.060 ms | 0.074 ms | wavefst 1.24× faster |
| Write own FST | own output | 6.66 ms | 9.48 ms | wavefst 1.42× faster |

wavefst output is 29,333 bytes and libfst output is 29,651 bytes. The one-signal wavefst rows use
`include_handles([1]).load_hierarchy(false)`, matching applications that already know the handle;
all format checks, geometry decoding, index validation, and event-count validation remain enabled.
Run the exact comparison with:

```bash
BENCH_CPU=12 SAMPLES=5 ITERATIONS=100 WARMUP=10 scripts/bench-libfst.sh
```

### Writer comparison: libfst and libfstwriter

The writer harness uses LZ4 chains and each implementation's packed binary API. `wavefst batch`
submits one timestamp as a slice; `wavefst scalar` submits individual changes. C libfst and C++14
libfstwriter receive packed 32-bit values through `fstWriterEmitValueChange32`. Parallel codecs are
disabled, so this measures single-threaded writer paths.

| Shape | Events | wavefst batch | wavefst scalar | libfst | libfstwriter |
|-------|-------:|--------------:|---------------:|-------:|-------------:|
| Dense: 512 × 128 | 65,536 | 1.70 ms | 2.81 ms | 4.81 ms | 6.36 ms |
| Wide: 8,192 × 256 | 2,097,152 | 48.14 ms | 75.70 ms | 157.04 ms | 176.33 ms |
| Long: 32 × 65,536 | 2,097,152 | 34.74 ms | 60.32 ms | 136.25 ms | 148.89 ms |

Against libfstwriter, wavefst batch is 3.75× faster on dense, 3.66× on wide, and 4.29× on long in
this harness. Output sizes remain directly comparable:

| Shape | wavefst bytes | libfst bytes | libfstwriter bytes |
|-------|--------------:|-------------:|-------------------:|
| Dense | 48,223 | 48,193 | 48,193 |
| Wide | 1,395,777 | 1,395,407 | 1,395,407 |
| Long | 1,049,820 | 1,258,921 | 1,258,921 |

Reproduce both timing and size tables with:

```bash
BENCH_CPU=12 SAMPLES=5 scripts/bench-writers.sh
```

### Choosing the fast path

The ordinary value-change iterator prioritises ergonomic per-event error handling. Hot paths can
choose ordered `try_for_each_parts`, cache-friendly handle-major `try_for_each_parts_unordered`, or
parallel thread-local `try_fold_parts_parallel`; ordered scalar reductions can use `try_fold_parts`
or the two-state `try_fold_binary` specialization. All retain bounds and format validation. Dense
single-bit writers should prefer `emit_binary_batch`.

Sparse consumers should set `include_handles` before opening the value-change stream instead of
decoding every chain and filtering callbacks afterward. It uses the FST chain index for direct
payload reads and also handles dynamic aliases without emitting an unselected canonical signal.

Large independent chain codecs use a lazily initialized pool capped at 32 workers, preventing
many-core hosts from spending more time on Rayon work stealing than on short FST streams. Explicit
parallel traversal APIs continue to use the application's global Rayon pool.

Single-threaded raw-chain writing assembles the final block directly without per-chain staging
buffers. Dynamic-chain deduplication uses a randomized fast hash while still comparing complete
chain bytes, so hash collisions cannot change alias correctness.

Gzip and zlib use bundled libdeflate through safe Rust bindings. Zlib value chains reuse compressor
state and scratch storage within a block, while chain readers reuse thread-local decompressors on
serial paths and one decompressor per Rayon partition. The reader's 64 KiB seek-aware buffer also
satisfies FST trailer/index backtracking from memory when possible instead of issuing redundant
kernel seeks. These optimizations do not change validation or the standard FST representation.

## Async, SIMD, and serde helpers

- `wavefst::async_support::{AsyncReader, AsyncWriter}` buffer async sources/sinks using `tokio`
  before delegating to the synchronous codecs. Useful when you cannot block the reactor thread.
- `wavefst::serde_support` (behind `serde`) snapshots hierarchy trees and value changes as owned data
  structures that plug directly into `serde_json`, CBOR, etc.
- `simd` enables an SSE2 fast path for ASCII vector packing; non-x86 targets automatically use the
  scalar implementation.

---

## Tooling

- **Tests** – `cargo test` (add `--features "async gzip serde simd"` to exercise optional paths).
- **Benches** – `cargo bench` compares reader/writer throughput across compression modes.
- **libfst comparison** – `scripts/bench-libfst.sh` measures cross-format full and selective reads
  plus dense zlib writing against the pinned C reference.
- **Rust reader comparison** – `scripts/bench-wellen.sh` compares full scans, indexed single-signal
  reads, and hierarchy opening against pinned Wellen across dense, wide, and long traces.
- **Writer comparison** – `scripts/bench-writers.sh` compares wavefst batch/scalar paths with pinned
  libfst and libfstwriter on identical LZ4 workloads.
- **Interop fixture** – the checked-in `hdl-example.fst` event count is compared with the reference
  `fstReaderIterBlocks` result.
- **Upstream oracle** – `scripts/test-libfst-interop.sh` compiles pinned libfst independently and
  checks the full generated corpus in both directions.
- **Example** – `cargo run --example write_fst -- out.fst` creates a GTKWave-ready trace; append
  `--wrapped` to exercise the whole-file gzip wrapper.

---

Static hierarchy aliases reuse their target handle, exactly as libfst does. Consequently,
`add_alias` returns the target handle and value-change iteration emits one canonical event rather
than inventing a second signal handle. `Header::time_zero` is signed metadata and is not added to
event timestamps.

---

## License

Licensed under the standard MIT License. See [LICENSE](./LICENSE) for details.
Release notes are maintained in [CHANGELOG.md](./CHANGELOG.md).
