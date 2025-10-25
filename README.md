# wavefst

[![Crates.io](https://img.shields.io/crates/v/wavefst.svg)](https://crates.io/crates/wavefst)
[![Documentation](https://img.shields.io/docsrs/wavefst)](https://docs.rs/wavefst)
[![License](https://img.shields.io/crates/l/wavefst.svg)](https://github.com/0xtaruhi/wavefst/blob/main/LICENSE)

> Modern Rust reader and writer for the Fast Signal Trace (FST) waveform format.

`wavefst` streams FST data without copying, stays compatible with the original C `libfst`
implementation, and layers ergonomic APIs on top of the low-level block layout. Readers, writers,
benchmarks, and fuzz targets all live in the same crate so you can inspect existing traces, build
new ones, or validate interoperability from a single dependency.

---

## Highlights

- **Full block coverage** – header, geometry, hierarchy, blackout, and every value-change variant
  (`FST_BL_VCDATA*`) are decoded and encoded symmetrically.
- **Zero-copy iteration** – value changes stream straight from decoded buffers with alias handling,
  initial frames, and timestamps resolved for you.
- **Pluggable compression** – raw, zlib, LZ4, and FastLZ logic is reusable between reader and writer
  pipelines, each gated behind a feature flag.
- **Async, SIMD, serde** – optional helpers wrap the synchronous APIs for async I/O, fast ASCII→bit
  packing, and serialisable hierarchy/value-change snapshots.
- **Tooling ready** – Criterion benches, libFuzzer harnesses, and integration tests are included to
  keep regressions in check.

---

## Installation

```bash
cargo add wavefst
```

The default feature set enables gzip/zlib (`gzip`), LZ4 (`lz4`), memory mapping (`mmap`), and the
SSE2 packed-bit fast path (`simd`). Disable them with `--no-default-features` and opt back into the
ones you need.

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
    header.vc_section_count = 1;
    header.end_time = 20;
    writer.write_header(header)?;

    writer.emit_change(0, bit, SignalValue::Bit('0'))?;
    writer.emit_change(10, bit, SignalValue::Bit('1'))?;
    writer.emit_change(12, vec, SignalValue::Vector(Cow::Borrowed("10101010")))?;
    writer.finish()?;
    Ok(())
}
```

---

## Feature Flags

| Feature    | Default | Description                                                                  |
|------------|:-------:|------------------------------------------------------------------------------|
| `gzip`     | ✅      | Enable zlib/deflate support (hierarchy and VC blocks, optional z-wrapper).   |
| `lz4`      | ✅      | Support LZ4-compressed hierarchy blocks and value-change chains.             |
| `fastlz`   | ⛔️     | Add FastLZ decompression/compression for value-change chains.                |
| `parallel` | ⛔️     | Use Rayon to decode chain payloads in parallel while keeping results sorted. |
| `serde`    | ⛔️     | Provide serialisable hierarchy and value-change snapshots (`serde_support`). |
| `mmap`     | ✅      | Expose the memory-mapped reader backend (`io::MemoryMap`).                   |
| `async`    | ⛔️     | Include buffered async wrappers (`async_support`) built on `tokio`.          |
| `simd`     | ✅      | Use SSE2 to accelerate ASCII vector packing (falls back to scalar elsewhere).|

Disable defaults with `--no-default-features` and enable the subset you need, for example:

```bash
cargo add wavefst --no-default-features --features "gzip parallel"
```

---

## Performance

### Criterion benches (release profile, Apple M-series, macOS 25.0.0)

| Benchmark                   | Raw     | Zlib    | LZ4     |
|-----------------------------|---------|---------|---------|
| `reader_next_value_changes` | 32.3 µs | 41.7 µs | 34.2 µs |
| `writer_emit_change`        | 115 µs  | 194 µs  | 125 µs  |

Run with:

```bash
cargo bench
```

### Comparison with C `libfst`

Using the upstream `fstReaderIterBlocks` helper (compiled with `clang -std=c99 -O2`):

| Scenario          | Rust (µs) | C helper (µs) | Speed-up |
|-------------------|----------:|--------------:|---------:|
| baseline, raw     |    50.9   |     335 551   | ×6 590   |
| baseline, wrapped |    57.0   |       6 436   | ×113     |
| wide, raw         |   130.3   |       2 933   | ×22.5    |
| wide, wrapped     |    65.3   |       2 191   | ×33.6    |

Debug builds show similar ratios (×4.6–×5.6). Event counters matched exactly between both
implementations.

---

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
- **Docs** – `doc/fst_format.md` describes the binary format; `doc/rust_crate_design.md` covers the
  crate layout and design notes (both ASCII safe).

---

## Roadmap

- Optional CLI (`wavefst-tool`) for inspecting traces and converting to other formats.
- Additional fixtures that exercise mixed compression, alias chains, and multi-block timelines.
- Configurable logging hooks (`tracing`) for long-running ingest jobs.

Contributions, bug reports, and ideas are very welcome. File an issue or open a pull request with a
reproduction trace if you hit a corner case.

---

## License

Licensed under the modified MIT License. See [LICENSE](./LICENSE) for details.
