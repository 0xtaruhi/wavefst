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
- **Pluggable compression** – uncompressed chains plus zlib, LZ4, and FastLZ payloads are supported
  behind feature flags. Raw chains still use the standard `'Z'` block marker.
- **Async, SIMD, serde** – optional helpers wrap the synchronous APIs for async I/O, fast ASCII→bit
  packing, and serialisable hierarchy/value-change snapshots.
- **Tooling ready** – Criterion benchmarks, feature-matrix tests, and conformance fixtures are
  included to keep regressions in check.

---

## Installation

```bash
cargo add wavefst
```

The default feature set enables gzip/zlib (`gzip`), LZ4 (`lz4`), memory mapping (`mmap`), parallel
chain codecs (`parallel`), and the SSE2 packed-bit fast path (`simd`). Disable them with
`--no-default-features` and opt back into the ones you need.

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
    // Time range, handle counts, and VC section count are backpatched by finish().
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
| `gzip`     | ✅      | Enable gzip hierarchy/wrapper and zlib VC compression.                       |
| `lz4`      | ✅      | Support LZ4-compressed hierarchy blocks and value-change chains.             |
| `fastlz`   | ⛔️     | Add FastLZ decompression/compression for value-change chains.                |
| `parallel` | ✅      | Use Rayon for large chain compression/decompression jobs.                    |
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

Criterion benchmarks cover full trace creation and full value-change traversal for raw, zlib, LZ4,
and optional FastLZ configurations. Run them on the target machine with:

```bash
cargo bench
```

The ordinary value-change iterator prioritises ergonomic per-event error handling. For hot analysis
loops, consume a block with `changes.try_for_each(|event| { ... })`; this removes the iterator's
`Option<Result<_>>` layer while retaining format validation.

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
- **Interop fixture** – the checked-in `hdl-example.fst` event count is compared with the reference
  `fstReaderIterBlocks` result.
- **Example** – `cargo run --example write_fst -- out.fst` creates a GTKWave-ready trace; append
  `--wrapped` to exercise the whole-file gzip wrapper.

---

Static hierarchy aliases reuse their target handle, exactly as libfst does. Consequently,
`add_alias` returns the target handle and value-change iteration emits one canonical event rather
than inventing a second signal handle. `Header::time_zero` is signed metadata and is not added to
event timestamps.

---

## License

Licensed under the modified MIT License. See [LICENSE](./LICENSE) for details.
