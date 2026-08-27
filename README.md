# wavefst

[![Crates.io](https://img.shields.io/crates/v/wavefst.svg)](https://crates.io/crates/wavefst)
[![Documentation](https://img.shields.io/docsrs/wavefst)](https://docs.rs/wavefst)
[![CI](https://github.com/0xtaruhi/wavefst/actions/workflows/ci.yml/badge.svg)](https://github.com/0xtaruhi/wavefst/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/wavefst.svg)](https://github.com/0xtaruhi/wavefst/blob/main/LICENSE)

> Safe, fast Rust reader and writer for GTKWave's Fast Signal Trace (FST) format.

`wavefst` reads existing FST waveforms and writes files accepted by GTKWave and the reference
`libfst` implementation. It supports indexed signal and time-range selection for interactive
viewers, bounded streaming for simulators, and the standard FST compression formats.

## Why wavefst?

- **Bidirectional compatibility** – a pinned libfst oracle verifies every supported disk layout in
  both directions.
- **Selective access** – load chosen handles and time windows without decompressing unrelated
  signal chains or value-change blocks.
- **Reader and writer** – one crate covers waveform analysis, viewers, converters, and simulators.
- **Controlled resources** – streaming block checkpoints and configurable allocation limits keep
  large traces bounded.
- **Explicit performance controls** – serial execution is the default; parallel codecs, async I/O,
  serde, FastLZ, and SIMD helpers are feature-selected.

## Installation

```bash
cargo add wavefst
```

Default features include the reader, writer, libdeflate-backed gzip/zlib, LZ4, and the SSE2 packed
bit writer path. The `gzip` feature builds bundled libdeflate and therefore requires a C compiler.

## Quick start

### Read selected signals and time

```rust
use wavefst::ReaderBuilder;

fn read_window(path: &str) -> wavefst::Result<()> {
    let file = std::fs::File::open(path)?;
    let mut reader = ReaderBuilder::new(file)
        .include_handles([1, 7, 42])
        .time_range(1_000..=2_000)
        .build()?;

    while let Some(changes) = reader.next_value_changes()? {
        changes.try_for_each_parts(|time, handle, alias_of, value| {
            println!("{time} {handle} {alias_of:?} {value:?}");
        })?;
    }
    Ok(())
}
```

Omit either filter to read every handle or timestamp. Handles are one-based and time ranges are
inclusive. Applications that already know their handles can add `.load_hierarchy(false)`.

### Write a trace

```rust
use wavefst::{FstWriter, GeomEntry, Header, ScopeType, SignalValue, VarDir, VarType};

fn write_trace(path: &str) -> wavefst::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = FstWriter::builder(file).build()?;

    writer.begin_scope(ScopeType::VcdModule, "tb", None)?;
    let signal = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "valid",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;
    writer.emit_change(0, signal, SignalValue::Bit('0'))?;
    writer.emit_change(10, signal, SignalValue::Bit('1'))?;
    writer.finish()?;
    Ok(())
}
```

Dense binary simulators can submit a whole timestamp through `emit_binary_batch`, avoiding
per-event type dispatch.

## Features

| Feature | Default | Purpose |
|---------|:-------:|---------|
| `reader` | yes | FST metadata, hierarchy, indexed selection, and value traversal |
| `writer` | yes | Streaming FST creation and chain deduplication |
| `gzip` | yes | libdeflate-backed gzip wrappers/hierarchies and zlib chains |
| `lz4` | yes | LZ4 hierarchy blocks and value chains |
| `simd` | yes | SSE2 ASCII-to-bit packing; implies `writer` |
| `fastlz` | no | FastLZ value-chain compression and decompression |
| `parallel` | no | Rayon codec and traversal support; runtime policy remains serial |
| `async-read` / `async-write` | no | Tokio-backed async front ends |
| `serde` | no | Serializable hierarchy and value-change snapshots |

Features are additive. Minimal reader-only and writer-only builds are supported:

```bash
cargo add wavefst --no-default-features --features "reader gzip lz4"
```

## Performance

The checked-in benchmark suite measures wall time, physical and logical I/O, peak RSS, CPU cycles,
output size, and delivered event counts. The headline selective-access workload contains 500,000
signals, 100 timestamps, and 50 million changes:

| Query | wavefst | libfst | fst-reader 0.17 | Wellen 0.25.6 |
|-------|--------:|-------:|----------------:|--------------:|
| 10 signals | **0.365 s** | 0.435 s | 0.697 s | 1.160 s |
| 100 signals | **0.363 s** | 0.444 s | 0.803 s | 1.192 s |
| 1% time | **0.041 s** | 0.473 s | 2.525 s | 130.959 s |
| 100 signals × 1% time | **0.022 s** | 0.030 s | 0.024 s | 1.215 s |
| 100 dragged viewports | **0.398 s** | 0.707 s | 0.805 s | 1.206 s |

These are reproducible workload results, not universal claims. In a paired cold-cache run, libfst
led the combined signal/time query by 0.6 ms while wavefst read fewer bytes and used fewer cycles.
See [BENCHMARKS.md](BENCHMARKS.md) for complete tables, methodology, limitations, pinned revisions,
raw CSV links, and reproduction commands.

## Compatibility and quality

The [compatibility matrix](COMPATIBILITY.md) maps every libfst disk tag to reader/writer support.
CI compiles the pinned C reference implementation and verifies a generated corpus bidirectionally,
in addition to running:

- Rustfmt, Clippy with all features, and rustdoc with warnings denied;
- stable tests on Linux, macOS, and Windows, plus beta Rust;
- reader/writer feature combinations and a selective-access smoke benchmark;
- package verification before release.

Useful entry points:

- [API documentation](https://docs.rs/wavefst)
- [Examples](./examples/)
- [Complete benchmarks](BENCHMARKS.md)
- [Format compatibility](COMPATIBILITY.md)
- [Release notes](CHANGELOG.md)

## License

Licensed under the standard [MIT License](./LICENSE).
