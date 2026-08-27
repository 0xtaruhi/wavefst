# wavefst benchmarks

This document contains the complete checked-in performance report. It separates reproducible
workload evidence from the concise project overview in the README. Absolute results depend on the
CPU, compiler, waveform shape, filesystem, and page-cache state; the scripts and raw data are
authoritative.

## Test system

Unless a section says otherwise, results were measured on an Intel Xeon Gold 6148 with one process
pinned to CPU 12, Rust 1.98.0, and GCC 15.2.0. The comparisons pin:

- [libfst](https://github.com/gtkwave/libfst) at `cf74bef`;
- [libfstwriter](https://github.com/gtkwave/libfstwriter) at `6397a1e`;
- [Wellen](https://github.com/ekiwi/wellen) 0.25.6;
- fst-reader 0.17 and wavefst 0.2.2 where shown.

Every benchmark validates the resulting item count. Timing tables use medians after warmup and
keep each library's public API costs visible rather than normalizing them away.

## 500k-signal selective access

The selective suite uses a 51,167,296-byte trace containing 500,000 one-bit signals, 100
timestamps, and 50 million changes. It records wall time, Linux physical and logical read bytes,
peak RSS, CPU cycles, delivered changes, and query counts in cold- and warm-cache modes.

The cases are:

- A: select 10 random signals;
- B: select 100 random signals;
- C: read the middle 1% of the complete time range;
- D: combine 100 selected signals with that 1% range;
- E: perform 100 consecutive 1%-wide viewport queries.

Warm-cache wall-time medians are:

| Case | wavefst current | wavefst 0.2.2 | fst-reader 0.17 | libfst | Wellen 0.25.6 |
|------|----------------:|--------------:|----------------:|-------:|--------------:|
| A — 10 signals | **0.365 s** | 10.878 s | 0.697 s | 0.435 s | 1.160 s |
| B — 100 signals | **0.363 s** | 11.101 s | 0.803 s | 0.444 s | 1.192 s |
| C — 1% time | **0.041 s** | 0.287 s | 2.525 s | 0.473 s | 130.959 s |
| D — 100 signals × 1% | **0.022 s** | 0.317 s | 0.024 s | 0.030 s | 1.215 s |
| E — 100 viewports | **0.398 s** | 23.389 s | 0.805 s | 0.707 s | 1.206 s |

The resource comparison against libfst uses cold-cache wall time and physical reads, plus
warm-cache RSS and cycles. The paired C/D run used three samples per tool. D's cold-wall median
varied in libfst's favour by 0.6 ms even though wavefst used fewer bytes and cycles.

| Case | Cold wall: wavefst / libfst | Cold read bytes: wavefst / libfst | Warm RSS KiB: wavefst / libfst | CPU cycles: wavefst / libfst |
|------|----------------------------:|----------------------------------:|--------------------------------:|-----------------------------:|
| A | **0.419 / 0.466 s** | 50,114,560 / **50,081,792** | **9,776 / 11,160** | **1,034M / 1,241M** |
| B | **0.413 / 0.476 s** | 50,114,560 / **50,081,792** | **9,860 / 12,128** | **1,036M / 1,245M** |
| C | **0.037 / 0.468 s** | **1,269,760 / 1,318,912** | **17,232 / 17,392** | **45M / 319M** |
| D | 0.0252 / **0.0246 s** | **1,269,760 / 1,318,912** | **9,352 / 12,072** | **17M / 27M** |
| E | **0.447 / 0.740 s** | 50,114,560 / **50,081,792** | **9,864 / 16,932** | **1,046M / 1,999M** |

The raw samples are checked in as:

- [`reference-results-wavefst-head-xeon-6148.csv`](benchmarks/selective/reference-results-wavefst-head-xeon-6148.csv)
- [`reference-results-cd-paired-xeon-6148.csv`](benchmarks/selective/reference-results-cd-paired-xeon-6148.csv)
- [`reference-results-xeon-6148.csv`](benchmarks/selective/reference-results-xeon-6148.csv)

Wellen 0.25.6 does not expose a time-range argument to `load_signals`, so Case C reports the cost
of its public API. Libfst emits block-frame values for some bounded queries, producing twice as many
C/D callbacks and 19,900 rather than 10,000 callbacks in E. See the
[selective benchmark protocol](benchmarks/selective/README.md) for the exact semantics.

Reproduce the suite with:

```bash
BENCH_CPU=12 SAMPLES=3 scripts/bench-selective.sh
```

## Rust reader comparison: Wellen

These zlib-chain traces contain deterministic one-bit values. `all` streams every change, `one`
loads only the first signal through each library's native filter, and `open` parses enough metadata
to count signals. Both readers use one thread.

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

```bash
BENCH_CPU=12 SAMPLES=5 scripts/bench-wellen.sh
```

## Reference reader and writer: libfst

The bidirectional harness uses 512 signals × 128 timestamps, zlib value chains, and both
implementations' output. This exposes format-dependent results instead of benchmarking each reader
only on its preferred layout.

| Operation | Input | wavefst | libfst | Result |
|-----------|-------|--------:|-------:|--------|
| Full read | wavefst FST | 2.29 ms | 2.75 ms | wavefst 1.20× faster |
| Full read | libfst FST | 2.19 ms | 2.67 ms | wavefst 1.22× faster |
| One signal | wavefst FST | 0.059 ms | 0.088 ms | wavefst 1.49× faster |
| One signal | libfst FST | 0.060 ms | 0.074 ms | wavefst 1.24× faster |
| Write own FST | own output | 6.66 ms | 9.48 ms | wavefst 1.42× faster |

Wavefst output is 29,333 bytes and libfst output is 29,651 bytes. The one-signal wavefst rows use
`include_handles([1]).load_hierarchy(false)`, matching applications that already know the handle;
format checks, geometry decoding, index validation, and event-count validation remain enabled.

```bash
BENCH_CPU=12 SAMPLES=5 ITERATIONS=100 WARMUP=10 scripts/bench-libfst.sh
```

## Writer comparison: libfst and libfstwriter

The writer harness uses LZ4 chains and each implementation's packed binary API. `wavefst batch`
submits one timestamp as a slice; `wavefst scalar` submits individual changes. C libfst and C++14
libfstwriter receive packed 32-bit values through `fstWriterEmitValueChange32`. Parallel codecs are
disabled, so this measures single-threaded writer paths.

| Shape | Events | wavefst batch | wavefst scalar | libfst | libfstwriter |
|-------|-------:|--------------:|---------------:|-------:|-------------:|
| Dense: 512 × 128 | 65,536 | 1.70 ms | 2.81 ms | 4.81 ms | 6.36 ms |
| Wide: 8,192 × 256 | 2,097,152 | 48.14 ms | 75.70 ms | 157.04 ms | 176.33 ms |
| Long: 32 × 65,536 | 2,097,152 | 34.74 ms | 60.32 ms | 136.25 ms | 148.89 ms |

Output sizes remain directly comparable:

| Shape | wavefst bytes | libfst bytes | libfstwriter bytes |
|-------|--------------:|-------------:|-------------------:|
| Dense | 48,223 | 48,193 | 48,193 |
| Wide | 1,395,777 | 1,395,407 | 1,395,407 |
| Long | 1,049,820 | 1,258,921 | 1,258,921 |

```bash
BENCH_CPU=12 SAMPLES=5 scripts/bench-writers.sh
```

## Choosing an API path

The ordinary value-change iterator prioritizes ergonomic per-event error handling. Hot paths can
choose:

- `try_for_each_parts` for ordered traversal without constructing event wrappers;
- `try_for_each_parts_unordered` for cache-friendly handle-major traversal;
- `try_fold_parts` or `try_fold_binary` for ordered scalar reductions;
- `try_fold_parts_parallel` for parallel thread-local reduction;
- `emit_binary_batch` for dense single-bit writer input.

Sparse consumers should set `include_handles` before opening the value-change stream instead of
decoding every chain and filtering callbacks afterward. For dragged viewports, keep the reader open
and update `set_time_range` and `set_included_handles`; parsed metadata and the source are retained.

The `parallel` feature compiles parallel capability but does not create workers by default. Reader
and writer builders remain serial until the application selects `CodecParallelism::Auto` or
`CodecParallelism::Threads(n)`. `Auto` uses at most 32 workers, private pools are created lazily and
reused by width, and explicit parallel traversal uses the application's global Rayon pool.

## Relevant implementation details

- Single-threaded raw-chain writing assembles the final block without per-chain staging buffers.
- Dynamic-chain deduplication uses a fast randomized hash and compares complete bytes, so hash
  collisions cannot affect correctness.
- Gzip and zlib use bundled libdeflate through safe Rust bindings; serial readers reuse thread-local
  decompressors and parallel readers keep one decompressor per Rayon partition.
- Sequential scans use an 8 KiB seek-aware buffer, while initially time-filtered scans use 1 KiB to
  avoid block-header read amplification and still satisfy nearby trailer/index backtracking.

These choices do not weaken validation or alter the standard FST representation.
