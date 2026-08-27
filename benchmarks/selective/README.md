# Selective FST reader benchmark

This benchmark measures interactive waveform access, not sequential whole-file throughput. It
uses one deterministic FST with 500,000 single-bit signals, 100 timestamps, and one value-change
block per timestamp. Every signal toggles at every timestamp so a narrow time or signal selection
does real decoding work and cannot win merely because the selected region is empty or a reader
coalesces duplicate assignments.

The compared implementations are pinned exactly:

- the current wavefst workspace (`wavefst-head`), single-threaded;
- `wavefst = 0.2.2`, single-threaded;
- `fst-reader = 0.17.0`;
- libfst revision `cf74bef8d0435eceb20524fe6f5674e0ecb68b25`;
- Wellen `0.25.6`, using `simple::Waveform::load_signals` with `multi_thread = false`.

## Workloads

| Case | Query |
|------|-------|
| A | deterministic random 10 of 500k signals, full time range |
| B | deterministic random 100 of 500k signals, full time range |
| C | all 500k signals, the middle 1% time window |
| D | deterministic random 100 signals, the middle 1% time window |
| E | 100 consecutive queries that drag a fixed-width 1% viewport from 0% through 100%, selecting the same 100 signals |
| F | each A–E sample after `fsync` and `POSIX_FADV_DONTNEED` on the trace |
| G | each A–E sample after a complete sequential read into the page cache |

Case E retains one reader/source session when the public API permits it. Current wavefst,
`fst-reader`, libfst, and Wellen therefore parse the hierarchy once; Wellen also loads the selected
signals once and serves all viewport positions from that result. wavefst 0.2.2 has an immutable
builder-time range, so it must reopen for each position. Wellen's `load_signals` has no time-range
argument: C/D load the requested signals in full and only then consume changes inside the viewport.
These results reflect the libraries' best public API paths and do not describe callback filtering
as native time-window skipping.

## Metrics

The CSV contains:

- `wall_ns`: monotonic wall time around the complete public-library operation;
- `read_bytes`: Linux `/proc/self/io` storage-layer bytes actually fetched for the process;
- `rchar`: logical bytes returned by read-family system calls, useful when `read_bytes` is zero on a
  warm cache;
- `peak_rss_kib`: `getrusage(RUSAGE_SELF).ru_maxrss`;
- `cpu_cycles`: `perf stat -e cycles:u`, or `NA` when hardware counters are unavailable;
- `changes`: consumed value changes, preventing dead-code elimination and exposing semantic
  mismatches;
- `queries`: 1 for A–D and 100 for E.

`POSIX_FADV_DONTNEED` is an advisory per-file eviction, not a machine-wide cache reset. For a
strictly isolated cold-cache run, execute on an otherwise idle host and arrange a privileged
machine-wide cache drop outside the script, or set `COLD_CACHE_CMD` to a command provided by the
benchmark operator. The trace path is exported to that command as `TRACE`. The script never invokes
`sudo` or changes global VM state by default. Linux `/proc/self/io` is process-attributed kernel I/O
accounting; on ZFS, network filesystems, and device-mapper stacks it must not be interpreted as
physical bytes observed at the storage device.

## Run

The full data set contains 50 million changes and is generated once, so allow substantial time and
disk space on the first run:

```bash
BENCH_CPU=12 SAMPLES=3 scripts/bench-selective.sh
```

For a quick harness smoke test without claiming representative results:

```bash
SIGNALS=1000 STEPS=100 SAMPLES=1 TRACE=target/selective-bench/smoke.fst \
  RESULTS=target/selective-bench/smoke.csv scripts/bench-selective.sh
```

Do not reuse a trace with different `SIGNALS` or `STEPS`; use a distinct `TRACE` path. Raw samples
are written to `target/selective-bench/results.csv` by default so medians and distributions can be
computed without hiding variance. The runner validates the trace's handle count, time range, and
value-change section count before measuring. `CASES`, `CACHE_MODES`, and `TOOLS` accept
space-separated subsets for focused reruns.

## Reference diagnostic run

The original pinned cross-tool diagnostic is checked in as
[`reference-results-xeon-6148.csv`](reference-results-xeon-6148.csv); current wavefst's three-sample
run is [`reference-results-wavefst-head-xeon-6148.csv`](reference-results-wavefst-head-xeon-6148.csv),
with paired post-optimization C/D samples in
[`reference-results-cd-paired-xeon-6148.csv`](reference-results-cd-paired-xeon-6148.csv).
They were measured on an Intel
Xeon Gold 6148, CPU 12, Linux 5.15, Rust 1.98.0, GCC 15.2.0, and an ext4-backed 51,167,296-byte
trace. The baseline columns below are the original single diagnostic sample; the current wavefst
column is a median of three samples. Run the suite on the target machine before making deployment
claims. The warm-cache wall times were:

| Case | wavefst current | wavefst 0.2.2 | fst-reader 0.17 | libfst | Wellen load_signals |
|------|----------------:|--------------:|----------------:|-------:|--------------------:|
| A | **0.365 s** | 10.878 s | 0.697 s | 0.435 s | 1.160 s |
| B | **0.363 s** | 11.101 s | 0.803 s | 0.444 s | 1.192 s |
| C | **0.041 s** | 0.287 s | 2.525 s | 0.473 s | 130.959 s |
| D | **0.022 s** | 0.317 s | 0.024 s | 0.030 s | 1.215 s |
| E | **0.398 s** | 23.389 s | 0.805 s | 0.707 s | 1.206 s |

The raw CSV contains every requested metric. Current wavefst's cold-cache medians are 0.419s,
0.413s, 0.037s, 0.025s, and 0.447s for A–E. It attributed 50,114,560 bytes for A/B/E and 1,269,760
bytes for C/D; warm runs recorded zero `read_bytes`. In the paired C/D run, libfst read 1,318,912
bytes and reached 17,392 KiB RSS in C, versus wavefst's 17,232 KiB. Logical I/O exposes
algorithmic amplification that cache state hides:
fst-reader C issued 30.5 GiB and Wellen C 381.6 GiB of `rchar`. Wellen C also reached 580.7 MiB
peak RSS. libfst reports twice as many C/D callbacks and 19,900 rather than 10,000 callbacks in E
because it exposes block-frame values at bounded-range starts.
