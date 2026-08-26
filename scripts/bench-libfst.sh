#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
libfst_source=${LIBFST_SOURCE:-"$repo_root/target/libfst-reference"}
libfst_revision=cf74bef8d0435eceb20524fe6f5674e0ecb68b25
bench_dir="$repo_root/target/libfst-bench"
rust_bin="$repo_root/target/release/examples/libfst_bench"
c_bin="$bench_dir/libfst-bench"
rust_trace="$bench_dir/wavefst.fst"
c_trace="$bench_dir/libfst.fst"
cpu=${BENCH_CPU:-12}
iterations=${ITERATIONS:-100}
warmup=${WARMUP:-10}

if [[ ! -f "$libfst_source/src/fstapi.c" ]]; then
    git clone --quiet https://github.com/gtkwave/libfst.git "$libfst_source"
    git -C "$libfst_source" checkout --quiet --detach "$libfst_revision"
fi
actual_revision=$(git -C "$libfst_source" rev-parse HEAD)
if [[ "$actual_revision" != "$libfst_revision" ]]; then
    echo "libfst source is at $actual_revision; expected $libfst_revision" >&2
    exit 1
fi

mkdir -p "$bench_dir"
cargo build --quiet --release --no-default-features --features gzip --example libfst_bench
cc -std=c11 -O3 -D_GNU_SOURCE \
    -I"$libfst_source/src" \
    "$repo_root/tests/reference/libfst_bench.c" \
    "$libfst_source/src/fstapi.c" \
    "$libfst_source/src/fastlz.c" \
    "$libfst_source/src/lz4.c" \
    -lz -o "$c_bin"

run_pinned() {
    taskset -c "$cpu" "$@"
}

rust_write=$(run_pinned "$rust_bin" write "$rust_trace" "$iterations" "$warmup")
libfst_write=$(run_pinned "$c_bin" write "$c_trace" "$iterations" "$warmup")
rust_read_rust=$(run_pinned "$rust_bin" read "$rust_trace" "$iterations" "$warmup")
libfst_read_rust=$(run_pinned "$c_bin" read "$rust_trace" "$iterations" "$warmup")
rust_read_libfst=$(run_pinned "$rust_bin" read "$c_trace" "$iterations" "$warmup")
libfst_read_libfst=$(run_pinned "$c_bin" read "$c_trace" "$iterations" "$warmup")

echo "implementation,operation,input,ns_per_iteration,file_bytes,events"
echo "wavefst,write,wavefst,$rust_write,$(stat -c %s "$rust_trace"),65536"
echo "libfst,write,libfst,$libfst_write,$(stat -c %s "$c_trace"),65536"
echo "wavefst,read,wavefst,$rust_read_rust,$(stat -c %s "$rust_trace"),65536"
echo "libfst,read,wavefst,$libfst_read_rust,$(stat -c %s "$rust_trace"),65536"
echo "wavefst,read,libfst,$rust_read_libfst,$(stat -c %s "$c_trace"),65536"
echo "libfst,read,libfst,$libfst_read_libfst,$(stat -c %s "$c_trace"),65536"
