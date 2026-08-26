#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
manifest="$repo_root/benchmarks/selective/Cargo.toml"
bench_dir=${BENCH_DIR:-"$repo_root/target/selective-bench"}
trace=${TRACE:-"$bench_dir/selective-500k.fst"}
libfst_source=${LIBFST_SOURCE:-"$repo_root/target/libfst-reference"}
libfst_revision=cf74bef8d0435eceb20524fe6f5674e0ecb68b25
rust_bin="$bench_dir/wavefst-selective-bench"
c_bin="$bench_dir/libfst-selective"
signals=${SIGNALS:-500000}
steps=${STEPS:-100}
samples=${SAMPLES:-3}
bench_cpu=${BENCH_CPU:-}
results=${RESULTS:-"$bench_dir/results.csv"}
cases=${CASES:-"A B C D E"}
cache_modes=${CACHE_MODES:-"F-cold G-warm"}
tools=${TOOLS:-"wavefst-head wavefst-0.2.2 fst-reader-0.17 libfst wellen-load-signals"}

if ((signals < 100 || steps < 2 || samples < 1)); then
    echo "SIGNALS must be at least 100, STEPS at least 2, and SAMPLES positive" >&2
    exit 1
fi

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
cargo build --quiet --release --manifest-path "$manifest"
cp "$(dirname "$manifest")/target/release/wavefst-selective-bench" "$rust_bin"
cc -std=c11 -O3 -DNDEBUG -D_GNU_SOURCE \
    -I"$libfst_source/src" \
    "$repo_root/benchmarks/selective/libfst_selective.c" \
    "$libfst_source/src/fstapi.c" \
    "$libfst_source/src/fastlz.c" \
    "$libfst_source/src/lz4.c" \
    -lz -o "$c_bin"

if [[ ! -f "$trace" ]]; then
    "$rust_bin" generate "$trace" "$signals" "$steps" >&2
fi
"$rust_bin" validate "$trace" "$signals" "$steps"

perf_available=0
if command -v perf >/dev/null 2>&1 && perf stat -e cycles:u -- true >/dev/null 2>&1; then
    perf_available=1
else
    echo "warning: perf hardware cycles are unavailable; cpu_cycles will be NA" >&2
fi

run_pinned() {
    if [[ -n "$bench_cpu" ]]; then
        taskset -c "$bench_cpu" "$@"
    else
        "$@"
    fi
}

run_one() {
    local tool=$1
    local case_name=$2
    local cache=$3
    local sample=$4
    local output
    local perf_output
    local cycles=NA
    local command_line=()
    local tool_name
    local reported_case
    local wall_ns
    local read_bytes
    local rchar
    local peak_rss_kib
    local changes
    local checksum
    local queries

    output=$(mktemp "$bench_dir/output.XXXXXX")
    perf_output=$(mktemp "$bench_dir/perf.XXXXXX")
    if [[ "$cache" == "F-cold" ]]; then
        if [[ -n "${COLD_CACHE_CMD:-}" ]]; then
            TRACE="$trace" bash -c "$COLD_CACHE_CMD"
        else
            "$rust_bin" cache evict "$trace"
        fi
    else
        "$rust_bin" cache warm "$trace"
    fi

    if [[ "$tool" == "libfst" ]]; then
        command_line=("$c_bin" "$case_name" "$trace" "$signals" "$steps")
    else
        command_line=("$rust_bin" run "$tool" "$case_name" "$trace" "$signals" "$steps")
    fi

    if ((perf_available)); then
        if [[ -n "$bench_cpu" ]]; then
            perf stat --no-big-num -x, -e cycles:u -o "$perf_output" -- \
                taskset -c "$bench_cpu" "${command_line[@]}" >"$output"
        else
            perf stat --no-big-num -x, -e cycles:u -o "$perf_output" -- \
                "${command_line[@]}" >"$output"
        fi
        cycles=$(awk -F, '$3 ~ /^cycles/ { gsub(/ /, "", $1); print $1; exit }' "$perf_output")
        [[ -n "$cycles" ]] || cycles=NA
    else
        run_pinned "${command_line[@]}" >"$output"
    fi

    IFS=$'\t' read -r tool_name reported_case wall_ns read_bytes rchar peak_rss_kib changes checksum queries <"$output"
    if [[ "$reported_case" != "$case_name" ]]; then
        echo "benchmark returned case $reported_case, expected $case_name" >&2
        exit 1
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$case_name" "$cache" "$tool_name" "$sample" "$wall_ns" "$read_bytes" "$rchar" \
        "$peak_rss_kib" "$cycles" "$changes" "$queries" | tee -a "$results"
    rm -f "$output" "$perf_output"
}

printf '%s\n' "case,cache,tool,sample,wall_ns,read_bytes,rchar,peak_rss_kib,cpu_cycles,changes,queries" >"$results"
for cache in $cache_modes; do
    for case_name in $cases; do
        for sample in $(seq 1 "$samples"); do
            for tool in $tools; do
                run_one "$tool" "$case_name" "$cache" "$sample"
            done
        done
    done
done

echo "results: $results" >&2
