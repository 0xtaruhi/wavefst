#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
manifest="$repo_root/benchmarks/ecosystem/Cargo.toml"
bench_dir="$repo_root/target/wellen-bench"
target_dir="$repo_root/target/wellen-bench-cargo"
bench_bin="$target_dir/release/wavefst-ecosystem-bench"
cpu=${BENCH_CPU:-12}
samples=${SAMPLES:-5}

if ((samples == 0 || samples % 2 == 0)); then
    echo "SAMPLES must be a positive odd integer" >&2
    exit 1
fi

mkdir -p "$bench_dir"
CARGO_TARGET_DIR="$target_dir" cargo build --quiet --release --manifest-path "$manifest"

run_pinned() {
    taskset -c "$cpu" "$@"
}

median_pinned() {
    local values=()
    local sample
    for ((sample = 0; sample < samples; ++sample)); do
        values+=("$(run_pinned "$@")")
    done
    printf '%s\n' "${values[@]}" | sort -n | sed -n "$((samples / 2 + 1))p"
}

run_scenario() {
    local scenario=$1
    local signals=$2
    local steps=$3
    local iterations=$4
    local warmup=$5
    local trace="$bench_dir/$scenario.fst"
    local mode
    local implementation
    local operation
    local items

    "$bench_bin" generate "$trace" "$signals" "$steps" 1 0 >/dev/null
    for operation in all one open; do
        case "$operation" in
            all) items=$((signals * steps)) ;;
            one) items=$steps ;;
            open) items=$signals ;;
        esac
        for implementation in wavefst wellen; do
            mode="$implementation-$operation"
            result=$(median_pinned \
                "$bench_bin" "$mode" "$trace" "$signals" "$steps" "$iterations" "$warmup")
            echo "$implementation,$scenario,$operation,$result,$(stat -c %s "$trace"),$items"
        done
    done
}

echo "implementation,scenario,operation,ns_per_iteration,file_bytes,items"
run_scenario dense 512 128 200 20
run_scenario wide 8192 256 10 2
run_scenario long 32 65536 10 2
