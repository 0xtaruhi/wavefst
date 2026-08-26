#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
libfst_source=${LIBFST_SOURCE:-"$repo_root/target/libfst-reference"}
libfst_revision=cf74bef8d0435eceb20524fe6f5674e0ecb68b25
libfstwriter_source=${LIBFSTWRITER_SOURCE:-"$repo_root/target/libfstwriter-reference"}
libfstwriter_revision=6397a1e2bb023324e8ccf51e48865bd27132be63
bench_dir="$repo_root/target/writer-bench"
rust_bin="$repo_root/target/release/examples/writer_ecosystem_bench"
libfst_bin="$bench_dir/libfst-writer-bench"
libfstwriter_bin="$bench_dir/libfstwriter-bench"
lz4_object="$bench_dir/lz4.o"
cpu=${BENCH_CPU:-12}
samples=${SAMPLES:-5}

if ((samples == 0 || samples % 2 == 0)); then
    echo "SAMPLES must be a positive odd integer" >&2
    exit 1
fi

if [[ ! -f "$libfst_source/src/fstapi.c" ]]; then
    git clone --quiet https://github.com/gtkwave/libfst.git "$libfst_source"
    git -C "$libfst_source" checkout --quiet --detach "$libfst_revision"
fi
if [[ "$(git -C "$libfst_source" rev-parse HEAD)" != "$libfst_revision" ]]; then
    echo "libfst checkout does not match pinned revision $libfst_revision" >&2
    exit 1
fi
if [[ ! -f "$libfstwriter_source/fstcpp/fstcpp_writer.cpp" ]]; then
    git clone --quiet https://github.com/gtkwave/libfstwriter.git "$libfstwriter_source"
    git -C "$libfstwriter_source" checkout --quiet --detach "$libfstwriter_revision"
fi
if [[ "$(git -C "$libfstwriter_source" rev-parse HEAD)" != "$libfstwriter_revision" ]]; then
    echo "libfstwriter checkout does not match pinned revision $libfstwriter_revision" >&2
    exit 1
fi

mkdir -p "$bench_dir"
cargo build --quiet --release --no-default-features --features writer,gzip,lz4 \
    --example writer_ecosystem_bench
cc -std=c11 -O3 -D_GNU_SOURCE \
    -I"$libfst_source/src" \
    "$repo_root/tests/reference/fst_writer_bench.c" \
    "$libfst_source/src/fstapi.c" \
    "$libfst_source/src/fastlz.c" \
    "$libfst_source/src/lz4.c" \
    -lz -o "$libfst_bin"
cc -O3 -I"$libfst_source/src" -c "$libfst_source/src/lz4.c" -o "$lz4_object"
g++ -std=c++14 -O3 -DNDEBUG -D_GNU_SOURCE -DFSTCPP_IGNORE_NO_EFFECT_API \
    -I"$libfstwriter_source" \
    -I"$libfst_source/src" \
    -I"$libfstwriter_source/integration_test/verilator_share" \
    -I"$libfstwriter_source/integration_test/verilator_share/gtkwave" \
    "$repo_root/tests/reference/fst_writer_bench.c" \
    "$libfstwriter_source/integration_test/verilator_share/gtkwave/fstapi.cpp" \
    "$libfstwriter_source/fstcpp/fstcpp_writer.cpp" \
    "$libfstwriter_source/fstcpp/fstcpp_variable_info.cpp" \
    "$lz4_object" -lz -o "$libfstwriter_bin"

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
    local events=$((signals * steps))
    local wavefst_batch="$bench_dir/$scenario-wavefst-batch.fst"
    local wavefst_scalar="$bench_dir/$scenario-wavefst-scalar.fst"
    local libfst_trace="$bench_dir/$scenario-libfst.fst"
    local libfstwriter_trace="$bench_dir/$scenario-libfstwriter.fst"
    local result

    result=$(median_pinned "$rust_bin" batch "$wavefst_batch" \
        "$signals" "$steps" "$iterations" "$warmup")
    echo "wavefst,$scenario,batch,$result,$(stat -c %s "$wavefst_batch"),$events"
    result=$(median_pinned "$rust_bin" scalar "$wavefst_scalar" \
        "$signals" "$steps" "$iterations" "$warmup")
    echo "wavefst,$scenario,scalar,$result,$(stat -c %s "$wavefst_scalar"),$events"
    result=$(median_pinned "$libfst_bin" "$libfst_trace" \
        "$signals" "$steps" "$iterations" "$warmup")
    echo "libfst,$scenario,packed,$result,$(stat -c %s "$libfst_trace"),$events"
    result=$(median_pinned "$libfstwriter_bin" "$libfstwriter_trace" \
        "$signals" "$steps" "$iterations" "$warmup")
    echo "libfstwriter,$scenario,packed,$result,$(stat -c %s "$libfstwriter_trace"),$events"
}

echo "implementation,scenario,api,ns_per_iteration,file_bytes,events"
run_scenario dense 512 128 100 10
run_scenario wide 8192 256 5 1
run_scenario long 32 65536 5 1
