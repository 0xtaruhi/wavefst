#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
libfst_source=${LIBFST_SOURCE:-"$repo_root/target/libfst-reference"}
libfst_revision=cf74bef8d0435eceb20524fe6f5674e0ecb68b25
corpus_dir="$repo_root/target/libfst-corpus"
rust_corpus_dir="$repo_root/target/wavefst-corpus"
oracle_bin="$repo_root/target/libfst-matrix"
verifier_bin="$repo_root/target/libfst-verify"

if [[ ! -f "$libfst_source/src/fstapi.c" ]]; then
    git clone --quiet https://github.com/gtkwave/libfst.git "$libfst_source"
    git -C "$libfst_source" checkout --quiet --detach "$libfst_revision"
fi

actual_revision=$(git -C "$libfst_source" rev-parse HEAD)
if [[ "$actual_revision" != "$libfst_revision" ]]; then
    echo "libfst source is at $actual_revision; expected $libfst_revision" >&2
    echo "set LIBFST_SOURCE to a checkout of the pinned revision" >&2
    exit 1
fi

mkdir -p "$corpus_dir"
rm -f "$corpus_dir"/*.fst
mkdir -p "$rust_corpus_dir"
rm -f "$rust_corpus_dir"/*.fst

cc -std=c11 -O2 -D_GNU_SOURCE \
    -I"$libfst_source/src" \
    "$repo_root/tests/reference/libfst_matrix.c" \
    "$libfst_source/src/fstapi.c" \
    "$libfst_source/src/fastlz.c" \
    "$libfst_source/src/lz4.c" \
    -lz -o "$oracle_bin"

"$oracle_bin" "$corpus_dir"

if command -v iverilog >/dev/null && command -v vvp >/dev/null && \
    vvp -h 2>&1 | grep -q -- '-fst'; then
    iverilog -g2012 -o "$repo_root/target/producer-iverilog" \
        "$repo_root/tests/reference/producer_tb.sv"
    (
        cd "$corpus_dir"
        vvp -fst "$repo_root/target/producer-iverilog"
        mv producer.fst icarus.fst
    )
else
    echo "Icarus Verilog FST output unavailable; skipping its real-producer corpus" >&2
fi

if command -v verilator >/dev/null; then
    mkdir -p "$repo_root/target/libfst-lz4"
    cc -O2 -I"$libfst_source/src" -c "$libfst_source/src/lz4.c" \
        -o "$repo_root/target/libfst-lz4/lz4.o"
    ar rcs "$repo_root/target/libfst-lz4/liblz4.a" \
        "$repo_root/target/libfst-lz4/lz4.o"
    verilator --binary --trace-fst --timing \
        --Mdir "$repo_root/target/verilator-producer" \
        -o producer-verilator \
        -CFLAGS "-I$libfst_source/src" \
        -LDFLAGS "-L$repo_root/target/libfst-lz4" \
        "$repo_root/tests/reference/producer_tb.sv"
    (
        cd "$corpus_dir"
        "$repo_root/target/verilator-producer/producer-verilator"
        mv producer.fst verilator.fst
    )
else
    echo "Verilator unavailable; skipping its real-producer corpus" >&2
fi

cc -std=c11 -O2 -D_GNU_SOURCE \
    -I"$libfst_source/src" \
    "$repo_root/tests/reference/libfst_verify.c" \
    "$libfst_source/src/fstapi.c" \
    "$libfst_source/src/fastlz.c" \
    "$libfst_source/src/lz4.c" \
    -lz -o "$verifier_bin"

# Exact equivalents of both tests shipped by upstream libfst: an empty FST and
# the simple one-bit trace are also emitted by the matrix generator/test suite.
WAVEFST_LIBFST_CORPUS="$corpus_dir" \
    cargo test --all-features --test libfst_interop \
    reads_every_upstream_libfst_format_variant -- --ignored --nocapture

WAVEFST_RUST_CORPUS="$rust_corpus_dir" \
    cargo test --all-features --test libfst_interop \
    writes_every_wavefst_format_variant_for_upstream_libfst -- --ignored --nocapture

"$verifier_bin" "$rust_corpus_dir"/*.fst
