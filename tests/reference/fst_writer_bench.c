/* Generic dense writer benchmark compiled against libfst and libfstwriter. */
#include <fstapi.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

static void fail(const char *message)
{
    fprintf(stderr, "fst_writer_bench: %s\n", message);
    exit(1);
}

static uint64_t now_ns(void)
{
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &value) != 0)
        fail("clock_gettime failed");
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static unsigned parse_count(const char *text, const char *name, int allow_zero)
{
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);
    if (!text[0] || !end || *end || value > UINT32_MAX || (!allow_zero && value == 0)) {
        fprintf(stderr, "fst_writer_bench: invalid %s\n", name);
        exit(1);
    }
    return (unsigned)value;
}

static uint64_t write_trace(const char *path, unsigned signals, unsigned steps)
{
    fstWriterContext *writer = fstWriterCreate(path, 1);
    fstHandle *handles = (fstHandle *)malloc((size_t)signals * sizeof(*handles));
    uint64_t *states = (uint64_t *)malloc((size_t)signals * sizeof(*states));
    struct stat metadata;
    unsigned signal;
    unsigned step;

    if (!writer || !handles || !states)
        fail("writer or benchmark-state allocation failed");
    fstWriterSetPackType(writer, FST_WR_PT_LZ4);
    fstWriterSetScope(writer, FST_ST_VCD_MODULE, "dense", NULL);
    for (signal = 0; signal < signals; ++signal) {
        char name[32];
        snprintf(name, sizeof(name), "s%u", signal);
        handles[signal] = fstWriterCreateVar(
            writer, FST_VT_VCD_WIRE, FST_VD_IMPLICIT, 1, name, 0);
        if (!handles[signal])
            fail("fstWriterCreateVar failed");
        states[signal] = (uint64_t)(signal + 1) * UINT64_C(0x9e3779b97f4a7c15);
    }
    fstWriterSetUpscope(writer);

    for (step = 0; step < steps; ++step) {
        fstWriterEmitTimeChange(writer, step);
        for (signal = 0; signal < signals; ++signal) {
            states[signal] ^= states[signal] << 13;
            states[signal] ^= states[signal] >> 7;
            states[signal] ^= states[signal] << 17;
            fstWriterEmitValueChange32(
                writer, handles[signal], 1, (uint32_t)(states[signal] & 1u));
        }
    }
    fstWriterClose(writer);
    free(states);
    free(handles);
    if (stat(path, &metadata) != 0)
        fail("stat failed after writing");
    return (uint64_t)metadata.st_size;
}

int main(int argc, char **argv)
{
    uint64_t accumulator = 0;
    uint64_t start;
    uint64_t elapsed;
    unsigned signals;
    unsigned steps;
    unsigned iterations;
    unsigned warmup;
    unsigned index;

    if (argc != 6)
        fail("usage: fst_writer_bench <file> <signals> <steps> <iterations> <warmup>");
    signals = parse_count(argv[2], "signals", 0);
    steps = parse_count(argv[3], "steps", 0);
    iterations = parse_count(argv[4], "iterations", 0);
    warmup = parse_count(argv[5], "warmup", 1);

    for (index = 0; index < warmup; ++index)
        accumulator += write_trace(argv[1], signals, steps);
    start = now_ns();
    for (index = 0; index < iterations; ++index)
        accumulator += write_trace(argv[1], signals, steps);
    elapsed = now_ns() - start;
    if (!accumulator)
        fail("benchmark result was unexpectedly zero");
    printf("%" PRIu64 "\n", elapsed / iterations);
    return 0;
}
