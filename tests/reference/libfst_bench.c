/* Dense single-bit benchmark paired with examples/libfst_bench.rs. */
#include <fstapi.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define SIGNALS 512u
#define STEPS 128u
#define EXPECTED_EVENTS ((uint64_t)SIGNALS * STEPS)
#define EXPECTED_SELECTED_EVENTS ((uint64_t)STEPS)

static void fail(const char *message)
{
    fprintf(stderr, "libfst_bench: %s\n", message);
    exit(1);
}

static uint64_t now_ns(void)
{
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &value) != 0)
        fail("clock_gettime failed");
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static uint64_t write_trace(const char *path)
{
    fstWriterContext *writer = fstWriterCreate(path, 1);
    fstHandle handles[SIGNALS];
    uint64_t states[SIGNALS];
    struct stat metadata;
    unsigned signal;
    unsigned step;

    if (!writer)
        fail("fstWriterCreate failed");
    fstWriterSetPackType(writer, FST_WR_PT_ZLIB);
    fstWriterSetScope(writer, FST_ST_VCD_MODULE, "dense", NULL);
    for (signal = 0; signal < SIGNALS; ++signal) {
        char name[32];
        snprintf(name, sizeof(name), "s%u", signal);
        handles[signal] = fstWriterCreateVar(
            writer, FST_VT_VCD_WIRE, FST_VD_IMPLICIT, 1, name, 0);
        if (!handles[signal])
            fail("fstWriterCreateVar failed");
        states[signal] = (uint64_t)(signal + 1) * UINT64_C(0x9e3779b97f4a7c15);
    }
    fstWriterSetUpscope(writer);

    for (step = 0; step < STEPS; ++step) {
        fstWriterEmitTimeChange(writer, step);
        for (signal = 0; signal < SIGNALS; ++signal) {
            const char *value;
            states[signal] ^= states[signal] << 13;
            states[signal] ^= states[signal] >> 7;
            states[signal] ^= states[signal] << 17;
            value = (states[signal] & 1u) ? "1" : "0";
            fstWriterEmitValueChange(writer, handles[signal], value);
        }
    }
    fstWriterClose(writer);
    if (stat(path, &metadata) != 0)
        fail("stat failed after writing");
    return (uint64_t)metadata.st_size;
}

static void count_change(void *user, uint64_t time, fstHandle handle, const unsigned char *value)
{
    uint64_t *count = user;
    (void)time;
    (void)handle;
    (void)value;
    ++*count;
}

static uint64_t read_trace(const char *path)
{
    fstReaderContext *reader = fstReaderOpen(path);
    uint64_t count = 0;
    if (!reader)
        fail("fstReaderOpen failed");
    fstReaderSetFacProcessMaskAll(reader);
    if (!fstReaderIterBlocks(reader, count_change, &count, NULL))
        fail("fstReaderIterBlocks failed");
    fstReaderClose(reader);
    if (count != EXPECTED_EVENTS) {
        fprintf(stderr,
                "libfst_bench: expected %" PRIu64 " events, decoded %" PRIu64 "\n",
                EXPECTED_EVENTS,
                count);
        exit(1);
    }
    return count;
}

static uint64_t read_trace_selected(const char *path)
{
    fstReaderContext *reader = fstReaderOpen(path);
    uint64_t count = 0;
    if (!reader)
        fail("fstReaderOpen failed");
    fstReaderSetFacProcessMask(reader, 1);
    if (!fstReaderIterBlocks(reader, count_change, &count, NULL))
        fail("fstReaderIterBlocks failed");
    fstReaderClose(reader);
    if (count != EXPECTED_SELECTED_EVENTS) {
        fprintf(stderr,
                "libfst_bench: expected %" PRIu64 " selected events, decoded %" PRIu64 "\n",
                EXPECTED_SELECTED_EVENTS,
                count);
        exit(1);
    }
    return count;
}

static unsigned parse_count(const char *text, const char *name, int allow_zero)
{
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);
    if (!text[0] || !end || *end || value > UINT32_MAX || (!allow_zero && value == 0)) {
        fprintf(stderr, "libfst_bench: invalid %s\n", name);
        exit(1);
    }
    return (unsigned)value;
}

int main(int argc, char **argv)
{
    uint64_t (*operation)(const char *);
    uint64_t accumulator = 0;
    uint64_t start;
    uint64_t elapsed;
    unsigned iterations;
    unsigned warmup;
    unsigned index;

    if (argc != 5 || (strcmp(argv[1], "read") && strcmp(argv[1], "read-one") && strcmp(argv[1], "write")))
        fail("usage: libfst_bench <read|read-one|write> <path> <iterations> <warmup>");
    operation = !strcmp(argv[1], "read") ? read_trace
              : !strcmp(argv[1], "read-one") ? read_trace_selected
                                               : write_trace;
    iterations = parse_count(argv[3], "iterations", 0);
    warmup = parse_count(argv[4], "warmup", 1);

    for (index = 0; index < warmup; ++index)
        accumulator += operation(argv[2]);
    start = now_ns();
    for (index = 0; index < iterations; ++index)
        accumulator += operation(argv[2]);
    elapsed = now_ns() - start;
    if (!accumulator)
        fail("benchmark result was unexpectedly zero");
    printf("%" PRIu64 "\n", elapsed / iterations);
    return 0;
}
