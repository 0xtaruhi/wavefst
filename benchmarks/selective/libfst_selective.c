#include <fstapi.h>

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

#define SEED UINT64_C(0x6a09e667f3bcc909)

struct counters {
    uint64_t changes;
    uint64_t checksum;
};

struct io_counters {
    uint64_t read_bytes;
    uint64_t rchar;
};

static void fail(const char *message)
{
    fprintf(stderr, "libfst-selective: %s\n", message);
    exit(EXIT_FAILURE);
}

static uint64_t parse_u64(const char *text, const char *label)
{
    char *end = NULL;
    unsigned long long value;
    errno = 0;
    value = strtoull(text, &end, 10);
    if (errno || !text[0] || !end || *end) {
        fprintf(stderr, "libfst-selective: invalid %s: %s\n", label, text);
        exit(EXIT_FAILURE);
    }
    return (uint64_t)value;
}

static uint64_t now_ns(void)
{
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &value) != 0)
        fail("clock_gettime failed");
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static uint64_t splitmix64(uint64_t value)
{
    value += UINT64_C(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static int compare_handle(const void *left, const void *right)
{
    fstHandle a = *(const fstHandle *)left;
    fstHandle b = *(const fstHandle *)right;
    return (a > b) - (a < b);
}

static fstHandle *random_handles(uint64_t signals, size_t count)
{
    fstHandle *handles = malloc(count * sizeof(*handles));
    uint64_t state = SEED;
    size_t used = 0;
    if (!handles)
        fail("handle allocation failed");
    while (used < count) {
        fstHandle candidate;
        size_t index;
        int duplicate = 0;
        state = splitmix64(state);
        candidate = (fstHandle)(state % signals + 1);
        for (index = 0; index < used; ++index) {
            if (handles[index] == candidate) {
                duplicate = 1;
                break;
            }
        }
        if (!duplicate)
            handles[used++] = candidate;
    }
    qsort(handles, count, sizeof(*handles), compare_handle);
    return handles;
}

static void count_change(void *user, uint64_t time, fstHandle handle, const unsigned char *value)
{
    struct counters *result = user;
    uint64_t value_code = value ? value[0] : 0;
    result->changes++;
    result->checksum += time ^ (uint64_t)handle ^ value_code;
}

static void percent_window(uint64_t end_time, uint64_t position, uint64_t *start, uint64_t *end)
{
    uint64_t timeline = end_time + 1;
    uint64_t exclusive_end;
    *start = timeline * position / 100;
    exclusive_end = timeline * (position + 1) / 100;
    *end = exclusive_end ? exclusive_end - 1 : *start;
    if (*end < *start)
        *end = *start;
}

static void run_query(const char *path,
                      const fstHandle *handles,
                      size_t handle_count,
                      int all_handles,
                      int has_time,
                      uint64_t start_time,
                      uint64_t end_time,
                      struct counters *result)
{
    fstReaderContext *reader = fstReaderOpen(path);
    size_t index;
    if (!reader)
        fail("fstReaderOpen failed");
    fstReaderClrFacProcessMaskAll(reader);
    if (all_handles) {
        fstReaderSetFacProcessMaskAll(reader);
    } else {
        for (index = 0; index < handle_count; ++index)
            fstReaderSetFacProcessMask(reader, handles[index]);
    }
    if (has_time)
        fstReaderSetLimitTimeRange(reader, start_time, end_time);
    if (!fstReaderIterBlocks(reader, count_change, result, NULL))
        fail("fstReaderIterBlocks failed");
    fstReaderClose(reader);
}

static void run_viewports(const char *path,
                          const fstHandle *handles,
                          size_t handle_count,
                          uint64_t end_time,
                          struct counters *result)
{
    fstReaderContext *reader = fstReaderOpen(path);
    size_t index;
    uint64_t position;
    if (!reader)
        fail("fstReaderOpen failed");
    fstReaderClrFacProcessMaskAll(reader);
    for (index = 0; index < handle_count; ++index)
        fstReaderSetFacProcessMask(reader, handles[index]);
    for (position = 0; position < 100; ++position) {
        uint64_t start_time;
        uint64_t stop_time;
        percent_window(end_time, position, &start_time, &stop_time);
        fstReaderSetLimitTimeRange(reader, start_time, stop_time);
        if (!fstReaderIterBlocks(reader, count_change, result, NULL))
            fail("fstReaderIterBlocks failed");
    }
    fstReaderClose(reader);
}

static struct io_counters read_io_counters(void)
{
    struct io_counters result = {0, 0};
    FILE *input = fopen("/proc/self/io", "r");
    char name[64];
    uint64_t value;
    if (!input)
        fail("cannot open /proc/self/io");
    while (fscanf(input, "%63[^:]: %" SCNu64 "\n", name, &value) == 2) {
        if (!strcmp(name, "read_bytes"))
            result.read_bytes = value;
        else if (!strcmp(name, "rchar"))
            result.rchar = value;
    }
    fclose(input);
    return result;
}

static uint64_t peak_rss_kib(void)
{
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) != 0)
        fail("getrusage failed");
    return (uint64_t)usage.ru_maxrss;
}

int main(int argc, char **argv)
{
    const char *case_name;
    const char *path;
    uint64_t signals;
    uint64_t steps;
    uint64_t end_time;
    uint64_t start_time = 0;
    uint64_t stop_time = 0;
    uint64_t started;
    uint64_t wall_ns;
    size_t selected_count;
    size_t queries;
    size_t query;
    int all_handles;
    int has_time;
    fstHandle *handles = NULL;
    struct counters result = {0, 0};
    struct io_counters before;
    struct io_counters after;

    if (argc != 5)
        fail("usage: libfst-selective <A|B|C|D|E> <file> <signals> <steps>");
    case_name = argv[1];
    path = argv[2];
    signals = parse_u64(argv[3], "signals");
    steps = parse_u64(argv[4], "steps");
    if (signals < 100 || signals > UINT32_MAX || steps < 2)
        fail("signals must be 100..UINT32_MAX and steps at least two");
    end_time = steps - 1;

    selected_count = !strcmp(case_name, "A") ? 10 : 100;
    all_handles = !strcmp(case_name, "C");
    has_time = !strcmp(case_name, "C") || !strcmp(case_name, "D") || !strcmp(case_name, "E");
    queries = !strcmp(case_name, "E") ? 100 : 1;
    if (strcmp(case_name, "A") && strcmp(case_name, "B") && strcmp(case_name, "C") &&
        strcmp(case_name, "D") && strcmp(case_name, "E"))
        fail("case must be A, B, C, D, or E");
    if (!all_handles)
        handles = random_handles(signals, selected_count);

    before = read_io_counters();
    started = now_ns();
    if (queries == 100) {
        run_viewports(path, handles, selected_count, end_time, &result);
    } else {
        for (query = 0; query < queries; ++query) {
            if (has_time)
                percent_window(end_time, 49, &start_time, &stop_time);
            run_query(path,
                      handles,
                      selected_count,
                      all_handles,
                      has_time,
                      start_time,
                      stop_time,
                      &result);
        }
    }
    wall_ns = now_ns() - started;
    after = read_io_counters();
    printf("libfst\t%s\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64
           "\t%" PRIu64 "\t%" PRIu64 "\t%zu\n",
           case_name,
           wall_ns,
           after.read_bytes - before.read_bytes,
           after.rchar - before.rchar,
           peak_rss_kib(),
           result.changes,
           result.checksum,
           queries);
    free(handles);
    return EXIT_SUCCESS;
}
