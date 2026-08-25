/*
 * Cross-implementation corpus generator for wavefst.
 *
 * This is intentionally compiled against the upstream gtkwave/libfst fstapi,
 * rather than linked into the Rust crate.  It exercises the public writer API
 * and leaves libfst as an independent format oracle.
 */
#include <fstapi.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fail(const char *message)
{
    fprintf(stderr, "libfst_matrix: %s\n", message);
    exit(1);
}

static int is_real_type(int type)
{
    return type == FST_VT_VCD_REAL || type == FST_VT_VCD_REAL_PARAMETER ||
           type == FST_VT_VCD_REALTIME || type == FST_VT_SV_SHORTREAL;
}

static uint32_t width_for_type(int type)
{
    switch (type) {
        case FST_VT_VCD_PORT:
            return 14; /* 3 * 4 logical bits + 2 port delimiters */
        case FST_VT_VCD_INTEGER:
        case FST_VT_SV_INT:
            return 32;
        case FST_VT_VCD_TIME:
        case FST_VT_SV_LONGINT:
            return 64;
        case FST_VT_SV_SHORTINT:
            return 16;
        case FST_VT_SV_BYTE:
            return 8;
        default:
            return 4;
    }
}

static void make_bits(char *buffer, uint32_t width, unsigned phase)
{
    uint32_t index;
    static const char extended[] = "xzhuwl-?";

    for (index = 0; index < width; ++index) {
        if (phase == 0) {
            buffer[index] = (index & 1) ? '1' : '0';
        } else if (width > 1 && index < sizeof(extended) - 1) {
            buffer[index] = extended[index];
        } else {
            buffer[index] = (index & 1) ? '0' : '1';
        }
    }
    buffer[width] = 0;
}

static void emit_all_values(fstWriterContext *writer, const fstHandle *handles, unsigned phase)
{
    int type;
    for (type = FST_VT_MIN; type <= FST_VT_MAX; ++type) {
        fstHandle handle = handles[type];
        if (type == FST_VT_GEN_STRING) {
            const unsigned char first[] = {'A', 0, 'B', 0xff};
            const unsigned char second[] = "variable length value";
            fstWriterEmitVariableLengthValueChange(writer,
                                                   handle,
                                                   phase ? second : first,
                                                   phase ? (uint32_t)(sizeof(second) - 1)
                                                         : (uint32_t)sizeof(first));
        } else if (is_real_type(type)) {
            double value = phase ? -1234.5 : 3.141592653589793;
            fstWriterEmitValueChange(writer, handle, &value);
        } else {
            uint32_t width = width_for_type(type);
            char bits[65];
            make_bits(bits, width, phase);
            fstWriterEmitValueChange(writer, handle, bits);
        }
    }
}

static void create_file(const char *path, enum fstWriterPackType pack, int wrapper)
{
    fstWriterContext *writer = fstWriterCreate(path, 1);
    fstHandle handles[FST_VT_MAX + 1];
    fstHandle alias;
    fstEnumHandle enum_handle;
    int type;
    int scope;
    const char *enum_literals[] = {"IDLE", "RUN", "BROKEN VALUE"};
    const char *enum_values[] = {"00", "01", "1x"};

    if (!writer)
        fail("fstWriterCreate failed");

    fstWriterSetPackType(writer, pack);
    fstWriterSetRepackOnClose(writer, wrapper);
    fstWriterSetFileType(writer, FST_FT_VERILOG_VHDL);
    fstWriterSetTimescale(writer, -12);
    fstWriterSetTimezero(writer, -37);
    fstWriterSetDate(writer, "2026-08-25 reference corpus");
    fstWriterSetVersion(writer, "wavefst libfst oracle");

    fstWriterSetComment(writer, "comment with newline\nnormalized by libfst");
    fstWriterSetEnvVar(writer, "SIM_MODE=oracle");
    fstWriterSetSourceStem(writer, "rtl/design.sv", 123, 0);
    fstWriterSetSourceInstantiationStem(writer, "tb/top.sv", 456, 0);
    /* Cross the one-byte varint boundary for binary source-path indices. */
    for (type = 0; type < 140; ++type) {
        char source_path[64];
        snprintf(source_path, sizeof(source_path), "rtl/generated_%03d.sv", type);
        fstWriterSetSourceStem(writer, source_path, (unsigned int)type, 0);
    }

    enum_handle = fstWriterCreateEnumTable(writer,
                                           "state_t",
                                           3,
                                           2,
                                           enum_literals,
                                           enum_values);
    if (!enum_handle)
        fail("fstWriterCreateEnumTable failed");

    /* Every scope token currently defined by fstapi.h. */
    for (scope = FST_ST_MIN; scope <= FST_ST_MAX; ++scope) {
        char name[32];
        snprintf(name, sizeof(name), "scope_%02d", scope);
        fstWriterSetScope(writer, (enum fstScopeType)scope, name, "component");
    }

    fstWriterSetAttrBegin(writer, FST_AT_ARRAY, FST_AR_PACKED, "packed_array", 4);
    fstWriterSetAttrBegin(writer, FST_AT_PACK, FST_PT_TAGGED_PACKED, "tagged", 2);
    fstWriterSetAttrEnd(writer);
    fstWriterSetAttrEnd(writer);

    for (type = FST_VT_MIN; type <= FST_VT_MAX; ++type) {
        char name[32];
        uint32_t width = type == FST_VT_GEN_STRING ? 0 : width_for_type(type);
        snprintf(name, sizeof(name), "var_%02d", type);
        if (type == FST_VT_SV_ENUM) {
            fstWriterEmitEnumTableRef(writer, enum_handle);
        }
        fstWriterSetValueList(writer, "0 1 x z");
        handles[type] = fstWriterCreateVar(writer,
                                           (enum fstVarType)type,
                                           (enum fstVarDir)(type % (FST_VD_MAX + 1)),
                                           width,
                                           name,
                                           0);
        if (!handles[type])
            fail("fstWriterCreateVar failed");
    }

    (void)fstWriterCreateVar2(writer,
                              FST_VT_VCD_WIRE,
                              FST_VD_INPUT,
                              1,
                              "vhdl_signal",
                              0,
                              "custom_logic_type",
                              FST_SVT_VHDL_SIGNAL,
                              FST_SDT_VHDL_STD_LOGIC);
    alias = fstWriterCreateVar(writer,
                               FST_VT_VCD_WIRE,
                               FST_VD_IMPLICIT,
                               4,
                               "wire_alias",
                               handles[FST_VT_VCD_WIRE]);
    if (alias != handles[FST_VT_VCD_WIRE])
        fail("static alias did not preserve its target handle");

    for (scope = FST_ST_MIN; scope <= FST_ST_MAX; ++scope)
        fstWriterSetUpscope(writer);

    fstWriterEmitTimeChange(writer, 0);
    emit_all_values(writer, handles, 0);
    fstWriterEmitValueChange(writer, alias, "0011");

    fstWriterEmitTimeChange(writer, 10);
    emit_all_values(writer, handles, 1);
    fstWriterEmitValueChange(writer, alias, "1100");
    fstWriterFlushContext(writer);

    fstWriterEmitTimeChange(writer, 20);
    fstWriterEmitValueChange32(writer, handles[FST_VT_SV_INT], 32, 0xdeadbeefU);
    fstWriterEmitValueChange64(writer,
                               handles[FST_VT_SV_LONGINT],
                               64,
                               UINT64_C(0x0123456789abcdef));
    {
        const uint32_t vec32[2] = {0x89abcdefU, 0x01234567U};
        const uint64_t vec64[1] = {UINT64_C(0xfedcba9876543210)};
        fstWriterEmitValueChangeVec32(writer, handles[FST_VT_VCD_TIME], 64, vec32);
        fstWriterEmitValueChangeVec64(writer, handles[FST_VT_SV_LONGINT], 64, vec64);
    }
    fstWriterEmitDumpActive(writer, 0);
    fstWriterEmitTimeChange(writer, 25);
    fstWriterEmitDumpActive(writer, 1);
    fstWriterEmitTimeChange(writer, 30);

    fstWriterClose(writer);
}

static void create_upstream_test_files(const char *directory)
{
    char path[4096];
    fstWriterContext *writer;
    fstHandle handle;

    /* Equivalent to upstream tests/empty_file.c. */
    snprintf(path, sizeof(path), "%s/empty.fst", directory);
    writer = fstWriterCreate(path, 1);
    if (!writer)
        fail("creating upstream empty test file failed");
    fstWriterClose(writer);

    /* Equivalent to upstream tests/write_and_read.c. */
    snprintf(path, sizeof(path), "%s/simple.fst", directory);
    writer = fstWriterCreate(path, 1);
    if (!writer)
        fail("creating upstream simple test file failed");
    handle = fstWriterCreateVar(writer,
                                FST_VT_VCD_WIRE,
                                FST_VD_IMPLICIT,
                                1,
                                "var",
                                0);
    fstWriterEmitTimeChange(writer, 0);
    fstWriterEmitValueChange(writer, handle, "0");
    fstWriterEmitTimeChange(writer, 1);
    fstWriterEmitValueChange(writer, handle, "1");
    fstWriterEmitTimeChange(writer, 2);
    fstWriterClose(writer);
}

int main(int argc, char **argv)
{
    char path[4096];
    const char *directory;

    if (argc != 2)
        fail("usage: libfst_matrix OUTPUT_DIRECTORY");
    directory = argv[1];

    create_upstream_test_files(directory);

    snprintf(path, sizeof(path), "%s/libfst-zlib.fst", directory);
    create_file(path, FST_WR_PT_ZLIB, 0);
    snprintf(path, sizeof(path), "%s/libfst-fastlz.fst", directory);
    create_file(path, FST_WR_PT_FASTLZ, 0);
    snprintf(path, sizeof(path), "%s/libfst-lz4.fst", directory);
    create_file(path, FST_WR_PT_LZ4, 0);
    snprintf(path, sizeof(path), "%s/libfst-wrapper.fst", directory);
    create_file(path, FST_WR_PT_ZLIB, 1);
    return 0;
}
