#include <fstapi.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static uint64_t changes;

static void value_change(void *user,
                         uint64_t time,
                         fstHandle handle,
                         const unsigned char *value)
{
    (void)user;
    (void)time;
    (void)handle;
    (void)value;
    ++changes;
}

static void variable_change(void *user,
                            uint64_t time,
                            fstHandle handle,
                            const unsigned char *value,
                            uint32_t length)
{
    (void)user;
    (void)time;
    (void)handle;
    (void)value;
    (void)length;
    ++changes;
}

int main(int argc, char **argv)
{
    int file_index;
    if (argc < 2) {
        fprintf(stderr, "usage: libfst_verify FILE...\n");
        return 2;
    }

    for (file_index = 1; file_index < argc; ++file_index) {
        fstReaderContext *reader = fstReaderOpen(argv[file_index]);
        struct fstHier *hier;
        unsigned scopes = 0;
        unsigned variables = 0;
        unsigned attributes = 0;
        int source_140 = 0;
        int port_width_4 = 0;

        if (!reader) {
            fprintf(stderr, "libfst rejected %s\n", argv[file_index]);
            return 1;
        }
        if (fstReaderGetVarCount(reader) == 0) {
            if (fstReaderGetStartTime(reader) != 0 || fstReaderGetEndTime(reader) != 0 ||
                fstReaderIterateHier(reader) != NULL) {
                fprintf(stderr, "empty-trace mismatch in %s\n", argv[file_index]);
                return 1;
            }
            fstReaderClose(reader);
            printf("libfst accepted %s (empty trace)\n", argv[file_index]);
            continue;
        }
        if (fstReaderGetFileType(reader) != FST_FT_VERILOG_VHDL ||
            fstReaderGetTimescale(reader) != -12 || fstReaderGetTimezero(reader) != -37 ||
            fstReaderGetStartTime(reader) != 0 || fstReaderGetEndTime(reader) != 30) {
            fprintf(stderr, "header mismatch in %s\n", argv[file_index]);
            return 1;
        }

        while ((hier = fstReaderIterateHier(reader)) != NULL) {
            scopes += hier->htyp == FST_HT_SCOPE;
            variables += hier->htyp == FST_HT_VAR;
            if (hier->htyp == FST_HT_VAR && hier->u.var.typ == FST_VT_VCD_PORT &&
                hier->u.var.length == 4)
                port_width_4 = 1;
            attributes += hier->htyp == FST_HT_ATTRBEGIN;
            if (hier->htyp == FST_HT_ATTRBEGIN && hier->u.attr.typ == FST_AT_MISC &&
                hier->u.attr.subtype == FST_MT_SOURCESTEM &&
                hier->u.attr.arg_from_name == 140)
                source_140 = 1;
        }
        if (scopes != 23 || variables != 31 || attributes < 10 || !source_140 ||
            !port_width_4) {
            fprintf(stderr,
                    "hierarchy mismatch in %s: scopes=%u vars=%u attrs=%u source140=%d port4=%d\n",
                    argv[file_index],
                    scopes,
                    variables,
                    attributes,
                    source_140,
                    port_width_4);
            return 1;
        }

        changes = 0;
        fstReaderSetFacProcessMaskAll(reader);
        if (!fstReaderIterBlocks2(reader, value_change, variable_change, NULL, NULL) ||
            changes < 61) {
            fprintf(stderr,
                    "value iteration failed in %s: changes=%" PRIu64 "\n",
                    argv[file_index],
                    changes);
            return 1;
        }
        fstReaderClose(reader);
        printf("libfst accepted %s (%" PRIu64 " changes)\n", argv[file_index], changes);
    }
    return 0;
}
