# FST compatibility

FST (Fast Signal Trace) is GTKWave's implementation-defined waveform format. There is no separate
IEEE/IEC FST specification or collection of competing “FST standards”; the public `fstapi.h`, the
`libfst` block-format notes, and files emitted by the ecosystem are the effective specification.
wavefst therefore treats upstream `gtkwave/libfst` as an independent byte-level oracle and tests
files from other producers separately.

The pinned reference revision is `cf74bef8d0435eceb20524fe6f5674e0ecb68b25`. Run the complete,
bidirectional check with:

```bash
scripts/test-libfst-interop.sh
```

The script compiles upstream fstapi independently, recreates both tests shipped by libfst, builds a
larger reference corpus, reads every event with wavefst, writes the reverse corpus with wavefst, and
has `fstReaderIterBlocks2` consume every reverse file. If installed, independent simulator
producers are added too. The current local run includes Verilator's newer `fstcpp` writer; Icarus is
reported as skipped when its installed `vvp` lacks FST output support.

## On-disk blocks

| libfst tag | Meaning | Read | Write | Oracle coverage |
|---:|---|:---:|:---:|:---:|
| 0 | Header | yes | yes | bidirectional |
| 1 | Value changes | yes | yes | bidirectional |
| 2 | Blackout/dump activity | yes | yes | bidirectional |
| 3 | Geometry | yes | yes | bidirectional |
| 4 | gzip hierarchy | yes | yes | bidirectional |
| 5 | legacy dynamic aliases | yes | yes | reverse-read by libfst |
| 6 | LZ4 hierarchy | yes | yes | bidirectional |
| 7 | double-LZ4 hierarchy | yes | yes | reverse-read by libfst |
| 8 | compact dynamic aliases | yes | yes | bidirectional |
| 254 | whole-file gzip wrapper | yes | yes | bidirectional |
| 255 | in-progress/end sentinel | yes | internal checkpoint only | upstream empty-file test |

The value-change pack markers supported in both directions are stored/raw chains, zlib, FastLZ,
and LZ4. Time tables support stored and zlib forms. Geometry supports stored and zlib forms.

## Hierarchy and values

- All scope codes 0–22, all variable kinds 0–29, all directions 0–5, and all file types 0–2 are
  represented by public Rust enums and exercised by the libfst corpus.
- Fixed vectors, all ten scalar logic states (`0`, `1`, `x`, `z`, `h`, `u`, `w`, `l`, `-`, `?`
  as applicable), IEEE-754 doubles, arbitrary variable-length bytes including embedded NULs,
  aliases, multiple VC blocks, and dump-active transitions are traversed by the oracle tests.
- VCD port variables preserve libfst's distinct logical width and `3 × width + 2` geometry storage
  length. Floating-point payloads are decoded according to the header byte-order marker, including
  files whose byte order is opposite to the host.
- Generic hierarchy attributes preserve type, subtype, argument, ordering, nesting, and exact name
  bytes. Exact bytes matter because source-stem attributes overload the name with a binary varint;
  the tests cross the 127/128 varint boundary.
- libfst's misc, array, enum, pack, value-list, enum-table, source-stem, source-instantiation-stem,
  and supplemental VHDL variable records are covered at the on-disk level. The reader preserves
  their raw representation, while the writer also provides typed constructors for these records.

`FST_HT_TREEBEGIN` and `FST_HT_TREEEND` are deliberately not listed as disk formats: upstream
`fstapi.h` says they are not used by FST and only exist when `fstHier` bridges other formats.
Reader query conveniences such as process masks or “value at time” are API features, not additional
FST encodings; wavefst offers streaming/fold APIs instead.

## External producers

| Producer | Local status | Test behavior |
|---|---|---|
| GTKWave/libfst | passed | mandatory pinned bidirectional oracle |
| Verilator 5.050 (`fstcpp`) | passed | generated and read when installed |
| Icarus Verilog | unavailable in current build | detected; only run when `vvp` advertises FST |
| GHDL | not installed locally | not claimed as executed |

Passing this matrix means compatibility with every disk tag and enum currently declared by libfst,
not compatibility with unrelated formats that share the `.fst` extension (for example OpenFst
finite-state transducers).
