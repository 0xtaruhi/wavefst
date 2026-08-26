#![cfg(all(feature = "reader", feature = "writer"))]

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fs::File;
use std::path::PathBuf;

use wavefst::{
    AggregatePackType, ArrayAttributeType, BlockType, ChainCompression, DynamicAliasEncoding,
    FileType, FstByteOrder, FstWriter, GeomEntry, Header, HierarchyCompression, MiscAttributeType,
    ReaderBuilder, ScopeType, SignalValue, SupplementalDataType, SupplementalVarType,
    SupplementalVariableMetadata, TimeCompression, VarDir, VarType,
};

fn corpus_files() -> Vec<PathBuf> {
    let directory = std::env::var_os("WAVEFST_LIBFST_CORPUS")
        .expect("WAVEFST_LIBFST_CORPUS must name the generated corpus directory");
    let mut files: Vec<_> = std::fs::read_dir(directory)
        .expect("read libfst corpus directory")
        .map(|entry| entry.expect("read corpus entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "fst"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "libfst corpus contains no .fst files");
    files
}

#[test]
#[ignore = "requires the upstream libfst oracle; run scripts/test-libfst-interop.sh"]
fn reads_every_upstream_libfst_format_variant() -> wavefst::Result<()> {
    for path in corpus_files() {
        let is_empty = path.file_name().is_some_and(|name| name == "empty.fst");
        let is_simple = path.file_name().is_some_and(|name| name == "simple.fst");
        let is_external_producer = path
            .file_name()
            .is_some_and(|name| name == "icarus.fst" || name == "verilator.fst");
        let mut reader = ReaderBuilder::new(File::open(&path)?).build()?;
        let mut event_count = 0usize;
        while let Some(changes) = reader.next_value_changes()? {
            changes.try_for_each(|_| event_count += 1)?;
        }

        if is_empty {
            assert_eq!(reader.header().var_count, 0, "{}", path.display());
            assert_eq!(event_count, 0, "{}", path.display());
            continue;
        }

        if is_simple {
            assert_eq!(reader.header().start_time, 0, "{}", path.display());
            assert_eq!(reader.header().end_time, 2, "{}", path.display());
            assert_eq!(reader.header().var_count, 1, "{}", path.display());
            assert_eq!(event_count, 2, "{}", path.display());
            let hierarchy = reader.hierarchy().expect("simple hierarchy");
            assert_eq!(hierarchy.variables[0].name, "var");
            assert_eq!(hierarchy.variables[0].length, Some(1));
            continue;
        }
        if is_external_producer {
            assert!(event_count > 10, "too few events in {}", path.display());
            assert!(reader.header().var_count >= 4, "{}", path.display());
            assert!(reader.hierarchy().is_some(), "{}", path.display());
            continue;
        }

        assert_eq!(reader.header().file_type as u8, 2, "{}", path.display());
        assert_eq!(
            reader.header().timescale_exponent,
            -12,
            "{}",
            path.display()
        );
        assert_eq!(reader.header().time_zero, -37, "{}", path.display());
        assert_eq!(reader.header().start_time, 0, "{}", path.display());
        assert_eq!(reader.header().end_time, 30, "{}", path.display());
        // libfst suppresses values identical to its type-specific initial frame.
        assert!(
            event_count >= 40,
            "too few events in {}: {event_count}",
            path.display()
        );

        let hierarchy = reader
            .hierarchy()
            .unwrap_or_else(|| panic!("missing hierarchy in {}", path.display()));
        let scopes: BTreeSet<_> = hierarchy
            .scopes
            .iter()
            .map(|scope| scope.scope_type as u8)
            .collect();
        assert_eq!(
            scopes,
            (0..=ScopeType::SvArray as u8).collect(),
            "{}",
            path.display()
        );

        let variable_types: BTreeSet<_> = hierarchy
            .variables
            .iter()
            .map(|variable| variable.var_type as u8)
            .collect();
        assert_eq!(
            variable_types,
            (0..=VarType::SvShortReal as u8).collect(),
            "{}",
            path.display()
        );
        assert!(hierarchy.variables.iter().any(|variable| variable.is_alias));
        let port = hierarchy
            .variables
            .iter()
            .find(|variable| variable.var_type == VarType::VcdPort)
            .expect("VCD port variable");
        assert_eq!(port.length, Some(4));
        assert_eq!(port.storage_length, Some(14));

        let attributes: BTreeSet<_> = hierarchy
            .attributes
            .iter()
            .map(|attribute| (attribute.attr_type, attribute.subtype))
            .collect();
        for expected in [
            (0, 0), // comment
            (0, 1), // environment
            (0, 2), // supplemental variable metadata
            (0, 3), // source pathname table
            (0, 4), // source stem
            (0, 5), // source instantiation stem
            (0, 6), // value list
            (0, 7), // enum table and reference
            (1, 2), // packed array
            (3, 3), // tagged packed aggregate
        ] {
            assert!(
                attributes.contains(&expected),
                "missing attribute {expected:?} in {}",
                path.display()
            );
        }
        assert!(
            hierarchy
                .attributes
                .iter()
                .any(|attribute| { attribute.source_path_index().ok().flatten() == Some(142) })
        );
        assert_eq!(reader.blackout().expect("blackout block").events.len(), 2);
    }
    Ok(())
}

#[test]
fn public_block_enum_covers_every_libfst_disk_tag() {
    let tags = [0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 254, 255];
    for tag in tags {
        assert!(
            BlockType::try_from(tag).is_ok(),
            "missing libfst block tag {tag}"
        );
    }
}

fn write_wavefst_matrix(
    path: PathBuf,
    chain: ChainCompression,
    hierarchy: HierarchyCompression,
    wrapped: bool,
    aliases: DynamicAliasEncoding,
    double_byte_order: FstByteOrder,
) -> wavefst::Result<()> {
    let mut writer = FstWriter::builder(File::create(path)?)
        .chain_compression(chain)
        .time_compression(TimeCompression::Zlib)
        .hierarchy_compression(hierarchy)
        .dynamic_alias_encoding(aliases)
        .wrap_with_zlib(wrapped)
        .block_change_limit(40)
        .timescale_exponent(-12)
        .build()?;

    for raw_scope in 0..=ScopeType::SvArray as u8 {
        writer.begin_scope(
            ScopeType::try_from(raw_scope).expect("known scope"),
            format!("scope_{raw_scope:02}"),
            Some("component".to_owned()),
        )?;
    }
    writer.add_misc_attribute(MiscAttributeType::Comment, "comment", 0)?;
    writer.add_misc_attribute(MiscAttributeType::EnvironmentVariable, "SIM_MODE=oracle", 0)?;
    writer.add_misc_attribute(MiscAttributeType::ValueList, "0 1 x z", 0)?;
    for index in 1..=140 {
        writer.add_source_path(format!("rtl/generated_{index:03}.sv"), index)?;
        writer.add_source_stem(index, index * 10)?;
    }
    writer.add_source_instantiation_stem(140, 1_401)?;
    let enum_handle = writer.create_enum_table(
        "state_t",
        &[("IDLE", "00"), ("RUN", "01"), ("BROKEN VALUE", "1x")],
        2,
    )?;
    writer.begin_array_attribute(ArrayAttributeType::Packed, "packed_array", 4)?;
    writer.begin_pack_attribute(AggregatePackType::TaggedPacked, "tagged_pack", 2)?;
    writer.end_attribute()?;
    writer.end_attribute()?;

    let mut handles = Vec::new();
    for raw_type in 0..=VarType::SvShortReal as u8 {
        let var_type = VarType::try_from(raw_type).expect("known variable type");
        let geometry = match var_type {
            VarType::VcdReal
            | VarType::VcdRealParameter
            | VarType::VcdRealtime
            | VarType::SvShortReal => GeomEntry::Real,
            VarType::GenString => GeomEntry::Variable,
            VarType::VcdPort => GeomEntry::vcd_port(4)?,
            _ => GeomEntry::Fixed(4),
        };
        if var_type == VarType::SvEnum {
            writer.add_enum_table_ref(enum_handle)?;
        }
        let direction = VarDir::try_from(raw_type % 6).expect("known direction");
        let handle = if var_type == VarType::VcdWire {
            writer.add_supplemental_variable(
                var_type,
                direction,
                format!("var_{raw_type:02}"),
                geometry,
                SupplementalVariableMetadata::new(
                    "custom_logic_type",
                    SupplementalVarType::VhdlSignal,
                    SupplementalDataType::VhdlStdLogic,
                ),
            )?
        } else {
            writer.add_variable(var_type, direction, format!("var_{raw_type:02}"), geometry)?
        };
        handles.push(handle);
    }
    let wire = handles[VarType::VcdWire as usize];
    writer.add_alias(VarType::VcdWire, VarDir::Implicit, "wire_alias", wire)?;
    for _ in 0..=ScopeType::SvArray as u8 {
        writer.end_scope()?;
    }

    writer.write_header(Header {
        version: "wavefst libfst reverse oracle".to_owned(),
        date: "2026-08-25 reference corpus".to_owned(),
        file_type: FileType::Mixed,
        time_zero: -37,
        double_byte_order,
        ..Header::default()
    })?;

    for (phase, timestamp) in [(0_usize, 0_u64), (1, 10)] {
        for (raw_type, &handle) in handles.iter().enumerate() {
            let var_type = VarType::try_from(raw_type as u8).expect("known variable type");
            let value = match var_type {
                VarType::VcdReal
                | VarType::VcdRealParameter
                | VarType::VcdRealtime
                | VarType::SvShortReal => SignalValue::Real(if phase == 0 {
                    std::f64::consts::PI
                } else {
                    -1234.5
                }),
                VarType::GenString => SignalValue::Bytes(Cow::Borrowed(if phase == 0 {
                    b"A\0B\xff"
                } else {
                    b"variable length"
                })),
                VarType::VcdPort => SignalValue::Vector(Cow::Borrowed(if phase == 0 {
                    "01010101010101"
                } else {
                    "xz-?xz-?xz-?xz"
                })),
                _ => SignalValue::Vector(Cow::Borrowed(if phase == 0 { "0101" } else { "xz-?" })),
            };
            writer.emit_change(timestamp, handle, value)?;
        }
    }
    writer.emit_dump_active(20, false)?;
    writer.emit_dump_active(25, true)?;
    writer.emit_change(30, wire, SignalValue::Vector(Cow::Borrowed("1010")))?;
    writer.finish()?;
    Ok(())
}

#[test]
#[ignore = "writes files for the upstream reader; run scripts/test-libfst-interop.sh"]
fn writes_every_wavefst_format_variant_for_upstream_libfst() -> wavefst::Result<()> {
    let directory = PathBuf::from(
        std::env::var_os("WAVEFST_RUST_CORPUS")
            .expect("WAVEFST_RUST_CORPUS must name an output directory"),
    );
    std::fs::create_dir_all(&directory)?;
    let mut empty =
        FstWriter::builder(File::create(directory.join("wavefst-empty.fst"))?).build()?;
    empty.write_header(Header::default())?;
    empty.finish()?;
    for (name, chain, hierarchy, wrapped, aliases, double_byte_order) in [
        (
            "wavefst-zlib.fst",
            ChainCompression::Zlib,
            HierarchyCompression::Zlib { level: 4 },
            false,
            DynamicAliasEncoding::Compact,
            FstByteOrder::native(),
        ),
        (
            "wavefst-fastlz.fst",
            ChainCompression::FastLz,
            HierarchyCompression::Lz4,
            false,
            DynamicAliasEncoding::Compact,
            FstByteOrder::native(),
        ),
        (
            "wavefst-lz4-duo.fst",
            ChainCompression::Lz4,
            HierarchyCompression::Lz4Duo,
            false,
            DynamicAliasEncoding::Compact,
            FstByteOrder::native(),
        ),
        (
            "wavefst-wrapper.fst",
            ChainCompression::Raw,
            HierarchyCompression::Zlib { level: 4 },
            true,
            DynamicAliasEncoding::Compact,
            FstByteOrder::native(),
        ),
        (
            "wavefst-legacy-alias.fst",
            ChainCompression::Zlib,
            HierarchyCompression::Zlib { level: 4 },
            false,
            DynamicAliasEncoding::Legacy,
            FstByteOrder::native(),
        ),
        (
            "wavefst-opposite-endian.fst",
            ChainCompression::Zlib,
            HierarchyCompression::Zlib { level: 4 },
            false,
            DynamicAliasEncoding::Compact,
            match FstByteOrder::native() {
                FstByteOrder::LittleEndian => FstByteOrder::BigEndian,
                FstByteOrder::BigEndian => FstByteOrder::LittleEndian,
            },
        ),
    ] {
        write_wavefst_matrix(
            directory.join(name),
            chain,
            hierarchy,
            wrapped,
            aliases,
            double_byte_order,
        )?;
    }
    Ok(())
}
