#![cfg(all(
    feature = "reader",
    feature = "writer",
    any(feature = "gzip", feature = "lz4")
))]

use std::io::Cursor;

#[cfg(feature = "mmap")]
use std::io::{Read, Seek, SeekFrom};

#[cfg(feature = "mmap")]
use tempfile::tempdir;
#[cfg(feature = "mmap")]
use wavefst::io::MemoryMap;
use wavefst::{
    ChainCompression, FstByteOrder, FstWriter, GeomEntry, Header, ReaderBuilder, ScopeType,
    SignalValue, TimeCompression, VarDir, VarType,
};

fn one_bit_writer(block_change_limit: usize) -> wavefst::Result<(FstWriter<Cursor<Vec<u8>>>, u32)> {
    let mut writer = FstWriter::builder(Cursor::new(Vec::new()))
        .chain_compression(ChainCompression::Raw)
        .time_compression(TimeCompression::Raw)
        .block_change_limit(block_change_limit)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "bit",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;
    Ok((writer, handle))
}

#[test]
fn finish_backpatches_header_and_checkpoints_each_block() -> wavefst::Result<()> {
    let (mut writer, handle) = one_bit_writer(2)?;
    writer.write_header(Header {
        start_time: 777,
        end_time: 999,
        vc_section_count: 42,
        ..Header::default()
    })?;
    writer.emit_change(0, handle, SignalValue::Bit('0'))?;
    writer.emit_change(1, handle, SignalValue::Bit('1'))?;
    writer.emit_change(2, handle, SignalValue::Bit('0'))?;
    let bytes = writer.finish()?.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    assert_eq!(reader.header().start_time, 0);
    assert_eq!(reader.header().end_time, 2);
    assert_eq!(reader.header().vc_section_count, 2);

    let first = reader.next_vc_block()?.expect("first VC block");
    assert_eq!(first.frame.as_slice(), b"x");
    let second = reader.next_vc_block()?.expect("second VC block");
    assert_eq!(second.frame.as_slice(), b"1");
    assert!(reader.next_vc_block()?.is_none());
    Ok(())
}

#[test]
fn aliases_share_handles_and_only_increase_variable_count() -> wavefst::Result<()> {
    let mut writer = FstWriter::builder(Cursor::new(Vec::new())).build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "canonical",
        GeomEntry::Fixed(1),
    )?;
    let alias = writer.add_alias(VarType::VcdWire, VarDir::Implicit, "alias", handle)?;
    writer.end_scope()?;
    assert_eq!(alias, handle);
    writer.write_header(Header::default())?;
    writer.emit_change(0, alias, SignalValue::Bit('1'))?;
    let bytes = writer.finish()?.into_inner();

    let reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    assert_eq!(reader.header().var_count, 2);
    assert_eq!(reader.header().max_handle, 1);
    let variables = &reader.hierarchy().expect("hierarchy").variables;
    assert_eq!(variables.len(), 2);
    assert!(variables[1].is_alias);
    assert_eq!(variables[1].handle, handle);
    Ok(())
}

#[test]
fn time_zero_is_signed_metadata_not_a_timestamp_offset() -> wavefst::Result<()> {
    let (mut writer, handle) = one_bit_writer(100)?;
    writer.write_header(Header {
        time_zero: -250,
        ..Header::default()
    })?;
    writer.emit_change(5, handle, SignalValue::Bit('1'))?;
    let bytes = writer.finish()?.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    assert_eq!(reader.header().time_zero, -250);
    let mut changes = reader.next_value_changes()?.expect("VC block");
    assert_eq!(changes.next().expect("event")?.timestamp, 5);
    Ok(())
}

#[test]
fn writer_rejects_non_monotonic_time_and_invalid_declarations() -> wavefst::Result<()> {
    let (mut writer, handle) = one_bit_writer(100)?;
    writer.write_header(Header::default())?;
    writer.emit_change(10, handle, SignalValue::Bit('0'))?;
    assert!(
        writer
            .emit_change(9, handle, SignalValue::Bit('1'))
            .is_err()
    );

    let mut invalid = FstWriter::builder(Cursor::new(Vec::new())).build()?;
    assert!(
        invalid
            .begin_scope(ScopeType::VcdModule, "bad\0scope", None)
            .is_err()
    );
    invalid.begin_scope(ScopeType::VcdModule, "top", None)?;
    assert!(
        invalid
            .add_variable(
                VarType::VcdWire,
                VarDir::Implicit,
                "zero",
                GeomEntry::Fixed(0),
            )
            .is_err()
    );
    assert!(
        invalid
            .add_variable(
                VarType::VcdReal,
                VarDir::Implicit,
                "wrong_real",
                GeomEntry::Fixed(64),
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn dump_activity_round_trips_through_blackout_block() -> wavefst::Result<()> {
    let (mut writer, handle) = one_bit_writer(100)?;
    writer.write_header(Header::default())?;
    writer.emit_change(0, handle, SignalValue::Bit('0'))?;
    writer.emit_dump_active(4, false)?;
    writer.emit_dump_active(8, true)?;
    let bytes = writer.finish()?.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    while reader.next_vc_block()?.is_some() {}
    let blackout = reader.blackout().expect("blackout block");
    assert_eq!(blackout.events.len(), 2);
    assert!(!blackout.events[0].is_on);
    assert_eq!(blackout.events[0].time, 4);
    assert!(blackout.events[1].is_on);
    assert_eq!(blackout.events[1].time, 8);
    Ok(())
}

#[test]
fn hierarchy_attributes_preserve_order_and_payload() -> wavefst::Result<()> {
    let mut writer = FstWriter::builder(Cursor::new(Vec::new())).build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    writer.add_attribute(0, 0, "generated by wavefst", 7)?;
    writer.begin_attribute(1, 2, "packed", 3)?;
    writer.end_attribute()?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;
    let bytes = writer.finish()?.into_inner();

    let reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let hierarchy = reader.hierarchy().expect("hierarchy");
    assert_eq!(hierarchy.attributes.len(), 2);
    assert_eq!(hierarchy.attributes[0].name, "generated by wavefst");
    assert_eq!(hierarchy.attributes[0].argument, 7);
    assert_eq!(hierarchy.attributes[1].subtype, 2);
    Ok(())
}

#[test]
fn real_values_follow_the_header_byte_order() -> wavefst::Result<()> {
    let byte_order = match FstByteOrder::native() {
        FstByteOrder::LittleEndian => FstByteOrder::BigEndian,
        FstByteOrder::BigEndian => FstByteOrder::LittleEndian,
    };
    let mut writer = FstWriter::builder(Cursor::new(Vec::new())).build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle =
        writer.add_variable(VarType::VcdReal, VarDir::Implicit, "real", GeomEntry::Real)?;
    writer.end_scope()?;
    writer.write_header(Header {
        double_byte_order: byte_order,
        ..Header::default()
    })?;
    writer.emit_change(3, handle, SignalValue::Real(-1234.5))?;
    let bytes = writer.finish()?.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    assert_eq!(reader.header().double_byte_order, byte_order);
    let mut changes = reader.next_value_changes()?.expect("real VC block");
    assert_eq!(
        changes.next().expect("real event")?.value,
        SignalValue::Real(-1234.5)
    );
    Ok(())
}

#[cfg(feature = "mmap")]
#[test]
fn memory_map_is_a_seekable_reader_backend() -> wavefst::Result<()> {
    let (mut writer, handle) = one_bit_writer(100)?;
    writer.write_header(Header::default())?;
    writer.emit_change(3, handle, SignalValue::Bit('1'))?;
    let bytes = writer.finish()?.into_inner();

    let directory = tempdir()?;
    let path = directory.path().join("mapped.fst");
    std::fs::write(&path, bytes)?;
    // SAFETY: the temporary file is immutable until the reader and mapping are dropped.
    let mmap = unsafe { MemoryMap::open(&path)? };
    let mut reader = ReaderBuilder::new(mmap).build()?;
    let changes = reader.next_value_changes()?.expect("VC block");
    let mut timestamp = None;
    changes.try_for_each(|event| timestamp = Some(event.timestamp))?;
    assert_eq!(timestamp, Some(3));
    Ok(())
}

#[test]
fn binary_batch_fast_path_matches_regular_changes() -> wavefst::Result<()> {
    let mut writer = FstWriter::builder(Cursor::new(Vec::new())).build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let first = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "first",
        GeomEntry::Fixed(1),
    )?;
    let second = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "second",
        GeomEntry::Fixed(1),
    )?;
    let vector = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "vector",
        GeomEntry::Fixed(8),
    )?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;
    writer.emit_binary_batch(0, &[(first, false), (second, true)])?;
    writer.emit_binary_change(4, first, true)?;
    assert!(writer.emit_binary_change(4, vector, false).is_err());
    assert!(
        writer
            .emit_binary_batch(5, &[(second, false), (vector, false)])
            .is_err()
    );
    let bytes = writer.finish()?.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes.clone())).build()?;
    let changes = reader.next_value_changes()?.expect("VC block");
    let mut events = Vec::new();
    changes.try_for_each(|event| {
        events.push((event.timestamp, event.handle, event.value.into_owned()))
    })?;
    assert_eq!(events.len(), 3);
    assert!(events.contains(&(0, first, SignalValue::Bit('0'))));
    assert!(events.contains(&(0, second, SignalValue::Bit('1'))));
    assert!(events.contains(&(4, first, SignalValue::Bit('1'))));

    let mut reader = ReaderBuilder::new(Cursor::new(bytes.clone())).build()?;
    let changes = reader.next_value_changes()?.expect("VC block");
    let mut unordered = Vec::new();
    changes.try_for_each_parts_unordered(|timestamp, handle, alias_of, value| {
        unordered.push((timestamp, handle, alias_of, value.into_owned()));
    })?;
    unordered.sort_by_key(|event| (event.0, event.1));
    let mut ordered: Vec<_> = events
        .into_iter()
        .map(|(timestamp, handle, value)| (timestamp, handle, None, value))
        .collect();
    ordered.sort_by_key(|event| (event.0, event.1));
    assert_eq!(unordered, ordered);

    let mut reader = ReaderBuilder::new(Cursor::new(bytes.clone())).build()?;
    let changes = reader.next_value_changes()?.expect("VC block");
    let binary_count = changes.try_fold_binary(0usize, |count, _, _, _, _| count + 1)?;
    assert_eq!(binary_count, 3);

    #[cfg(feature = "parallel")]
    {
        let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
        let changes = reader.next_value_changes()?.expect("VC block");
        let count = changes.try_fold_parts_parallel(
            || 0usize,
            |count, _, _, _, _| *count += 1,
            |left, right| left + right,
        )?;
        assert_eq!(count, 3);
    }

    let (mut extended_writer, extended_handle) = one_bit_writer(100)?;
    extended_writer.write_header(Header::default())?;
    extended_writer.emit_change(0, extended_handle, SignalValue::Bit('x'))?;
    let extended = extended_writer.finish()?.into_inner();
    let mut reader = ReaderBuilder::new(Cursor::new(extended)).build()?;
    let changes = reader.next_value_changes()?.expect("VC block");
    assert!(
        changes
            .try_fold_binary(0usize, |count, _, _, _, _| count + 1)
            .is_err()
    );
    Ok(())
}

#[cfg(all(feature = "gzip", feature = "lz4", feature = "parallel"))]
#[test]
fn explicit_parallel_codec_pool_round_trips_zlib_and_lz4_chains() -> wavefst::Result<()> {
    const SIGNALS: usize = 64;
    const STEPS: usize = 2_048;

    use std::num::NonZeroUsize;
    use wavefst::CodecParallelism;

    let parallelism =
        CodecParallelism::Threads(NonZeroUsize::new(2).expect("non-zero worker count"));
    for compression in [ChainCompression::Zlib, ChainCompression::Lz4] {
        let mut writer = FstWriter::builder(Cursor::new(Vec::new()))
            .codec_parallelism(parallelism)
            .chain_compression(compression)
            .time_compression(TimeCompression::Zlib)
            .build()?;
        writer.begin_scope(ScopeType::VcdModule, "parallel", None)?;
        let mut batch = Vec::with_capacity(SIGNALS);
        for signal in 0..SIGNALS {
            let handle = writer.add_variable(
                VarType::VcdWire,
                VarDir::Implicit,
                format!("s{signal}"),
                GeomEntry::Fixed(1),
            )?;
            batch.push((handle, false));
        }
        writer.end_scope()?;
        writer.write_header(Header::default())?;

        let mut states = (1..=SIGNALS)
            .map(|signal| (signal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .collect::<Vec<_>>();
        for step in 0..STEPS {
            for (signal, (_, value)) in batch.iter_mut().enumerate() {
                let state = &mut states[signal];
                *state ^= *state << 13;
                *state ^= *state >> 7;
                *state ^= *state << 17;
                *value = *state & 1 != 0;
            }
            writer.emit_binary_batch(step as u64, &batch)?;
        }
        let bytes = writer.finish()?.into_inner();

        let mut reader = ReaderBuilder::new(Cursor::new(bytes))
            .codec_parallelism(parallelism)
            .build()?;
        let changes = reader.next_value_changes()?.expect("VC block");
        let count = changes.try_fold_binary(0usize, |count, _, _, _, _| count + 1)?;
        assert_eq!(count, SIGNALS * STEPS);
    }
    Ok(())
}

#[cfg(not(feature = "parallel"))]
#[test]
fn runtime_parallel_codec_policy_requires_the_cargo_feature() {
    use wavefst::CodecParallelism;

    let Err(writer_error) = FstWriter::builder(Cursor::new(Vec::new()))
        .codec_parallelism(CodecParallelism::Auto)
        .build()
    else {
        panic!("writer must reject unavailable parallel support");
    };
    assert!(writer_error.to_string().contains("`parallel` feature"));

    let Err(reader_error) = ReaderBuilder::new(Cursor::new(Vec::<u8>::new()))
        .codec_parallelism(CodecParallelism::Auto)
        .build()
    else {
        panic!("reader must reject unavailable parallel support");
    };
    assert!(reader_error.to_string().contains("`parallel` feature"));
}

#[test]
fn binary_batch_handles_empty_duplicate_and_flush_boundary() -> wavefst::Result<()> {
    let mut writer = FstWriter::builder(Cursor::new(Vec::new()))
        .chain_compression(ChainCompression::Raw)
        .time_compression(TimeCompression::Raw)
        .block_change_limit(2)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let first = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "first",
        GeomEntry::Fixed(1),
    )?;
    let second = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "second",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;

    writer.emit_binary_batch(100, &[])?;
    writer.emit_binary_batch(1, &[(first, false), (first, true), (second, true)])?;
    let bytes = writer.finish()?.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let mut events = Vec::new();
    while let Some(changes) = reader.next_value_changes()? {
        events =
            changes.try_fold_binary(events, |mut events, timestamp, handle, alias_of, value| {
                events.push((timestamp, handle, alias_of, value));
                events
            })?;
    }
    assert_eq!(
        events,
        vec![
            (1, first, None, false),
            (1, first, None, true),
            (1, second, None, true),
        ]
    );
    assert_eq!(reader.header().vc_section_count, 2);
    Ok(())
}

#[test]
fn binary_fold_expands_dynamic_aliases_and_rejects_vectors() -> wavefst::Result<()> {
    let mut writer = FstWriter::builder(Cursor::new(Vec::new()))
        .chain_compression(ChainCompression::Raw)
        .time_compression(TimeCompression::Raw)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let first = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "first",
        GeomEntry::Fixed(1),
    )?;
    let second = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "second",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;
    writer.emit_binary_batch(0, &[(first, false), (second, false)])?;
    writer.emit_binary_batch(2, &[(first, true), (second, true)])?;
    let bytes = writer.finish()?.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let changes = reader.next_value_changes()?.expect("VC block");
    let events = changes.try_fold_binary(
        Vec::new(),
        |mut events, timestamp, handle, alias_of, value| {
            events.push((timestamp, handle, alias_of, value));
            events
        },
    )?;
    assert_eq!(events.len(), 4);
    assert!(
        events
            .iter()
            .any(|event| event.1 == second && event.2 == Some(first))
    );

    let mut vector_writer = FstWriter::builder(Cursor::new(Vec::new()))
        .chain_compression(ChainCompression::Raw)
        .time_compression(TimeCompression::Raw)
        .build()?;
    vector_writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let vector = vector_writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "vector",
        GeomEntry::Fixed(2),
    )?;
    vector_writer.end_scope()?;
    vector_writer.write_header(Header::default())?;
    vector_writer.emit_change(0, vector, SignalValue::Vector("01".into()))?;
    let bytes = vector_writer.finish()?.into_inner();
    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let changes = reader.next_value_changes()?.expect("VC block");
    assert!(
        changes
            .try_fold_binary(0usize, |count, _, _, _, _| count + 1)
            .is_err()
    );
    Ok(())
}

#[test]
fn truncated_inputs_never_panic() -> wavefst::Result<()> {
    let (mut writer, handle) = one_bit_writer(100)?;
    writer.write_header(Header::default())?;
    writer.emit_binary_change(0, handle, false)?;
    writer.emit_binary_change(5, handle, true)?;
    let bytes = writer.finish()?.into_inner();

    for end in 0..bytes.len() {
        if let Ok(mut reader) = ReaderBuilder::new(Cursor::new(&bytes[..end])).build()
            && let Ok(Some(changes)) = reader.next_value_changes()
        {
            let _ = changes.try_for_each_parts(|_, _, _, _| {});
        }
    }
    Ok(())
}

#[test]
fn reader_limits_reject_valid_files_before_large_allocations() -> wavefst::Result<()> {
    let (mut writer, handle) = one_bit_writer(100)?;
    writer.write_header(Header::default())?;
    writer.emit_binary_change(0, handle, false)?;
    let bytes = writer.finish()?.into_inner();

    assert!(
        ReaderBuilder::new(Cursor::new(bytes.clone()))
            .max_handles(0)
            .build()
            .is_err()
    );
    assert!(
        ReaderBuilder::new(Cursor::new(bytes))
            .max_block_bytes(1)
            .build()
            .is_err()
    );
    Ok(())
}

#[cfg(feature = "mmap")]
#[test]
fn memory_map_seek_rejects_out_of_bounds_positions() -> wavefst::Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("seek.bin");
    std::fs::write(&path, b"abcd")?;
    // SAFETY: the temporary file remains immutable while the mapping exists.
    let mut mmap = unsafe { MemoryMap::open(&path)? };

    assert_eq!(mmap.seek(SeekFrom::End(0))?, 4);
    assert_eq!(mmap.seek(SeekFrom::Current(-2))?, 2);
    let mut tail = [0u8; 2];
    mmap.read_exact(&mut tail)?;
    assert_eq!(&tail, b"cd");
    assert!(mmap.seek(SeekFrom::Start(5)).is_err());
    assert!(mmap.seek(SeekFrom::Current(-5)).is_err());
    Ok(())
}
