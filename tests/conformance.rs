#![cfg(any(feature = "gzip", feature = "lz4"))]

use std::io::Cursor;

use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ReaderBuilder, ScopeType, SignalValue,
    TimeCompression, VarDir, VarType,
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
