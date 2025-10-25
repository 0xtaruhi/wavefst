use std::borrow::Cow;
use std::convert::TryInto;
use std::io::Cursor;

use anyhow::Result;
#[cfg(feature = "gzip")]
use flate2::read::GzDecoder;
use wavefst::encoding::decode_varint_with_len;
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, PackType, ReaderBuilder, ScopeType,
    SignalValue, TimeCompression, VarDir, VarType,
};

#[test]
fn writer_emits_header_and_metadata() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink).build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "sig",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "fst-writer-test".into(),
        end_time: 10,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    writer.emit_change(5, handle, SignalValue::Bit('1'))?;

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let geom = reader
        .geometry()
        .expect("geometry block should be present after writer emits metadata");
    assert_eq!(geom.entries.len(), 1);

    let hier = reader
        .hierarchy()
        .expect("hierarchy block should be present after writer emits metadata");
    assert_eq!(hier.scopes.len(), 1);
    assert_eq!(hier.variables.len(), 1);

    let mut changes = reader
        .next_value_changes()?
        .expect("expected a value-change block after emitting a change");
    let change = changes.next().expect("iterator should yield one entry")?;
    assert_eq!(change.handle, handle);
    assert_eq!(change.timestamp, 5);
    assert_eq!(change.value, SignalValue::Bit('1'));
    assert!(changes.next().is_none());

    Ok(())
}

#[test]
fn writer_applies_compression_preferences() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink)
        .chain_compression(ChainCompression::Lz4)
        .time_compression(TimeCompression::Zlib)
        .build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "sig",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "fst-writer-test".into(),
        end_time: 10_000,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    for step in 0..2048u64 {
        let bit = if step & 1 == 0 { '0' } else { '1' };
        writer.emit_change(step, handle, SignalValue::Bit(bit))?;
    }

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let block = reader
        .next_vc_block()?
        .expect("expected a value-change block after emitting changes");

    assert_eq!(block.header.pack_type(), PackType::Lz4);
    assert_eq!(block.header.frame_max_handle, 1);
    assert_eq!(block.header.frame_uncompressed_len, 1);
    assert_eq!(block.frame.as_slice(), b"1");
    assert!(
        block.time_section.compressed_len <= block.time_section.uncompressed_len,
        "time section must not grow after compression"
    );
    if block.time_section.uncompressed_len > 0 {
        assert!(
            block.time_section.compressed_len < block.time_section.uncompressed_len,
            "time section should benefit from zlib compression for repetitive data"
        );
    }

    assert!(
        !block.chain_buffer.is_empty(),
        "expected chain payload to be present"
    );
    let (stored_len, consumed) = decode_varint_with_len(&block.chain_buffer)?;
    assert!(
        stored_len > 0,
        "stored_len should be non-zero when compression is applied"
    );
    let compressed_len = block.chain_buffer.len() - consumed;
    assert!(
        compressed_len < stored_len as usize,
        "compressed chain should be smaller than the raw payload (compressed {compressed_len}, raw {stored_len})"
    );

    Ok(())
}

#[cfg(feature = "fastlz")]
#[test]
fn writer_fastlz_chain_compression() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink)
        .chain_compression(ChainCompression::FastLz)
        .time_compression(TimeCompression::Raw)
        .build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "sig",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "fst-fastlz-test".into(),
        end_time: 512,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    for step in 0..512u64 {
        let bit = if (step & 3) == 0 { '1' } else { '0' };
        writer.emit_change(step, handle, SignalValue::Bit(bit))?;
    }

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes.clone())).build()?;
    let block = reader
        .next_vc_block()?
        .expect("value-change block must be present");
    assert_eq!(block.header.pack_type(), PackType::FastLz);
    assert_eq!(block.header.frame_max_handle, 1);
    assert_eq!(block.header.frame_uncompressed_len, 1);
    assert_eq!(block.frame.as_slice(), b"0");

    let mut iter_reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let mut iter = iter_reader
        .next_value_changes()?
        .expect("change iterator should decode fastlz payloads");
    let first = iter.next().expect("iterator yields entries")?;
    assert_eq!(first.handle, handle);
    assert_eq!(first.timestamp, 0);
    assert_eq!(first.value, SignalValue::Bit('1'));
    Ok(())
}

#[test]
fn writer_handles_multiple_signals_with_raw_encoding() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink)
        .chain_compression(ChainCompression::Raw)
        .time_compression(TimeCompression::Raw)
        .build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let a = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "sig_a",
        GeomEntry::Fixed(1),
    )?;
    let b = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "sig_b",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "multi-handle".into(),
        end_time: 40,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    writer.emit_change(0, a, SignalValue::Bit('1'))?;
    writer.emit_change(10, b, SignalValue::Bit('0'))?;
    writer.emit_change(15, a, SignalValue::Bit('0'))?;
    writer.emit_change(25, b, SignalValue::Bit('x'))?;

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes.clone())).build()?;
    let block = reader
        .next_vc_block()?
        .expect("value-change block must be present");
    assert_eq!(block.header.pack_type(), PackType::None);
    assert_eq!(block.header.frame_max_handle, 2);
    assert_eq!(block.header.frame_uncompressed_len, 2);
    assert_eq!(block.header.frame_compressed_len, 2);
    assert_eq!(block.frame.as_slice(), b"0x");

    let (stored_len_a, consumed_a) = decode_varint_with_len(&block.chain_buffer)?;
    assert_eq!(stored_len_a, 0, "raw chain should use stored_len = 0");
    assert!(consumed_a < block.chain_buffer.len());

    let mut iter_reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let mut iter = iter_reader
        .next_value_changes()?
        .expect("iterator should be present for raw block");
    let mut events = Vec::new();
    for event in &mut iter {
        events.push(event?);
    }

    assert_eq!(events.len(), 4);
    assert_eq!(events[0].timestamp, 0);
    assert_eq!(events[0].handle, a);
    assert_eq!(events[0].value, SignalValue::Bit('1'));
    assert_eq!(events[1].timestamp, 10);
    assert_eq!(events[1].handle, b);
    assert_eq!(events[1].value, SignalValue::Bit('0'));
    assert_eq!(events[2].timestamp, 15);
    assert_eq!(events[2].handle, a);
    assert_eq!(events[2].value, SignalValue::Bit('0'));
    assert_eq!(events[3].timestamp, 25);
    assert_eq!(events[3].handle, b);
    assert_eq!(events[3].value, SignalValue::Bit('x'));

    Ok(())
}

#[test]
fn writer_emits_extended_bit_states() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink).build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "sig",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "bit-states".into(),
        end_time: 30,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    writer.emit_change(0, handle, SignalValue::Bit('z'))?;
    writer.emit_change(5, handle, SignalValue::Bit('1'))?;
    writer.emit_change(10, handle, SignalValue::Bit('U'))?;

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let mut changes = reader
        .next_value_changes()?
        .expect("value-change iterator should be available");

    let mut collected = Vec::new();
    for evt in &mut changes {
        collected.push(evt?);
    }

    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0].value, SignalValue::Bit('z'));
    assert_eq!(collected[1].value, SignalValue::Bit('1'));
    assert_eq!(collected[2].value, SignalValue::Bit('u'));

    Ok(())
}

#[test]
fn writer_emits_vector_changes() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink).build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "bus",
        GeomEntry::Fixed(4),
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "vector-test".into(),
        end_time: 10,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    writer.emit_change(0, handle, SignalValue::Vector(Cow::Borrowed("01xz")))?;
    writer.emit_change(6, handle, SignalValue::Vector(Cow::Borrowed("zzzz")))?;

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes.clone())).build()?;
    let block = reader
        .next_vc_block()?
        .expect("value-change block must be present");
    assert_eq!(block.header.frame_uncompressed_len, 4);
    assert_eq!(block.frame.as_slice(), b"zzzz");

    let mut iter_reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let mut iter = iter_reader
        .next_value_changes()?
        .expect("vector iterator should be available");

    let first = iter.next().expect("first vector change")?;
    assert_eq!(first.timestamp, 0);
    assert_eq!(first.value, SignalValue::Vector(Cow::Borrowed("01xz")));

    let second = iter.next().expect("second vector change")?;
    assert_eq!(second.timestamp, 6);
    assert_eq!(second.value, SignalValue::Vector(Cow::Borrowed("zzzz")));

    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn writer_compresses_redundant_frame_payloads() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink)
        .chain_compression(ChainCompression::Raw)
        .time_compression(TimeCompression::Raw)
        .build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle_count = 512;
    let mut handles = Vec::with_capacity(handle_count);
    for idx in 0..handle_count {
        handles.push(writer.add_variable(
            VarType::VcdWire,
            VarDir::Implicit,
            format!("sig_{idx}"),
            GeomEntry::Fixed(1),
        )?);
    }
    writer.end_scope()?;

    let header = Header {
        version: "frame-compress".into(),
        end_time: 0,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    for handle in &handles {
        writer.emit_change(0, *handle, SignalValue::Bit('0'))?;
    }

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let block = reader
        .next_vc_block()?
        .expect("value-change block must be present");

    assert!(
        block.header.frame_compressed_len < block.header.frame_uncompressed_len,
        "expected frame compression to shrink payload ({} !< {})",
        block.header.frame_compressed_len,
        block.header.frame_uncompressed_len
    );
    assert_eq!(
        block.frame.as_slice(),
        vec![b'0'; handle_count].as_slice(),
        "frame bytes should track the most recent state for each handle"
    );

    Ok(())
}

#[test]
fn writer_handles_alias_packed_real_and_varlen() -> Result<()> {
    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink).build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let bus = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "bus",
        GeomEntry::Fixed(8),
    )?;
    let bus_alias = writer.add_alias(VarType::VcdWire, VarDir::Implicit, "bus_alias", bus)?;
    let real_sig = writer.add_variable(
        VarType::VcdReal,
        VarDir::Implicit,
        "real_sig",
        GeomEntry::Real,
    )?;
    let str_sig = writer.add_variable(
        VarType::GenString,
        VarDir::Implicit,
        "str_sig",
        GeomEntry::Variable,
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "alias-varlen".into(),
        end_time: 20,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    writer.emit_change(0, bus, SignalValue::Vector(Cow::Borrowed("10101010")))?;
    writer.emit_change(5, real_sig, SignalValue::Real(3.125))?;
    writer.emit_change(
        10,
        bus_alias,
        SignalValue::Vector(Cow::Borrowed("01010101")),
    )?;
    writer.emit_change(12, str_sig, SignalValue::Bytes(Cow::Borrowed(b"hello")))?;

    let sink = writer.finish()?;
    let bytes = sink.into_inner();

    let mut reader = ReaderBuilder::new(Cursor::new(bytes)).build()?;
    let mut iter = reader
        .next_value_changes()?
        .expect("value-change iterator should be available");

    let mut events = Vec::new();
    for entry in &mut iter {
        events.push(entry?);
    }

    assert_eq!(
        events.len(),
        6,
        "expected canonical + alias events plus reals/varlen"
    );

    assert_eq!(events[0].timestamp, 0);
    assert_eq!(events[0].handle, bus);
    assert_eq!(events[0].alias_of, None);
    assert_eq!(
        events[0].value,
        SignalValue::PackedBits {
            width: 8,
            bits: Cow::Owned(vec![0b10101010])
        }
    );

    assert_eq!(events[1].timestamp, 0);
    assert_eq!(events[1].handle, bus_alias);
    assert_eq!(events[1].alias_of, Some(bus));
    assert_eq!(events[1].value, events[0].value);

    assert_eq!(events[2].timestamp, 5);
    assert_eq!(events[2].handle, real_sig);
    assert_eq!(events[2].value, SignalValue::Real(3.125));

    assert_eq!(events[3].timestamp, 10);
    assert_eq!(events[3].handle, bus);
    assert_eq!(
        events[3].value,
        SignalValue::PackedBits {
            width: 8,
            bits: Cow::Owned(vec![0b01010101])
        }
    );

    assert_eq!(events[4].timestamp, 10);
    assert_eq!(events[4].handle, bus_alias);
    assert_eq!(events[4].alias_of, Some(bus));
    assert_eq!(events[4].value, events[3].value);

    assert_eq!(events[5].timestamp, 12);
    assert_eq!(events[5].handle, str_sig);
    assert_eq!(
        events[5].value,
        SignalValue::Bytes(Cow::Owned(b"hello".to_vec()))
    );

    Ok(())
}

#[cfg(feature = "gzip")]
#[test]
fn writer_wraps_with_zlib_envelope() -> Result<()> {
    use std::io::Read;

    let sink = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(sink).wrap_with_zlib(true).build()?;

    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let handle = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "sig",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;

    let header = Header {
        version: "zwrapper-test".into(),
        end_time: 5,
        vc_section_count: 1,
        ..Header::default()
    };
    writer.write_header(header)?;

    writer.emit_change(2, handle, SignalValue::Bit('1'))?;

    let sink = writer.finish()?;
    let bytes = sink.into_inner();
    assert!(!bytes.is_empty());
    assert_eq!(bytes[0], wavefst::BlockType::ZWrapper as u8);

    let section_length = u64::from_be_bytes(bytes[1..9].try_into()?);
    let uncompressed_len = u64::from_be_bytes(bytes[9..17].try_into()?);
    let payload_len = u64::from_be_bytes(bytes[17..25].try_into()?);
    assert_eq!(section_length, payload_len + 16 + 8);
    assert!(uncompressed_len > 0);

    let mut decoder = GzDecoder::new(&bytes[25..25 + payload_len as usize]);
    let mut inner = Vec::new();
    decoder.read_to_end(&mut inner)?;
    assert_eq!(inner.len() as u64, uncompressed_len);

    let mut reader = ReaderBuilder::new(Cursor::new(inner)).build()?;
    let mut iter = reader
        .next_value_changes()?
        .expect("value-change iterator should be available");
    let event = iter.next().expect("expected one event")?;
    assert_eq!(event.timestamp, 2);
    assert_eq!(event.handle, handle);
    assert_eq!(event.value, SignalValue::Bit('1'));
    assert!(iter.next().is_none());

    Ok(())
}
