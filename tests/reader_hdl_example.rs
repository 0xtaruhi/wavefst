use std::convert::TryInto;
use std::fs::{File, read};
use std::path::PathBuf;

use anyhow::Result;
use wavefst::ReaderBuilder;
use wavefst::encoding::decode_varint_with_len;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/hdl-example.fst")
}

#[test]
fn inspect_hdl_example_frame() -> Result<()> {
    let path = fixture_path();
    let bytes = read(&path)?;
    let mut pos = 0usize;

    assert_eq!(bytes[pos], 0, "expected header block type");
    pos += 1;

    let section_length = u64::from_be_bytes(bytes[pos..pos + 8].try_into().unwrap());
    pos += 8;
    pos += section_length as usize - 8;

    let block_type = bytes[pos];
    pos += 1;
    assert_eq!(block_type, 8, "expected first VC block");

    let vc_length = u64::from_be_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
    pos += 8;
    let payload = &bytes[pos..pos + vc_length];

    let mut offset = 0usize;
    let begin = u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let end = u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let required = u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let (frame_uncompressed, consumed) = decode_varint_with_len(&payload[offset..])?;
    offset += consumed;
    let (frame_compressed, consumed) = decode_varint_with_len(&payload[offset..])?;
    offset += consumed;
    let (frame_max_handle, consumed) = decode_varint_with_len(&payload[offset..])?;
    offset += consumed;
    let (vc_max_handle, consumed) = decode_varint_with_len(&payload[offset..])?;
    offset += consumed;
    let pack_marker = payload[offset];
    offset += 1;

    println!(
        "begin={begin} end={end} required={required} frame_u={frame_uncompressed} frame_c={frame_compressed} frame_max={frame_max_handle} vc_max={vc_max_handle} pack_marker={pack_marker:02x} ({})",
        pack_marker as char
    );

    #[allow(unused_variables)]
    let frame_bytes = &payload[offset..offset + frame_compressed as usize];
    offset += frame_compressed as usize;
    let _ = offset;

    Ok(())
}

#[cfg_attr(not(feature = "gzip"), ignore = "requires gzip feature")]
#[test]
fn parse_hdl_example_fst() -> Result<()> {
    let path = fixture_path();
    let file = File::open(&path)?;
    let mut reader = ReaderBuilder::new(file).build()?;

    let header = reader.header().clone();
    println!(
        "header start={} end={} max_handle={}",
        header.start_time, header.end_time, header.max_handle
    );

    let meta = reader
        .next_vc_block()?
        .expect("expected at least one VC block");

    println!(
        "vc block: handles={} begin={} end={} index_len={} chain_buf={} time_items={} frame_bytes={} frame_comp={} frame_uncomp={}",
        meta.header.vc_max_handle,
        meta.header.begin_time,
        meta.header.end_time,
        meta.header.index_length,
        meta.chain_buffer.len(),
        meta.time_table.timestamps.len(),
        meta.frame.data.len(),
        meta.header.frame_compressed_len,
        meta.header.frame_uncompressed_len
    );

    assert!(!meta.chain_buffer.is_empty());
    assert!(!meta.time_table.timestamps.is_empty());

    Ok(())
}

#[cfg_attr(not(feature = "gzip"), ignore = "requires gzip feature")]
#[test]
fn iterate_hdl_example_changes() -> Result<()> {
    let path = fixture_path();
    let file = File::open(&path)?;
    let mut reader = ReaderBuilder::new(file).build()?;

    {
        let mut changes = reader
            .next_value_changes()?
            .expect("expected value-change block");

        let mut count = 0usize;
        for change in &mut changes {
            change?;
            count += 1;
        }
        assert_eq!(
            count, 0,
            "fixture is expected to contain only the initial frame"
        );
    }

    assert!(
        reader.geometry().is_some(),
        "geometry metadata should be available after iterating"
    );
    assert!(
        reader.hierarchy().is_some(),
        "hierarchy metadata should be available after iterating"
    );

    Ok(())
}
