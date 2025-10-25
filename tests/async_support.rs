#![cfg(feature = "async")]

use std::path::PathBuf;

use anyhow::Result;
use tempfile::tempdir;
use tokio::runtime::Runtime;
use wavefst::{
    ChainCompression, GeomEntry, Header, ScopeType, SignalValue, VarDir, VarType,
    async_support::{AsyncReader, AsyncWriter},
};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/hdl-example.fst")
}

#[test]
fn async_reader_loads_fixture() -> Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let reader = AsyncReader::from_file(fixture_path()).await?;
        assert!(reader.reader().header().vc_section_count > 0);
        Result::<()>::Ok(())
    })?;
    Ok(())
}

#[test]
fn async_writer_round_trip() -> Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let dir = tempdir()?;
        let path = dir.path().join("async-output.fst");
        let sink = tokio::fs::File::create(&path).await?;

        let mut writer = AsyncWriter::builder(sink)
            .chain_compression(ChainCompression::Raw)
            .build()?;

        writer.begin_scope(ScopeType::VcdModule, "tb", None)?;
        let bit = writer.add_variable(
            VarType::VcdWire,
            VarDir::Implicit,
            "bit_sig",
            GeomEntry::Fixed(1),
        )?;
        writer.end_scope()?;

        let header = Header {
            version: "async-writer".into(),
            vc_section_count: 1,
            end_time: 10,
            ..Header::default()
        };
        writer.write_header(header)?;
        writer.emit_change(0, bit, SignalValue::Bit('0'))?;
        writer.emit_change(10, bit, SignalValue::Bit('1'))?;
        let sink = writer.finish().await?;
        drop(sink);

        let produced = tokio::fs::read(&path).await?;
        assert!(!produced.is_empty(), "async writer should produce bytes");

        // Ensure the generated trace can be parsed by the async reader.
        let mut reader = AsyncReader::from_file(&path).await?;
        let mut changes = reader
            .next_value_changes()?
            .expect("expected value change block");
        // Collect remaining blocks to ensure iterator remains usable after finish.
        let mut values = Vec::new();
        for change in &mut changes {
            values.push(change?.value.into_owned());
        }
        // Ensure no additional blocks remain.
        assert!(reader.next_value_changes()?.is_none());

        assert_eq!(
            values
                .iter()
                .filter(|value| matches!(value, SignalValue::Bit(_)))
                .count(),
            2
        );
        Result::<()>::Ok(())
    })?;
    Ok(())
}
