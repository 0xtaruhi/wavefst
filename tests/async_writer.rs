#![cfg(feature = "async-write")]

use anyhow::Result;
use tempfile::tempdir;
use tokio::runtime::Runtime;
use wavefst::{
    AsyncWriter, ChainCompression, GeomEntry, Header, ScopeType, SignalValue, VarDir, VarType,
};

#[test]
fn async_writer_produces_a_trace() -> Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let directory = tempdir()?;
        let path = directory.path().join("async-output.fst");
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
        writer.write_header(Header::default())?;
        writer.emit_change(0, bit, SignalValue::Bit('0'))?;
        writer.emit_change(10, bit, SignalValue::Bit('1'))?;
        drop(writer.finish().await?);

        let produced = tokio::fs::read(path).await?;
        assert!(!produced.is_empty());
        Result::<()>::Ok(())
    })?;
    Ok(())
}
