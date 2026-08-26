#![cfg(feature = "async-read")]

use std::path::PathBuf;

use anyhow::Result;
use tokio::runtime::Runtime;
use wavefst::AsyncReader;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/hdl-example.fst")
}

#[test]
fn async_reader_loads_fixture() -> Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let reader = AsyncReader::from_file(fixture_path()).await?;
        assert!(reader.reader().header().vc_section_count > 0);
        Result::<()>::Ok(())
    })?;
    Ok(())
}
