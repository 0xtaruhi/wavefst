//! Reproducible dense-writer benchmark used by `scripts/bench-writers.sh`.

use std::env;
use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, HierarchyCompression, ScopeType,
    TimeCompression, VarDir, VarType,
};

fn main() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 7 {
        bail!(
            "usage: writer_ecosystem_bench <batch|scalar> <file> <signals> <steps> <iterations> <warmup>"
        );
    }
    let mode = args[1].as_str();
    if !matches!(mode, "batch" | "scalar") {
        bail!("unknown mode {mode}");
    }
    let path = Path::new(&args[2]);
    let signals = args[3].parse::<usize>()?;
    let steps = args[4].parse::<usize>()?;
    let iterations = args[5].parse::<usize>()?;
    let warmup = args[6].parse::<usize>()?;
    if signals == 0 || steps == 0 || iterations == 0 {
        bail!("signals, steps, and iterations must be nonzero");
    }

    for _ in 0..warmup {
        black_box(write_trace(path, signals, steps, mode)?);
    }
    let start = Instant::now();
    let mut bytes = 0u64;
    for _ in 0..iterations {
        bytes = bytes.wrapping_add(black_box(write_trace(path, signals, steps, mode)?));
    }
    black_box(bytes);
    println!("{}", start.elapsed().as_nanos() / iterations as u128);
    Ok(())
}

fn write_trace(path: &Path, signals: usize, steps: usize, mode: &str) -> Result<u64> {
    let mut writer = FstWriter::builder(File::create(path)?)
        .chain_compression(ChainCompression::Lz4)
        .time_compression(TimeCompression::Zlib)
        .hierarchy_compression(HierarchyCompression::Lz4)
        .block_change_limit(usize::MAX)
        .block_size_limit(usize::MAX)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "dense", None)?;
    let handles = (0..signals)
        .map(|signal| {
            writer.add_variable(
                VarType::VcdWire,
                VarDir::Implicit,
                format!("s{signal}"),
                GeomEntry::Fixed(1),
            )
        })
        .collect::<wavefst::Result<Vec<_>>>()?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;

    let mut states = (1..=signals)
        .map(|signal| (signal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .collect::<Vec<_>>();
    let mut batch = handles
        .into_iter()
        .map(|handle| (handle, false))
        .collect::<Vec<_>>();
    for step in 0..steps {
        for (signal, item) in batch.iter_mut().enumerate() {
            let state = &mut states[signal];
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            item.1 = *state & 1 != 0;
        }
        if mode == "batch" {
            writer.emit_binary_batch(step as u64, &batch)?;
        } else {
            for &(handle, value) in &batch {
                writer.emit_binary_change(step as u64, handle, value)?;
            }
        }
    }
    let file = writer.finish()?;
    file.metadata()
        .map(|metadata| metadata.len())
        .context("read output metadata")
}
