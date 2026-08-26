//! Reproducible dense-trace benchmark used by `scripts/bench-libfst.sh`.

use std::env;
use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, anyhow, bail};
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ReaderBuilder, ScopeType, TimeCompression,
    VarDir, VarType,
};

const SIGNALS: usize = 512;
const STEPS: usize = 128;
const EXPECTED_EVENTS: usize = SIGNALS * STEPS;
const EXPECTED_SELECTED_EVENTS: usize = STEPS;

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_default();
    let path = args.next().unwrap_or_default();
    let iterations = parse_count(args.next(), "iterations")?;
    let warmup = parse_count(args.next(), "warmup")?;
    if args.next().is_some()
        || path.is_empty()
        || !matches!(mode.as_str(), "read" | "read-one" | "write")
    {
        bail!("usage: libfst_bench <read|read-one|write> <path> <iterations> <warmup>");
    }

    let operation: fn(&Path) -> Result<usize> = match mode.as_str() {
        "read" => read_trace,
        "read-one" => read_trace_selected,
        "write" => write_trace,
        _ => unreachable!(),
    };
    let path = Path::new(&path);
    for _ in 0..warmup {
        black_box(operation(path)?);
    }

    let start = Instant::now();
    let mut result = 0usize;
    for _ in 0..iterations {
        result = result.wrapping_add(black_box(operation(path)?));
    }
    black_box(result);
    let nanos = start.elapsed().as_nanos() / iterations as u128;
    println!("{nanos}");
    Ok(())
}

fn parse_count(value: Option<String>, name: &str) -> Result<usize> {
    let value = value.ok_or_else(|| anyhow!("missing {name}"))?;
    let count = value
        .parse::<usize>()
        .map_err(|_| anyhow!("invalid {name}"))?;
    if count == 0 && name == "iterations" {
        bail!("iterations must be non-zero");
    }
    Ok(count)
}

fn write_trace(path: &Path) -> Result<usize> {
    let mut writer = FstWriter::builder(File::create(path)?)
        .chain_compression(ChainCompression::Zlib)
        .time_compression(TimeCompression::Zlib)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "dense", None)?;
    let handles: Vec<_> = (0..SIGNALS)
        .map(|signal| {
            writer.add_variable(
                VarType::VcdWire,
                VarDir::Implicit,
                format!("s{signal}"),
                GeomEntry::Fixed(1),
            )
        })
        .collect::<wavefst::Result<_>>()?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;

    let mut states: Vec<u64> = (1..=SIGNALS)
        .map(|signal| (signal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .collect();
    let mut batch: Vec<_> = handles.into_iter().map(|handle| (handle, false)).collect();
    for step in 0..STEPS {
        for (signal, item) in batch.iter_mut().enumerate() {
            let state = &mut states[signal];
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            item.1 = *state & 1 != 0;
        }
        writer.emit_binary_batch(step as u64, &batch)?;
    }
    let file = writer.finish()?;
    Ok(usize::try_from(file.metadata()?.len())?)
}

fn read_trace(path: &Path) -> Result<usize> {
    let mut reader = ReaderBuilder::new(File::open(path)?).build()?;
    let mut count = 0usize;
    while let Some(changes) = reader.next_value_changes()? {
        count = changes.try_fold_binary(count, |count, time, handle, alias, value| {
            black_box((time, handle, alias, value));
            count + 1
        })?;
    }
    if count != EXPECTED_EVENTS {
        bail!("benchmark expected {EXPECTED_EVENTS} events, decoded {count}");
    }
    Ok(count)
}

fn read_trace_selected(path: &Path) -> Result<usize> {
    let mut reader = ReaderBuilder::new(File::open(path)?)
        .include_handles([1])
        .load_hierarchy(false)
        .build()?;
    let mut count = 0usize;
    while let Some(changes) = reader.next_value_changes()? {
        count = changes.try_fold_binary(count, |count, time, handle, alias, value| {
            black_box((time, handle, alias, value));
            count + 1
        })?;
    }
    if count != EXPECTED_SELECTED_EVENTS {
        bail!("benchmark expected {EXPECTED_SELECTED_EVENTS} selected events, decoded {count}");
    }
    Ok(count)
}
