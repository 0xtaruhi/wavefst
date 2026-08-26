use std::env;
use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ReaderBuilder, ScopeType, TimeCompression,
    VarDir, VarType,
};
use wellen::stream::Filter;

fn main() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 7 {
        bail!(
            "usage: wavefst-ecosystem-bench <generate|wavefst-all|wavefst-one|wavefst-open|wellen-all|wellen-one|wellen-open> <file> <signals> <steps> <iterations> <warmup>"
        );
    }
    let mode = args[1].as_str();
    let path = Path::new(&args[2]);
    let signals = args[3].parse::<usize>()?;
    let steps = args[4].parse::<usize>()?;
    let iterations = args[5].parse::<usize>()?;
    let warmup = args[6].parse::<usize>()?;
    if signals == 0 || steps == 0 {
        bail!("signals and steps must be nonzero");
    }
    if mode == "generate" {
        println!("{}", generate_trace(path, signals, steps)?);
        return Ok(());
    }
    if iterations == 0 {
        bail!("iterations must be nonzero");
    }

    let expected = match mode {
        "wavefst-all" | "wellen-all" => signals
            .checked_mul(steps)
            .context("expected event count overflow")?,
        "wavefst-one" | "wellen-one" => steps,
        "wavefst-open" | "wellen-open" => signals,
        _ => bail!("unknown mode {mode}"),
    };
    for _ in 0..warmup {
        black_box(run(mode, path, expected)?);
    }
    let start = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iterations {
        checksum = checksum.wrapping_add(black_box(run(mode, path, expected)?));
    }
    black_box(checksum);
    println!("{}", start.elapsed().as_nanos() / iterations as u128);
    Ok(())
}

fn run(mode: &str, path: &Path, expected: usize) -> Result<usize> {
    let count = match mode {
        "wavefst-all" => wavefst_stream(path, false)?,
        "wavefst-one" => wavefst_stream(path, true)?,
        "wavefst-open" => wavefst_open(path)?,
        "wellen-all" => wellen_stream(path, false)?,
        "wellen-one" => wellen_stream(path, true)?,
        "wellen-open" => wellen_open(path)?,
        _ => bail!("unknown mode {mode}"),
    };
    if count != expected {
        bail!("{mode} expected {expected}, got {count}");
    }
    Ok(count)
}

fn generate_trace(path: &Path, signals: usize, steps: usize) -> Result<u64> {
    let mut writer = FstWriter::builder(File::create(path)?)
        .chain_compression(ChainCompression::Zlib)
        .time_compression(TimeCompression::Zlib)
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
        writer.emit_binary_batch(step as u64, &batch)?;
    }
    let file = writer.finish()?;
    Ok(file.metadata()?.len())
}

fn wavefst_stream(path: &Path, one: bool) -> Result<usize> {
    let builder = ReaderBuilder::new(File::open(path)?);
    let mut reader = if one {
        builder.include_handles([1]).build()?
    } else {
        builder.build()?
    };
    let mut count = 0usize;
    while let Some(changes) = reader.next_value_changes()? {
        count = changes.try_fold_binary(count, |count, time, handle, alias, value| {
            black_box((time, handle, alias, value));
            count + 1
        })?;
    }
    Ok(count)
}

fn wavefst_open(path: &Path) -> Result<usize> {
    let reader = ReaderBuilder::new(File::open(path)?).build()?;
    Ok(reader
        .hierarchy()
        .context("wavefst did not decode a hierarchy")?
        .variables
        .len())
}

fn wellen_stream(path: &Path, one: bool) -> Result<usize> {
    let options = wellen::LoadOptions {
        multi_thread: false,
        ..Default::default()
    };
    let mut waveform = wellen::stream::read_from_file(path, &options)?;
    let first = waveform
        .hierarchy()
        .signals()
        .next()
        .context("wellen did not decode a signal")?;
    let selected = [first];
    let filter = if one {
        Filter::include_signals(&selected)
    } else {
        Filter::all()
    };
    let mut count = 0usize;
    waveform
        .stream_changes(filter, |time, signal, value| {
            black_box((time, signal, value));
            count += 1;
            Ok::<_, std::convert::Infallible>(())
        })
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    Ok(count)
}

fn wellen_open(path: &Path) -> Result<usize> {
    let options = wellen::LoadOptions {
        multi_thread: false,
        ..Default::default()
    };
    let waveform = wellen::stream::read_from_file(path, &options)?;
    Ok(waveform.hierarchy().signals().count())
}
