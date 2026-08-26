use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufReader, Read};
use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};
use fst_reader::{FstFilter, FstReader as UpstreamReader, FstSignalHandle, FstSignalValue};
use wavefst_022::ReaderBuilder;
use wavefst_head::ReaderBuilder as HeadReaderBuilder;
use wavefst_head::{
    ChainCompression, FstWriter, GeomEntry, Header, ScopeType, TimeCompression, VarDir, VarType,
};
use wellen::SignalRef;

const DEFAULT_SIGNALS: usize = 500_000;
const DEFAULT_STEPS: usize = 100;
const SEED: u64 = 0x6a09_e667_f3bc_c909;

#[derive(Clone, Copy)]
struct IoCounters {
    read_bytes: u64,
    rchar: u64,
}

#[derive(Clone)]
struct Query {
    handles: Option<Vec<u32>>,
    time: Option<RangeInclusive<u64>>,
}

fn main() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some("generate") => generate_command(&args),
        Some("validate") => validate_command(&args),
        Some("cache") => cache_command(&args),
        Some("run") => run_command(&args),
        _ => bail!(
            "usage:\n  wavefst-selective-bench generate <file> [signals] [steps]\n  wavefst-selective-bench validate <file> <signals> <steps>\n  wavefst-selective-bench cache <warm|evict> <file>\n  wavefst-selective-bench run <wavefst-head|wavefst-0.2.2|fst-reader-0.17|wellen-load-signals> <A|B|C|D|E> <file> [signals] [steps]"
        ),
    }
}

fn generate_command(args: &[String]) -> Result<()> {
    ensure!(
        (3..=5).contains(&args.len()),
        "generate expects <file> [signals] [steps]"
    );
    let signals = parse_or(args.get(3), DEFAULT_SIGNALS, "signals")?;
    let steps = parse_or(args.get(4), DEFAULT_STEPS, "steps")?;
    ensure!(
        signals > 0 && steps >= 2,
        "signals must be positive and steps at least two"
    );
    let path = Path::new(&args[2]);
    let bytes = generate_trace(path, signals, steps)?;
    println!("generated\t{signals}\t{steps}\t{bytes}\t{}", path.display());
    Ok(())
}

fn validate_command(args: &[String]) -> Result<()> {
    ensure!(args.len() == 5, "validate expects <file> <signals> <steps>");
    let signals = parse_or(args.get(3), DEFAULT_SIGNALS, "signals")?;
    let steps = parse_or(args.get(4), DEFAULT_STEPS, "steps")?;
    ensure!(
        signals <= u32::MAX as usize,
        "signals exceeds the FST handle limit"
    );
    ensure!(steps >= 2, "steps must be at least two");
    let reader = ReaderBuilder::new(File::open(&args[2])?).build()?;
    let header = reader.header();
    ensure!(
        header.max_handle == signals as u64,
        "trace has {} handles, benchmark requested {signals}",
        header.max_handle
    );
    ensure!(
        header.start_time == 0 && header.end_time == (steps - 1) as u64,
        "trace time range is {}..={}, expected 0..={}",
        header.start_time,
        header.end_time,
        steps - 1
    );
    ensure!(
        header.vc_section_count == steps as u64,
        "trace has {} value-change sections, expected {steps}",
        header.vc_section_count
    );
    Ok(())
}

fn cache_command(args: &[String]) -> Result<()> {
    ensure!(args.len() == 4, "cache expects <warm|evict> <file>");
    let file = File::open(&args[3])?;
    match args[2].as_str() {
        "warm" => {
            let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
            let mut buffer = vec![0_u8; 8 * 1024 * 1024];
            while reader.read(&mut buffer)? != 0 {}
        }
        "evict" => {
            #[cfg(target_os = "linux")]
            {
                use std::os::fd::AsRawFd;
                file.sync_all()?;
                let result = unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED)
                };
                ensure!(
                    result == 0,
                    "posix_fadvise(DONTNEED) failed with errno {result}"
                );
            }
            #[cfg(not(target_os = "linux"))]
            bail!("cold-cache eviction is implemented only on Linux");
        }
        mode => bail!("unknown cache mode {mode}"),
    }
    Ok(())
}

fn run_command(args: &[String]) -> Result<()> {
    ensure!(
        (5..=7).contains(&args.len()),
        "run expects <tool> <case> <file> [signals] [steps]"
    );
    let tool = args[2].as_str();
    let case = args[3].as_str();
    let path = Path::new(&args[4]);
    let signals = parse_or(args.get(5), DEFAULT_SIGNALS, "signals")?;
    let steps = parse_or(args.get(6), DEFAULT_STEPS, "steps")?;
    let queries = queries(case, signals, steps)?;

    let io_before = read_io_counters()?;
    let started = Instant::now();
    let mut changes = 0_u64;
    let mut checksum = 0_u64;
    if case == "E" && tool == "wavefst-head" {
        (changes, checksum) = run_wavefst_head_viewports(path, &queries)?;
    } else if case == "E" && tool == "fst-reader-0.17" {
        (changes, checksum) = run_fst_reader_viewports(path, &queries)?;
    } else if case == "E" && tool == "wellen-load-signals" {
        (changes, checksum) = run_wellen_viewports(path, &queries)?;
    } else {
        for query in &queries {
            let (query_changes, query_checksum) = match tool {
                "wavefst-head" => run_wavefst_head(path, query)?,
                "wavefst-0.2.2" => run_wavefst(path, query)?,
                "fst-reader-0.17" => run_fst_reader(path, query)?,
                "wellen-load-signals" => run_wellen(path, query)?,
                _ => bail!("unknown tool {tool}"),
            };
            changes = changes.wrapping_add(query_changes);
            checksum = checksum.wrapping_add(query_checksum);
        }
    }
    black_box((changes, checksum));
    let wall_ns = started.elapsed().as_nanos();
    let io_after = read_io_counters()?;
    let peak_rss_kib = peak_rss_kib();
    println!(
        "{tool}\t{case}\t{wall_ns}\t{}\t{}\t{peak_rss_kib}\t{changes}\t{checksum}\t{}",
        io_after.read_bytes.saturating_sub(io_before.read_bytes),
        io_after.rchar.saturating_sub(io_before.rchar),
        queries.len(),
    );
    Ok(())
}

fn parse_or(value: Option<&String>, default: usize, label: &str) -> Result<usize> {
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .with_context(|| format!("invalid {label}: {value}"))
    })
}

fn queries(case: &str, signals: usize, steps: usize) -> Result<Vec<Query>> {
    ensure!(
        signals >= 100,
        "benchmark cases require at least 100 signals"
    );
    ensure!(steps >= 2, "benchmark cases require at least two steps");
    let end_time = (steps - 1) as u64;
    let middle = percent_window(end_time, 49);
    let out = match case {
        "A" => vec![Query {
            handles: Some(random_handles(signals, 10)),
            time: None,
        }],
        "B" => vec![Query {
            handles: Some(random_handles(signals, 100)),
            time: None,
        }],
        "C" => vec![Query {
            handles: None,
            time: Some(middle),
        }],
        "D" => vec![Query {
            handles: Some(random_handles(signals, 100)),
            time: Some(middle),
        }],
        "E" => (0..100)
            .map(|position| Query {
                handles: Some(random_handles(signals, 100)),
                time: Some(percent_window(end_time, position)),
            })
            .collect(),
        _ => bail!("unknown case {case}; expected A, B, C, D, or E"),
    };
    Ok(out)
}

fn percent_window(end_time: u64, position: u64) -> RangeInclusive<u64> {
    let timeline = end_time.saturating_add(1);
    let start = timeline.saturating_mul(position) / 100;
    let exclusive_end = timeline.saturating_mul(position + 1) / 100;
    start..=exclusive_end.saturating_sub(1).max(start)
}

fn random_handles(signals: usize, count: usize) -> Vec<u32> {
    let mut state = SEED;
    let mut seen = HashSet::with_capacity(count);
    while seen.len() < count {
        state = splitmix64(state);
        seen.insert((state % signals as u64 + 1) as u32);
    }
    let mut handles = seen.into_iter().collect::<Vec<_>>();
    handles.sort_unstable();
    handles
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn generate_trace(path: &Path, signals: usize, steps: usize) -> Result<u64> {
    ensure!(signals <= u32::MAX as usize, "too many signals");
    let mut writer = FstWriter::builder(File::create(path)?)
        .chain_compression(ChainCompression::Zlib)
        .time_compression(TimeCompression::Zlib)
        .block_change_limit(signals)
        .block_size_limit(usize::MAX)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "selective", None)?;
    let mut batch = Vec::with_capacity(signals);
    for signal in 0..signals {
        let handle = writer.add_variable(
            VarType::VcdWire,
            VarDir::Implicit,
            format!("s{signal:06}"),
            GeomEntry::Fixed(1),
        )?;
        batch.push((handle, false));
    }
    writer.end_scope()?;
    writer.write_header(Header::default())?;

    for step in 0..steps {
        for (index, (_, value)) in batch.iter_mut().enumerate() {
            *value = (index ^ step) & 1 != 0;
        }
        writer.emit_binary_batch(step as u64, &batch)?;
    }
    Ok(writer.finish()?.metadata()?.len())
}

fn run_wavefst(path: &Path, query: &Query) -> Result<(u64, u64)> {
    let mut builder = ReaderBuilder::new(File::open(path)?);
    if let Some(handles) = &query.handles {
        builder = builder.include_handles(handles.iter().copied());
    }
    if let Some(time) = &query.time {
        builder = builder.time_range(time.clone());
    }
    let mut reader = builder.build()?;
    let mut count = 0_u64;
    let mut checksum = 0_u64;
    while let Some(changes) = reader.next_value_changes()? {
        (count, checksum) = changes.try_fold_binary(
            (count, checksum),
            |(count, checksum), time, handle, alias, value| {
                let mixed = time ^ u64::from(handle) ^ alias.map_or(0, u64::from) ^ value as u64;
                (count + 1, checksum.wrapping_add(mixed))
            },
        )?;
    }
    Ok((count, checksum))
}

fn run_wavefst_head(path: &Path, query: &Query) -> Result<(u64, u64)> {
    let mut builder = HeadReaderBuilder::new(File::open(path)?).load_hierarchy(false);
    if let Some(handles) = &query.handles {
        builder = builder.include_handles(handles.iter().copied());
    }
    if let Some(time) = &query.time {
        builder = builder.time_range(time.clone());
    }
    let mut reader = builder.build()?;
    read_wavefst_head_query(&mut reader)
}

fn run_wavefst_head_viewports(path: &Path, queries: &[Query]) -> Result<(u64, u64)> {
    let first = queries.first().context("wavefst query list is empty")?;
    let mut builder = HeadReaderBuilder::new(File::open(path)?).load_hierarchy(false);
    if let Some(handles) = &first.handles {
        builder = builder.include_handles(handles.iter().copied());
    }
    let mut reader = builder.build()?;
    let mut count = 0_u64;
    let mut checksum = 0_u64;
    for query in queries {
        ensure!(
            query.handles == first.handles,
            "wavefst viewport queries must select the same signals"
        );
        reader.set_time_range(query.time.clone())?;
        let (query_count, query_checksum) = read_wavefst_head_query(&mut reader)?;
        count += query_count;
        checksum = checksum.wrapping_add(query_checksum);
    }
    Ok((count, checksum))
}

fn read_wavefst_head_query(
    reader: &mut wavefst_head::reader::FstReader<File>,
) -> Result<(u64, u64)> {
    let mut count = 0_u64;
    let mut checksum = 0_u64;
    while let Some(changes) = reader.next_value_changes()? {
        (count, checksum) = changes.try_fold_binary(
            (count, checksum),
            |(count, checksum), time, handle, alias, value| {
                let mixed = time ^ u64::from(handle) ^ alias.map_or(0, u64::from) ^ value as u64;
                (count + 1, checksum.wrapping_add(mixed))
            },
        )?;
    }
    Ok((count, checksum))
}

fn run_fst_reader(path: &Path, query: &Query) -> Result<(u64, u64)> {
    let input = BufReader::with_capacity(64 * 1024, File::open(path)?);
    let mut reader = UpstreamReader::open(input)?;
    let mut count = 0_u64;
    let mut checksum = 0_u64;
    read_fst_reader_query(&mut reader, query, &mut count, &mut checksum)?;
    Ok((count, checksum))
}

fn run_fst_reader_viewports(path: &Path, queries: &[Query]) -> Result<(u64, u64)> {
    let input = BufReader::with_capacity(64 * 1024, File::open(path)?);
    let mut reader = UpstreamReader::open(input)?;
    let mut count = 0_u64;
    let mut checksum = 0_u64;
    for query in queries {
        read_fst_reader_query(&mut reader, query, &mut count, &mut checksum)?;
    }
    Ok((count, checksum))
}

fn read_fst_reader_query(
    reader: &mut UpstreamReader<BufReader<File>>,
    query: &Query,
    count: &mut u64,
    checksum: &mut u64,
) -> Result<()> {
    let filter = upstream_filter(query);
    reader
        .read_signals(&filter, |time, handle, value| {
            let value_code = match value {
                FstSignalValue::String(bytes) => bytes.first().copied().unwrap_or_default() as u64,
                FstSignalValue::Real(value) => value.to_bits(),
            };
            *count += 1;
            *checksum = checksum.wrapping_add(time ^ handle.get_index() as u64 ^ value_code);
            Ok::<_, std::convert::Infallible>(())
        })
        .map_err(|error| anyhow::anyhow!("fst-reader read failed: {error:?}"))?;
    Ok(())
}

fn upstream_filter(query: &Query) -> FstFilter {
    let handles = query.handles.as_ref().map(|handles| {
        handles
            .iter()
            .map(|handle| FstSignalHandle::from_index((*handle - 1) as usize))
            .collect::<Vec<_>>()
    });
    match (&query.time, handles) {
        (Some(time), Some(handles)) => FstFilter::new(*time.start(), *time.end(), handles),
        (Some(time), None) => FstFilter::filter_time(*time.start(), *time.end()),
        (None, Some(handles)) => FstFilter::filter_signals(handles),
        (None, None) => FstFilter::all(),
    }
}

fn run_wellen(path: &Path, query: &Query) -> Result<(u64, u64)> {
    run_wellen_queries(path, std::slice::from_ref(query))
}

fn run_wellen_viewports(path: &Path, queries: &[Query]) -> Result<(u64, u64)> {
    run_wellen_queries(path, queries)
}

fn run_wellen_queries(path: &Path, queries: &[Query]) -> Result<(u64, u64)> {
    let first_query = queries.first().context("Wellen query list is empty")?;
    let options = wellen::LoadOptions {
        multi_thread: false,
        ..Default::default()
    };
    let mut waveform = wellen::simple::read_with_options(path, &options)?;
    let signal_count = waveform.hierarchy().signals().count();
    let ids = match &first_query.handles {
        Some(handles) => handles
            .iter()
            .map(|handle| SignalRef::from_index((*handle - 1) as usize).expect("valid handle"))
            .collect::<Vec<_>>(),
        None => (0..signal_count)
            .map(|index| SignalRef::from_index(index).expect("valid signal index"))
            .collect::<Vec<_>>(),
    };
    waveform.load_signals(&ids);

    let mut count = 0_u64;
    let mut checksum = 0_u64;
    let time_table = waveform.time_table();
    for query in queries {
        ensure!(
            query.handles == first_query.handles,
            "Wellen viewport queries must select the same signals"
        );
        for &id in &ids {
            let signal = waveform
                .get_signal(id)
                .context("Wellen did not return a requested signal")?;
            for (time_index, value) in signal.iter_changes() {
                let time = time_table[time_index as usize];
                if query
                    .time
                    .as_ref()
                    .is_none_or(|range| range.contains(&time))
                {
                    count += 1;
                    checksum = checksum.wrapping_add(time ^ id.index() as u64 ^ value_code(value));
                }
            }
        }
    }
    Ok((count, checksum))
}

fn value_code(value: wellen::SignalValueRef<'_>) -> u64 {
    match value {
        wellen::SignalValueRef::Event => 0,
        wellen::SignalValueRef::BitVec(value) => value
            .be_bytes()
            .and_then(|bytes| bytes.first().copied())
            .map_or_else(|| u64::from(value.get_bit(0)), u64::from),
        wellen::SignalValueRef::String(value) => {
            value.as_bytes().first().copied().unwrap_or_default() as u64
        }
        wellen::SignalValueRef::Real(value) => value.to_bits(),
    }
}

fn read_io_counters() -> Result<IoCounters> {
    #[cfg(target_os = "linux")]
    {
        let contents = std::fs::read_to_string("/proc/self/io")?;
        let mut read_bytes = None;
        let mut rchar = None;
        for line in contents.lines() {
            let Some((name, value)) = line.split_once(": ") else {
                continue;
            };
            match name {
                "read_bytes" => read_bytes = Some(value.parse()?),
                "rchar" => rchar = Some(value.parse()?),
                _ => {}
            }
        }
        Ok(IoCounters {
            read_bytes: read_bytes.context("/proc/self/io has no read_bytes")?,
            rchar: rchar.context("/proc/self/io has no rchar")?,
        })
    }
    #[cfg(not(target_os = "linux"))]
    Ok(IoCounters {
        read_bytes: 0,
        rchar: 0,
    })
}

fn peak_rss_kib() -> u64 {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if result == 0 {
            let usage = unsafe { usage.assume_init() };
            #[cfg(target_os = "macos")]
            return (usage.ru_maxrss as u64) / 1024;
            #[cfg(not(target_os = "macos"))]
            return usage.ru_maxrss as u64;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_windows_partition_the_timeline() {
        let mut covered = Vec::new();
        for position in 0..100 {
            covered.extend(percent_window(99, position));
        }
        assert_eq!(covered, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn selected_handles_are_deterministic_unique_and_one_based() {
        let handles = random_handles(500_000, 100);
        assert_eq!(handles, random_handles(500_000, 100));
        assert_eq!(handles.len(), 100);
        assert!(handles.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(handles.iter().all(|handle| (1..=500_000).contains(handle)));
    }

    #[test]
    fn viewport_case_keeps_one_percent_queries_and_one_signal_set() {
        let viewports = queries("E", 500_000, 100).expect("valid benchmark case");
        assert_eq!(viewports.len(), 100);
        assert!(
            viewports
                .iter()
                .all(|query| query.handles == viewports[0].handles)
        );
        assert_eq!(
            viewports.first().and_then(|query| query.time.clone()),
            Some(0..=0)
        );
        assert_eq!(
            viewports.last().and_then(|query| query.time.clone()),
            Some(99..=99)
        );
    }
}
