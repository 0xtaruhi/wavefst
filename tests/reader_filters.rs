#![cfg(any(feature = "gzip", feature = "lz4"))]

#[cfg(all(feature = "gzip", feature = "lz4"))]
use std::collections::BTreeSet;
use std::io::Cursor;

use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ReaderBuilder, ScopeType, TimeCompression,
    VarDir, VarType,
};

type BinaryEvent = (u64, u32, Option<u32>, bool);

fn write_binary_trace(
    signals: usize,
    steps: usize,
    block_change_limit: usize,
    compression: ChainCompression,
) -> wavefst::Result<Vec<u8>> {
    let time_compression = if matches!(compression, ChainCompression::Zlib) {
        TimeCompression::Zlib
    } else {
        TimeCompression::Raw
    };
    let mut writer = FstWriter::builder(Cursor::new(Vec::new()))
        .chain_compression(compression)
        .time_compression(time_compression)
        .block_change_limit(block_change_limit)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "filtered", None)?;
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
        writer.emit_binary_batch((step * 2) as u64, &batch)?;
    }
    Ok(writer.finish()?.into_inner())
}

fn collect_binary(
    bytes: &[u8],
    handles: impl IntoIterator<Item = u32>,
    start: u64,
    end: u64,
) -> wavefst::Result<Vec<BinaryEvent>> {
    let mut reader = ReaderBuilder::new(Cursor::new(bytes))
        .include_handles(handles)
        .time_range(start..=end)
        .build()?;
    let mut events = Vec::new();
    while let Some(changes) = reader.next_value_changes()? {
        events =
            changes.try_fold_binary(events, |mut events, timestamp, handle, alias_of, value| {
                events.push((timestamp, handle, alias_of, value));
                events
            })?;
    }
    Ok(events)
}

#[test]
fn selected_handles_only_load_their_chain_bytes_for_every_codec() -> wavefst::Result<()> {
    let mut compressions = vec![ChainCompression::Raw];
    #[cfg(feature = "gzip")]
    compressions.push(ChainCompression::Zlib);
    #[cfg(feature = "lz4")]
    compressions.push(ChainCompression::Lz4);

    const SIGNALS: usize = 64;
    const STEPS: usize = 512;
    let selected = [1, 32, 64];
    for compression in compressions {
        let bytes = write_binary_trace(SIGNALS, STEPS, usize::MAX, compression)?;

        let mut full_reader = ReaderBuilder::new(Cursor::new(&bytes)).build()?;
        let full = full_reader.next_vc_block()?.expect("full block");
        let mut selected_reader = ReaderBuilder::new(Cursor::new(&bytes))
            .include_handles(selected)
            .build()?;
        assert_eq!(
            selected_reader.options().included_handles.as_deref(),
            Some(selected.as_slice())
        );
        let filtered = selected_reader.next_vc_block()?.expect("filtered block");

        assert!(filtered.chain_buffer.len() < full.chain_buffer.len());
        assert!(filtered.decoded_chain_buffer.len() <= full.decoded_chain_buffer.len());
        if !full.decoded_chain_buffer.is_empty() {
            assert!(filtered.decoded_chain_buffer.len() < full.decoded_chain_buffer.len());
        }
        assert!(
            filtered
                .chains
                .iter()
                .flatten()
                .all(|chain| selected.contains(&chain.handle) || chain.alias_of.is_none())
        );

        let events = collect_binary(&bytes, selected, 0, u64::MAX)?;
        assert_eq!(events.len(), selected.len() * STEPS);
        assert!(events.iter().all(|event| selected.contains(&event.1)));
    }
    Ok(())
}

#[test]
fn selecting_a_dynamic_alias_loads_but_does_not_emit_its_canonical() -> wavefst::Result<()> {
    const STEPS: usize = 128;
    let mut writer = FstWriter::builder(Cursor::new(Vec::new()))
        .chain_compression(ChainCompression::Raw)
        .time_compression(TimeCompression::Raw)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "aliases", None)?;
    let canonical = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "canonical",
        GeomEntry::Fixed(1),
    )?;
    let alias = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "deduplicated",
        GeomEntry::Fixed(1),
    )?;
    writer.end_scope()?;
    writer.write_header(Header::default())?;
    for step in 0..STEPS {
        let value = step & 1 != 0;
        writer.emit_binary_batch(step as u64, &[(canonical, value), (alias, value)])?;
    }
    let bytes = writer.finish()?.into_inner();

    let events = collect_binary(&bytes, [alias], 0, u64::MAX)?;
    assert_eq!(events.len(), STEPS);
    assert!(
        events
            .iter()
            .all(|event| event.1 == alias && event.2 == Some(canonical))
    );

    let both = collect_binary(&bytes, [alias, canonical, alias], 0, u64::MAX)?;
    assert_eq!(both.len(), STEPS * 2);
    assert_eq!(
        both.iter().filter(|event| event.1 == canonical).count(),
        STEPS
    );
    assert_eq!(both.iter().filter(|event| event.1 == alias).count(), STEPS);
    Ok(())
}

#[test]
fn inclusive_time_range_skips_blocks_and_filters_boundary_events() -> wavefst::Result<()> {
    let bytes = write_binary_trace(2, 6, 4, ChainCompression::Raw)?;
    let expected = vec![(4, 2), (6, 2), (8, 2)];

    let events = collect_binary(&bytes, [2], 4, 8)?;
    assert_eq!(
        events
            .iter()
            .map(|event| (event.0, event.1))
            .collect::<Vec<_>>(),
        expected
    );

    let mut reader = ReaderBuilder::new(Cursor::new(&bytes))
        .include_handles([2])
        .time_range(4..=8)
        .build()?;
    let first = reader.next_vc_block()?.expect("block beginning at 4");
    assert_eq!((first.header.begin_time, first.header.end_time), (4, 6));
    let second = reader.next_vc_block()?.expect("block containing 8");
    assert_eq!((second.header.begin_time, second.header.end_time), (8, 10));
    assert!(reader.next_vc_block()?.is_none());

    let exact = collect_binary(&bytes, [2], 6, 6)?;
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].0, 6);

    let mut time_only_reader = ReaderBuilder::new(Cursor::new(&bytes))
        .time_range(6..=6)
        .build()?;
    let mut time_only = Vec::new();
    while let Some(changes) = time_only_reader.next_value_changes()? {
        changes.try_for_each_parts(|timestamp, handle, _, _| {
            time_only.push((timestamp, handle));
        })?;
    }
    time_only.sort_unstable();
    assert_eq!(time_only, vec![(6, 1), (6, 2)]);

    assert!(collect_binary(&bytes, [2], 11, 20)?.is_empty());
    assert!(collect_binary(&bytes, [2], 10, 9)?.is_empty());
    Ok(())
}

#[test]
fn filters_apply_to_iterator_ordered_unordered_and_parallel_paths() -> wavefst::Result<()> {
    let bytes = write_binary_trace(2, 6, 4, ChainCompression::Raw)?;

    let mut iterator_reader = ReaderBuilder::new(Cursor::new(&bytes))
        .include_handles([2])
        .time_range(4..=8)
        .build()?;
    let mut iterator_events = Vec::new();
    while let Some(changes) = iterator_reader.next_value_changes()? {
        for event in changes {
            let event = event?;
            iterator_events.push((event.timestamp, event.handle));
        }
    }
    assert_eq!(iterator_events, vec![(4, 2), (6, 2), (8, 2)]);

    let mut ordered_reader = ReaderBuilder::new(Cursor::new(&bytes))
        .include_handles([2])
        .time_range(4..=8)
        .build()?;
    let mut ordered = Vec::new();
    while let Some(changes) = ordered_reader.next_value_changes()? {
        changes.try_for_each_parts(|timestamp, handle, _, _| ordered.push((timestamp, handle)))?;
    }
    assert_eq!(ordered, iterator_events);

    let mut unordered_reader = ReaderBuilder::new(Cursor::new(&bytes))
        .include_handles([2])
        .time_range(4..=8)
        .build()?;
    let mut unordered = Vec::new();
    while let Some(changes) = unordered_reader.next_value_changes()? {
        changes.try_for_each_parts_unordered(|timestamp, handle, _, _| {
            unordered.push((timestamp, handle));
        })?;
    }
    assert_eq!(unordered, iterator_events);

    #[cfg(feature = "parallel")]
    {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut parallel_reader = ReaderBuilder::new(Cursor::new(&bytes))
            .include_handles([2])
            .time_range(4..=8)
            .build()?;
        let count = AtomicUsize::new(0);
        while let Some(changes) = parallel_reader.next_value_changes()? {
            changes.try_for_each_parts_parallel(|timestamp, handle, _, _| {
                assert!((4..=8).contains(&timestamp));
                assert_eq!(handle, 2);
                count.fetch_add(1, Ordering::Relaxed);
            })?;
        }
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }
    Ok(())
}

#[test]
fn empty_and_invalid_handle_selections_are_well_defined() -> wavefst::Result<()> {
    let bytes = write_binary_trace(2, 4, usize::MAX, ChainCompression::Raw)?;

    let mut empty = ReaderBuilder::new(Cursor::new(&bytes))
        .include_handles([])
        .build()?;
    let block = empty
        .next_vc_block()?
        .expect("metadata block remains visible");
    assert!(block.chain_buffer.is_empty());
    assert!(block.decoded_chain_buffer.is_empty());
    assert!(block.chains.iter().all(Option::is_none));

    assert!(
        ReaderBuilder::new(Cursor::new(&bytes))
            .include_handles([0])
            .build()
            .is_err()
    );
    assert!(
        ReaderBuilder::new(Cursor::new(&bytes))
            .include_handles([3])
            .build()
            .is_err()
    );

    let normalized = ReaderBuilder::new(Cursor::new(&bytes))
        .include_handles([2, 2])
        .build()?;
    assert_eq!(
        normalized.options().included_handles.as_deref(),
        Some([2].as_slice())
    );
    let all = ReaderBuilder::new(Cursor::new(&bytes))
        .include_handles([2, 1, 2])
        .build()?;
    assert!(all.options().included_handles.is_none());
    Ok(())
}

#[cfg(all(feature = "gzip", feature = "lz4"))]
#[test]
fn filtered_libfst_fixture_matches_post_filtering_the_full_stream() -> wavefst::Result<()> {
    let bytes = include_bytes!("data/hdl-example.fst");
    let mut full_reader = ReaderBuilder::new(Cursor::new(bytes.as_slice())).build()?;
    let mut full = Vec::new();
    while let Some(changes) = full_reader.next_value_changes()? {
        changes.try_for_each_parts(|timestamp, handle, alias_of, value| {
            full.push((timestamp, handle, alias_of, value.into_owned()));
        })?;
    }
    assert!(!full.is_empty());

    let unique_handles = full
        .iter()
        .map(|event| event.1)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let selected = [
        unique_handles[0],
        unique_handles[unique_handles.len() / 2],
        *unique_handles.last().expect("at least one handle"),
    ];
    let first_time = full.first().expect("first event").0;
    let last_time = full.last().expect("last event").0;
    let span = last_time - first_time;
    let start = first_time + span / 3;
    let end = first_time + span * 2 / 3;
    let mut expected = full
        .into_iter()
        .filter(|event| selected.contains(&event.1) && (start..=end).contains(&event.0))
        .collect::<Vec<_>>();

    let mut filtered_reader = ReaderBuilder::new(Cursor::new(bytes.as_slice()))
        .include_handles(selected)
        .time_range(start..=end)
        .build()?;
    let mut filtered = Vec::new();
    while let Some(changes) = filtered_reader.next_value_changes()? {
        changes.try_for_each_parts(|timestamp, handle, alias_of, value| {
            filtered.push((timestamp, handle, alias_of, value.into_owned()));
        })?;
    }
    expected.sort_by_key(|event| (event.0, event.1, event.2));
    filtered.sort_by_key(|event| (event.0, event.1, event.2));
    assert_eq!(filtered, expected);
    Ok(())
}
