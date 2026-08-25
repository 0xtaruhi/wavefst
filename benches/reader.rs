use std::borrow::Cow;
use std::io::Cursor;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ReaderBuilder, ScopeType, SignalValue,
    TimeCompression, VarDir, VarType,
};

const TOGGLE_COUNT: usize = 256;
const DENSE_SIGNALS: usize = 512;
const DENSE_STEPS: usize = 128;

fn generate_trace(chain: ChainCompression, time: TimeCompression) -> Vec<u8> {
    let cursor = Cursor::new(Vec::new());
    let mut writer = FstWriter::builder(cursor)
        .chain_compression(chain)
        .time_compression(time)
        .build()
        .expect("construct writer");

    writer
        .begin_scope(ScopeType::VcdModule, "bench", None)
        .expect("begin scope");
    let bit = writer
        .add_variable(
            VarType::VcdWire,
            VarDir::Implicit,
            "bit_sig",
            GeomEntry::Fixed(1),
        )
        .expect("add bit");
    let vector = writer
        .add_variable(
            VarType::VcdWire,
            VarDir::Implicit,
            "vector_sig",
            GeomEntry::Fixed(64),
        )
        .expect("add vector");
    let varlen = writer
        .add_variable(
            VarType::GenString,
            VarDir::Implicit,
            "payload",
            GeomEntry::Variable,
        )
        .expect("add varlen");
    writer.end_scope().expect("end scope");

    let header = Header {
        version: "reader-bench".into(),
        vc_section_count: 2,
        end_time: (TOGGLE_COUNT as u64) * 6,
        ..Header::default()
    };
    writer.write_header(header).expect("write header");

    let vector_a = "01".repeat(32);
    let vector_b = "10".repeat(32);
    let payload_a = vec![b'A'; 96];
    let payload_b = vec![b'B'; 96];

    for idx in 0..TOGGLE_COUNT {
        let base = (idx as u64) * 6;
        let bit_val = if idx % 4 == 0 {
            '0'
        } else if idx % 4 == 1 {
            '1'
        } else if idx % 4 == 2 {
            'x'
        } else {
            'z'
        };
        writer
            .emit_change(base, bit, SignalValue::Bit(bit_val))
            .expect("emit bit");
        let (vector_val, payload_val) = if idx % 2 == 0 {
            (
                Cow::Borrowed(vector_a.as_str()),
                Cow::Borrowed(payload_a.as_slice()),
            )
        } else {
            (
                Cow::Borrowed(vector_b.as_str()),
                Cow::Borrowed(payload_b.as_slice()),
            )
        };
        writer
            .emit_change(base + 2, vector, SignalValue::Vector(vector_val.clone()))
            .expect("emit vector");
        writer
            .emit_change(base + 3, varlen, SignalValue::Bytes(payload_val.clone()))
            .expect("emit bytes");

        if idx == TOGGLE_COUNT / 2 {
            writer.flush().expect("flush mid-way");
        }
    }

    let cursor = writer.finish().expect("finish writer");
    cursor.into_inner()
}

fn generate_dense_trace(chain: ChainCompression, time: TimeCompression) -> Vec<u8> {
    let mut writer = FstWriter::builder(Cursor::new(Vec::new()))
        .chain_compression(chain)
        .time_compression(time)
        .build()
        .expect("construct writer");
    writer
        .begin_scope(ScopeType::VcdModule, "dense", None)
        .expect("begin scope");
    let handles: Vec<_> = (0..DENSE_SIGNALS)
        .map(|signal| {
            writer
                .add_variable(
                    VarType::VcdWire,
                    VarDir::Implicit,
                    format!("s{signal}"),
                    GeomEntry::Fixed(1),
                )
                .expect("add bit")
        })
        .collect();
    writer.end_scope().expect("end scope");
    writer.write_header(Header::default()).expect("header");

    let mut states: Vec<u64> = (1..=DENSE_SIGNALS)
        .map(|signal| (signal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .collect();
    let mut batch: Vec<_> = handles.into_iter().map(|handle| (handle, false)).collect();
    for step in 0..DENSE_STEPS {
        for (signal, item) in batch.iter_mut().enumerate() {
            let state = &mut states[signal];
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            item.1 = *state & 1 != 0;
        }
        writer
            .emit_binary_batch(step as u64, &batch)
            .expect("emit binary batch");
    }
    writer.finish().expect("finish").into_inner()
}

fn bench_placeholder(c: &mut Criterion) {
    #[allow(unused_mut, clippy::useless_vec)]
    let mut configs = vec![("raw", ChainCompression::Raw, TimeCompression::Raw)];
    #[cfg(feature = "gzip")]
    {
        configs.push(("zlib", ChainCompression::Zlib, TimeCompression::Zlib));
    }
    #[cfg(feature = "lz4")]
    {
        let time = if cfg!(feature = "gzip") {
            TimeCompression::Zlib
        } else {
            TimeCompression::Raw
        };
        configs.push(("lz4", ChainCompression::Lz4, time));
    }
    #[cfg(feature = "fastlz")]
    {
        configs.push(("fastlz", ChainCompression::FastLz, TimeCompression::Raw));
    }

    let traces: Vec<_> = configs
        .iter()
        .map(|(label, chain, time)| (*label, generate_trace(*chain, *time)))
        .collect();

    let mut group = c.benchmark_group("reader_next_value_changes");
    for (label, bytes) in &traces {
        group.bench_with_input(BenchmarkId::from_parameter(label), bytes, |b, data| {
            b.iter(|| {
                let cursor = Cursor::new(data.as_slice());
                let mut reader = ReaderBuilder::new(cursor).build().unwrap();
                while let Some(mut changes) = reader.next_value_changes().unwrap() {
                    for event in &mut changes {
                        if event.is_err() {
                            break;
                        }
                    }
                }
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("reader_try_for_each");
    for (label, bytes) in &traces {
        group.bench_with_input(BenchmarkId::from_parameter(label), bytes, |b, data| {
            b.iter(|| {
                let cursor = Cursor::new(data.as_slice());
                let mut reader = ReaderBuilder::new(cursor).build().unwrap();
                while let Some(changes) = reader.next_value_changes().unwrap() {
                    changes
                        .try_for_each(|event| {
                            std::hint::black_box(event);
                        })
                        .unwrap();
                }
            });
        });
    }
    group.finish();

    #[cfg(feature = "gzip")]
    let dense = generate_dense_trace(ChainCompression::Zlib, TimeCompression::Zlib);
    #[cfg(all(not(feature = "gzip"), feature = "lz4"))]
    let dense = generate_dense_trace(ChainCompression::Lz4, TimeCompression::Raw);
    #[cfg(not(any(feature = "gzip", feature = "lz4")))]
    let dense = generate_dense_trace(ChainCompression::Raw, TimeCompression::Raw);

    let mut group = c.benchmark_group("reader_dense_scan");
    group.bench_function("ordered_binary_fold", |b| {
        b.iter(|| {
            let mut reader = ReaderBuilder::new(Cursor::new(dense.as_slice()))
                .build()
                .unwrap();
            let mut count = 0usize;
            while let Some(changes) = reader.next_value_changes().unwrap() {
                count = changes
                    .try_fold_binary(count, |count, timestamp, handle, alias, value| {
                        std::hint::black_box((timestamp, handle, alias, value));
                        count + 1
                    })
                    .unwrap();
            }
            std::hint::black_box(count);
        });
    });
    group.bench_function("handle_major", |b| {
        b.iter(|| {
            let mut reader = ReaderBuilder::new(Cursor::new(dense.as_slice()))
                .build()
                .unwrap();
            while let Some(changes) = reader.next_value_changes().unwrap() {
                changes
                    .try_for_each_parts_unordered(|timestamp, handle, alias, value| {
                        std::hint::black_box((timestamp, handle, alias, value));
                    })
                    .unwrap();
            }
        });
    });
    #[cfg(feature = "parallel")]
    group.bench_function("parallel_fold", |b| {
        b.iter(|| {
            let mut reader = ReaderBuilder::new(Cursor::new(dense.as_slice()))
                .build()
                .unwrap();
            let mut count = 0usize;
            while let Some(changes) = reader.next_value_changes().unwrap() {
                count += changes
                    .try_fold_parts_parallel(
                        || 0usize,
                        |count, _, _, _, _| *count += 1,
                        |left, right| left + right,
                    )
                    .unwrap();
            }
            std::hint::black_box(count);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_placeholder);
criterion_main!(benches);
