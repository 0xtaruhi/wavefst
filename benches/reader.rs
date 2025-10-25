use std::borrow::Cow;
use std::io::Cursor;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ReaderBuilder, ScopeType, SignalValue,
    TimeCompression, VarDir, VarType,
};

const TOGGLE_COUNT: usize = 256;

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

fn bench_placeholder(c: &mut Criterion) {
    let mut configs = vec![("raw", ChainCompression::Raw, TimeCompression::Raw)];
    #[cfg(feature = "gzip")]
    {
        configs.push(("zlib", ChainCompression::Zlib, TimeCompression::Zlib));
    }
    #[cfg(feature = "lz4")]
    {
        configs.push(("lz4", ChainCompression::Lz4, TimeCompression::Zlib));
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
}

criterion_group!(benches, bench_placeholder);
criterion_main!(benches);
