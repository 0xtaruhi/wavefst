use std::borrow::Cow;
use std::io::Cursor;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use wavefst::{
    ChainCompression, FstWriter, GeomEntry, Header, ScopeType, SignalValue, TimeCompression,
    VarDir, VarType,
};

const TOGGLE_COUNT: usize = 256;

fn emit_sample_trace<W: wavefst::io::WriteSeek>(writer: &mut FstWriter<W>) {
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
        version: "writer-bench".into(),
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
            .expect("emit payload");

        if idx == TOGGLE_COUNT / 2 {
            writer.flush().expect("flush mid-way");
        }
    }
}

fn bench_writer(c: &mut Criterion) {
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

    let mut group = c.benchmark_group("writer_emit_change");
    for (label, chain, time) in configs {
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &(chain, time),
            |b, &(chain, time)| {
                b.iter(|| {
                    let cursor = Cursor::new(Vec::new());
                    let mut writer = FstWriter::builder(cursor)
                        .chain_compression(chain)
                        .time_compression(time)
                        .build()
                        .unwrap();
                    emit_sample_trace(&mut writer);
                    let cursor = writer.finish().unwrap();
                    std::hint::black_box(cursor.into_inner())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_writer);
criterion_main!(benches);
