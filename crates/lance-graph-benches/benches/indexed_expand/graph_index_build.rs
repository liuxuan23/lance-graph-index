// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! CSR construction benchmarks. Index construction is deliberately measured
//! separately from query execution so query benchmarks can reuse one index.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lance_graph::CsrIndexBuilder;

fn make_edges(num_vertices: usize, average_degree: usize) -> RecordBatch {
    let edge_count = num_vertices.saturating_mul(average_degree);
    let src: Vec<i64> = (0..edge_count).map(|i| (i % num_vertices) as i64).collect();
    let dst: Vec<i64> = (0..edge_count)
        .map(|i| ((i + 1) % num_vertices) as i64)
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(src)),
            Arc::new(Int64Array::from(dst)),
        ],
    )
    .unwrap()
}

fn bench_csr_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_index_build");
    for &(vertices, degree) in &[(1_000usize, 1usize), (10_000, 4), (100_000, 4)] {
        let edges = make_edges(vertices, degree);
        group.throughput(Throughput::Elements(edges.num_rows() as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("v{vertices}_degree{degree}"), edges.num_rows()),
            &edges,
            |b, edges| {
                b.iter(|| {
                    let index = CsrIndexBuilder::new()
                        .with_num_vertices(vertices as u64)
                        .add_edges_from_batch(edges)
                        .unwrap()
                        .try_build()
                        .unwrap();
                    black_box((index.num_vertices(), index.num_edges()));
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_csr_index_build);
criterion_main!(benches);
