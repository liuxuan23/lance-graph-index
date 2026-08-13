//! Layered GIN-style covering adjacency benchmarks.
//!
//! `pure_lookup` measures source-to-posting access only. `expand_only`
//! measures DataFusion input-row replay without target-node materialization.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::TryStreamExt;
use lance_graph::datafusion_planner::indexed_expand::CoveringAdjacencyExpandExec;
use lance_graph::{
    AdjacencyLookupOptions, CoveringAdjacencyIndexBuilder, CoveringAdjacencyIndexHandle,
    CoveringAdjacencyIndexStore, CoveringAdjacencyMetadata, CoveringAdjacencyWriteOptions,
    GraphIndexKey, IndexDirection,
};

const SOURCE_COUNT: usize = 100_000;
const DEGREE: usize = 10;
const EDGE_COUNT: usize = SOURCE_COUNT * DEGREE;

fn edges() -> RecordBatch {
    let mut sources = Vec::with_capacity(EDGE_COUNT);
    let mut targets = Vec::with_capacity(EDGE_COUNT);
    for source in 0..SOURCE_COUNT {
        for offset in 1..=DEGREE {
            sources.push(source as i64);
            targets.push(((source + offset) % SOURCE_COUNT) as i64);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(sources)),
            Arc::new(Int64Array::from(targets)),
        ],
    )
    .unwrap()
}

fn hub_edges(degree: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![42_i64; degree])),
            Arc::new(Int64Array::from(
                (0..degree).map(|value| value as i64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn sparse_edges() -> RecordBatch {
    let mut sources = Vec::new();
    let mut targets = Vec::new();
    for source in (0..SOURCE_COUNT).step_by(100) {
        sources.push(source as i64);
        targets.push(((source + 1) % SOURCE_COUNT) as i64);
    }
    edge_batch(sources, targets)
}

fn power_law_edges() -> RecordBatch {
    let mut sources = Vec::new();
    let mut targets = Vec::new();
    for source in 0..10_000_usize {
        let degree = (10_000 / (source + 1)).clamp(1, 2_000);
        for offset in 1..=degree {
            sources.push(source as i64);
            targets.push(((source + offset) % SOURCE_COUNT) as i64);
        }
    }
    edge_batch(sources, targets)
}

fn edge_batch(sources: Vec<i64>, targets: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(sources)),
            Arc::new(Int64Array::from(targets)),
        ],
    )
    .unwrap()
}

fn build_handle(
    rt: &tokio::runtime::Runtime,
) -> (tempfile::TempDir, Arc<CoveringAdjacencyIndexHandle>) {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().join("generation-1");
    let descriptor = rt
        .block_on(
            CoveringAdjacencyIndexBuilder::new(CoveringAdjacencyMetadata::new(
                GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
                "person_id",
                "person_id",
                DataType::Int64,
                1,
            ))
            .unwrap()
            .build_sorted_batch_and_persist(
                &edges(),
                uri.to_str().unwrap(),
                CoveringAdjacencyWriteOptions {
                    entry_page_target_bytes: 64 * 1024,
                    inline_posting_threshold_bytes: 8 * 1024,
                    posting_page_target_bytes: 256 * 1024,
                },
            ),
        )
        .unwrap();
    let handle = rt
        .block_on(CoveringAdjacencyIndexStore::load(
            &descriptor,
            Default::default(),
        ))
        .unwrap();
    (directory, Arc::new(handle))
}

fn build_hub_handle(
    rt: &tokio::runtime::Runtime,
    degree: usize,
) -> (tempfile::TempDir, Arc<CoveringAdjacencyIndexHandle>) {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().join("hub-generation-1");
    let descriptor = rt
        .block_on(
            CoveringAdjacencyIndexBuilder::new(CoveringAdjacencyMetadata::new(
                GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
                "person_id",
                "person_id",
                DataType::Int64,
                1,
            ))
            .unwrap()
            .build_sorted_batch_and_persist(
                &hub_edges(degree),
                uri.to_str().unwrap(),
                CoveringAdjacencyWriteOptions::default(),
            ),
        )
        .unwrap();
    assert_eq!(descriptor.metadata.num_posting_tree_sources, 1);
    let handle = rt
        .block_on(CoveringAdjacencyIndexStore::load(
            &descriptor,
            Default::default(),
        ))
        .unwrap();
    (directory, Arc::new(handle))
}

fn build_distribution_handle(
    rt: &tokio::runtime::Runtime,
    name: &str,
    edges: RecordBatch,
) -> (tempfile::TempDir, Arc<CoveringAdjacencyIndexHandle>) {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().join(format!("{name}-generation-1"));
    let descriptor = rt
        .block_on(
            CoveringAdjacencyIndexBuilder::new(CoveringAdjacencyMetadata::new(
                GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
                "person_id",
                "person_id",
                DataType::Int64,
                1,
            ))
            .unwrap()
            .build_sorted_batch_and_persist(
                &edges,
                uri.to_str().unwrap(),
                Default::default(),
            ),
        )
        .unwrap();
    let handle = rt
        .block_on(CoveringAdjacencyIndexStore::load(
            &descriptor,
            Default::default(),
        ))
        .unwrap();
    (directory, Arc::new(handle))
}

fn verify_and_report_metrics(
    rt: &tokio::runtime::Runtime,
    name: &str,
    handle: &CoveringAdjacencyIndexHandle,
    source_ids: Arc<Int64Array>,
) {
    let before = handle.index.metrics().snapshot();
    let chunks = rt
        .block_on(
            handle
                .index
                .lookup(source_ids, AdjacencyLookupOptions::default()),
        )
        .unwrap();
    let after = handle.index.metrics().snapshot();
    eprintln!(
        "covering_metrics workload={name} edges={} range_requests={} entry_reads={} posting_reads={} entry_bytes={} posting_bytes={} entry_cache_hits={} posting_cache_hits={} inline_hits={} posting_hits={} checksums={} adjacency_take_rows=0",
        chunks.iter().map(|chunk| chunk.dst_ids.len()).sum::<usize>(),
        after.range_requests - before.range_requests,
        after.entry_page_reads - before.entry_page_reads,
        after.posting_page_reads - before.posting_page_reads,
        after.entry_bytes_read - before.entry_bytes_read,
        after.posting_bytes_read - before.posting_bytes_read,
        after.entry_page_cache_hits - before.entry_page_cache_hits,
        after.posting_page_cache_hits - before.posting_page_cache_hits,
        after.inline_posting_hits - before.inline_posting_hits,
        after.posting_tree_hits - before.posting_tree_hits,
        after.checksums_verified - before.checksums_verified,
    );
}

fn frontier(size: usize) -> Arc<Int64Array> {
    Arc::new(Int64Array::from(
        (0..size).map(|value| value as i64).collect::<Vec<_>>(),
    ))
}

fn expand_exec(
    handle: Arc<CoveringAdjacencyIndexHandle>,
    source_ids: Arc<Int64Array>,
) -> CoveringAdjacencyExpandExec {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a__person_id",
        DataType::Int64,
        false,
    )]));
    let input_batch = RecordBatch::try_new(schema.clone(), vec![source_ids]).unwrap();
    let input: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[vec![input_batch]], schema, None).unwrap();
    CoveringAdjacencyExpandExec::try_new(
        input,
        handle,
        "a__person_id",
        Arc::new(Field::new("friend_of__dst_id", DataType::Int64, false)),
        8192,
    )
    .unwrap()
}

fn collect_rows(rt: &tokio::runtime::Runtime, stream: SendableRecordBatchStream) -> usize {
    rt.block_on(stream.try_collect::<Vec<_>>())
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

fn bench_covering_layers(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_directory, handle) = build_handle(&rt);
    let mut group = c.benchmark_group("covering_adjacency_layers");

    for frontier_size in [1_usize, 16, 256, 4096] {
        let source_ids = frontier(frontier_size);
        let expected_edges = frontier_size * DEGREE;
        group.throughput(Throughput::Elements(expected_edges as u64));

        let lookup = rt
            .block_on(
                handle
                    .index
                    .lookup(source_ids.clone(), AdjacencyLookupOptions::default()),
            )
            .unwrap();
        assert_eq!(
            lookup
                .iter()
                .map(|chunk| chunk.dst_ids.len())
                .sum::<usize>(),
            expected_edges
        );

        group.bench_with_input(
            BenchmarkId::new("pure_lookup", frontier_size),
            &frontier_size,
            |b, _| {
                b.iter(|| {
                    let chunks = rt
                        .block_on(
                            handle
                                .index
                                .lookup(source_ids.clone(), AdjacencyLookupOptions::default()),
                        )
                        .unwrap();
                    black_box(
                        chunks
                            .iter()
                            .map(|chunk| chunk.dst_ids.len())
                            .sum::<usize>(),
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("expand_only", frontier_size),
            &frontier_size,
            |b, _| {
                b.iter(|| {
                    let exec = expand_exec(handle.clone(), source_ids.clone());
                    let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
                    black_box(collect_rows(&rt, stream))
                })
            },
        );
    }

    let hub_degree = 10_000_usize;
    let (_hub_directory, hub_handle) = build_hub_handle(&rt, hub_degree);
    let hub_source = Arc::new(Int64Array::from(vec![42_i64]));
    group.throughput(Throughput::Elements(hub_degree as u64));
    group.bench_function(BenchmarkId::new("pure_lookup_hub", hub_degree), |b| {
        b.iter(|| {
            let chunks = rt
                .block_on(
                    hub_handle
                        .index
                        .lookup(hub_source.clone(), AdjacencyLookupOptions::default()),
                )
                .unwrap();
            black_box(
                chunks
                    .iter()
                    .map(|chunk| chunk.dst_ids.len())
                    .sum::<usize>(),
            )
        })
    });
    group.bench_function(BenchmarkId::new("expand_only_hub", hub_degree), |b| {
        b.iter(|| {
            let exec = expand_exec(hub_handle.clone(), hub_source.clone());
            let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
            black_box(collect_rows(&rt, stream))
        })
    });

    let (_sparse_directory, sparse_handle) =
        build_distribution_handle(&rt, "sparse", sparse_edges());
    let sparse_frontier = Arc::new(Int64Array::from(
        (0..4096_usize)
            .map(|value| value as i64)
            .collect::<Vec<_>>(),
    ));
    verify_and_report_metrics(&rt, "sparse", &sparse_handle, sparse_frontier.clone());
    group.bench_function("pure_lookup_sparse_frontier_4096", |b| {
        b.iter(|| {
            black_box(
                rt.block_on(
                    sparse_handle
                        .index
                        .lookup(sparse_frontier.clone(), AdjacencyLookupOptions::default()),
                )
                .unwrap()
                .iter()
                .map(|chunk| chunk.dst_ids.len())
                .sum::<usize>(),
            )
        })
    });

    let (_power_law_directory, power_law_handle) =
        build_distribution_handle(&rt, "power_law", power_law_edges());
    let power_law_frontier = Arc::new(Int64Array::from(
        (0..256_usize).map(|value| value as i64).collect::<Vec<_>>(),
    ));
    verify_and_report_metrics(
        &rt,
        "power_law",
        &power_law_handle,
        power_law_frontier.clone(),
    );
    group.bench_function("pure_lookup_power_law_frontier_256", |b| {
        b.iter(|| {
            black_box(
                rt.block_on(power_law_handle.index.lookup(
                    power_law_frontier.clone(),
                    AdjacencyLookupOptions::default(),
                ))
                .unwrap()
                .iter()
                .map(|chunk| chunk.dst_ids.len())
                .sum::<usize>(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, bench_covering_layers);
criterion_main!(benches);
