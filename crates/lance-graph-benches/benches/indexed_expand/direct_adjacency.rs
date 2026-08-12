// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Direct adjacency persistence stages for the fixed star workload.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use lance_graph::{
    DirectAdjacencyIndexBuilder, DirectAdjacencyIndexStore, DirectAdjacencyMetadata, GraphIndexKey,
    IndexDirection,
};

const SOURCE_COUNT: usize = 1_000_000;
const EDGE_COUNT: usize = 10_000_000;
const QUERY_SOURCE_ID: usize = 42;

fn make_edges(query_degree: usize) -> RecordBatch {
    assert!(query_degree < SOURCE_COUNT);
    let mut src = Vec::with_capacity(EDGE_COUNT);
    let mut dst = Vec::with_capacity(EDGE_COUNT);
    for offset in 1..=query_degree {
        src.push(QUERY_SOURCE_ID as i64);
        dst.push(((QUERY_SOURCE_ID + offset) % SOURCE_COUNT) as i64);
    }
    'fill: for offset in 1usize.. {
        for source in 0..SOURCE_COUNT {
            if source == QUERY_SOURCE_ID {
                continue;
            }
            if src.len() == EDGE_COUNT {
                break 'fill;
            }
            src.push(source as i64);
            dst.push(((source + offset) % SOURCE_COUNT) as i64);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(src)),
            Arc::new(Int64Array::from(dst)),
        ],
    )
    .unwrap()
}

fn metadata() -> DirectAdjacencyMetadata {
    DirectAdjacencyMetadata {
        key: GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
        source_id_field: "src_id".into(),
        target_id_field: "person_id".into(),
        adjacency_field: "dst_ids".into(),
        id_data_type: DataType::Int64,
        num_sources: 0,
        num_edges: 0,
        dataset_uri: String::new(),
        dataset_version: 0,
        scalar_index_name: "src_id_btree".into(),
        source_uri: Some("benchmark://friend_of".into()),
        source_version: Some(1),
        generation: 1,
    }
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(&path, files);
        } else if path.is_file() {
            files.push(path);
        }
    }
}

#[cfg(target_os = "linux")]
fn drop_file_cache(files: &[PathBuf]) {
    use std::fs::File;
    use std::os::fd::AsRawFd;
    for path in files {
        if let Ok(file) = File::open(path) {
            let result =
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            debug_assert_eq!(result, 0);
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn drop_file_cache(_files: &[PathBuf]) {}

fn bench_direct_adjacency_stages(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("direct_adjacency_persistence");
    for degree in [10usize, 100, 1_000, 10_000] {
        let edges = make_edges(degree);
        group.throughput(Throughput::Elements(EDGE_COUNT as u64));
        group.bench_with_input(
            BenchmarkId::new("build_and_persist", format!("degree_{degree}")),
            &degree,
            |b, _| {
                b.iter_batched(
                    || tempfile::tempdir().unwrap(),
                    |directory| {
                        let uri = directory.path().join("generation-1");
                        let builder = DirectAdjacencyIndexBuilder::new(metadata())
                            .unwrap()
                            .add_edges_from_batch(&edges)
                            .unwrap();
                        black_box(
                            rt.block_on(
                                builder
                                    .build_and_persist(uri.to_str().unwrap(), Default::default()),
                            )
                            .unwrap(),
                        );
                    },
                    BatchSize::PerIteration,
                )
            },
        );

        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("generation-1");
        let descriptor = rt
            .block_on(
                DirectAdjacencyIndexBuilder::new(metadata())
                    .unwrap()
                    .add_edges_from_batch(&edges)
                    .unwrap()
                    .build_and_persist(uri.to_str().unwrap(), Default::default()),
            )
            .unwrap();
        let mut files = Vec::new();
        collect_files(&uri, &mut files);
        let bytes = files
            .iter()
            .map(|file| std::fs::metadata(file).unwrap().len())
            .sum::<u64>();
        eprintln!("direct adjacency degree={degree} persisted_bytes={bytes}");

        group.bench_with_input(
            BenchmarkId::new("load_warm", format!("degree_{degree}")),
            &degree,
            |b, _| {
                b.iter(|| {
                    black_box(
                        rt.block_on(DirectAdjacencyIndexStore::load(
                            &descriptor,
                            Default::default(),
                        ))
                        .unwrap(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("load_cold_local", format!("degree_{degree}")),
            &degree,
            |b, _| {
                b.iter_batched(
                    || {
                        drop_file_cache(&files);
                        ()
                    },
                    |_| {
                        black_box(
                            rt.block_on(DirectAdjacencyIndexStore::load(
                                &descriptor,
                                Default::default(),
                            ))
                            .unwrap(),
                        );
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_direct_adjacency_stages);
criterion_main!(benches);
