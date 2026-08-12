// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Persisted CSR write and warm-load benchmarks.
//!
//! CSR construction happens before timing. Every write uses a new immutable
//! generation URI; every load reconstructs a new in-memory CsrIndex.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use lance_graph::{
    CsrIndexBuilder, CsrIndexHandle, CsrIndexStore, GraphIndexKey, GraphIndexMetadata,
    InMemoryGraphIndexRegistry, IndexDirection,
};

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
        } else if path.is_file() {
            files.push(path);
        }
    }
}

// This represents a Linux local page-cache cold start. It does not clear
// filesystem metadata caches or remote object-store caches.
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

fn make_edges(source_count: usize, degree: usize) -> RecordBatch {
    let edge_count = source_count.checked_mul(degree).unwrap();
    let mut sources = Vec::with_capacity(edge_count);
    let mut targets = Vec::with_capacity(edge_count);
    for source in 0..source_count {
        for offset in 1..=degree {
            sources.push(source as i64);
            targets.push(((source + offset) % source_count) as i64);
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

fn make_handle(source_count: usize, degree: usize) -> CsrIndexHandle {
    let edges = make_edges(source_count, degree);
    let index = CsrIndexBuilder::new()
        .with_num_vertices(source_count as u64)
        .add_edges_from_batch(&edges)
        .unwrap()
        .try_build()
        .unwrap();
    CsrIndexHandle {
        index: Arc::new(index),
        metadata: GraphIndexMetadata {
            key: GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
            source_id_field: "person_id".into(),
            target_id_field: "person_id".into(),
            id_data_type: DataType::Int64,
            num_vertices: source_count as u64,
            num_edges: edges.num_rows() as u64,
            source_uri: Some("benchmark://friend_of".into()),
            source_version: Some(1),
            generation: 1,
        },
    }
}

fn bench_persisted_index(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let scenarios = [
        (1_000usize, 10usize),
        (10_000usize, 10usize),
        (100_000usize, 10usize),
    ];

    let mut write_group = c.benchmark_group("persisted_csr_write");
    for &(source_count, degree) in &scenarios {
        let handle = make_handle(source_count, degree);
        let edge_count = source_count * degree;
        write_group.throughput(Throughput::Elements(edge_count as u64));
        write_group.bench_with_input(
            BenchmarkId::new(
                "write",
                format!("sources_{source_count}_degree_{degree}_edges_{edge_count}"),
            ),
            &(source_count, degree),
            |b, _| {
                b.iter_batched(
                    || {
                        let directory = tempfile::tempdir().unwrap();
                        let uri = directory.path().join("generation-1");
                        (directory, uri.to_str().unwrap().to_string())
                    },
                    |(_directory, uri)| {
                        black_box(
                            rt.block_on(CsrIndexStore::write(&uri, &handle, Default::default()))
                                .unwrap(),
                        )
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }
    write_group.finish();

    let mut load_group = c.benchmark_group("persisted_csr_load_warm");
    for &(source_count, degree) in &scenarios {
        let handle = make_handle(source_count, degree);
        let edge_count = source_count * degree;
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("generation-1");
        let descriptor = rt
            .block_on(CsrIndexStore::write(
                uri.to_str().unwrap(),
                &handle,
                Default::default(),
            ))
            .unwrap();
        drop(handle);
        load_group.throughput(Throughput::Elements(edge_count as u64));
        load_group.bench_with_input(
            BenchmarkId::new(
                "load",
                format!("sources_{source_count}_degree_{degree}_edges_{edge_count}"),
            ),
            &(source_count, degree),
            |b, _| {
                b.iter(|| {
                    let registry = InMemoryGraphIndexRegistry::new();
                    black_box(
                        rt.block_on(CsrIndexStore::load_into_registry(
                            &descriptor,
                            Default::default(),
                            &registry,
                        ))
                        .unwrap(),
                    )
                })
            },
        );
    }
    load_group.finish();

    let mut cold_load_group = c.benchmark_group("persisted_csr_load_cold_local");
    for &(source_count, degree) in &scenarios {
        let handle = make_handle(source_count, degree);
        let edge_count = source_count * degree;
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("generation-1");
        let descriptor = rt
            .block_on(CsrIndexStore::write(
                uri.to_str().unwrap(),
                &handle,
                Default::default(),
            ))
            .unwrap();
        drop(handle);
        let mut files = Vec::new();
        collect_files(&uri, &mut files);
        cold_load_group.throughput(Throughput::Elements(edge_count as u64));
        cold_load_group.bench_with_input(
            BenchmarkId::new(
                "load",
                format!("sources_{source_count}_degree_{degree}_edges_{edge_count}"),
            ),
            &(source_count, degree),
            |b, _| {
                b.iter_batched(
                    || {
                        drop_file_cache(&files);
                        InMemoryGraphIndexRegistry::new()
                    },
                    |registry| {
                        black_box(
                            rt.block_on(CsrIndexStore::load_into_registry(
                                &descriptor,
                                Default::default(),
                                &registry,
                            ))
                            .unwrap(),
                        )
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }
    cold_load_group.finish();
}

criterion_group!(benches, bench_persisted_index);
criterion_main!(benches);
