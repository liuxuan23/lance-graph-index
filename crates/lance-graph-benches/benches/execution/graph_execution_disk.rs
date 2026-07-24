// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Graph query execution benchmarks against Lance datasets on disk.
//!
//! The datasets are written once before measurement. Each benchmark iteration
//! resolves the tables through a directory namespace and executes the query
//! against Lance-backed DataFusion table providers.
//! The benchmark reports both repeated warm-cache queries and Linux local
//! page-cache cold-start queries.
//!
//! Run with:
//! ```
//! cargo bench --bench graph_execution_disk
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_graph::{CypherQuery, DirNamespace, GraphConfig};
use tempfile::TempDir;

fn make_people_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("person_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let ids: Vec<i32> = (0..n as i32).collect();
    let names: Vec<String> = (0..n).map(|i| format!("name_{}", i)).collect();
    let ages: Vec<i32> = (0..n as i32).map(|i| 20 + (i % 60)).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(ages)),
        ],
    )
    .unwrap()
}

fn make_friendship_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("person1_id", DataType::Int32, false),
        Field::new("person2_id", DataType::Int32, false),
        Field::new("friendship_type", DataType::Utf8, false),
    ]));
    let src: Vec<i32> = (0..n as i32).collect();
    let dst: Vec<i32> = (0..n as i32).map(|i| (i + 1) % n as i32).collect();
    let friendship_type: Vec<&str> = std::iter::repeat_n("friend", n).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(src)),
            Arc::new(Int32Array::from(dst)),
            Arc::new(StringArray::from(friendship_type)),
        ],
    )
    .unwrap()
}

struct DiskGraph {
    // Keep the temporary directory alive for the whole benchmark run.
    _tmpdir: TempDir,
    namespace: DirNamespace,
    files: Vec<PathBuf>,
}

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

// Drop file-backed pages before a cold query on Linux. This does not clear filesystem
// metadata caches or remote object-store caches, so it represents a local
// page-cache cold start rather than every possible kind of cold storage.
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

fn create_disk_graph(rt: &tokio::runtime::Runtime, n: usize) -> DiskGraph {
    let tmpdir = tempfile::tempdir().unwrap();
    let person_path = tmpdir.path().join("Person.lance");
    let friendship_path = tmpdir.path().join("FRIEND_OF.lance");
    let people = make_people_batch(n);
    let friendship = make_friendship_batch(n);

    rt.block_on(async {
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(people.clone())].into_iter(), people.schema()),
            person_path.to_str().unwrap(),
            Some(WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        Dataset::write(
            RecordBatchIterator::new(
                vec![Ok(friendship.clone())].into_iter(),
                friendship.schema(),
            ),
            friendship_path.to_str().unwrap(),
            Some(WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    });

    let namespace = DirNamespace::new(tmpdir.path().to_string_lossy().into_owned());
    let mut files = Vec::new();
    collect_files(tmpdir.path(), &mut files);
    DiskGraph {
        _tmpdir: tmpdir,
        namespace,
        files,
    }
}

fn execute_disk_query(
    rt: &tokio::runtime::Runtime,
    query: &CypherQuery,
    namespace: &DirNamespace,
) -> RecordBatch {
    rt.block_on(query.execute_with_namespace(namespace.clone(), None))
        .unwrap()
}

fn bench_cypher_execution_disk(c: &mut Criterion) {
    let sizes = [100usize, 10_000usize, 1_000_000usize];
    let rt = tokio::runtime::Runtime::new().unwrap();

    let make_config = || {
        GraphConfig::builder()
            .with_node_label("Person", "person_id")
            .with_relationship("FRIEND_OF", "person1_id", "person2_id")
            .build()
            .unwrap()
    };

    let q_basic = CypherQuery::new("MATCH (n:Person) WHERE n.age > 50 RETURN n.name")
        .unwrap()
        .with_config(make_config());
    let q_single_hop = CypherQuery::new("MATCH (a:Person)-[:FRIEND_OF]->(b:Person) RETURN b.name")
        .unwrap()
        .with_config(make_config());
    let q_two_hop = CypherQuery::new(
        "MATCH (a:Person)-[:FRIEND_OF]->(b:Person)-[:FRIEND_OF]->(c:Person) RETURN c.name",
    )
    .unwrap()
    .with_config(make_config());

    // Persist each size once, outside the measured iterations.
    let disk_graphs: Vec<DiskGraph> = sizes.iter().map(|&n| create_disk_graph(&rt, n)).collect();

    let mut warm_group = c.benchmark_group("cypher_execution_disk_warm");
    for (index, &n) in sizes.iter().enumerate() {
        warm_group.throughput(Throughput::Elements(n as u64));
        warm_group.bench_with_input(BenchmarkId::new("basic_node_filter", n), &n, |b, _| {
            let namespace = &disk_graphs[index].namespace;
            b.iter(|| {
                let out = execute_disk_query(&rt, &q_basic, namespace);
                black_box(out.num_rows());
            })
        });
    }

    for (index, &n) in sizes.iter().enumerate() {
        warm_group.throughput(Throughput::Elements(n as u64));
        warm_group.bench_with_input(BenchmarkId::new("single_hop_expand", n), &n, |b, _| {
            let namespace = &disk_graphs[index].namespace;
            b.iter(|| {
                let out = execute_disk_query(&rt, &q_single_hop, namespace);
                black_box(out.num_rows());
            })
        });
    }

    for (index, &n) in sizes.iter().enumerate() {
        warm_group.throughput(Throughput::Elements(n as u64));
        warm_group.bench_with_input(BenchmarkId::new("two_hop_expand", n), &n, |b, _| {
            let namespace = &disk_graphs[index].namespace;
            b.iter(|| {
                let out = execute_disk_query(&rt, &q_two_hop, namespace);
                black_box(out.num_rows());
            })
        });
    }
    warm_group.finish();

    let mut cold_group = c.benchmark_group("cypher_execution_disk_cold");
    for (index, &n) in sizes.iter().enumerate() {
        cold_group.throughput(Throughput::Elements(n as u64));
        cold_group.bench_with_input(BenchmarkId::new("basic_node_filter", n), &n, |b, _| {
            let graph = &disk_graphs[index];
            b.iter_batched(
                || drop_file_cache(&graph.files),
                |_| {
                    let out = execute_disk_query(&rt, &q_basic, &graph.namespace);
                    black_box(out.num_rows());
                },
                BatchSize::SmallInput,
            )
        });
    }

    for (index, &n) in sizes.iter().enumerate() {
        cold_group.throughput(Throughput::Elements(n as u64));
        cold_group.bench_with_input(BenchmarkId::new("single_hop_expand", n), &n, |b, _| {
            let graph = &disk_graphs[index];
            b.iter_batched(
                || drop_file_cache(&graph.files),
                |_| {
                    let out = execute_disk_query(&rt, &q_single_hop, &graph.namespace);
                    black_box(out.num_rows());
                },
                BatchSize::SmallInput,
            )
        });
    }

    for (index, &n) in sizes.iter().enumerate() {
        cold_group.throughput(Throughput::Elements(n as u64));
        cold_group.bench_with_input(BenchmarkId::new("two_hop_expand", n), &n, |b, _| {
            let graph = &disk_graphs[index];
            b.iter_batched(
                || drop_file_cache(&graph.files),
                |_| {
                    let out = execute_disk_query(&rt, &q_two_hop, &graph.namespace);
                    black_box(out.num_rows());
                },
                BatchSize::SmallInput,
            )
        });
    }
    cold_group.finish();
}

criterion_group!(benches, bench_cypher_execution_disk);
criterion_main!(benches);
