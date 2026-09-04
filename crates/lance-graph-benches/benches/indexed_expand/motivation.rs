// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Controlled motivation experiments for dedicated graph adjacency indexes.
//!
//! This benchmark separates two levels of comparison:
//!
//! 1. end-to-end Cypher: relational Join versus CSR expansion + indexed GetV;
//! 2. adjacency access only: repeated full edge scans versus an ordinary
//!    `src_id` BTree on edge rows versus in-memory CSR.
//!
//! Index construction, persistence, and opening are outside timed iterations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lance::datafusion::LanceTableProvider;
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexInternalExt;
use lance_graph::{
    CsrIndex, CsrIndexBuilder, CsrIndexHandle, CsrIndexStore, CypherQuery, ExpandExecutionMode,
    GraphConfig, GraphIndexKey, GraphIndexMetadata, InMemoryCatalog, InMemoryGraphIndexRegistry,
    IndexDirection,
};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{
    BuiltinIndexType, SargableQuery, ScalarIndex, ScalarIndexParams, SearchResult,
};
use lance_index::{DatasetIndexExt, IndexCriteria, IndexType};

struct MotivationGraph {
    catalog: Arc<dyn lance_graph::GraphSourceCatalog>,
    join_context: SessionContext,
    indexed_context: SessionContext,
    indexes: Arc<InMemoryGraphIndexRegistry>,
    edge_dataset: Arc<Dataset>,
    edge_scalar_index: Option<Arc<dyn ScalarIndex>>,
    csr: Arc<CsrIndex>,
    _data_dir: tempfile::TempDir,
    source_count: usize,
    edge_count: usize,
    fanout: usize,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(default)
}

fn env_usize_list(name: &str, defaults: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().parse::<usize>().unwrap())
                .collect()
        })
        .unwrap_or_else(|| defaults.to_vec())
}

fn make_nodes(source_count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "person_id",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(
            (0..source_count).map(|id| id as i64),
        ))],
    )
    .unwrap()
}

fn make_edges(source_count: usize, fanout: usize) -> RecordBatch {
    assert!(fanout > 0);
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    let mut sources = Vec::with_capacity(source_count * fanout);
    let mut targets = Vec::with_capacity(source_count * fanout);
    for source in 0..source_count {
        for offset in 1..=fanout {
            sources.push(source as i64);
            targets.push(((source * fanout + offset) % source_count) as i64);
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(sources)),
            Arc::new(Int64Array::from(targets)),
        ],
    )
    .unwrap()
}

fn make_hub_edges(source_count: usize, edge_count: usize, hub_degree: usize) -> RecordBatch {
    assert!(source_count > 1);
    assert!(hub_degree <= edge_count);
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    let remaining = edge_count - hub_degree;
    let base_degree = remaining / (source_count - 1);
    let extra_sources = remaining % (source_count - 1);
    let mut sources = Vec::with_capacity(edge_count);
    let mut targets = Vec::with_capacity(edge_count);
    for source in 0..source_count {
        let degree = if source == 0 {
            hub_degree
        } else {
            base_degree + usize::from(source <= extra_sources)
        };
        for offset in 1..=degree {
            sources.push(source as i64);
            targets.push(((source + offset) % source_count) as i64);
        }
    }
    assert_eq!(sources.len(), edge_count);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(sources)),
            Arc::new(Int64Array::from(targets)),
        ],
    )
    .unwrap()
}

fn setup_graph(
    rt: &tokio::runtime::Runtime,
    source_count: usize,
    fanout: usize,
    build_edge_scalar_index: bool,
) -> MotivationGraph {
    let edges = make_edges(source_count, fanout);
    setup_graph_from_edges(rt, source_count, fanout, edges, build_edge_scalar_index)
}

fn setup_graph_from_edges(
    rt: &tokio::runtime::Runtime,
    source_count: usize,
    nominal_fanout: usize,
    edges: RecordBatch,
    build_edge_scalar_index: bool,
) -> MotivationGraph {
    let nodes = make_nodes(source_count);
    let edge_count = edges.num_rows();
    let data_dir = tempfile::tempdir().unwrap();
    let node_uri = data_dir.path().join("Person.lance");
    let edge_uri = data_dir.path().join("FRIEND_OF.lance");

    let node_reader = RecordBatchIterator::new(vec![Ok(nodes.clone())], nodes.schema());
    let mut node_dataset = rt
        .block_on(Dataset::write(
            node_reader,
            node_uri.to_str().unwrap(),
            Some(WriteParams::default()),
        ))
        .unwrap();
    rt.block_on(node_dataset.create_index(
        &["person_id"],
        IndexType::BTree,
        Some("person_id_btree".into()),
        &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
        false,
    ))
    .unwrap();
    let node_dataset = Arc::new(
        rt.block_on(Dataset::open(node_uri.to_str().unwrap()))
            .unwrap(),
    );
    let node_table: Arc<dyn TableProvider> =
        Arc::new(LanceTableProvider::new(node_dataset, false, false));

    let edge_reader = RecordBatchIterator::new(vec![Ok(edges.clone())], edges.schema());
    let mut edge_dataset = rt
        .block_on(Dataset::write(
            edge_reader,
            edge_uri.to_str().unwrap(),
            Some(WriteParams::default()),
        ))
        .unwrap();
    let edge_scalar_index = if build_edge_scalar_index {
        rt.block_on(edge_dataset.create_index(
            &["src_id"],
            IndexType::BTree,
            Some("src_id_btree".into()),
            &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
            false,
        ))
        .unwrap();
        let scalar_metadata = rt
            .block_on(
                edge_dataset.load_scalar_index(
                    IndexCriteria::default()
                        .with_name("src_id_btree")
                        .supports_exact_equality(),
                ),
            )
            .unwrap()
            .unwrap();
        Some(
            rt.block_on(edge_dataset.open_scalar_index(
                "src_id",
                &scalar_metadata.uuid.to_string(),
                &NoOpMetricsCollector,
            ))
            .unwrap(),
        )
    } else {
        None
    };
    let edge_dataset = Arc::new(edge_dataset);
    let edge_source_uri = edge_dataset.uri().to_string();
    let edge_source_version = edge_dataset.version().version;
    let edge_table: Arc<dyn TableProvider> =
        Arc::new(LanceTableProvider::new(edge_dataset.clone(), false, false));

    let join_context = SessionContext::new();
    join_context
        .register_table("person", node_table.clone())
        .unwrap();
    join_context
        .register_table("friend_of", edge_table.clone())
        .unwrap();
    let indexed_context = SessionContext::new();
    indexed_context
        .register_table("person", node_table.clone())
        .unwrap();
    indexed_context
        .register_table("friend_of", edge_table.clone())
        .unwrap();
    let catalog = Arc::new(
        InMemoryCatalog::new()
            .with_node_source("Person", Arc::new(DefaultTableSource::new(node_table)))
            .with_relationship_source("FRIEND_OF", Arc::new(DefaultTableSource::new(edge_table))),
    );

    let csr = Arc::new(
        CsrIndexBuilder::new()
            .with_num_vertices(source_count as u64)
            .add_edges_from_batch(&edges)
            .unwrap()
            .try_build()
            .unwrap(),
    );
    let handle = CsrIndexHandle {
        index: csr.clone(),
        metadata: GraphIndexMetadata {
            key: GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
            source_id_field: "person_id".into(),
            target_id_field: "person_id".into(),
            id_data_type: DataType::Int64,
            num_vertices: source_count as u64,
            num_edges: edge_count as u64,
            source_uri: Some(edge_source_uri),
            source_version: Some(edge_source_version),
            generation: 1,
        },
    };
    let index_uri = data_dir.path().join("csr-generation-1");
    let descriptor = rt
        .block_on(CsrIndexStore::write(
            index_uri.to_str().unwrap(),
            &handle,
            Default::default(),
        ))
        .unwrap();
    let indexes = Arc::new(InMemoryGraphIndexRegistry::new());
    assert!(rt
        .block_on(CsrIndexStore::load_into_registry(
            &descriptor,
            Default::default(),
            indexes.as_ref(),
        ))
        .unwrap());

    MotivationGraph {
        catalog,
        join_context,
        indexed_context,
        indexes,
        edge_dataset,
        edge_scalar_index,
        csr,
        _data_dir: data_dir,
        source_count,
        edge_count,
        fanout: nominal_fanout,
    }
}

fn make_query(depth: usize, frontier_size: usize) -> CypherQuery {
    assert!(depth > 0);
    assert!(frontier_size > 0);
    let mut cypher = if frontier_size == 1 {
        "MATCH (v0:Person {person_id: 0})".to_string()
    } else {
        "MATCH (v0:Person)".to_string()
    };
    for hop in 1..=depth {
        cypher.push_str(&format!("-[:FRIEND_OF]->(v{hop}:Person)"));
    }
    if frontier_size > 1 {
        cypher.push_str(&format!(" WHERE v0.person_id < {frontier_size}"));
    }
    cypher.push_str(&format!(" RETURN v{depth}.person_id"));
    CypherQuery::new(&cypher).unwrap().with_config(
        GraphConfig::builder()
            .with_node_label("Person", "person_id")
            .with_relationship("FRIEND_OF", "src_id", "dst_id")
            .build()
            .unwrap(),
    )
}

fn expected_rows(frontier_size: usize, fanout: usize, depth: usize) -> usize {
    frontier_size
        .checked_mul(fanout.checked_pow(depth as u32).unwrap())
        .unwrap()
}

fn run_join(
    rt: &tokio::runtime::Runtime,
    graph: &MotivationGraph,
    query: &CypherQuery,
) -> RecordBatch {
    rt.block_on(
        query.execute_with_catalog_and_context(graph.catalog.clone(), graph.join_context.clone()),
    )
    .unwrap()
}

fn run_indexed(
    rt: &tokio::runtime::Runtime,
    graph: &MotivationGraph,
    query: &CypherQuery,
) -> RecordBatch {
    rt.block_on(query.execute_with_catalog_context_and_indexes(
        graph.catalog.clone(),
        graph.indexed_context.clone(),
        graph.indexes.clone(),
        ExpandExecutionMode::Csr,
    ))
    .unwrap()
}

fn sorted_ids(batch: &RecordBatch) -> Vec<i64> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values = (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn batch_edges(batch: &RecordBatch) -> impl Iterator<Item = (i64, i64)> + '_ {
    let sources = batch
        .column_by_name("src_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let targets = batch
        .column_by_name("dst_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows()).map(|row| (sources.value(row), targets.value(row)))
}

fn replay_frontier(frontier: &[i64], adjacency: &HashMap<i64, Vec<i64>>) -> Vec<i64> {
    let mut next = Vec::new();
    for source in frontier {
        if let Some(targets) = adjacency.get(source) {
            next.extend_from_slice(targets);
        }
    }
    next
}

async fn traverse_full_edge_scan(
    graph: &MotivationGraph,
    starts: &[i64],
    depth: usize,
) -> Vec<i64> {
    let mut frontier = starts.to_vec();
    for _ in 0..depth {
        let requested = frontier.iter().copied().collect::<HashSet<_>>();
        let mut scanner = graph.edge_dataset.scan();
        scanner.project(&["src_id", "dst_id"]).unwrap();
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut adjacency = HashMap::<i64, Vec<i64>>::new();
        for batch in &batches {
            for (source, target) in batch_edges(batch) {
                if requested.contains(&source) {
                    adjacency.entry(source).or_default().push(target);
                }
            }
        }
        frontier = replay_frontier(&frontier, &adjacency);
    }
    frontier
}

async fn traverse_scalar_edge_index(
    graph: &MotivationGraph,
    starts: &[i64],
    depth: usize,
) -> Vec<i64> {
    let mut frontier = starts.to_vec();
    for _ in 0..depth {
        let query = frontier
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|id| ScalarValue::Int64(Some(id)))
            .collect::<Vec<_>>();
        let search = graph
            .edge_scalar_index
            .as_ref()
            .expect("edge scalar index is required for this benchmark")
            .search(&SargableQuery::IsIn(query), &NoOpMetricsCollector)
            .await
            .unwrap();
        let row_ids = match search {
            SearchResult::Exact(ids) | SearchResult::AtMost(ids) => ids,
            SearchResult::AtLeast(_) => panic!("edge scalar index returned incomplete row IDs"),
        }
        .row_ids()
        .unwrap()
        .map(u64::from)
        .collect::<Vec<_>>();
        if row_ids.is_empty() {
            return Vec::new();
        }
        let fetched = graph
            .edge_dataset
            .take_rows(&row_ids, graph.edge_dataset.schema().clone())
            .await
            .unwrap();
        let mut adjacency = HashMap::<i64, Vec<i64>>::new();
        for (source, target) in batch_edges(&fetched) {
            adjacency.entry(source).or_default().push(target);
        }
        frontier = replay_frontier(&frontier, &adjacency);
    }
    frontier
}

fn traverse_csr(graph: &MotivationGraph, starts: &[i64], depth: usize) -> Vec<i64> {
    let mut frontier = starts.to_vec();
    for _ in 0..depth {
        let mut next = Vec::new();
        for source in frontier {
            next.extend(
                graph
                    .csr
                    .neighbors(source as u64)
                    .iter()
                    .map(|target| *target as i64),
            );
        }
        frontier = next;
    }
    frontier
}

fn verify_end_to_end(
    rt: &tokio::runtime::Runtime,
    graph: &MotivationGraph,
    query: &CypherQuery,
    expected: usize,
) {
    let join = run_join(rt, graph, query);
    let indexed = run_indexed(rt, graph, query);
    assert_eq!(join.num_rows(), expected);
    assert_eq!(sorted_ids(&indexed), sorted_ids(&join));
}

fn explain_join(
    rt: &tokio::runtime::Runtime,
    graph: &MotivationGraph,
    query: &CypherQuery,
) -> String {
    rt.block_on(query.explain_with_catalog_context_and_indexes(
        graph.catalog.clone(),
        graph.join_context.clone(),
        graph.indexes.clone(),
        ExpandExecutionMode::Join,
    ))
    .unwrap()
}

fn bench_relationship_scale(c: &mut Criterion) {
    let source_counts = env_usize_list("LANCE_GRAPH_SCALE_SOURCES", &[1_000, 10_000, 100_000]);
    let depth = env_usize("LANCE_GRAPH_SCALE_DEPTH", 3);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("graph_execution_relationship_scale");

    for source_count in source_counts {
        let graph = setup_graph(&rt, source_count, 1, false);
        let scalar_graph = setup_graph(&rt, source_count, 1, true);
        let query = make_query(depth, 1);
        verify_end_to_end(&rt, &graph, &query, 1);
        verify_end_to_end(&rt, &scalar_graph, &query, 1);
        let no_index_plan = explain_join(&rt, &graph, &query);
        let scalar_plan = explain_join(&rt, &scalar_graph, &query);
        assert!(no_index_plan.contains("HashJoinExec"));
        assert!(scalar_plan.contains("HashJoinExec"));
        assert!(!no_index_plan.contains("IndexedExpandExec"));
        assert!(!scalar_plan.contains("IndexedExpandExec"));
        eprintln!(
            "relationship scale edges={} depth={} edge_btree_present={} join_hash_joins={} relationship_scan_mentions={}",
            graph.edge_count,
            depth,
            scalar_graph.edge_scalar_index.is_some(),
            scalar_plan.match_indices("HashJoinExec").count(),
            scalar_plan
                .to_lowercase()
                .match_indices("friend_of")
                .count()
        );
        let parameter = format!("edges_{}_depth_{depth}_rows_1", graph.edge_count);
        group.bench_function(BenchmarkId::new("join_no_edge_index", &parameter), |b| {
            b.iter(|| black_box(run_join(&rt, &graph, &query).num_rows()))
        });
        group.bench_function(BenchmarkId::new("join_edge_btree", &parameter), |b| {
            b.iter(|| black_box(run_join(&rt, &scalar_graph, &query).num_rows()))
        });
        group.bench_function(BenchmarkId::new("csr_get_v", &parameter), |b| {
            b.iter(|| black_box(run_indexed(&rt, &graph, &query).num_rows()))
        });
    }
    group.finish();
}

fn bench_start_frontier(c: &mut Criterion) {
    let source_count = env_usize("LANCE_GRAPH_FRONTIER_SOURCES", 100_000);
    let fanout = env_usize("LANCE_GRAPH_FRONTIER_FANOUT", 4);
    let depth = env_usize("LANCE_GRAPH_FRONTIER_DEPTH", 2);
    let frontiers = env_usize_list("LANCE_GRAPH_FRONTIER_SIZES", &[1, 10, 100, 1_000]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = setup_graph(&rt, source_count, fanout, false);
    let scalar_graph = setup_graph(&rt, source_count, fanout, true);
    let mut group = c.benchmark_group("graph_execution_start_frontier");

    for frontier in frontiers {
        let rows = expected_rows(frontier, fanout, depth);
        assert!(rows < graph.source_count, "result must remain selective");
        let query = make_query(depth, frontier);
        verify_end_to_end(&rt, &graph, &query, rows);
        verify_end_to_end(&rt, &scalar_graph, &query, rows);
        group.throughput(Throughput::Elements(rows as u64));
        let parameter = format!(
            "edges_{}_frontier_{}_fanout_{}_depth_{}_rows_{}",
            graph.edge_count, frontier, fanout, depth, rows
        );
        group.bench_function(BenchmarkId::new("join_no_edge_index", &parameter), |b| {
            b.iter(|| black_box(run_join(&rt, &graph, &query).num_rows()))
        });
        group.bench_function(BenchmarkId::new("join_edge_btree", &parameter), |b| {
            b.iter(|| black_box(run_join(&rt, &scalar_graph, &query).num_rows()))
        });
        group.bench_function(BenchmarkId::new("csr_get_v", &parameter), |b| {
            b.iter(|| black_box(run_indexed(&rt, &graph, &query).num_rows()))
        });
    }
    group.finish();
}

fn bench_adjacency_access(c: &mut Criterion) {
    let source_count = env_usize("LANCE_GRAPH_ACCESS_SOURCES", 100_000);
    let fanout = env_usize("LANCE_GRAPH_ACCESS_FANOUT", 4);
    let depth = env_usize("LANCE_GRAPH_ACCESS_DEPTH", 3);
    let frontiers = env_usize_list("LANCE_GRAPH_ACCESS_FRONTIERS", &[1, 10, 100, 1_000]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = setup_graph(&rt, source_count, fanout, true);
    let mut group = c.benchmark_group("adjacency_access_scan_vs_scalar_vs_csr");

    for frontier in frontiers {
        let starts = (0..frontier).map(|id| id as i64).collect::<Vec<_>>();
        let rows = expected_rows(frontier, fanout, depth);
        assert!(rows < graph.source_count, "result must remain selective");
        let mut full_scan = rt.block_on(traverse_full_edge_scan(&graph, &starts, depth));
        let mut scalar = rt.block_on(traverse_scalar_edge_index(&graph, &starts, depth));
        let mut csr = traverse_csr(&graph, &starts, depth);
        full_scan.sort_unstable();
        scalar.sort_unstable();
        csr.sort_unstable();
        assert_eq!(full_scan.len(), rows);
        assert_eq!(scalar, full_scan);
        assert_eq!(csr, full_scan);

        let visited_edges = (1..=depth)
            .map(|hop| expected_rows(frontier, fanout, hop))
            .sum::<usize>();
        eprintln!(
            "motivation access sources={} edges={} frontier={} fanout={} depth={} output_rows={} full_scan_rows_if_no_pushdown={} scalar_edge_rows_fetched={} csr_neighbor_visits={}",
            graph.source_count,
            graph.edge_count,
            frontier,
            graph.fanout,
            depth,
            rows,
            graph.edge_count * depth,
            visited_edges,
            visited_edges
        );

        group.throughput(Throughput::Elements(rows as u64));
        let parameter = format!(
            "edges_{}_frontier_{}_fanout_{}_depth_{}_rows_{}",
            graph.edge_count, frontier, fanout, depth, rows
        );
        group.bench_function(BenchmarkId::new("full_edge_scan", &parameter), |b| {
            b.to_async(&rt).iter(|| async {
                black_box(traverse_full_edge_scan(&graph, &starts, depth).await.len())
            })
        });
        group.bench_function(BenchmarkId::new("edge_scalar_btree", &parameter), |b| {
            b.to_async(&rt).iter(|| async {
                black_box(
                    traverse_scalar_edge_index(&graph, &starts, depth)
                        .await
                        .len(),
                )
            })
        });
        group.bench_function(BenchmarkId::new("csr", &parameter), |b| {
            b.iter(|| black_box(traverse_csr(&graph, &starts, depth).len()))
        });
    }
    group.finish();
}

fn bench_high_degree_access(c: &mut Criterion) {
    let source_count = env_usize("LANCE_GRAPH_DEGREE_SOURCES", 100_000);
    let edge_count = env_usize("LANCE_GRAPH_DEGREE_EDGES", 400_000);
    let degrees = env_usize_list("LANCE_GRAPH_DEGREES", &[1, 10, 100, 1_000, 10_000]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("adjacency_access_high_degree");

    for degree in degrees {
        assert!(degree < source_count);
        let edges = make_hub_edges(source_count, edge_count, degree);
        let graph = setup_graph_from_edges(&rt, source_count, 0, edges, true);
        let starts = [0_i64];
        let full_scan = rt.block_on(traverse_full_edge_scan(&graph, &starts, 1));
        let scalar = rt.block_on(traverse_scalar_edge_index(&graph, &starts, 1));
        let csr = traverse_csr(&graph, &starts, 1);
        assert_eq!(full_scan.len(), degree);
        assert_eq!(scalar.len(), degree);
        assert_eq!(csr.len(), degree);

        eprintln!(
            "motivation degree sources={} edges={} hub_degree={} full_scan_rows_if_no_pushdown={} scalar_edge_rows_fetched={} csr_neighbor_visits={}",
            source_count, edge_count, degree, edge_count, degree, degree
        );
        group.throughput(Throughput::Elements(degree as u64));
        let parameter = format!("edges_{edge_count}_hub_degree_{degree}");
        group.bench_function(BenchmarkId::new("full_edge_scan", &parameter), |b| {
            b.to_async(&rt).iter(|| async {
                black_box(traverse_full_edge_scan(&graph, &starts, 1).await.len())
            })
        });
        group.bench_function(BenchmarkId::new("edge_scalar_btree", &parameter), |b| {
            b.to_async(&rt).iter(|| async {
                black_box(traverse_scalar_edge_index(&graph, &starts, 1).await.len())
            })
        });
        group.bench_function(BenchmarkId::new("csr", &parameter), |b| {
            b.iter(|| black_box(traverse_csr(&graph, &starts, 1).len()))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_relationship_scale,
    bench_start_frontier,
    bench_adjacency_access,
    bench_high_degree_access
);
criterion_main!(benches);
