// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Fixed-start, exact-depth traversal benchmark for Join and CSR paths.
//!
//! The relationship table size stays fixed while the query depth changes. A
//! fanout of one isolates the cost of repeatedly scanning and joining the same
//! relationship table: every query returns exactly one row. A fanout greater
//! than one additionally exposes frontier growth. Index construction,
//! persistence, and loading are outside query timing.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::context::SessionContext;
use lance::datafusion::LanceTableProvider;
use lance::dataset::{Dataset, WriteParams};
use lance_graph::{
    CsrIndexBuilder, CsrIndexHandle, CsrIndexStore, CypherQuery, ExpandExecutionMode, GraphConfig,
    GraphIndexKey, GraphIndexMetadata, InMemoryCatalog, InMemoryGraphIndexRegistry, IndexDirection,
};
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};

const QUERY_SOURCE_ID: usize = 0;

struct DepthGraph {
    catalog: Arc<dyn lance_graph::GraphSourceCatalog>,
    join_context: SessionContext,
    indexed_context: SessionContext,
    indexes: Arc<InMemoryGraphIndexRegistry>,
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
    let edge_count = source_count.checked_mul(fanout).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    let mut sources = Vec::with_capacity(edge_count);
    let mut targets = Vec::with_capacity(edge_count);
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

fn setup_graph(rt: &tokio::runtime::Runtime, source_count: usize, fanout: usize) -> DepthGraph {
    let nodes = make_nodes(source_count);
    let edges = make_edges(source_count, fanout);
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
    let edge_dataset = Arc::new(
        rt.block_on(Dataset::write(
            edge_reader,
            edge_uri.to_str().unwrap(),
            Some(WriteParams::default()),
        ))
        .unwrap(),
    );
    let edge_source_uri = edge_dataset.uri().to_string();
    let edge_source_version = edge_dataset.version().version;
    let edge_table: Arc<dyn TableProvider> =
        Arc::new(LanceTableProvider::new(edge_dataset, false, false));

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

    let index = CsrIndexBuilder::new()
        .with_num_vertices(source_count as u64)
        .add_edges_from_batch(&edges)
        .unwrap()
        .try_build()
        .unwrap();
    assert_eq!(index.neighbors(QUERY_SOURCE_ID as u64).len(), fanout);
    let handle = CsrIndexHandle {
        index: Arc::new(index),
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
    drop(handle);
    let indexes = Arc::new(InMemoryGraphIndexRegistry::new());
    assert!(rt
        .block_on(CsrIndexStore::load_into_registry(
            &descriptor,
            Default::default(),
            indexes.as_ref(),
        ))
        .unwrap());

    DepthGraph {
        catalog,
        join_context,
        indexed_context,
        indexes,
        _data_dir: data_dir,
        source_count,
        edge_count,
        fanout,
    }
}

fn make_query(depth: usize) -> CypherQuery {
    assert!(depth > 0);
    let mut cypher = format!("MATCH (v0:Person {{person_id: {QUERY_SOURCE_ID}}})");
    for hop in 1..=depth {
        cypher.push_str(&format!("-[:FRIEND_OF]->(v{hop}:Person)"));
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

fn expected_rows(fanout: usize, depth: usize) -> usize {
    fanout.checked_pow(depth as u32).unwrap()
}

fn run_join(rt: &tokio::runtime::Runtime, graph: &DepthGraph, query: &CypherQuery) -> RecordBatch {
    rt.block_on(
        query.execute_with_catalog_and_context(graph.catalog.clone(), graph.join_context.clone()),
    )
    .unwrap()
}

fn run_indexed(
    rt: &tokio::runtime::Runtime,
    graph: &DepthGraph,
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

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
}

fn bench_depth_execution(c: &mut Criterion) {
    let source_count = env_usize("LANCE_GRAPH_DEPTH_SOURCES", 100_000);
    let fanouts = env_usize_list("LANCE_GRAPH_DEPTH_FANOUTS", &[1, 4]);
    let depths = env_usize_list("LANCE_GRAPH_DEPTHS", &[1, 2, 3, 4, 5]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("graph_execution_depth_join_vs_csr");

    for fanout in fanouts {
        let graph = setup_graph(&rt, source_count, fanout);
        for &depth in &depths {
            let rows = expected_rows(fanout, depth);
            assert!(
                rows < graph.source_count,
                "fanout^depth must remain below source count to keep the benchmark selective"
            );
            let query = make_query(depth);

            let join_result = run_join(&rt, &graph, &query);
            let indexed_result = run_indexed(&rt, &graph, &query);
            assert_eq!(join_result.num_rows(), rows);
            assert_eq!(sorted_ids(&indexed_result), sorted_ids(&join_result));

            let join_plan = rt
                .block_on(query.explain_with_catalog_context_and_indexes(
                    graph.catalog.clone(),
                    graph.join_context.clone(),
                    graph.indexes.clone(),
                    ExpandExecutionMode::Join,
                ))
                .unwrap();
            let indexed_plan = rt
                .block_on(query.explain_with_catalog_context_and_indexes(
                    graph.catalog.clone(),
                    graph.indexed_context.clone(),
                    graph.indexes.clone(),
                    ExpandExecutionMode::Csr,
                ))
                .unwrap();
            assert!(
                count_occurrences(&join_plan, "HashJoinExec") >= depth,
                "depth {depth} Join plan did not contain the expected repeated joins:\n{join_plan}"
            );
            assert_eq!(
                count_occurrences(&indexed_plan, "IndexedExpandExec"),
                depth,
                "depth {depth} indexed plan did not contain one indexed expand per hop:\n{indexed_plan}"
            );
            assert!(
                !indexed_plan.to_lowercase().contains("tablescan: friend_of"),
                "indexed depth plan scanned the relationship table:\n{indexed_plan}"
            );

            eprintln!(
                "depth workload sources={} edges={} fanout={} depth={} output_rows={} join_relationship_rows_if_full_scan={} indexed_neighbor_visits={}",
                graph.source_count,
                graph.edge_count,
                graph.fanout,
                depth,
                rows,
                graph.edge_count * depth,
                (1..=depth)
                    .map(|hop| expected_rows(fanout, hop))
                    .sum::<usize>()
            );

            group.throughput(Throughput::Elements(rows as u64));
            let parameter = format!(
                "sources_{}_edges_{}_fanout_{}_depth_{}_rows_{}",
                graph.source_count, graph.edge_count, fanout, depth, rows
            );
            group.bench_with_input(BenchmarkId::new("join", &parameter), &depth, |b, _| {
                b.iter(|| black_box(run_join(&rt, &graph, &query).num_rows()))
            });
            group.bench_with_input(BenchmarkId::new("csr_get_v", &parameter), &depth, |b, _| {
                b.iter(|| black_box(run_indexed(&rt, &graph, &query).num_rows()))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_depth_execution);
criterion_main!(benches);
