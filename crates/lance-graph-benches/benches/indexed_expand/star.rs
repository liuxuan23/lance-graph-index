// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Multi-star benchmark for the relationship Join and CSR indexed paths.
//!
//! Every vertex is a source with the same fan-out, so the relationship table is
//! much larger than the result of the selective query. The query fixes one hub
//! and compares scanning/Joining the relationship table with a direct CSR
//! adjacency lookup. The persisted-loaded case writes and reloads the same CSR
//! during setup, outside query timing.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::execution::context::SessionContext;
use lance_graph::{
    CsrIndexBuilder, CsrIndexHandle, CsrIndexStore, CypherQuery, GraphConfig, GraphIndexKey,
    GraphIndexMetadata, InMemoryCatalog, InMemoryGraphIndexRegistry, IndexDirection,
    IndexUsagePolicy,
};

struct StarGraph {
    query_hub: CypherQuery,
    catalog: Arc<dyn lance_graph::GraphSourceCatalog>,
    join_context: SessionContext,
    indexed_context: SessionContext,
    persisted_context: SessionContext,
    indexes: Arc<InMemoryGraphIndexRegistry>,
    persisted_indexes: Arc<InMemoryGraphIndexRegistry>,
    _persisted_dir: tempfile::TempDir,
    expected_rows: usize,
}

fn make_nodes(source_count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..source_count as i64).collect();
    let names: Vec<String> = (0..source_count).map(|i| format!("person_{i}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn make_star_edges(source_count: usize, degree: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    let edge_count = source_count.checked_mul(degree).unwrap();
    let mut src = Vec::with_capacity(edge_count);
    let mut dst = Vec::with_capacity(edge_count);
    for source in 0..source_count {
        for offset in 1..=degree {
            src.push(source as i64);
            dst.push(((source + offset) % source_count) as i64);
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(src)),
            Arc::new(Int64Array::from(dst)),
        ],
    )
    .unwrap()
}

fn setup_graph(rt: &tokio::runtime::Runtime, source_count: usize, degree: usize) -> StarGraph {
    assert!(
        degree < source_count,
        "star degree must be smaller than the source count"
    );
    assert!(source_count > 42, "selected hub 42 must exist");

    let nodes = make_nodes(source_count);
    let edges = make_star_edges(source_count, degree);
    let node_schema = nodes.schema();
    let edge_schema = edges.schema();
    let node_table = Arc::new(MemTable::try_new(node_schema, vec![vec![nodes]]).unwrap());
    let edge_table = Arc::new(MemTable::try_new(edge_schema, vec![vec![edges.clone()]]).unwrap());

    // Keep the contexts separate: the indexed API installs a custom planner,
    // which must not affect the Join baseline.
    let indexed_context = SessionContext::new();
    indexed_context
        .register_table("person", node_table.clone())
        .unwrap();
    indexed_context
        .register_table("friend_of", edge_table.clone())
        .unwrap();
    let persisted_context = SessionContext::new();
    persisted_context
        .register_table("person", node_table.clone())
        .unwrap();
    persisted_context
        .register_table("friend_of", edge_table.clone())
        .unwrap();
    let join_context = SessionContext::new();
    join_context
        .register_table("person", node_table.clone())
        .unwrap();
    join_context
        .register_table("friend_of", edge_table.clone())
        .unwrap();

    let catalog = Arc::new(
        InMemoryCatalog::new()
            .with_node_source("Person", Arc::new(DefaultTableSource::new(node_table)))
            .with_relationship_source("FRIEND_OF", Arc::new(DefaultTableSource::new(edge_table))),
    );

    // Build once during setup; CSR construction is intentionally excluded from
    // the query timings below.
    let index = CsrIndexBuilder::new()
        .with_num_vertices(source_count as u64)
        .add_edges_from_batch(&edges)
        .unwrap()
        .try_build()
        .unwrap();
    let handle = CsrIndexHandle {
        index: Arc::new(index),
        metadata: GraphIndexMetadata {
            key: GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
            source_id_field: "person_id".into(),
            target_id_field: "person_id".into(),
            id_data_type: DataType::Int64,
            num_vertices: source_count as u64,
            num_edges: edges.num_rows() as u64,
            source_uri: None,
            source_version: None,
            generation: 1,
        },
    };
    let indexes = Arc::new(InMemoryGraphIndexRegistry::new());
    indexes.register_csr(handle.clone()).unwrap();

    // Persist and reload before measurement. This registry contains a newly
    // reconstructed CsrIndex, not the handle used by the in-memory case.
    let persisted_dir = tempfile::tempdir().unwrap();
    let index_uri = persisted_dir.path().join("generation-1");
    let descriptor = rt
        .block_on(CsrIndexStore::write(
            index_uri.to_str().unwrap(),
            &handle,
            Default::default(),
        ))
        .unwrap();
    drop(handle);
    let persisted_indexes = Arc::new(InMemoryGraphIndexRegistry::new());
    assert!(rt
        .block_on(CsrIndexStore::load_into_registry(
            &descriptor,
            Default::default(),
            persisted_indexes.as_ref(),
            IndexUsagePolicy::Require,
        ))
        .unwrap());

    let config = GraphConfig::builder()
        .with_node_label("Person", "person_id")
        .with_relationship("FRIEND_OF", "src_id", "dst_id")
        .build()
        .unwrap();
    let query_hub =
        CypherQuery::new("MATCH (a:Person {person_id: 42})-[:FRIEND_OF]->(b:Person) RETURN b.name")
            .unwrap()
            .with_config(config);

    StarGraph {
        query_hub,
        catalog,
        join_context,
        indexed_context,
        persisted_context,
        indexes,
        persisted_indexes,
        _persisted_dir: persisted_dir,
        expected_rows: degree,
    }
}

fn run_join_query(rt: &tokio::runtime::Runtime, graph: &StarGraph, query: &CypherQuery) -> usize {
    rt.block_on(
        query.execute_with_catalog_and_context(graph.catalog.clone(), graph.join_context.clone()),
    )
    .unwrap()
    .num_rows()
}

fn run_indexed_query(
    rt: &tokio::runtime::Runtime,
    graph: &StarGraph,
    query: &CypherQuery,
) -> usize {
    rt.block_on(query.execute_with_catalog_context_and_indexes(
        graph.catalog.clone(),
        graph.indexed_context.clone(),
        graph.indexes.clone(),
        IndexUsagePolicy::Require,
    ))
    .unwrap()
    .num_rows()
}

fn run_persisted_query(
    rt: &tokio::runtime::Runtime,
    graph: &StarGraph,
    query: &CypherQuery,
) -> usize {
    rt.block_on(query.execute_with_catalog_context_and_indexes(
        graph.catalog.clone(),
        graph.persisted_context.clone(),
        graph.persisted_indexes.clone(),
        IndexUsagePolicy::Require,
    ))
    .unwrap()
    .num_rows()
}

fn bench_star_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_execution_star_join_vs_indexed");
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Keep the selected hub's output fixed while growing the number of other
    // sources and therefore the relationship table scanned by the Join path.
    for &(source_count, degree) in &[
        (1_000usize, 10usize),
        (10_000usize, 10usize),
        (100_000usize, 10usize),
    ] {
        let graph = setup_graph(&rt, source_count, degree);
        let edge_count = source_count * degree;
        group.throughput(Throughput::Elements(degree as u64));

        assert_eq!(
            run_join_query(&rt, &graph, &graph.query_hub),
            graph.expected_rows
        );
        assert_eq!(
            run_indexed_query(&rt, &graph, &graph.query_hub),
            graph.expected_rows
        );
        assert_eq!(
            run_persisted_query(&rt, &graph, &graph.query_hub),
            graph.expected_rows
        );
        group.bench_with_input(
            BenchmarkId::new(
                "join",
                format!("sources_{source_count}_degree_{degree}_edges_{edge_count}"),
            ),
            &(source_count, degree),
            |b, _| b.iter(|| black_box(run_join_query(&rt, &graph, &graph.query_hub))),
        );
        group.bench_with_input(
            BenchmarkId::new(
                "indexed",
                format!("sources_{source_count}_degree_{degree}_edges_{edge_count}"),
            ),
            &(source_count, degree),
            |b, _| b.iter(|| black_box(run_indexed_query(&rt, &graph, &graph.query_hub))),
        );
        group.bench_with_input(
            BenchmarkId::new(
                "persisted_loaded",
                format!("sources_{source_count}_degree_{degree}_edges_{edge_count}"),
            ),
            &(source_count, degree),
            |b, _| b.iter(|| black_box(run_persisted_query(&rt, &graph, &graph.query_hub))),
        );
    }
    group.finish();
}

criterion_group!(benches, bench_star_execution);
criterion_main!(benches);
