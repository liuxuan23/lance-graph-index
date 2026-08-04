// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Multi-star end-to-end benchmark for the Join and fully indexed paths.
//!
//! The graph always contains one million sources and ten million edges. The
//! selected hub's degree varies while the remaining edges are distributed over
//! the other sources, keeping total relationship-table size fixed. The query
//! compares scanning/Joining the Lance-backed relationship and target tables
//! with CSR adjacency lookup plus scalar-indexed target-node GetV. The indexed
//! case writes and reloads its CSR during setup, outside query timing.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::context::SessionContext;
use lance::datafusion::LanceTableProvider;
use lance::dataset::{Dataset, WriteParams};
use lance_graph::{
    CsrIndexBuilder, CsrIndexHandle, CsrIndexStore, CypherQuery, GraphConfig, GraphIndexKey,
    GraphIndexMetadata, InMemoryCatalog, InMemoryGraphIndexRegistry, IndexDirection,
    IndexUsagePolicy,
};
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};

const QUERY_SOURCE_ID: usize = 42;
const SOURCE_COUNT: usize = 1_000_000;
const EDGE_COUNT: usize = 10_000_000;

struct NodeFixture {
    table: Arc<dyn TableProvider>,
    _data_dir: tempfile::TempDir,
}

struct StarGraph {
    query_hub: CypherQuery,
    catalog: Arc<dyn lance_graph::GraphSourceCatalog>,
    join_context: SessionContext,
    indexed_context: SessionContext,
    indexes: Arc<InMemoryGraphIndexRegistry>,
    _data_dir: tempfile::TempDir,
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

fn make_star_edges(source_count: usize, edge_count: usize, query_degree: usize) -> RecordBatch {
    assert!(query_degree < source_count);
    assert!(query_degree <= edge_count);
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    let mut src = Vec::with_capacity(edge_count);
    let mut dst = Vec::with_capacity(edge_count);

    for offset in 1..=query_degree {
        src.push(QUERY_SOURCE_ID as i64);
        dst.push(((QUERY_SOURCE_ID + offset) % source_count) as i64);
    }

    'offsets: for offset in 1usize.. {
        for source in 0..source_count {
            if source == QUERY_SOURCE_ID {
                continue;
            }
            if src.len() == edge_count {
                break 'offsets;
            }
            src.push(source as i64);
            dst.push(((source + offset) % source_count) as i64);
        }
    }
    assert_eq!(src.len(), edge_count);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(src)),
            Arc::new(Int64Array::from(dst)),
        ],
    )
    .unwrap()
}

fn setup_nodes(rt: &tokio::runtime::Runtime, source_count: usize) -> NodeFixture {
    let nodes = make_nodes(source_count);
    let data_dir = tempfile::tempdir().unwrap();
    let node_uri = data_dir.path().join("Person.lance");

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
    NodeFixture {
        table: Arc::new(LanceTableProvider::new(node_dataset, false, false)),
        _data_dir: data_dir,
    }
}

fn setup_graph(
    rt: &tokio::runtime::Runtime,
    node_table: Arc<dyn TableProvider>,
    source_count: usize,
    edge_count: usize,
    degree: usize,
) -> StarGraph {
    assert!(source_count > QUERY_SOURCE_ID, "selected hub must exist");
    assert!(
        degree < source_count,
        "degree must be smaller than source count"
    );

    let edges = make_star_edges(source_count, edge_count, degree);
    let data_dir = tempfile::tempdir().unwrap();
    let edge_uri = data_dir.path().join("FRIEND_OF.lance");

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

    // Keep the contexts separate: the indexed API installs a custom planner,
    // which must not affect the Join baseline.
    let indexed_context = SessionContext::new();
    indexed_context
        .register_table("person", node_table.clone())
        .unwrap();
    indexed_context
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
    assert_eq!(index.neighbors(QUERY_SOURCE_ID as u64).len(), degree);
    let handle = CsrIndexHandle {
        index: Arc::new(index),
        metadata: GraphIndexMetadata {
            key: GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
            source_id_field: "person_id".into(),
            target_id_field: "person_id".into(),
            id_data_type: DataType::Int64,
            num_vertices: source_count as u64,
            num_edges: edges.num_rows() as u64,
            source_uri: Some(edge_source_uri),
            source_version: Some(edge_source_version),
            generation: 1,
        },
    };
    // Persist and reload before measurement. Query execution sees only the
    // reconstructed in-memory CSR, while persistence and loading stay outside
    // the timed iterations.
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
            IndexUsagePolicy::Require,
        ))
        .unwrap());

    let config = GraphConfig::builder()
        .with_node_label("Person", "person_id")
        .with_relationship("FRIEND_OF", "src_id", "dst_id")
        .build()
        .unwrap();
    let query_hub = CypherQuery::new(&format!(
        "MATCH (a:Person {{person_id: {QUERY_SOURCE_ID}}})-[:FRIEND_OF]->(b:Person) RETURN b.name"
    ))
    .unwrap()
    .with_config(config);

    StarGraph {
        query_hub,
        catalog,
        join_context,
        indexed_context,
        indexes,
        _data_dir: data_dir,
        expected_rows: degree,
    }
}

fn run_join_query(
    rt: &tokio::runtime::Runtime,
    graph: &StarGraph,
    query: &CypherQuery,
) -> RecordBatch {
    rt.block_on(
        query.execute_with_catalog_and_context(graph.catalog.clone(), graph.join_context.clone()),
    )
    .unwrap()
}

fn run_indexed_get_v_query(
    rt: &tokio::runtime::Runtime,
    graph: &StarGraph,
    query: &CypherQuery,
) -> RecordBatch {
    rt.block_on(query.execute_with_catalog_context_and_indexes(
        graph.catalog.clone(),
        graph.indexed_context.clone(),
        graph.indexes.clone(),
        IndexUsagePolicy::Require,
    ))
    .unwrap()
}

fn sorted_names(batch: &RecordBatch) -> Vec<String> {
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut values = (0..names.len())
        .map(|row| names.value(row).to_string())
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn assert_indexed_get_v_plan(plan: &str, case_name: &str) {
    assert!(
        plan.contains("IndexedExpandExec"),
        "{case_name} did not use IndexedExpandExec:\n{plan}"
    );
    assert!(
        plan.contains("LanceGetVByIdExec"),
        "{case_name} did not use LanceGetVByIdExec:\n{plan}"
    );
    assert!(
        !plan.contains("HashJoinExec"),
        "{case_name} unexpectedly retained a HashJoinExec:\n{plan}"
    );
    assert!(
        !plan.to_lowercase().contains("tablescan: friend_of"),
        "{case_name} unexpectedly scanned the relationship table:\n{plan}"
    );
}

fn bench_star_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_execution_star_join_vs_indexed");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let nodes = setup_nodes(&rt, SOURCE_COUNT);
    for degree in [10usize, 100, 1_000, 10_000] {
        let graph = setup_graph(&rt, nodes.table.clone(), SOURCE_COUNT, EDGE_COUNT, degree);
        group.throughput(Throughput::Elements(degree as u64));

        // Verify both result semantics and physical plan shape before timing.
        // Comparing sorted rows preserves duplicate multiplicity while ignoring
        // non-semantic output ordering differences between the paths.
        let join_result = run_join_query(&rt, &graph, &graph.query_hub);
        let indexed_result = run_indexed_get_v_query(&rt, &graph, &graph.query_hub);
        assert_eq!(join_result.num_rows(), graph.expected_rows);
        assert_eq!(sorted_names(&indexed_result), sorted_names(&join_result));

        let indexed_plan = rt
            .block_on(graph.query_hub.explain_with_catalog_context_and_indexes(
                graph.catalog.clone(),
                graph.indexed_context.clone(),
                graph.indexes.clone(),
                IndexUsagePolicy::Require,
            ))
            .unwrap();
        assert_indexed_get_v_plan(&indexed_plan, "indexed_get_v");

        group.bench_with_input(
            BenchmarkId::new(
                "join",
                format!("sources_{SOURCE_COUNT}_degree_{degree}_edges_{EDGE_COUNT}"),
            ),
            &degree,
            |b, _| b.iter(|| black_box(run_join_query(&rt, &graph, &graph.query_hub).num_rows())),
        );
        group.bench_with_input(
            BenchmarkId::new(
                "indexed_get_v",
                format!("sources_{SOURCE_COUNT}_degree_{degree}_edges_{EDGE_COUNT}"),
            ),
            &degree,
            |b, _| {
                b.iter(|| {
                    black_box(run_indexed_get_v_query(&rt, &graph, &graph.query_hub).num_rows())
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_star_execution);
criterion_main!(benches);
