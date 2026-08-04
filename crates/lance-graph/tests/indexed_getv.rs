// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::execution::context::SessionContext;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use lance::datafusion::LanceTableProvider;
use lance::dataset::{Dataset, WriteParams};
use lance_graph::datafusion_planner::get_v::LanceGetVByIdExec;
use lance_graph::{
    CsrIndexBuilder, CsrIndexHandle, CypherQuery, GraphConfig, GraphIndexKey, GraphIndexMetadata,
    InMemoryCatalog, InMemoryGraphIndexRegistry, IndexDirection, IndexUsagePolicy,
};
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};

struct TestGraph {
    query: CypherQuery,
    catalog: Arc<InMemoryCatalog>,
    context: SessionContext,
    indexes: Arc<InMemoryGraphIndexRegistry>,
}

fn node_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["p0", "p1", "p2", "p3", "p4"])),
            Arc::new(Int64Array::from(vec![20, 20, 30, 20, 40])),
        ],
    )
    .unwrap()
}

fn edge_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0, 0, 0, 0, 0, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 9, 0, 2, 1, 1])),
        ],
    )
    .unwrap()
}

fn graph_config() -> GraphConfig {
    GraphConfig::builder()
        .with_node_label("Person", "person_id")
        .with_relationship("FRIEND_OF", "src_id", "dst_id")
        .build()
        .unwrap()
}

fn graph_indexes() -> Arc<InMemoryGraphIndexRegistry> {
    let index = CsrIndexBuilder::new()
        .with_num_vertices(10)
        .add_edge(0, 1)
        .add_edge(0, 1)
        .add_edge(0, 2)
        .add_edge(0, 9)
        .add_edge(0, 0)
        .add_edge(2, 2)
        .add_edge(3, 1)
        .add_edge(4, 1)
        .try_build()
        .unwrap();
    let indexes = Arc::new(InMemoryGraphIndexRegistry::new());
    indexes
        .register_csr(CsrIndexHandle {
            index: Arc::new(index),
            metadata: GraphIndexMetadata {
                key: GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
                source_id_field: "person_id".into(),
                target_id_field: "person_id".into(),
                id_data_type: DataType::Int64,
                num_vertices: 10,
                num_edges: 8,
                source_uri: None,
                source_version: None,
                generation: 1,
            },
        })
        .unwrap();
    indexes
}

fn mem_graph() -> TestGraph {
    let nodes = node_batch();
    let edges = edge_batch();
    let node_provider = Arc::new(MemTable::try_new(nodes.schema(), vec![vec![nodes]]).unwrap());
    let edge_provider = Arc::new(MemTable::try_new(edges.schema(), vec![vec![edges]]).unwrap());
    let context = SessionContext::new();
    context
        .register_table("person", node_provider.clone())
        .unwrap();
    context
        .register_table("friend_of", edge_provider.clone())
        .unwrap();
    let catalog = Arc::new(
        InMemoryCatalog::new()
            .with_node_source("Person", Arc::new(DefaultTableSource::new(node_provider)))
            .with_relationship_source(
                "FRIEND_OF",
                Arc::new(DefaultTableSource::new(edge_provider)),
            ),
    );
    let query = CypherQuery::new(
        "MATCH (a:Person)-[:FRIEND_OF]->(b:Person {age: 20}) \
         RETURN a.person_id, a.name, b.person_id, b.name",
    )
    .unwrap()
    .with_config(graph_config());
    TestGraph {
        query,
        catalog,
        context,
        indexes: graph_indexes(),
    }
}

async fn lance_graph(with_scalar_index: bool) -> (tempfile::TempDir, TestGraph) {
    let temp = tempfile::tempdir().unwrap();
    let uri = temp.path().join("person.lance");
    let nodes = node_batch();
    let reader = RecordBatchIterator::new(vec![Ok(nodes.clone())], nodes.schema());
    let mut dataset = Dataset::write(reader, uri.to_str().unwrap(), Some(WriteParams::default()))
        .await
        .unwrap();
    if with_scalar_index {
        dataset
            .create_index(
                &["person_id"],
                IndexType::BTree,
                Some("person_id_btree".into()),
                &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
                false,
            )
            .await
            .unwrap();
    }
    let dataset = Arc::new(Dataset::open(uri.to_str().unwrap()).await.unwrap());
    let node_provider = Arc::new(LanceTableProvider::new(dataset, false, false));
    let edges = edge_batch();
    let edge_provider = Arc::new(MemTable::try_new(edges.schema(), vec![vec![edges]]).unwrap());
    let context = SessionContext::new();
    context
        .register_table("person", node_provider.clone())
        .unwrap();
    context
        .register_table("friend_of", edge_provider.clone())
        .unwrap();
    let catalog = Arc::new(
        InMemoryCatalog::new()
            .with_node_source("Person", Arc::new(DefaultTableSource::new(node_provider)))
            .with_relationship_source(
                "FRIEND_OF",
                Arc::new(DefaultTableSource::new(edge_provider)),
            ),
    );
    let query = CypherQuery::new(
        "MATCH (a:Person)-[:FRIEND_OF]->(b:Person {age: 20}) \
         RETURN a.person_id, a.name, b.person_id, b.name",
    )
    .unwrap()
    .with_config(graph_config());
    (
        temp,
        TestGraph {
            query,
            catalog,
            context,
            indexes: graph_indexes(),
        },
    )
}

async fn indexed_dataset(batch: RecordBatch, id_field: &str) -> (tempfile::TempDir, Arc<Dataset>) {
    let temp = tempfile::tempdir().unwrap();
    let uri = temp.path().join("target.lance");
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    let mut dataset = Dataset::write(reader, uri.to_str().unwrap(), Some(WriteParams::default()))
        .await
        .unwrap();
    dataset
        .create_index(
            &[id_field],
            IndexType::BTree,
            Some(format!("{id_field}_btree")),
            &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
            false,
        )
        .await
        .unwrap();
    let dataset = Arc::new(Dataset::open(uri.to_str().unwrap()).await.unwrap());
    (temp, dataset)
}

fn memory_exec(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
    TestMemoryExec::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap()
}

fn get_v_exec(
    input: RecordBatch,
    dataset: Arc<Dataset>,
    input_id_column: &str,
    target_schema: Arc<Schema>,
    max_lookup_keys: usize,
) -> Arc<LanceGetVByIdExec> {
    Arc::new(
        LanceGetVByIdExec::try_new(
            memory_exec(input),
            dataset.clone(),
            input_id_column,
            "person_id",
            "b",
            target_schema,
            vec![],
            "person_id_btree",
            dataset.version().version,
            max_lookup_keys,
        )
        .unwrap(),
    )
}

fn rows_as_multiset(batch: &RecordBatch) -> BTreeMap<(i64, String, i64, String), usize> {
    let source_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let source_names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let target_ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let target_names = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut rows = BTreeMap::new();
    for row in 0..batch.num_rows() {
        *rows
            .entry((
                source_ids.value(row),
                source_names.value(row).to_string(),
                target_ids.value(row),
                target_names.value(row).to_string(),
            ))
            .or_insert(0) += 1;
    }
    rows
}

#[tokio::test]
async fn indexed_expand_getv_matches_join_and_removes_target_hash_join() {
    let (_temp, graph) = lance_graph(true).await;
    let baseline = graph
        .query
        .execute_with_catalog_and_context(graph.catalog.clone(), graph.context.clone())
        .await
        .unwrap();
    let explain = graph
        .query
        .explain_with_catalog_context_and_indexes(
            graph.catalog.clone(),
            graph.context.clone(),
            graph.indexes.clone(),
            IndexUsagePolicy::Require,
        )
        .await
        .unwrap();
    assert!(
        explain.contains("GetV:"),
        "missing logical GetV:\n{explain}"
    );
    assert!(
        explain.contains("LanceGetVByIdExec"),
        "missing physical GetV:\n{explain}"
    );
    assert!(
        explain.contains("IndexedExpandExec"),
        "missing physical IndexedExpand:\n{explain}"
    );
    assert!(
        !explain.contains("HashJoinExec"),
        "GetV path retained endpoint-target HashJoinExec:\n{explain}"
    );
    assert!(
        !explain.to_lowercase().contains("tablescan: friend_of"),
        "CSR path scanned the relationship table:\n{explain}"
    );

    let getv = graph
        .query
        .execute_with_catalog_context_and_indexes(
            graph.catalog,
            graph.context,
            graph.indexes,
            IndexUsagePolicy::Require,
        )
        .await
        .unwrap();
    assert_eq!(rows_as_multiset(&getv), rows_as_multiset(&baseline));
    let rows = rows_as_multiset(&getv);
    assert_eq!(rows.values().sum::<usize>(), 5);
    assert_eq!(rows.get(&(0, "p0".into(), 1, "p1".into())), Some(&2));
    assert_eq!(rows.get(&(0, "p0".into(), 0, "p0".into())), Some(&1));
    assert_eq!(rows.get(&(3, "p3".into(), 1, "p1".into())), Some(&1));
    assert_eq!(rows.get(&(4, "p4".into(), 1, "p1".into())), Some(&1));
}

#[tokio::test]
async fn lance_target_without_scalar_index_falls_back_to_target_join() {
    let (_temp, graph) = lance_graph(false).await;
    let explain = graph
        .query
        .explain_with_catalog_context_and_indexes(
            graph.catalog,
            graph.context,
            graph.indexes,
            IndexUsagePolicy::Require,
        )
        .await
        .unwrap();
    assert!(
        explain.contains("IndexedExpandExec"),
        "missing CSR path:\n{explain}"
    );
    assert!(!explain.contains("GetV:"), "unexpected GetV:\n{explain}");
    assert!(
        explain.contains("HashJoinExec"),
        "missing target join fallback:\n{explain}"
    );
}

#[tokio::test]
async fn memtable_and_missing_csr_keep_the_existing_join_fallbacks() {
    let graph = mem_graph();
    let explain = graph
        .query
        .explain_with_catalog_context_and_indexes(
            graph.catalog.clone(),
            graph.context.clone(),
            graph.indexes,
            IndexUsagePolicy::Require,
        )
        .await
        .unwrap();
    assert!(
        explain.contains("IndexedExpandExec"),
        "missing CSR path:\n{explain}"
    );
    assert!(!explain.contains("GetV:"), "MemTable used GetV:\n{explain}");
    assert!(
        explain.contains("HashJoinExec"),
        "MemTable target did not use target join:\n{explain}"
    );

    let graph = mem_graph();
    let empty_indexes = Arc::new(InMemoryGraphIndexRegistry::new());
    let explain = graph
        .query
        .explain_with_catalog_context_and_indexes(
            graph.catalog.clone(),
            graph.context.clone(),
            empty_indexes.clone(),
            IndexUsagePolicy::Prefer,
        )
        .await
        .unwrap();
    assert!(
        !explain.contains("IndexedExpandExec"),
        "missing CSR unexpectedly used IndexedExpand:\n{explain}"
    );
    assert!(
        explain.matches("HashJoinExec").count() >= 2,
        "missing CSR did not restore relationship and target joins:\n{explain}"
    );
    let error = graph
        .query
        .explain_with_catalog_context_and_indexes(
            graph.catalog,
            graph.context,
            empty_indexes,
            IndexUsagePolicy::Require,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("CSR index required"), "{error}");
}

#[tokio::test]
async fn getv_chunks_unique_keys_and_replays_duplicate_input_rows() {
    let target = node_batch();
    let target_schema = target.schema();
    let (_temp, dataset) = indexed_dataset(target, "person_id").await;
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("source_context", DataType::Int64, false),
        Field::new("dst_id", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12, 13])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 99])),
        ],
    )
    .unwrap();
    let plan = get_v_exec(input, dataset, "dst_id", target_schema, 1);
    let batches = collect(plan.clone(), Arc::new(TaskContext::default()))
        .await
        .unwrap();
    let output = arrow::compute::concat_batches(&plan.schema(), &batches).unwrap();
    assert_eq!(output.num_rows(), 3);
    let contexts = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let target_ids = output
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(contexts.values(), &[10, 11, 12]);
    assert_eq!(target_ids.values(), &[1, 1, 2]);
    let metrics = plan.metrics().unwrap();
    assert_eq!(metrics.sum_by_name("lookup_batches").unwrap().as_usize(), 3);
    assert_eq!(
        metrics
            .sum_by_name("duplicate_lookup_keys")
            .unwrap()
            .as_usize(),
        1
    );
    assert_eq!(
        metrics
            .sum_by_name("target_ids_not_found")
            .unwrap()
            .as_usize(),
        1
    );
}

#[tokio::test]
async fn getv_rejects_duplicate_target_ids_and_handles_empty_input() {
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let target = RecordBatch::try_new(
        target_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec!["first", "duplicate"])),
        ],
    )
    .unwrap();
    let (_temp, dataset) = indexed_dataset(target, "person_id").await;
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "dst_id",
        DataType::Int64,
        false,
    )]));
    let input = RecordBatch::try_new(
        input_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let plan = get_v_exec(input, dataset.clone(), "dst_id", target_schema.clone(), 8);
    let error = collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not unique"), "{error}");

    let empty = RecordBatch::new_empty(input_schema);
    let empty_plan = get_v_exec(empty, dataset, "dst_id", target_schema, 8);
    let batches = collect(empty_plan.clone(), Arc::new(TaskContext::default()))
        .await
        .unwrap();
    assert!(batches.is_empty());
    assert_eq!(empty_plan.schema().fields().len(), 3);
}

#[tokio::test]
async fn getv_supports_all_csr_integer_id_types() {
    let cases: Vec<(&str, ArrayRef, ArrayRef)> = vec![
        (
            "int32",
            Arc::new(Int32Array::from(vec![-1, 1, 2])),
            Arc::new(Int32Array::from(vec![-1, 2])),
        ),
        (
            "int64",
            Arc::new(Int64Array::from(vec![-1, 1, 2])),
            Arc::new(Int64Array::from(vec![-1, 2])),
        ),
        (
            "uint32",
            Arc::new(UInt32Array::from(vec![0, 1, 2])),
            Arc::new(UInt32Array::from(vec![0, 2])),
        ),
        (
            "uint64",
            Arc::new(UInt64Array::from(vec![0, 1, 2])),
            Arc::new(UInt64Array::from(vec![0, 2])),
        ),
    ];
    for (name, target_ids, input_ids) in cases {
        let data_type = target_ids.data_type().clone();
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "person_id",
            data_type.clone(),
            false,
        )]));
        let target = RecordBatch::try_new(target_schema.clone(), vec![target_ids]).unwrap();
        let (_temp, dataset) = indexed_dataset(target, "person_id").await;
        let input_schema = Arc::new(Schema::new(vec![Field::new("dst_id", data_type, false)]));
        let input = RecordBatch::try_new(input_schema, vec![input_ids]).unwrap();
        let plan = get_v_exec(input, dataset, "dst_id", target_schema, 8);
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap_or_else(|error| panic!("{name} GetV failed: {error}"));
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2,
            "{name}"
        );
    }
}
