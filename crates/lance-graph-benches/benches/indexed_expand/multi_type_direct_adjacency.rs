// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Multi-type Direct Adjacency bundle load and exact-component lookup benchmarks.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::{DefaultTableSource, MemTable, TableProvider};
use datafusion::execution::context::SessionContext;
use lance::datafusion::LanceTableProvider;
use lance::dataset::{Dataset, WriteParams};
use lance_graph::{
    CsrIndexBuilder, CsrIndexHandle, CypherQuery, DirectAdjacencyIndexBuilder,
    DirectAdjacencyMetadata, ExpandExecutionMode, GraphConfig, GraphIndexKey, GraphIndexMetadata,
    GraphIndexRegistry, InMemoryCatalog, InMemoryGraphIndexRegistry, IndexDirection,
    MultiTypeDirectAdjacencyIndexBuilder, MultiTypeDirectAdjacencyIndexStore,
};
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};

const SOURCE_COUNT: usize = 1_000_000;
const EDGE_COUNT: usize = 10_000_000;
const TYPE_DISTRIBUTION: [(&str, usize); 3] = [
    ("FRIEND_OF", 7_000_000),
    ("FOLLOWS", 2_000_000),
    ("BLOCKS", 1_000_000),
];

fn edges(edge_count: usize, type_offset: usize) -> RecordBatch {
    let mut src = Vec::with_capacity(edge_count);
    let mut dst = Vec::with_capacity(edge_count);
    for edge in 0..edge_count {
        let source = edge % SOURCE_COUNT;
        let hop = edge / SOURCE_COUNT + 1 + type_offset;
        src.push(source as i64);
        dst.push(((source + hop) % SOURCE_COUNT) as i64);
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

fn metadata(relationship_type: &str, generation: u64) -> DirectAdjacencyMetadata {
    DirectAdjacencyMetadata {
        key: GraphIndexKey::new(
            relationship_type,
            "Person",
            "Person",
            IndexDirection::Outgoing,
        ),
        source_id_field: "src_id".into(),
        target_id_field: "person_id".into(),
        adjacency_field: "dst_ids".into(),
        id_data_type: DataType::Int64,
        num_sources: 0,
        num_edges: 0,
        dataset_uri: String::new(),
        dataset_version: 0,
        scalar_index_name: format!("{}_src_btree", relationship_type.to_lowercase()),
        source_uri: Some(format!("benchmark://{relationship_type}")),
        source_version: Some(1),
        generation,
    }
}

fn sorted_person_ids(batch: &RecordBatch) -> Vec<i64> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values = ids.values().to_vec();
    values.sort_unstable();
    values
}

fn build_bundle(
    rt: &tokio::runtime::Runtime,
    root: &std::path::Path,
    component_count: usize,
    full_three_type_workload: bool,
) -> (
    lance_graph::PersistedMultiTypeDirectAdjacencyDescriptor,
    Vec<(String, RecordBatch)>,
) {
    let mut builder = MultiTypeDirectAdjacencyIndexBuilder::new("social_adjacency", 1).unwrap();
    let mut edge_batches = Vec::with_capacity(component_count);
    for component in 0..component_count {
        let (relationship_type, edge_count): (&str, usize) = if component < TYPE_DISTRIBUTION.len()
        {
            let (relationship_type, full_edge_count) = TYPE_DISTRIBUTION[component];
            (
                relationship_type,
                if full_three_type_workload {
                    full_edge_count
                } else {
                    1
                },
            )
        } else {
            (
                Box::leak(format!("TYPE_{component}").into_boxed_str()) as &str,
                1,
            )
        };
        let batch = edges(edge_count, component);
        let uri = root.join(format!("component-{component}"));
        let descriptor = rt
            .block_on(
                DirectAdjacencyIndexBuilder::new(metadata(relationship_type, 1))
                    .unwrap()
                    .add_edges_from_batch(&batch)
                    .unwrap()
                    .build_and_persist(uri.to_str().unwrap(), Default::default()),
            )
            .unwrap();
        builder = builder.add_component(descriptor).unwrap();
        edge_batches.push((relationship_type.to_string(), batch));
    }
    let bundle_uri = root.join("bundle-generation-1");
    (
        rt.block_on(builder.build_and_persist(bundle_uri.to_str().unwrap()))
            .unwrap(),
        edge_batches,
    )
}

fn setup_query_resources(
    rt: &tokio::runtime::Runtime,
    root: &std::path::Path,
    descriptor: &lance_graph::PersistedMultiTypeDirectAdjacencyDescriptor,
    edge_batches: &[(String, RecordBatch)],
) -> (
    Arc<InMemoryGraphIndexRegistry>,
    Arc<InMemoryCatalog>,
    SessionContext,
    SessionContext,
    GraphConfig,
) {
    let node_uri = root.join("person.lance");
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "person_id",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..SOURCE_COUNT).map(|value| value as i64),
        ))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(nodes.clone())], nodes.schema());
    let mut dataset = rt
        .block_on(Dataset::write(
            reader,
            node_uri.to_str().unwrap(),
            Some(WriteParams::default()),
        ))
        .unwrap();
    rt.block_on(dataset.create_index(
        &["person_id"],
        IndexType::BTree,
        Some("person_id_btree".into()),
        &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
        false,
    ))
    .unwrap();
    let dataset = Arc::new(
        rt.block_on(Dataset::open(node_uri.to_str().unwrap()))
            .unwrap(),
    );
    let node_table = Arc::new(LanceTableProvider::new(dataset, false, false));
    let indexed_context = SessionContext::new();
    indexed_context
        .register_table("person", node_table.clone())
        .unwrap();
    let join_context = SessionContext::new();
    join_context
        .register_table("person", node_table.clone())
        .unwrap();
    let mut catalog = InMemoryCatalog::new()
        .with_node_source("Person", Arc::new(DefaultTableSource::new(node_table)));
    let indexes = Arc::new(InMemoryGraphIndexRegistry::new());
    indexes
        .register_direct_adjacency_bundle(
            rt.block_on(MultiTypeDirectAdjacencyIndexStore::load(
                descriptor,
                Default::default(),
            ))
            .unwrap(),
        )
        .unwrap();
    for (relationship_type, batch) in edge_batches {
        let edge_table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap());
        indexed_context
            .register_table(&relationship_type.to_lowercase(), edge_table.clone())
            .unwrap();
        join_context
            .register_table(&relationship_type.to_lowercase(), edge_table.clone())
            .unwrap();
        catalog = catalog.with_relationship_source(
            relationship_type,
            Arc::new(DefaultTableSource::new(edge_table)),
        );
        let index = CsrIndexBuilder::new()
            .with_num_vertices(SOURCE_COUNT as u64)
            .add_edges_from_batch(batch)
            .unwrap()
            .try_build()
            .unwrap();
        indexes
            .register_csr(CsrIndexHandle {
                index: Arc::new(index),
                metadata: GraphIndexMetadata {
                    key: GraphIndexKey::new(
                        relationship_type,
                        "Person",
                        "Person",
                        IndexDirection::Outgoing,
                    ),
                    source_id_field: "person_id".into(),
                    target_id_field: "person_id".into(),
                    id_data_type: DataType::Int64,
                    num_vertices: SOURCE_COUNT as u64,
                    num_edges: batch.num_rows() as u64,
                    source_uri: None,
                    source_version: None,
                    generation: 1,
                },
            })
            .unwrap();
    }
    let config = GraphConfig::builder()
        .with_node_label("Person", "person_id")
        .with_relationship("FRIEND_OF", "src_id", "dst_id")
        .with_relationship("FOLLOWS", "src_id", "dst_id")
        .with_relationship("BLOCKS", "src_id", "dst_id")
        .build()
        .unwrap();
    (
        indexes,
        Arc::new(catalog),
        indexed_context,
        join_context,
        config,
    )
}

fn bench_multi_type_direct_adjacency(c: &mut Criterion) {
    assert_eq!(
        TYPE_DISTRIBUTION
            .iter()
            .map(|(_, edges)| edges)
            .sum::<usize>(),
        EDGE_COUNT
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_type_direct_adjacency");

    // The three-component workload contains exactly 1M sources and 10M edges
    // distributed as FRIEND_OF/FOLLOWS/BLOCKS = 7M/2M/1M.
    let query_directory = tempfile::tempdir().unwrap();
    let (query_descriptor, query_edge_batches) = build_bundle(&rt, query_directory.path(), 3, true);
    let query_handle = rt
        .block_on(MultiTypeDirectAdjacencyIndexStore::load(
            &query_descriptor,
            Default::default(),
        ))
        .unwrap();
    let registry = InMemoryGraphIndexRegistry::new();
    registry
        .register_direct_adjacency_bundle(query_handle)
        .unwrap();
    group.throughput(Throughput::Elements(1));
    for relationship_type in ["FRIEND_OF", "FOLLOWS", "BLOCKS"] {
        let key = GraphIndexKey::new(
            relationship_type,
            "Person",
            "Person",
            IndexDirection::Outgoing,
        );
        group.bench_with_input(
            BenchmarkId::new("exact_component_registry_lookup", relationship_type),
            &key,
            |b, key| {
                b.iter(|| {
                    black_box(
                        registry
                            .get_direct_adjacency("social_adjacency", key)
                            .unwrap()
                            .unwrap(),
                    )
                })
            },
        );
    }

    let (query_indexes, catalog, indexed_context, join_context, config) = setup_query_resources(
        &rt,
        query_directory.path(),
        &query_descriptor,
        &query_edge_batches,
    );
    for (relationship_type, expected_rows) in [("FRIEND_OF", 7usize), ("FOLLOWS", 2), ("BLOCKS", 1)]
    {
        let query = CypherQuery::new(&format!(
            "MATCH (a:Person {{person_id: 42}})-[:{relationship_type}]->(b:Person) RETURN b.person_id"
        ))
        .unwrap()
        .with_config(config.clone());
        let mode = ExpandExecutionMode::direct_adjacency("social_adjacency").unwrap();
        let result = rt
            .block_on(query.execute_with_catalog_context_and_indexes(
                catalog.clone(),
                indexed_context.clone(),
                query_indexes.clone(),
                mode.clone(),
            ))
            .unwrap();
        assert_eq!(result.num_rows(), expected_rows);
        let explain = rt
            .block_on(query.explain_with_catalog_context_and_indexes(
                catalog.clone(),
                indexed_context.clone(),
                query_indexes.clone(),
                mode.clone(),
            ))
            .unwrap();
        assert!(
            explain.contains(&format!(
                "relationship_type={}",
                relationship_type.to_lowercase()
            )),
            "{explain}"
        );
        let join_result = rt
            .block_on(query.execute_with_catalog_and_context(catalog.clone(), join_context.clone()))
            .unwrap();
        let csr_result = rt
            .block_on(query.execute_with_catalog_context_and_indexes(
                catalog.clone(),
                indexed_context.clone(),
                query_indexes.clone(),
                ExpandExecutionMode::Csr,
            ))
            .unwrap();
        assert_eq!(join_result.num_rows(), expected_rows);
        assert_eq!(csr_result.num_rows(), expected_rows);
        assert_eq!(sorted_person_ids(&result), sorted_person_ids(&join_result));
        assert_eq!(
            sorted_person_ids(&csr_result),
            sorted_person_ids(&join_result)
        );
        group.bench_with_input(
            BenchmarkId::new("join_query", relationship_type),
            &query,
            |b, query| {
                b.iter(|| {
                    black_box(
                        rt.block_on(query.execute_with_catalog_and_context(
                            catalog.clone(),
                            join_context.clone(),
                        ))
                        .unwrap()
                        .num_rows(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("csr_query", relationship_type),
            &query,
            |b, query| {
                b.iter(|| {
                    black_box(
                        rt.block_on(query.execute_with_catalog_context_and_indexes(
                            catalog.clone(),
                            indexed_context.clone(),
                            query_indexes.clone(),
                            ExpandExecutionMode::Csr,
                        ))
                        .unwrap()
                        .num_rows(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("exact_type_query", relationship_type),
            &query,
            |b, query| {
                b.iter(|| {
                    black_box(
                        rt.block_on(query.execute_with_catalog_context_and_indexes(
                            catalog.clone(),
                            indexed_context.clone(),
                            query_indexes.clone(),
                            mode.clone(),
                        ))
                        .unwrap()
                        .num_rows(),
                    )
                })
            },
        );
    }

    // Measure bundle eager-open cost as component count grows. Components past
    // the first three contain one edge to isolate descriptor/Dataset/index open
    // overhead without multiplying the 10M-edge build cost.
    for component_count in [1usize, 3, 10, 50] {
        let directory = tempfile::tempdir().unwrap();
        let (descriptor, _edge_batches) =
            build_bundle(&rt, directory.path(), component_count, false);
        group.bench_with_input(
            BenchmarkId::new("load_warm", format!("components_{component_count}")),
            &component_count,
            |b, _| {
                b.iter(|| {
                    black_box(
                        rt.block_on(MultiTypeDirectAdjacencyIndexStore::load(
                            &descriptor,
                            Default::default(),
                        ))
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_multi_type_direct_adjacency);
criterion_main!(benches);
