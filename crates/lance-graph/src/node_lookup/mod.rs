// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance node semantic-ID lookup resources used by GetV execution.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow_schema::DataType;
use datafusion::datasource::source_as_provider;
use lance::datafusion::LanceTableProvider;
use lance::index::DatasetIndexInternalExt;
use lance::Dataset;
use lance_graph_catalog::GraphSourceCatalog;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::ScalarIndex;
use lance_index::DatasetIndexExt;

use crate::config::GraphConfig;
use crate::error::{GraphError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeLookupKey {
    pub target_label: String,
    pub target_id_field: String,
}

impl NodeLookupKey {
    pub fn new(target_label: impl Into<String>, target_id_field: impl Into<String>) -> Self {
        Self {
            target_label: target_label.into().to_lowercase(),
            target_id_field: target_id_field.into().to_lowercase(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLookupMetadata {
    pub key: NodeLookupKey,
    pub id_data_type: DataType,
    pub scalar_index_name: String,
    pub dataset_uri: String,
    pub dataset_version: u64,
    pub index_dataset_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeLookupReference {
    pub key: NodeLookupKey,
    pub scalar_index_name: String,
    pub dataset_version: u64,
}

#[derive(Debug)]
pub struct LanceNodeLookupHandle {
    pub dataset: Arc<Dataset>,
    pub scalar_index: Arc<dyn ScalarIndex>,
    pub metadata: NodeLookupMetadata,
}

impl LanceNodeLookupHandle {
    pub fn reference(&self) -> NodeLookupReference {
        NodeLookupReference {
            key: self.metadata.key.clone(),
            scalar_index_name: self.metadata.scalar_index_name.clone(),
            dataset_version: self.metadata.dataset_version,
        }
    }
}

pub trait NodeLookupRegistry: Send + Sync {
    fn get(&self, key: &NodeLookupKey) -> Result<Option<Arc<LanceNodeLookupHandle>>>;
}

#[derive(Debug, Default)]
pub struct InMemoryNodeLookupRegistry {
    lookups: RwLock<HashMap<NodeLookupKey, Arc<LanceNodeLookupHandle>>>,
}

impl InMemoryNodeLookupRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, handle: LanceNodeLookupHandle) -> Result<()> {
        let key = handle.metadata.key.clone();
        self.lookups
            .write()
            .map_err(|_| GraphError::PlanError {
                message: "node lookup registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .insert(key, Arc::new(handle));
        Ok(())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self
            .lookups
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "node lookup registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .is_empty())
    }
}

impl NodeLookupRegistry for InMemoryNodeLookupRegistry {
    fn get(&self, key: &NodeLookupKey) -> Result<Option<Arc<LanceNodeLookupHandle>>> {
        Ok(self
            .lookups
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "node lookup registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .get(key)
            .cloned())
    }
}

/// Discover target semantic-ID scalar indices from Lance-backed catalog entries.
/// Non-Lance providers and Lance datasets without a scalar index are valid and
/// simply do not receive a GetV lookup handle.
pub async fn discover_lance_node_lookups(
    config: &GraphConfig,
    catalog: Arc<dyn GraphSourceCatalog>,
) -> Result<Arc<InMemoryNodeLookupRegistry>> {
    let registry = Arc::new(InMemoryNodeLookupRegistry::new());

    for mapping in config.node_mappings.values() {
        let Some(source) = catalog.node_source(&mapping.label) else {
            continue;
        };
        let Ok(provider) = source_as_provider(&source) else {
            continue;
        };
        let Some(lance_provider) = provider.as_any().downcast_ref::<LanceTableProvider>() else {
            continue;
        };
        let dataset = lance_provider.dataset();
        let Some(id_field) = dataset.schema().field(&mapping.id_field) else {
            return Err(GraphError::PlanError {
                message: format!(
                    "Lance node dataset '{}' is missing configured ID field '{}'",
                    mapping.label, mapping.id_field
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        };
        let id_data_type = source
            .schema()
            .field_with_name(&mapping.id_field)
            .map_err(|e| GraphError::PlanError {
                message: format!(
                    "Lance node dataset '{}' ID field '{}' is invalid: {e}",
                    mapping.label, mapping.id_field
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .data_type()
            .clone();

        let indices = dataset
            .load_indices()
            .await
            .map_err(|e| GraphError::PlanError {
                message: format!(
                    "failed to load scalar-index metadata for node '{}': {e}",
                    mapping.label
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        let current_fragment_ids = dataset
            .get_fragments()
            .iter()
            .map(|fragment| fragment.id() as u32)
            .collect::<Vec<_>>();
        let mut candidates = indices
            .iter()
            .filter(|index| index.fields.as_slice() == [id_field.id])
            // Direct ScalarIndex::search only sees fragments covered by that
            // index handle. If rows were appended without updating the index,
            // fall back to the existing target Join instead of returning an
            // incomplete GetV result.
            .filter(|index| {
                index.fragment_bitmap.as_ref().is_some_and(|covered| {
                    current_fragment_ids
                        .iter()
                        .all(|fragment_id| covered.contains(*fragment_id))
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|index| &index.name);

        let mut selected = None;
        for index in candidates {
            match dataset
                .open_scalar_index(
                    &mapping.id_field,
                    &index.uuid.to_string(),
                    &NoOpMetricsCollector,
                )
                .await
            {
                Ok(scalar_index) => {
                    selected = Some((index, scalar_index));
                    break;
                }
                Err(_) => continue,
            }
        }

        let Some((index, scalar_index)) = selected else {
            continue;
        };

        registry.register(LanceNodeLookupHandle {
            dataset: dataset.clone(),
            scalar_index,
            metadata: NodeLookupMetadata {
                key: NodeLookupKey::new(&mapping.label, &mapping.id_field),
                id_data_type,
                scalar_index_name: index.name.clone(),
                dataset_uri: dataset.uri().to_string(),
                dataset_version: dataset.version().version,
                index_dataset_version: index.dataset_version,
            },
        })?;
    }

    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{Field, Schema};
    use datafusion::datasource::{DefaultTableSource, MemTable};
    use lance::dataset::{WriteMode, WriteParams};
    use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
    use lance_index::IndexType;

    #[tokio::test]
    async fn discovers_lance_id_scalar_index() {
        let temp = tempfile::tempdir().unwrap();
        let uri = temp.path().join("person.lance");
        let schema = Arc::new(Schema::new(vec![
            Field::new("person_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let mut dataset =
            Dataset::write(reader, uri.to_str().unwrap(), Some(WriteParams::default()))
                .await
                .unwrap();
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
        let dataset = Arc::new(Dataset::open(uri.to_str().unwrap()).await.unwrap());
        let provider = Arc::new(LanceTableProvider::new(dataset, true, true));
        let catalog = Arc::new(
            lance_graph_catalog::InMemoryCatalog::new()
                .with_node_source("Person", Arc::new(DefaultTableSource::new(provider))),
        );
        let config = GraphConfig::builder()
            .with_node_label("Person", "person_id")
            .build()
            .unwrap();

        let registry = discover_lance_node_lookups(&config, catalog).await.unwrap();
        let handle = registry
            .get(&NodeLookupKey::new("person", "person_id"))
            .unwrap()
            .unwrap();
        assert_eq!(handle.metadata.scalar_index_name, "person_id_btree");
        assert_eq!(handle.metadata.id_data_type, DataType::Int64);
    }

    #[tokio::test]
    async fn ignores_lance_without_scalar_index_and_memtable() {
        let temp = tempfile::tempdir().unwrap();
        let uri = temp.path().join("person.lance");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "person_id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![0, 1]))])
                .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());
        let dataset = Arc::new(
            Dataset::write(reader, uri.to_str().unwrap(), Some(WriteParams::default()))
                .await
                .unwrap(),
        );
        let lance_provider = Arc::new(LanceTableProvider::new(dataset, false, false));
        let mem_provider = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let catalog = Arc::new(
            lance_graph_catalog::InMemoryCatalog::new()
                .with_node_source("Person", Arc::new(DefaultTableSource::new(lance_provider)))
                .with_node_source("Other", Arc::new(DefaultTableSource::new(mem_provider))),
        );
        let config = GraphConfig::builder()
            .with_node_label("Person", "person_id")
            .with_node_label("Other", "person_id")
            .build()
            .unwrap();

        let registry = discover_lance_node_lookups(&config, catalog).await.unwrap();
        assert!(registry.is_empty().unwrap());
    }

    #[tokio::test]
    async fn ignores_scalar_index_that_does_not_cover_appended_fragments() {
        let temp = tempfile::tempdir().unwrap();
        let uri = temp.path().join("person.lance");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "person_id",
            DataType::Int64,
            false,
        )]));
        let initial =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![0, 1]))])
                .unwrap();
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial)], schema.clone()),
            uri.to_str().unwrap(),
            Some(WriteParams::default()),
        )
        .await
        .unwrap();
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

        let appended =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![2]))])
                .unwrap();
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(appended)], schema),
            uri.to_str().unwrap(),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..WriteParams::default()
            }),
        )
        .await
        .unwrap();

        let dataset = Arc::new(Dataset::open(uri.to_str().unwrap()).await.unwrap());
        let provider = Arc::new(LanceTableProvider::new(dataset, true, true));
        let catalog = Arc::new(
            lance_graph_catalog::InMemoryCatalog::new()
                .with_node_source("Person", Arc::new(DefaultTableSource::new(provider))),
        );
        let config = GraphConfig::builder()
            .with_node_label("Person", "person_id")
            .build()
            .unwrap();

        let registry = discover_lance_node_lookups(&config, catalog).await.unwrap();
        assert!(registry.is_empty().unwrap());
    }
}
