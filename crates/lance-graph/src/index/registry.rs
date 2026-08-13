use super::metadata::{
    CoveringAdjacencyIndexHandle, CsrIndexHandle, DirectAdjacencyIndexHandle, GraphIndexKey,
    MultiTypeCoveringAdjacencyIndexHandle, MultiTypeDirectAdjacencyIndexHandle,
};
use crate::error::{GraphError, GraphIndexErrorKind, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub trait GraphIndexRegistry: Send + Sync {
    fn get_csr(&self, key: &GraphIndexKey) -> Result<Option<Arc<CsrIndexHandle>>>;
    fn get_direct_adjacency(
        &self,
        index_name: &str,
        key: &GraphIndexKey,
    ) -> Result<Option<Arc<DirectAdjacencyIndexHandle>>>;
    fn get_direct_adjacency_bundle(
        &self,
        index_name: &str,
    ) -> Result<Option<Arc<MultiTypeDirectAdjacencyIndexHandle>>>;
    fn get_covering_adjacency(
        &self,
        index_name: &str,
        key: &GraphIndexKey,
    ) -> Result<Option<Arc<CoveringAdjacencyIndexHandle>>>;
    fn get_covering_adjacency_bundle(
        &self,
        index_name: &str,
    ) -> Result<Option<Arc<MultiTypeCoveringAdjacencyIndexHandle>>>;
}

#[derive(Debug, Default)]
pub struct InMemoryGraphIndexRegistry {
    csr_indexes: RwLock<HashMap<GraphIndexKey, Arc<CsrIndexHandle>>>,
    direct_adjacency_indexes: RwLock<HashMap<String, Arc<MultiTypeDirectAdjacencyIndexHandle>>>,
    covering_adjacency_indexes: RwLock<HashMap<String, Arc<MultiTypeCoveringAdjacencyIndexHandle>>>,
}

impl InMemoryGraphIndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_csr(&self, mut handle: CsrIndexHandle) -> Result<()> {
        if handle.metadata.num_vertices != handle.index.num_vertices()
            || handle.metadata.num_edges != handle.index.num_edges()
        {
            return Err(GraphError::PlanError {
                message: "CSR metadata does not match index dimensions".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        let mut indexes = self
            .csr_indexes
            .write()
            .map_err(|_| GraphError::PlanError {
                message: "index registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        if let Some(previous) = indexes.get(&handle.metadata.key) {
            if handle.metadata.generation <= previous.metadata.generation {
                handle.metadata.generation = previous.metadata.generation.saturating_add(1);
            }
        }
        indexes.insert(handle.metadata.key.clone(), Arc::new(handle));
        Ok(())
    }

    /// Register a handle loaded from an immutable persisted generation.
    /// Unlike `register_csr`, this never rewrites its generation.
    pub fn register_loaded_csr(&self, handle: CsrIndexHandle) -> Result<()> {
        if handle.metadata.num_vertices != handle.index.num_vertices()
            || handle.metadata.num_edges != handle.index.num_edges()
        {
            return Err(GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                message: "persisted CSR metadata does not match index dimensions".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        let mut indexes = self
            .csr_indexes
            .write()
            .map_err(|_| GraphError::PlanError {
                message: "index registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        if let Some(previous) = indexes.get(&handle.metadata.key) {
            if handle.metadata.generation < previous.metadata.generation {
                return Err(GraphError::IndexError {
                    kind: GraphIndexErrorKind::GenerationConflict,
                    message: format!(
                        "persisted generation {} is older than registered generation {}",
                        handle.metadata.generation, previous.metadata.generation
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                });
            }
            if handle.metadata.generation == previous.metadata.generation {
                if handle.metadata == previous.metadata {
                    return Ok(());
                }
                return Err(GraphError::IndexError {
                    kind: GraphIndexErrorKind::GenerationConflict,
                    message: format!(
                        "persisted generation {} conflicts with registered metadata",
                        handle.metadata.generation
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                });
            }
        }
        indexes.insert(handle.metadata.key.clone(), Arc::new(handle));
        Ok(())
    }

    pub fn register_direct_adjacency_bundle(
        &self,
        handle: MultiTypeDirectAdjacencyIndexHandle,
    ) -> Result<()> {
        validate_direct_adjacency_bundle(&handle)?;
        let mut indexes =
            self.direct_adjacency_indexes
                .write()
                .map_err(|_| GraphError::PlanError {
                    message: "direct adjacency registry lock poisoned".into(),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;
        let index_name = normalize_index_name(&handle.metadata.index_name)?;
        if let Some(previous) = indexes.get(&index_name) {
            if handle.metadata.bundle_generation < previous.metadata.bundle_generation {
                return Err(GraphError::IndexError {
                    kind: GraphIndexErrorKind::GenerationConflict,
                    message: format!(
                        "direct adjacency bundle generation {} is older than registered generation {} for {}",
                        handle.metadata.bundle_generation,
                        previous.metadata.bundle_generation,
                        index_name,
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                });
            }
            if handle.metadata.bundle_generation == previous.metadata.bundle_generation {
                if bundle_identity_matches(&handle, previous) {
                    return Ok(());
                }
                return Err(GraphError::IndexError {
                    kind: GraphIndexErrorKind::GenerationConflict,
                    message: format!(
                        "direct adjacency bundle generation {} conflicts with registered bundle {}",
                        handle.metadata.bundle_generation, index_name
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                });
            }
        }
        indexes.insert(index_name, Arc::new(handle));
        Ok(())
    }

    pub fn register_covering_adjacency_bundle(
        &self,
        handle: MultiTypeCoveringAdjacencyIndexHandle,
    ) -> Result<()> {
        validate_covering_adjacency_bundle(&handle)?;
        let index_name = normalize_index_name(&handle.metadata.index_name)?;
        let mut indexes =
            self.covering_adjacency_indexes
                .write()
                .map_err(|_| GraphError::PlanError {
                    message: "covering adjacency registry lock poisoned".into(),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;
        if let Some(previous) = indexes.get(&index_name) {
            if handle.metadata.bundle_generation < previous.metadata.bundle_generation {
                return Err(GraphError::IndexError {
                    kind: GraphIndexErrorKind::GenerationConflict,
                    message: format!(
                        "covering adjacency bundle generation {} is older than registered generation {} for {}",
                        handle.metadata.bundle_generation,
                        previous.metadata.bundle_generation,
                        index_name
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                });
            }
            if handle.metadata.bundle_generation == previous.metadata.bundle_generation {
                if covering_bundle_identity_matches(&handle, previous) {
                    return Ok(());
                }
                return Err(GraphError::IndexError {
                    kind: GraphIndexErrorKind::GenerationConflict,
                    message: format!(
                        "covering adjacency bundle generation {} conflicts with registered bundle {}",
                        handle.metadata.bundle_generation, index_name
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                });
            }
        }
        indexes.insert(index_name, Arc::new(handle));
        Ok(())
    }

    pub fn remove_covering_adjacency(
        &self,
        index_name: &str,
    ) -> Result<Option<Arc<MultiTypeCoveringAdjacencyIndexHandle>>> {
        Ok(self
            .covering_adjacency_indexes
            .write()
            .map_err(|_| GraphError::PlanError {
                message: "covering adjacency registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .remove(&normalize_index_name(index_name)?))
    }

    pub fn remove_direct_adjacency(
        &self,
        index_name: &str,
    ) -> Result<Option<Arc<MultiTypeDirectAdjacencyIndexHandle>>> {
        Ok(self
            .direct_adjacency_indexes
            .write()
            .map_err(|_| GraphError::PlanError {
                message: "direct adjacency registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .remove(&normalize_index_name(index_name)?))
    }
    pub fn remove(&self, key: &GraphIndexKey) -> Result<Option<Arc<CsrIndexHandle>>> {
        Ok(self
            .csr_indexes
            .write()
            .map_err(|_| GraphError::PlanError {
                message: "index registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .remove(key))
    }
}

fn validate_direct_adjacency_bundle(handle: &MultiTypeDirectAdjacencyIndexHandle) -> Result<()> {
    let index_name = normalize_index_name(&handle.metadata.index_name)?;
    if index_name != handle.metadata.index_name {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Incompatible,
            message: "direct adjacency bundle name must be normalized".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    if handle.components.is_empty()
        || handle.metadata.num_components != handle.components.len() as u64
    {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Corrupt,
            message: "direct adjacency bundle component count mismatch".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    let mut num_sources = 0_u64;
    let mut num_edges = 0_u64;
    for (key, component) in &handle.components {
        if key != &component.metadata.key {
            return Err(GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                message: "direct adjacency bundle component key mismatch".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        validate_direct_adjacency_handle(component)?;
        num_sources = num_sources
            .checked_add(component.metadata.num_sources)
            .ok_or_else(|| GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                message: "direct adjacency bundle source count overflow".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        num_edges = num_edges
            .checked_add(component.metadata.num_edges)
            .ok_or_else(|| GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                message: "direct adjacency bundle edge count overflow".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
    }
    if handle.metadata.num_sources != num_sources || handle.metadata.num_edges != num_edges {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Corrupt,
            message: "direct adjacency bundle aggregate counts mismatch".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    Ok(())
}

fn validate_covering_adjacency_bundle(
    handle: &MultiTypeCoveringAdjacencyIndexHandle,
) -> Result<()> {
    let index_name = normalize_index_name(&handle.metadata.index_name)?;
    if index_name != handle.metadata.index_name
        || handle.components.is_empty()
        || handle.metadata.num_components != handle.components.len() as u64
    {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Corrupt,
            message: "covering adjacency bundle identity or component count is invalid".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    let mut num_sources = 0_u64;
    let mut num_edges = 0_u64;
    for (key, component) in &handle.components {
        if key != &component.metadata.key
            || component.metadata != *component.index.metadata()
            || component.metadata.num_sources
                != component.metadata.num_inline_sources
                    + component.metadata.num_posting_tree_sources
        {
            return Err(GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                message: "covering adjacency component metadata is inconsistent".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        num_sources = num_sources
            .checked_add(component.metadata.num_sources)
            .ok_or_else(|| GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                message: "covering adjacency source count overflow".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        num_edges = num_edges
            .checked_add(component.metadata.num_edges)
            .ok_or_else(|| GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                message: "covering adjacency edge count overflow".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
    }
    if num_sources != handle.metadata.num_sources || num_edges != handle.metadata.num_edges {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Corrupt,
            message: "covering adjacency bundle aggregate counts mismatch".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    Ok(())
}

fn validate_direct_adjacency_handle(handle: &DirectAdjacencyIndexHandle) -> Result<()> {
    if handle.metadata.dataset_version != handle.dataset.version().version {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Stale,
            message: format!(
                "direct adjacency dataset version {} does not match metadata version {}",
                handle.dataset.version().version,
                handle.metadata.dataset_version
            ),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    if handle.metadata.source_version.is_some() && handle.metadata.source_uri.is_none() {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Incompatible,
            message: "direct adjacency source_version requires source_uri".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    if handle.metadata.source_id_field.is_empty() || handle.metadata.adjacency_field.is_empty() {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Incompatible,
            message: "direct adjacency field names must not be empty".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    Ok(())
}

fn normalize_index_name(index_name: &str) -> Result<String> {
    let normalized = index_name.trim().to_lowercase();
    if normalized.is_empty() {
        return Err(GraphError::IndexError {
            kind: GraphIndexErrorKind::Incompatible,
            message: "direct adjacency index name must not be empty".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    Ok(normalized)
}

fn bundle_identity_matches(
    left: &MultiTypeDirectAdjacencyIndexHandle,
    right: &MultiTypeDirectAdjacencyIndexHandle,
) -> bool {
    left.metadata == right.metadata
        && left.components.len() == right.components.len()
        && left.components.iter().all(|(key, component)| {
            right.components.get(key).is_some_and(|other| {
                component.metadata == other.metadata
                    && component.dataset.version().version == other.dataset.version().version
            })
        })
}

fn covering_bundle_identity_matches(
    left: &MultiTypeCoveringAdjacencyIndexHandle,
    right: &MultiTypeCoveringAdjacencyIndexHandle,
) -> bool {
    left.metadata == right.metadata
        && left.components.len() == right.components.len()
        && left.components.iter().all(|(key, component)| {
            right
                .components
                .get(key)
                .is_some_and(|other| component.metadata == other.metadata)
        })
}

impl GraphIndexRegistry for InMemoryGraphIndexRegistry {
    fn get_csr(&self, key: &GraphIndexKey) -> Result<Option<Arc<CsrIndexHandle>>> {
        Ok(self
            .csr_indexes
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "index registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .get(key)
            .cloned())
    }

    fn get_direct_adjacency(
        &self,
        index_name: &str,
        key: &GraphIndexKey,
    ) -> Result<Option<Arc<DirectAdjacencyIndexHandle>>> {
        let index_name = normalize_index_name(index_name)?;
        Ok(self
            .direct_adjacency_indexes
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "direct adjacency registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .get(&index_name)
            .and_then(|bundle| bundle.get(key)))
    }

    fn get_direct_adjacency_bundle(
        &self,
        index_name: &str,
    ) -> Result<Option<Arc<MultiTypeDirectAdjacencyIndexHandle>>> {
        Ok(self
            .direct_adjacency_indexes
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "direct adjacency registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .get(&normalize_index_name(index_name)?)
            .cloned())
    }

    fn get_covering_adjacency(
        &self,
        index_name: &str,
        key: &GraphIndexKey,
    ) -> Result<Option<Arc<CoveringAdjacencyIndexHandle>>> {
        Ok(self
            .covering_adjacency_indexes
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "covering adjacency registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .get(&normalize_index_name(index_name)?)
            .and_then(|bundle| bundle.get(key)))
    }

    fn get_covering_adjacency_bundle(
        &self,
        index_name: &str,
    ) -> Result<Option<Arc<MultiTypeCoveringAdjacencyIndexHandle>>> {
        Ok(self
            .covering_adjacency_indexes
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "covering adjacency registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .get(&normalize_index_name(index_name)?)
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{
        CoveringAdjacencyCompression, CoveringAdjacencyIndexBuilder, CoveringAdjacencyMetadata,
        DirectAdjacencyIndexBuilder, DirectAdjacencyMetadata, GraphIndexKey, GraphIndexMetadata,
        IndexDirection, MultiTypeCoveringAdjacencyIndexBuilder,
        MultiTypeCoveringAdjacencyIndexStore, MultiTypeDirectAdjacencyIndexBuilder,
        MultiTypeDirectAdjacencyIndexStore,
    };
    use crate::CsrIndexBuilder;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::DataType;
    use arrow_schema::{Field, Schema};

    #[test]
    fn register_and_lookup_is_case_insensitive() {
        let index = CsrIndexBuilder::new()
            .with_num_vertices(2)
            .add_edge(0, 1)
            .try_build()
            .unwrap();
        let key = GraphIndexKey::new("KNOWS", "Person", "PERSON", IndexDirection::Outgoing);
        let handle = CsrIndexHandle {
            index: Arc::new(index),
            metadata: GraphIndexMetadata {
                key: key.clone(),
                source_id_field: "id".into(),
                target_id_field: "id".into(),
                id_data_type: DataType::Int64,
                num_vertices: 2,
                num_edges: 1,
                source_uri: None,
                source_version: None,
                generation: 7,
            },
        };
        let registry = InMemoryGraphIndexRegistry::new();
        registry.register_csr(handle).unwrap();
        let lookup = registry
            .get_csr(&GraphIndexKey::new(
                "knows",
                "person",
                "person",
                IndexDirection::Outgoing,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(lookup.metadata.generation, 7);
    }

    #[test]
    fn persisted_generation_registration_is_strict() {
        let make_handle = |generation| CsrIndexHandle {
            index: Arc::new(
                CsrIndexBuilder::new()
                    .with_num_vertices(2)
                    .add_edge(0, 1)
                    .try_build()
                    .unwrap(),
            ),
            metadata: GraphIndexMetadata {
                key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
                source_id_field: "id".into(),
                target_id_field: "id".into(),
                id_data_type: DataType::Int64,
                num_vertices: 2,
                num_edges: 1,
                source_uri: Some("memory://knows".into()),
                source_version: Some(1),
                generation,
            },
        };
        let registry = InMemoryGraphIndexRegistry::new();
        registry.register_loaded_csr(make_handle(7)).unwrap();
        registry.register_loaded_csr(make_handle(7)).unwrap();
        registry.register_loaded_csr(make_handle(8)).unwrap();
        let error = registry.register_loaded_csr(make_handle(6)).unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::GenerationConflict,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn named_direct_bundles_are_isolated_and_replace_atomically() {
        async fn bundle(
            root: &std::path::Path,
            name: &str,
            bundle_generation: u64,
            component_generation: u64,
        ) -> MultiTypeDirectAdjacencyIndexHandle {
            let component_uri = root.join(format!("{name}-component-{bundle_generation}"));
            let bundle_uri = root.join(format!("{name}-bundle-{bundle_generation}"));
            let edges = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("src_id", DataType::Int64, false),
                    Field::new("dst_id", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![2])),
                ],
            )
            .unwrap();
            let descriptor = DirectAdjacencyIndexBuilder::new(DirectAdjacencyMetadata {
                key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
                source_id_field: "src_id".into(),
                target_id_field: "person_id".into(),
                adjacency_field: "dst_ids".into(),
                id_data_type: DataType::Int64,
                num_sources: 0,
                num_edges: 0,
                dataset_uri: String::new(),
                dataset_version: 0,
                scalar_index_name: format!("{name}_src_btree"),
                source_uri: None,
                source_version: None,
                generation: component_generation,
            })
            .unwrap()
            .add_edges_from_batch(&edges)
            .unwrap()
            .build_and_persist(component_uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();
            let bundle = MultiTypeDirectAdjacencyIndexBuilder::new(name, bundle_generation)
                .unwrap()
                .add_component(descriptor)
                .unwrap()
                .build_and_persist(bundle_uri.to_str().unwrap())
                .await
                .unwrap();
            MultiTypeDirectAdjacencyIndexStore::load(&bundle, Default::default())
                .await
                .unwrap()
        }

        let directory = tempfile::tempdir().unwrap();
        let registry = InMemoryGraphIndexRegistry::new();
        let first = bundle(directory.path(), "primary", 1, 1).await;
        let old_component = first.get(&GraphIndexKey::new(
            "KNOWS",
            "Person",
            "Person",
            IndexDirection::Outgoing,
        ));
        registry.register_direct_adjacency_bundle(first).unwrap();
        registry
            .register_direct_adjacency_bundle(bundle(directory.path(), "experimental", 1, 1).await)
            .unwrap();
        registry
            .register_direct_adjacency_bundle(bundle(directory.path(), "primary", 2, 2).await)
            .unwrap();

        let key = GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing);
        assert_eq!(
            registry
                .get_direct_adjacency_bundle("PRIMARY")
                .unwrap()
                .unwrap()
                .metadata
                .bundle_generation,
            2
        );
        assert_eq!(
            registry
                .get_direct_adjacency("experimental", &key)
                .unwrap()
                .unwrap()
                .metadata
                .generation,
            1
        );
        assert_eq!(old_component.unwrap().metadata.generation, 1);

        let error = registry
            .register_direct_adjacency_bundle(bundle(directory.path(), "primary", 0, 3).await)
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::GenerationConflict,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn named_covering_bundles_replace_only_with_newer_generations() {
        async fn bundle(
            root: &std::path::Path,
            bundle_generation: u64,
            component_generation: u64,
        ) -> MultiTypeCoveringAdjacencyIndexHandle {
            let component_uri = root.join(format!("covering-component-{bundle_generation}"));
            let bundle_uri = root.join(format!("covering-bundle-{bundle_generation}"));
            let edges = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("src_id", DataType::Int64, false),
                    Field::new("dst_id", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1, 1])),
                    Arc::new(Int64Array::from(vec![2, 3])),
                ],
            )
            .unwrap();
            let descriptor = CoveringAdjacencyIndexBuilder::new(CoveringAdjacencyMetadata {
                key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
                source_id_field: "person_id".into(),
                target_id_field: "person_id".into(),
                source_id_data_type: DataType::Int64,
                target_id_data_type: DataType::Int64,
                num_sources: 0,
                num_edges: 0,
                max_degree: 0,
                generation: component_generation,
                format_version: crate::COVERING_ADJACENCY_INDEX_FORMAT_VERSION,
                index_uri: String::new(),
                entry_directory_uri: String::new(),
                posting_directory_uri: String::new(),
                entry_pages_uri: String::new(),
                posting_pages_uri: String::new(),
                entry_page_target_bytes: 0,
                inline_posting_threshold_bytes: 0,
                posting_page_target_bytes: 0,
                compression: CoveringAdjacencyCompression::None,
                num_entry_pages: 0,
                num_inline_sources: 0,
                num_posting_tree_sources: 0,
                num_posting_pages: 0,
                source_uri: None,
                source_version: None,
            })
            .unwrap()
            .add_edges_from_batch(&edges)
            .unwrap()
            .build_and_persist(component_uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();
            let descriptor =
                MultiTypeCoveringAdjacencyIndexBuilder::new("social_covering", bundle_generation)
                    .unwrap()
                    .add_component(descriptor)
                    .unwrap()
                    .build_and_persist(bundle_uri.to_str().unwrap())
                    .await
                    .unwrap();
            MultiTypeCoveringAdjacencyIndexStore::load(&descriptor, Default::default())
                .await
                .unwrap()
        }

        let directory = tempfile::tempdir().unwrap();
        let registry = InMemoryGraphIndexRegistry::new();
        registry
            .register_covering_adjacency_bundle(bundle(directory.path(), 1, 1).await)
            .unwrap();
        registry
            .register_covering_adjacency_bundle(bundle(directory.path(), 2, 2).await)
            .unwrap();
        let key = GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing);
        assert_eq!(
            registry
                .get_covering_adjacency("SOCIAL_COVERING", &key)
                .unwrap()
                .unwrap()
                .metadata
                .generation,
            2
        );
        let error = registry
            .register_covering_adjacency_bundle(bundle(directory.path(), 0, 3).await)
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::GenerationConflict,
                ..
            }
        ));
    }
}
