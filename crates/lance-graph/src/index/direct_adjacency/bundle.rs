use super::{
    index_error, index_io_error, DirectAdjacencyIndexStore, PersistedDirectAdjacencyDescriptor,
    PersistedDirection, PersistedKey,
};
use crate::error::{GraphIndexErrorKind, Result};
use crate::index::{
    DirectAdjacencyLoadOptions, GraphIndexKey, GraphSourceIdentity, IndexDirection,
    IndexSourceValidation, MultiTypeDirectAdjacencyIndexHandle, MultiTypeDirectAdjacencyMetadata,
};
use lance_io::object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

const BUNDLE_DESCRIPTOR_FILE: &str = "bundle-descriptor.json";
const BUNDLE_FORMAT_NAME: &str = "lance-graph-multi-type-direct-adjacency";

pub const MULTI_TYPE_DIRECT_ADJACENCY_INDEX_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectAdjacencyComponentDescriptorRef {
    pub key: GraphIndexKey,
    pub component_descriptor_uri: String,
    pub component_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedMultiTypeDirectAdjacencyDescriptor {
    pub index_uri: String,
    pub format_version: u32,
    pub metadata: MultiTypeDirectAdjacencyMetadata,
    pub components: Vec<DirectAdjacencyComponentDescriptorRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MultiTypeSourceValidation {
    RequireExact(BTreeMap<GraphIndexKey, GraphSourceIdentity>),
    #[default]
    AllowUnknown,
}

#[derive(Debug, Clone, Default)]
pub struct MultiTypeDirectAdjacencyLoadOptions {
    pub source_validation: MultiTypeSourceValidation,
}

#[derive(Debug, Clone)]
pub struct MultiTypeDirectAdjacencyIndexBuilder {
    index_name: String,
    bundle_generation: u64,
    components: BTreeMap<GraphIndexKey, PersistedDirectAdjacencyDescriptor>,
}

impl MultiTypeDirectAdjacencyIndexBuilder {
    pub fn new(index_name: impl Into<String>, bundle_generation: u64) -> Result<Self> {
        Ok(Self {
            index_name: normalize_index_name(index_name.into())?,
            bundle_generation,
            components: BTreeMap::new(),
        })
    }

    pub fn add_component(mut self, descriptor: PersistedDirectAdjacencyDescriptor) -> Result<Self> {
        if descriptor.index_uri.trim().is_empty() {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "direct adjacency component descriptor URI must not be empty",
            ));
        }
        let key = descriptor.metadata.key.clone();
        if self.components.insert(key.clone(), descriptor).is_some() {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                format!("duplicate direct adjacency component for {key:?}"),
            ));
        }
        Ok(self)
    }

    pub async fn build_and_persist(
        self,
        index_uri: &str,
    ) -> Result<PersistedMultiTypeDirectAdjacencyDescriptor> {
        MultiTypeDirectAdjacencyIndexStore::write(
            index_uri,
            &self.index_name,
            self.bundle_generation,
            self.components.into_values().collect(),
        )
        .await
    }
}

pub struct MultiTypeDirectAdjacencyIndexStore;

impl MultiTypeDirectAdjacencyIndexStore {
    pub async fn write(
        index_uri: &str,
        index_name: &str,
        bundle_generation: u64,
        components: Vec<PersistedDirectAdjacencyDescriptor>,
    ) -> Result<PersistedMultiTypeDirectAdjacencyDescriptor> {
        if index_uri.trim().is_empty() {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "multi-type direct adjacency index URI must not be empty",
            ));
        }
        let index_name = normalize_index_name(index_name.to_string())?;
        if components.is_empty() {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "multi-type direct adjacency bundle requires at least one component",
            ));
        }

        let (store, base_path) = ObjectStore::from_uri(index_uri)
            .await
            .map_err(|error| bundle_io_error(index_uri, error))?;
        let descriptor_path = base_path.child(BUNDLE_DESCRIPTOR_FILE);
        if store
            .inner
            .exists(&descriptor_path)
            .await
            .map_err(|error| bundle_io_error(index_uri, error))?
        {
            return Err(index_error(
                GraphIndexErrorKind::AlreadyExists,
                format!("multi-type direct adjacency bundle already exists at {index_uri}"),
            ));
        }

        let mut refs = BTreeMap::new();
        let mut num_sources = 0_u64;
        let mut num_edges = 0_u64;
        for descriptor in components {
            let persisted =
                DirectAdjacencyIndexStore::read_descriptor(&descriptor.index_uri).await?;
            if persisted != descriptor {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!(
                        "direct adjacency component descriptor changed at {}",
                        descriptor.index_uri
                    ),
                ));
            }
            DirectAdjacencyIndexStore::load(&persisted, Default::default()).await?;
            let key = persisted.metadata.key.clone();
            let entry = DirectAdjacencyComponentDescriptorRef {
                key: key.clone(),
                component_descriptor_uri: portable_component_uri(index_uri, &persisted.index_uri),
                component_generation: persisted.metadata.generation,
            };
            if refs.insert(key.clone(), entry).is_some() {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!("duplicate direct adjacency component for {key:?}"),
                ));
            }
            num_sources = num_sources
                .checked_add(persisted.metadata.num_sources)
                .ok_or_else(|| {
                    index_error(
                        GraphIndexErrorKind::Corrupt,
                        "multi-type direct adjacency source count overflow",
                    )
                })?;
            num_edges = num_edges
                .checked_add(persisted.metadata.num_edges)
                .ok_or_else(|| {
                    index_error(
                        GraphIndexErrorKind::Corrupt,
                        "multi-type direct adjacency edge count overflow",
                    )
                })?;
        }

        let metadata = MultiTypeDirectAdjacencyMetadata {
            index_name,
            bundle_generation,
            num_components: refs.len() as u64,
            num_sources,
            num_edges,
        };
        let descriptor = PersistedMultiTypeDirectAdjacencyDescriptor {
            index_uri: index_uri.to_string(),
            format_version: MULTI_TYPE_DIRECT_ADJACENCY_INDEX_FORMAT_VERSION,
            metadata,
            components: refs.into_values().collect(),
        };
        let persisted = PersistedBundleDescriptor::from_public(&descriptor);
        let bytes = serde_json::to_vec_pretty(&persisted).map_err(|error| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                format!("failed to serialize multi-type direct adjacency descriptor: {error}"),
            )
        })?;
        store
            .put(&descriptor_path, &bytes)
            .await
            .map_err(|error| bundle_io_error(index_uri, error))?;
        Ok(descriptor)
    }

    pub async fn read_descriptor(
        index_uri: &str,
    ) -> Result<PersistedMultiTypeDirectAdjacencyDescriptor> {
        read_persisted_bundle(index_uri).await?.to_public(index_uri)
    }

    pub async fn load(
        descriptor: &PersistedMultiTypeDirectAdjacencyDescriptor,
        options: MultiTypeDirectAdjacencyLoadOptions,
    ) -> Result<MultiTypeDirectAdjacencyIndexHandle> {
        let persisted = read_persisted_bundle(&descriptor.index_uri).await?;
        let on_disk = persisted.to_public(&descriptor.index_uri)?;
        if &on_disk != descriptor {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "multi-type direct adjacency descriptor does not match persisted metadata",
            ));
        }

        let expected_sources = match &options.source_validation {
            MultiTypeSourceValidation::AllowUnknown => None,
            MultiTypeSourceValidation::RequireExact(expected) => {
                let actual_keys = descriptor
                    .components
                    .iter()
                    .map(|component| component.key.clone())
                    .collect::<Vec<_>>();
                let expected_keys = expected.keys().cloned().collect::<Vec<_>>();
                if actual_keys != expected_keys {
                    return Err(index_error(
                        GraphIndexErrorKind::Incompatible,
                        "multi-type direct adjacency source validation keys do not match bundle components",
                    ));
                }
                Some(expected)
            }
        };

        let mut components = BTreeMap::new();
        let mut num_sources = 0_u64;
        let mut num_edges = 0_u64;
        for entry in &descriptor.components {
            let component_uri =
                resolve_component_uri(&descriptor.index_uri, &entry.component_descriptor_uri);
            let component_descriptor =
                DirectAdjacencyIndexStore::read_descriptor(&component_uri).await?;
            if component_descriptor.metadata.key != entry.key
                || component_descriptor.metadata.generation != entry.component_generation
            {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!(
                        "multi-type direct adjacency component identity mismatch for {:?}",
                        entry.key
                    ),
                ));
            }
            let source_validation = expected_sources
                .and_then(|expected| expected.get(&entry.key))
                .cloned()
                .map(IndexSourceValidation::RequireExact)
                .unwrap_or(IndexSourceValidation::AllowUnknown);
            let handle = DirectAdjacencyIndexStore::load(
                &component_descriptor,
                DirectAdjacencyLoadOptions { source_validation },
            )
            .await?;
            num_sources = num_sources
                .checked_add(handle.metadata.num_sources)
                .ok_or_else(|| {
                    index_error(
                        GraphIndexErrorKind::Corrupt,
                        "multi-type direct adjacency source count overflow",
                    )
                })?;
            num_edges = num_edges
                .checked_add(handle.metadata.num_edges)
                .ok_or_else(|| {
                    index_error(
                        GraphIndexErrorKind::Corrupt,
                        "multi-type direct adjacency edge count overflow",
                    )
                })?;
            if components
                .insert(entry.key.clone(), Arc::new(handle))
                .is_some()
            {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!("duplicate direct adjacency component for {:?}", entry.key),
                ));
            }
        }
        if descriptor.metadata.num_components != components.len() as u64
            || descriptor.metadata.num_sources != num_sources
            || descriptor.metadata.num_edges != num_edges
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "multi-type direct adjacency aggregate counts do not match components",
            ));
        }
        Ok(MultiTypeDirectAdjacencyIndexHandle {
            metadata: descriptor.metadata.clone(),
            components,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedBundleDescriptor {
    format: String,
    format_version: u32,
    index_kind: String,
    index_name: String,
    bundle_generation: u64,
    num_components: u64,
    num_sources: u64,
    num_edges: u64,
    components: Vec<PersistedComponentRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedComponentRef {
    key: PersistedKey,
    component_descriptor_uri: String,
    component_generation: u64,
}

impl PersistedBundleDescriptor {
    fn from_public(descriptor: &PersistedMultiTypeDirectAdjacencyDescriptor) -> Self {
        Self {
            format: BUNDLE_FORMAT_NAME.into(),
            format_version: descriptor.format_version,
            index_kind: "direct_adjacency_bundle".into(),
            index_name: descriptor.metadata.index_name.clone(),
            bundle_generation: descriptor.metadata.bundle_generation,
            num_components: descriptor.metadata.num_components,
            num_sources: descriptor.metadata.num_sources,
            num_edges: descriptor.metadata.num_edges,
            components: descriptor
                .components
                .iter()
                .map(|component| PersistedComponentRef {
                    key: PersistedKey {
                        relationship_type: component.key.relationship_type.clone(),
                        source_label: component.key.source_label.clone(),
                        target_label: component.key.target_label.clone(),
                        direction: match component.key.direction {
                            IndexDirection::Outgoing => PersistedDirection::Outgoing,
                            IndexDirection::Incoming => PersistedDirection::Incoming,
                        },
                    },
                    component_descriptor_uri: component.component_descriptor_uri.clone(),
                    component_generation: component.component_generation,
                })
                .collect(),
        }
    }

    fn to_public(&self, index_uri: &str) -> Result<PersistedMultiTypeDirectAdjacencyDescriptor> {
        self.validate()?;
        let mut components = self
            .components
            .iter()
            .map(|component| DirectAdjacencyComponentDescriptorRef {
                key: GraphIndexKey::new(
                    &component.key.relationship_type,
                    &component.key.source_label,
                    &component.key.target_label,
                    match component.key.direction {
                        PersistedDirection::Outgoing => IndexDirection::Outgoing,
                        PersistedDirection::Incoming => IndexDirection::Incoming,
                    },
                ),
                component_descriptor_uri: component.component_descriptor_uri.clone(),
                component_generation: component.component_generation,
            })
            .collect::<Vec<_>>();
        components.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(PersistedMultiTypeDirectAdjacencyDescriptor {
            index_uri: index_uri.to_string(),
            format_version: self.format_version,
            metadata: MultiTypeDirectAdjacencyMetadata {
                index_name: normalize_index_name(self.index_name.clone())?,
                bundle_generation: self.bundle_generation,
                num_components: self.num_components,
                num_sources: self.num_sources,
                num_edges: self.num_edges,
            },
            components,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.format != BUNDLE_FORMAT_NAME
            || self.index_kind != "direct_adjacency_bundle"
            || self.format_version != MULTI_TYPE_DIRECT_ADJACENCY_INDEX_FORMAT_VERSION
        {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "unsupported multi-type direct adjacency descriptor format",
            ));
        }
        normalize_index_name(self.index_name.clone())?;
        if self.components.is_empty() || self.num_components != self.components.len() as u64 {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "multi-type direct adjacency component count is invalid",
            ));
        }
        let mut keys = BTreeMap::new();
        for component in &self.components {
            if component.component_descriptor_uri.trim().is_empty() {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    "multi-type direct adjacency component URI must not be empty",
                ));
            }
            let key = GraphIndexKey::new(
                &component.key.relationship_type,
                &component.key.source_label,
                &component.key.target_label,
                match component.key.direction {
                    PersistedDirection::Outgoing => IndexDirection::Outgoing,
                    PersistedDirection::Incoming => IndexDirection::Incoming,
                },
            );
            if keys.insert(key.clone(), ()).is_some() {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!("duplicate direct adjacency component for {key:?}"),
                ));
            }
        }
        Ok(())
    }
}

async fn read_persisted_bundle(index_uri: &str) -> Result<PersistedBundleDescriptor> {
    let (store, base_path) = ObjectStore::from_uri(index_uri)
        .await
        .map_err(|error| bundle_io_error(index_uri, error))?;
    let path = base_path.child(BUNDLE_DESCRIPTOR_FILE);
    if !store
        .inner
        .exists(&path)
        .await
        .map_err(|error| bundle_io_error(index_uri, error))?
    {
        return Err(index_error(
            GraphIndexErrorKind::Missing,
            format!("multi-type direct adjacency descriptor is missing at {index_uri}"),
        ));
    }
    let bytes = store
        .read_one_all(&path)
        .await
        .map_err(|error| bundle_io_error(index_uri, error))?;
    let descriptor: PersistedBundleDescriptor =
        serde_json::from_slice(&bytes).map_err(|error| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                format!("invalid multi-type direct adjacency descriptor at {index_uri}: {error}"),
            )
        })?;
    descriptor.validate()?;
    Ok(descriptor)
}

fn normalize_index_name(index_name: String) -> Result<String> {
    let normalized = index_name.trim().to_lowercase();
    if normalized.is_empty() {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "multi-type direct adjacency index name must not be empty",
        ));
    }
    Ok(normalized)
}

fn portable_component_uri(bundle_uri: &str, component_uri: &str) -> String {
    let prefix = format!("{}/", bundle_uri.trim_end_matches('/'));
    component_uri
        .strip_prefix(&prefix)
        .unwrap_or(component_uri)
        .to_string()
}

fn resolve_component_uri(bundle_uri: &str, component_uri: &str) -> String {
    if component_uri.contains("://") || component_uri.starts_with('/') {
        component_uri.to_string()
    } else {
        format!(
            "{}/{}",
            bundle_uri.trim_end_matches('/'),
            component_uri.trim_start_matches('/')
        )
    }
}

fn bundle_io_error(uri: &str, error: impl std::fmt::Display) -> crate::error::GraphError {
    index_io_error(uri, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{DirectAdjacencyIndexBuilder, DirectAdjacencyMetadata};
    use crate::GraphIndexErrorKind;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn edges(src: &[i64], dst: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("src_id", DataType::Int64, false),
                Field::new("dst_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(src.to_vec())),
                Arc::new(Int64Array::from(dst.to_vec())),
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
            source_uri: Some(format!("memory://{relationship_type}")),
            source_version: Some(1),
            generation,
        }
    }

    #[tokio::test]
    async fn bundle_round_trips_multiple_relationship_types() {
        let directory = tempfile::tempdir().unwrap();
        let friend_uri = directory.path().join("friend");
        let follows_uri = directory.path().join("follows");
        let bundle_uri = directory.path().join("bundle");
        let friend = DirectAdjacencyIndexBuilder::new(metadata("FRIEND_OF", 7))
            .unwrap()
            .add_edges_from_batch(&edges(&[1, 1, 2], &[2, 3, 4]))
            .unwrap()
            .build_and_persist(friend_uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        let follows = DirectAdjacencyIndexBuilder::new(metadata("FOLLOWS", 7))
            .unwrap()
            .add_edges_from_batch(&edges(&[1, 3], &[4, 2]))
            .unwrap()
            .build_and_persist(follows_uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        let descriptor = MultiTypeDirectAdjacencyIndexBuilder::new("Social", 7)
            .unwrap()
            .add_component(friend)
            .unwrap()
            .add_component(follows)
            .unwrap()
            .build_and_persist(bundle_uri.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(descriptor.metadata.index_name, "social");
        assert_eq!(descriptor.metadata.num_components, 2);
        assert_eq!(descriptor.metadata.num_sources, 4);
        assert_eq!(descriptor.metadata.num_edges, 5);
        let read =
            MultiTypeDirectAdjacencyIndexStore::read_descriptor(bundle_uri.to_str().unwrap())
                .await
                .unwrap();
        assert_eq!(read, descriptor);
        let handle = MultiTypeDirectAdjacencyIndexStore::load(&read, Default::default())
            .await
            .unwrap();
        assert!(handle
            .get(&GraphIndexKey::new(
                "friend_of",
                "person",
                "person",
                IndexDirection::Outgoing,
            ))
            .is_some());
        assert!(handle
            .get(&GraphIndexKey::new(
                "follows",
                "person",
                "person",
                IndexDirection::Outgoing,
            ))
            .is_some());
    }

    #[tokio::test]
    async fn bundle_rejects_duplicate_keys_and_mismatched_source_validation() {
        let directory = tempfile::tempdir().unwrap();
        let component_uri = directory.path().join("friend");
        let descriptor = DirectAdjacencyIndexBuilder::new(metadata("FRIEND_OF", 1))
            .unwrap()
            .add_edges_from_batch(&edges(&[1], &[2]))
            .unwrap()
            .build_and_persist(component_uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        let duplicate_error = MultiTypeDirectAdjacencyIndexBuilder::new("social", 1)
            .unwrap()
            .add_component(descriptor.clone())
            .unwrap()
            .add_component(descriptor.clone())
            .unwrap_err();
        assert!(matches!(
            duplicate_error,
            crate::GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                ..
            }
        ));

        let bundle_uri = directory.path().join("bundle");
        let bundle = MultiTypeDirectAdjacencyIndexBuilder::new("social", 1)
            .unwrap()
            .add_component(descriptor)
            .unwrap()
            .build_and_persist(bundle_uri.to_str().unwrap())
            .await
            .unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(
            bundle.components[0].key.clone(),
            GraphSourceIdentity {
                uri: "memory://wrong".into(),
                version: Some(1),
            },
        );
        let error = MultiTypeDirectAdjacencyIndexStore::load(
            &bundle,
            MultiTypeDirectAdjacencyLoadOptions {
                source_validation: MultiTypeSourceValidation::RequireExact(expected),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            crate::GraphError::IndexError {
                kind: GraphIndexErrorKind::Stale,
                ..
            }
        ));
    }
}
