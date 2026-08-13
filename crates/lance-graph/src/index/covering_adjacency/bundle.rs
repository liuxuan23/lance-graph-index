use super::{
    component_uri, index_error, CoveringAdjacencyIndexStore, CoveringAdjacencyLoadOptions,
    PersistedCoveringAdjacencyDescriptor,
};
use crate::error::{GraphIndexErrorKind, Result};
use crate::index::{
    GraphIndexKey, GraphSourceIdentity, IndexSourceValidation,
    MultiTypeCoveringAdjacencyIndexHandle, MultiTypeCoveringAdjacencyMetadata,
};
use lance_io::object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

const BUNDLE_DESCRIPTOR_FILE: &str = "bundle-descriptor.json";
const BUNDLE_FORMAT_NAME: &str = "lance-graph-multi-type-covering-adjacency";

pub const MULTI_TYPE_COVERING_ADJACENCY_INDEX_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoveringComponentDescriptorRef {
    pub key: GraphIndexKey,
    pub component_descriptor_uri: String,
    pub component_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedMultiTypeCoveringAdjacencyDescriptor {
    pub index_uri: String,
    pub format_version: u32,
    pub metadata: MultiTypeCoveringAdjacencyMetadata,
    pub components: Vec<CoveringComponentDescriptorRef>,
}

#[derive(Debug, Clone, Default)]
pub struct MultiTypeCoveringAdjacencyLoadOptions {
    pub source_validation: BTreeMap<GraphIndexKey, GraphSourceIdentity>,
}

#[derive(Debug, Clone)]
pub struct MultiTypeCoveringAdjacencyIndexBuilder {
    index_name: String,
    bundle_generation: u64,
    components: BTreeMap<GraphIndexKey, PersistedCoveringAdjacencyDescriptor>,
}

impl MultiTypeCoveringAdjacencyIndexBuilder {
    pub fn new(index_name: impl Into<String>, bundle_generation: u64) -> Result<Self> {
        Ok(Self {
            index_name: normalize_index_name(index_name.into())?,
            bundle_generation,
            components: BTreeMap::new(),
        })
    }

    pub fn add_component(
        mut self,
        descriptor: PersistedCoveringAdjacencyDescriptor,
    ) -> Result<Self> {
        let key = descriptor.metadata.key.clone();
        if self.components.insert(key.clone(), descriptor).is_some() {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                format!("duplicate covering adjacency component for {key:?}"),
            ));
        }
        Ok(self)
    }

    pub async fn build_and_persist(
        self,
        index_uri: &str,
    ) -> Result<PersistedMultiTypeCoveringAdjacencyDescriptor> {
        MultiTypeCoveringAdjacencyIndexStore::write(
            index_uri,
            &self.index_name,
            self.bundle_generation,
            self.components.into_values().collect(),
        )
        .await
    }
}

pub struct MultiTypeCoveringAdjacencyIndexStore;

impl MultiTypeCoveringAdjacencyIndexStore {
    pub async fn write(
        index_uri: &str,
        index_name: &str,
        bundle_generation: u64,
        components: Vec<PersistedCoveringAdjacencyDescriptor>,
    ) -> Result<PersistedMultiTypeCoveringAdjacencyDescriptor> {
        if components.is_empty() {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "multi-type covering adjacency bundle requires at least one component",
            ));
        }
        let index_name = normalize_index_name(index_name.to_string())?;
        let descriptor_uri = component_uri(index_uri, BUNDLE_DESCRIPTOR_FILE);
        let (store, path) = ObjectStore::from_uri(&descriptor_uri)
            .await
            .map_err(|error| bundle_io_error(&descriptor_uri, error))?;
        if store
            .inner
            .exists(&path)
            .await
            .map_err(|error| bundle_io_error(&descriptor_uri, error))?
        {
            return Err(index_error(
                GraphIndexErrorKind::AlreadyExists,
                format!("multi-type covering adjacency bundle already exists at {index_uri}"),
            ));
        }

        let mut refs = BTreeMap::new();
        let mut num_sources = 0_u64;
        let mut num_edges = 0_u64;
        for descriptor in components {
            let on_disk =
                CoveringAdjacencyIndexStore::read_descriptor(&descriptor.index_uri).await?;
            if on_disk != descriptor {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    "covering adjacency component descriptor changed before bundle publication",
                ));
            }
            CoveringAdjacencyIndexStore::load(&descriptor, Default::default()).await?;
            let key = descriptor.metadata.key.clone();
            let reference = CoveringComponentDescriptorRef {
                key: key.clone(),
                component_descriptor_uri: portable_component_uri(index_uri, &descriptor.index_uri),
                component_generation: descriptor.metadata.generation,
            };
            if refs.insert(key.clone(), reference).is_some() {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!("duplicate covering adjacency component for {key:?}"),
                ));
            }
            num_sources = num_sources
                .checked_add(descriptor.metadata.num_sources)
                .ok_or_else(|| {
                    index_error(
                        GraphIndexErrorKind::Corrupt,
                        "covering bundle source count overflow",
                    )
                })?;
            num_edges = num_edges
                .checked_add(descriptor.metadata.num_edges)
                .ok_or_else(|| {
                    index_error(
                        GraphIndexErrorKind::Corrupt,
                        "covering bundle edge count overflow",
                    )
                })?;
        }
        let descriptor = PersistedMultiTypeCoveringAdjacencyDescriptor {
            index_uri: index_uri.into(),
            format_version: MULTI_TYPE_COVERING_ADJACENCY_INDEX_FORMAT_VERSION,
            metadata: MultiTypeCoveringAdjacencyMetadata {
                index_name,
                bundle_generation,
                num_components: refs.len() as u64,
                num_sources,
                num_edges,
            },
            components: refs.into_values().collect(),
        };
        let bytes = serde_json::to_vec_pretty(&PersistedBundle::from_public(&descriptor)).map_err(
            |error| {
                index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!("failed to serialize covering bundle: {error}"),
                )
            },
        )?;
        store
            .put(&path, &bytes)
            .await
            .map_err(|error| bundle_io_error(&descriptor_uri, error))?;
        Ok(descriptor)
    }

    pub async fn read_descriptor(
        index_uri: &str,
    ) -> Result<PersistedMultiTypeCoveringAdjacencyDescriptor> {
        read_bundle(index_uri).await?.to_public(index_uri)
    }

    pub async fn load(
        descriptor: &PersistedMultiTypeCoveringAdjacencyDescriptor,
        options: MultiTypeCoveringAdjacencyLoadOptions,
    ) -> Result<MultiTypeCoveringAdjacencyIndexHandle> {
        let on_disk = Self::read_descriptor(&descriptor.index_uri).await?;
        if &on_disk != descriptor {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "covering adjacency bundle descriptor does not match persisted metadata",
            ));
        }
        if !options.source_validation.is_empty()
            && options
                .source_validation
                .keys()
                .ne(descriptor.components.iter().map(|entry| &entry.key))
        {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "covering adjacency source validation keys do not match bundle components",
            ));
        }
        let mut components = BTreeMap::new();
        let mut num_sources = 0_u64;
        let mut num_edges = 0_u64;
        for reference in &descriptor.components {
            let uri =
                resolve_component_uri(&descriptor.index_uri, &reference.component_descriptor_uri);
            let component = CoveringAdjacencyIndexStore::read_descriptor(&uri).await?;
            if component.metadata.key != reference.key
                || component.metadata.generation != reference.component_generation
            {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!(
                        "covering component identity mismatch for {:?}",
                        reference.key
                    ),
                ));
            }
            let source_validation = options
                .source_validation
                .get(&reference.key)
                .cloned()
                .map(IndexSourceValidation::RequireExact)
                .unwrap_or(IndexSourceValidation::AllowUnknown);
            let handle = CoveringAdjacencyIndexStore::load(
                &component,
                CoveringAdjacencyLoadOptions { source_validation },
            )
            .await?;
            num_sources += handle.metadata.num_sources;
            num_edges += handle.metadata.num_edges;
            components.insert(reference.key.clone(), Arc::new(handle));
        }
        if descriptor.metadata.num_components != components.len() as u64
            || descriptor.metadata.num_sources != num_sources
            || descriptor.metadata.num_edges != num_edges
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "covering adjacency bundle aggregate counts mismatch",
            ));
        }
        Ok(MultiTypeCoveringAdjacencyIndexHandle {
            metadata: descriptor.metadata.clone(),
            components,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedBundle {
    format: String,
    format_version: u32,
    index_kind: String,
    index_name: String,
    bundle_generation: u64,
    num_components: u64,
    num_sources: u64,
    num_edges: u64,
    components: Vec<CoveringComponentDescriptorRef>,
}

impl PersistedBundle {
    fn from_public(value: &PersistedMultiTypeCoveringAdjacencyDescriptor) -> Self {
        Self {
            format: BUNDLE_FORMAT_NAME.into(),
            format_version: value.format_version,
            index_kind: "covering_adjacency_bundle".into(),
            index_name: value.metadata.index_name.clone(),
            bundle_generation: value.metadata.bundle_generation,
            num_components: value.metadata.num_components,
            num_sources: value.metadata.num_sources,
            num_edges: value.metadata.num_edges,
            components: value.components.clone(),
        }
    }

    fn to_public(&self, index_uri: &str) -> Result<PersistedMultiTypeCoveringAdjacencyDescriptor> {
        if self.format != BUNDLE_FORMAT_NAME
            || self.index_kind != "covering_adjacency_bundle"
            || self.format_version != MULTI_TYPE_COVERING_ADJACENCY_INDEX_FORMAT_VERSION
            || self.components.is_empty()
            || self.num_components != self.components.len() as u64
        {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "unsupported or invalid covering adjacency bundle descriptor",
            ));
        }
        let mut seen = BTreeMap::new();
        for component in &self.components {
            if seen.insert(component.key.clone(), ()).is_some() {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!("duplicate covering component for {:?}", component.key),
                ));
            }
        }
        Ok(PersistedMultiTypeCoveringAdjacencyDescriptor {
            index_uri: index_uri.into(),
            format_version: self.format_version,
            metadata: MultiTypeCoveringAdjacencyMetadata {
                index_name: normalize_index_name(self.index_name.clone())?,
                bundle_generation: self.bundle_generation,
                num_components: self.num_components,
                num_sources: self.num_sources,
                num_edges: self.num_edges,
            },
            components: self.components.clone(),
        })
    }
}

async fn read_bundle(index_uri: &str) -> Result<PersistedBundle> {
    let uri = component_uri(index_uri, BUNDLE_DESCRIPTOR_FILE);
    let (store, path) = ObjectStore::from_uri(&uri)
        .await
        .map_err(|error| bundle_io_error(&uri, error))?;
    if !store
        .inner
        .exists(&path)
        .await
        .map_err(|error| bundle_io_error(&uri, error))?
    {
        return Err(index_error(
            GraphIndexErrorKind::Missing,
            format!("covering adjacency bundle descriptor is missing at {index_uri}"),
        ));
    }
    let bytes = store
        .read_one_all(&path)
        .await
        .map_err(|error| bundle_io_error(&uri, error))?;
    serde_json::from_slice(&bytes).map_err(|error| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            format!("invalid covering adjacency bundle descriptor: {error}"),
        )
    })
}

fn normalize_index_name(value: String) -> Result<String> {
    let normalized = value.trim().to_lowercase();
    if normalized.is_empty() {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "covering adjacency index name must not be empty",
        ));
    }
    Ok(normalized)
}

fn portable_component_uri(bundle_uri: &str, component_uri: &str) -> String {
    component_uri
        .strip_prefix(&format!("{}/", bundle_uri.trim_end_matches('/')))
        .unwrap_or(component_uri)
        .to_string()
}

fn resolve_component_uri(bundle_uri: &str, component_ref: &str) -> String {
    if component_ref.contains("://") || component_ref.starts_with('/') {
        component_ref.into()
    } else {
        component_uri(bundle_uri, component_ref)
    }
}

fn bundle_io_error(uri: &str, error: impl std::fmt::Display) -> crate::error::GraphError {
    index_error(
        GraphIndexErrorKind::Io,
        format!("covering adjacency bundle I/O failed at {uri}: {error}"),
    )
}
