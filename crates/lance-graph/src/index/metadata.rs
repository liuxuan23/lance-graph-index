use crate::csr_index::CsrIndex;
use crate::index::covering_adjacency::CoveringAdjacencyIndex;
use arrow_schema::DataType;
use lance::Dataset;
use lance_index::scalar::ScalarIndex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IndexDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GraphIndexKey {
    pub relationship_type: String,
    pub source_label: String,
    pub target_label: String,
    pub direction: IndexDirection,
}

impl GraphIndexKey {
    pub fn new(
        relationship_type: impl Into<String>,
        source_label: impl Into<String>,
        target_label: impl Into<String>,
        direction: IndexDirection,
    ) -> Self {
        Self {
            relationship_type: relationship_type.into().to_lowercase(),
            source_label: source_label.into().to_lowercase(),
            target_label: target_label.into().to_lowercase(),
            direction,
        }
    }
}
impl PartialEq for GraphIndexKey {
    fn eq(&self, other: &Self) -> bool {
        self.relationship_type == other.relationship_type
            && self.source_label == other.source_label
            && self.target_label == other.target_label
            && self.direction == other.direction
    }
}
impl Hash for GraphIndexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.relationship_type.hash(state);
        self.source_label.hash(state);
        self.target_label.hash(state);
        self.direction.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphIndexMetadata {
    pub key: GraphIndexKey,
    pub source_id_field: String,
    pub target_id_field: String,
    pub id_data_type: DataType,
    pub num_vertices: u64,
    pub num_edges: u64,
    pub source_uri: Option<String>,
    pub source_version: Option<u64>,
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub struct CsrIndexHandle {
    pub index: Arc<CsrIndex>,
    pub metadata: GraphIndexMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectAdjacencyMetadata {
    pub key: GraphIndexKey,
    pub source_id_field: String,
    pub target_id_field: String,
    pub adjacency_field: String,
    pub id_data_type: DataType,
    pub num_sources: u64,
    pub num_edges: u64,
    pub dataset_uri: String,
    pub dataset_version: u64,
    pub scalar_index_name: String,
    pub source_uri: Option<String>,
    pub source_version: Option<u64>,
    pub generation: u64,
}

#[derive(Debug)]
pub struct DirectAdjacencyIndexHandle {
    pub dataset: Arc<Dataset>,
    pub scalar_index: Arc<dyn ScalarIndex>,
    pub metadata: DirectAdjacencyMetadata,
}

impl DirectAdjacencyIndexHandle {
    pub fn reference(
        &self,
        index_name: impl Into<String>,
        bundle_generation: u64,
    ) -> DirectAdjacencyReference {
        DirectAdjacencyReference {
            index_name: index_name.into(),
            bundle_generation,
            key: self.metadata.key.clone(),
            component_generation: self.metadata.generation,
            dataset_version: self.metadata.dataset_version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTypeDirectAdjacencyMetadata {
    pub index_name: String,
    pub bundle_generation: u64,
    pub num_components: u64,
    pub num_sources: u64,
    pub num_edges: u64,
}

#[derive(Debug)]
pub struct MultiTypeDirectAdjacencyIndexHandle {
    pub metadata: MultiTypeDirectAdjacencyMetadata,
    pub components: BTreeMap<GraphIndexKey, Arc<DirectAdjacencyIndexHandle>>,
}

impl MultiTypeDirectAdjacencyIndexHandle {
    pub fn get(&self, key: &GraphIndexKey) -> Option<Arc<DirectAdjacencyIndexHandle>> {
        self.components.get(key).cloned()
    }

    pub fn keys(&self) -> impl Iterator<Item = &GraphIndexKey> {
        self.components.keys()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexReference {
    pub key: GraphIndexKey,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DirectAdjacencyReference {
    pub index_name: String,
    pub bundle_generation: u64,
    pub key: GraphIndexKey,
    pub component_generation: u64,
    pub dataset_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoveringAdjacencyCompression {
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoveringAdjacencyMetadata {
    pub key: GraphIndexKey,
    pub source_id_field: String,
    pub target_id_field: String,
    pub source_id_data_type: DataType,
    pub target_id_data_type: DataType,
    pub num_sources: u64,
    pub num_edges: u64,
    pub max_degree: u64,
    pub generation: u64,
    pub format_version: u32,
    pub index_uri: String,
    pub entry_directory_uri: String,
    pub posting_directory_uri: String,
    pub entry_pages_uri: String,
    pub posting_pages_uri: String,
    pub entry_page_target_bytes: u64,
    pub inline_posting_threshold_bytes: u64,
    pub posting_page_target_bytes: u64,
    pub compression: CoveringAdjacencyCompression,
    pub num_entry_pages: u64,
    pub num_inline_sources: u64,
    pub num_posting_tree_sources: u64,
    pub num_posting_pages: u64,
    pub source_uri: Option<String>,
    pub source_version: Option<u64>,
}

impl CoveringAdjacencyMetadata {
    pub fn new(
        key: GraphIndexKey,
        source_id_field: impl Into<String>,
        target_id_field: impl Into<String>,
        id_data_type: DataType,
        generation: u64,
    ) -> Self {
        Self {
            key,
            source_id_field: source_id_field.into(),
            target_id_field: target_id_field.into(),
            source_id_data_type: id_data_type.clone(),
            target_id_data_type: id_data_type,
            num_sources: 0,
            num_edges: 0,
            max_degree: 0,
            generation,
            format_version:
                crate::index::covering_adjacency::COVERING_ADJACENCY_INDEX_FORMAT_VERSION,
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
        }
    }

    pub fn with_source_identity(
        mut self,
        source_uri: impl Into<String>,
        source_version: Option<u64>,
    ) -> Self {
        self.source_uri = Some(source_uri.into());
        self.source_version = source_version;
        self
    }
}

#[derive(Debug)]
pub struct CoveringAdjacencyIndexHandle {
    pub index: Arc<CoveringAdjacencyIndex>,
    pub metadata: CoveringAdjacencyMetadata,
}

impl CoveringAdjacencyIndexHandle {
    pub fn reference(
        &self,
        index_name: impl Into<String>,
        bundle_generation: u64,
    ) -> CoveringAdjacencyReference {
        CoveringAdjacencyReference {
            index_name: index_name.into(),
            bundle_generation,
            key: self.metadata.key.clone(),
            component_generation: self.metadata.generation,
            format_version: self.metadata.format_version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTypeCoveringAdjacencyMetadata {
    pub index_name: String,
    pub bundle_generation: u64,
    pub num_components: u64,
    pub num_sources: u64,
    pub num_edges: u64,
}

#[derive(Debug)]
pub struct MultiTypeCoveringAdjacencyIndexHandle {
    pub metadata: MultiTypeCoveringAdjacencyMetadata,
    pub components: BTreeMap<GraphIndexKey, Arc<CoveringAdjacencyIndexHandle>>,
}

impl MultiTypeCoveringAdjacencyIndexHandle {
    pub fn get(&self, key: &GraphIndexKey) -> Option<Arc<CoveringAdjacencyIndexHandle>> {
        self.components.get(key).cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CoveringAdjacencyReference {
    pub index_name: String,
    pub bundle_generation: u64,
    pub key: GraphIndexKey,
    pub component_generation: u64,
    pub format_version: u32,
}
