use super::{
    CsrIndexHandle, GraphIndexKey, GraphIndexMetadata, InMemoryGraphIndexRegistry, IndexDirection,
    IndexUsagePolicy,
};
use crate::csr_index::CsrIndex;
use crate::error::{GraphError, GraphIndexErrorKind, Result};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_io::object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const FORMAT_NAME: &str = "lance-graph-csr";
const COMPLETE_STATE: &str = "complete";
const MANIFEST_FILE: &str = "manifest.json";
const OFFSETS_DATASET: &str = "offsets.lance";
const NEIGHBORS_DATASET: &str = "neighbors.lance";

pub const CSR_INDEX_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedCsrIndexDescriptor {
    pub index_uri: String,
    pub format_version: u32,
    pub metadata: GraphIndexMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphSourceIdentity {
    pub uri: String,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum IndexSourceValidation {
    RequireExact(GraphSourceIdentity),
    #[default]
    AllowUnknown,
}

#[derive(Debug, Clone)]
pub struct CsrIndexWriteOptions {
    pub batch_size: usize,
}

impl Default for CsrIndexWriteOptions {
    fn default() -> Self {
        Self { batch_size: 65_536 }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CsrIndexLoadOptions {
    pub source_validation: IndexSourceValidation,
    pub max_vertices: Option<u64>,
    pub max_edges: Option<u64>,
    pub max_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CsrIndexManifest {
    format: String,
    format_version: u32,
    state: String,
    key: PersistedGraphIndexKey,
    source_id_field: String,
    target_id_field: String,
    id_data_type: PersistedVertexIdType,
    num_vertices: u64,
    num_edges: u64,
    source: Option<PersistedSourceIdentity>,
    generation: u64,
    components: PersistedComponents,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedGraphIndexKey {
    relationship_type: String,
    source_label: String,
    target_label: String,
    direction: PersistedIndexDirection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedIndexDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedVertexIdType {
    UInt32,
    UInt64,
    Int32,
    Int64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSourceIdentity {
    uri: String,
    version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedComponents {
    offsets: PersistedComponent,
    neighbors: PersistedComponent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedComponent {
    path: String,
    dataset_version: u64,
    rows: u64,
}

impl CsrIndexManifest {
    fn new(
        metadata: &GraphIndexMetadata,
        offsets_version: u64,
        neighbors_version: u64,
    ) -> Result<Self> {
        let source = match (&metadata.source_uri, metadata.source_version) {
            (Some(uri), version) => Some(PersistedSourceIdentity {
                uri: uri.clone(),
                version,
            }),
            (None, None) => None,
            (None, Some(_)) => {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    "source_version requires source_uri",
                ));
            }
        };
        Ok(Self {
            format: FORMAT_NAME.into(),
            format_version: CSR_INDEX_FORMAT_VERSION,
            state: COMPLETE_STATE.into(),
            key: PersistedGraphIndexKey::from(&metadata.key),
            source_id_field: metadata.source_id_field.clone(),
            target_id_field: metadata.target_id_field.clone(),
            id_data_type: PersistedVertexIdType::try_from(&metadata.id_data_type)?,
            num_vertices: metadata.num_vertices,
            num_edges: metadata.num_edges,
            source,
            generation: metadata.generation,
            components: PersistedComponents {
                offsets: PersistedComponent {
                    path: OFFSETS_DATASET.into(),
                    dataset_version: offsets_version,
                    rows: metadata.num_vertices,
                },
                neighbors: PersistedComponent {
                    path: NEIGHBORS_DATASET.into(),
                    dataset_version: neighbors_version,
                    rows: metadata.num_edges,
                },
            },
        })
    }

    fn validate(&self) -> Result<()> {
        if self.format != FORMAT_NAME {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!("unsupported index format {:?}", self.format),
            ));
        }
        if self.format_version != CSR_INDEX_FORMAT_VERSION {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!(
                    "unsupported CSR format version {}; expected {}",
                    self.format_version, CSR_INDEX_FORMAT_VERSION
                ),
            ));
        }
        if self.state != COMPLETE_STATE {
            return Err(index_error(
                GraphIndexErrorKind::Incomplete,
                format!("index manifest state is {:?}", self.state),
            ));
        }
        if self.components.offsets.path != OFFSETS_DATASET
            || self.components.neighbors.path != NEIGHBORS_DATASET
        {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "v1 component paths must be offsets.lance and neighbors.lance",
            ));
        }
        if self.components.offsets.rows != self.num_vertices
            || self.components.neighbors.rows != self.num_edges
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "component row counts do not match manifest dimensions",
            ));
        }
        Ok(())
    }

    fn metadata(&self) -> GraphIndexMetadata {
        GraphIndexMetadata {
            key: self.key.to_runtime(),
            source_id_field: self.source_id_field.clone(),
            target_id_field: self.target_id_field.clone(),
            id_data_type: self.id_data_type.to_runtime(),
            num_vertices: self.num_vertices,
            num_edges: self.num_edges,
            source_uri: self.source.as_ref().map(|source| source.uri.clone()),
            source_version: self.source.as_ref().and_then(|source| source.version),
            generation: self.generation,
        }
    }
}

impl From<&GraphIndexKey> for PersistedGraphIndexKey {
    fn from(key: &GraphIndexKey) -> Self {
        Self {
            relationship_type: key.relationship_type.clone(),
            source_label: key.source_label.clone(),
            target_label: key.target_label.clone(),
            direction: match key.direction {
                IndexDirection::Outgoing => PersistedIndexDirection::Outgoing,
                IndexDirection::Incoming => PersistedIndexDirection::Incoming,
            },
        }
    }
}

impl PersistedGraphIndexKey {
    fn to_runtime(&self) -> GraphIndexKey {
        GraphIndexKey::new(
            &self.relationship_type,
            &self.source_label,
            &self.target_label,
            match self.direction {
                PersistedIndexDirection::Outgoing => IndexDirection::Outgoing,
                PersistedIndexDirection::Incoming => IndexDirection::Incoming,
            },
        )
    }
}

impl TryFrom<&DataType> for PersistedVertexIdType {
    type Error = GraphError;

    fn try_from(value: &DataType) -> Result<Self> {
        match value {
            DataType::UInt32 => Ok(Self::UInt32),
            DataType::UInt64 => Ok(Self::UInt64),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            other => Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!("unsupported persisted vertex ID type {other}"),
            )),
        }
    }
}

impl PersistedVertexIdType {
    fn to_runtime(self) -> DataType {
        match self {
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
        }
    }
}

pub struct CsrIndexStore;

impl CsrIndexStore {
    pub async fn write(
        index_uri: &str,
        handle: &CsrIndexHandle,
        options: CsrIndexWriteOptions,
    ) -> Result<PersistedCsrIndexDescriptor> {
        if index_uri.trim().is_empty() {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "index URI must not be empty",
            ));
        }
        if options.batch_size == 0 {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "CSR persistence batch size must be greater than zero",
            ));
        }
        validate_handle_dimensions(handle)?;

        let (store, base_path) = ObjectStore::from_uri(index_uri)
            .await
            .map_err(|error| index_io_error(index_uri, error))?;
        let manifest_path = base_path.child(MANIFEST_FILE);
        if store
            .inner
            .exists(&manifest_path)
            .await
            .map_err(|error| index_io_error(index_uri, error))?
        {
            return Err(index_error(
                GraphIndexErrorKind::AlreadyExists,
                format!("persisted CSR generation already exists at {index_uri}"),
            ));
        }

        let offsets_uri = component_uri(index_uri, OFFSETS_DATASET);
        let neighbors_uri = component_uri(index_uri, NEIGHBORS_DATASET);
        let offsets_dataset = Dataset::write(
            OffsetsBatchReader::new(handle.index.clone(), options.batch_size),
            offsets_uri.as_str(),
            Some(create_write_params()),
        )
        .await
        .map_err(|error| index_io_error(&offsets_uri, error))?;
        let neighbors_dataset = Dataset::write(
            NeighborsBatchReader::new(handle.index.clone(), options.batch_size),
            neighbors_uri.as_str(),
            Some(create_write_params()),
        )
        .await
        .map_err(|error| index_io_error(&neighbors_uri, error))?;

        validate_dataset_rows(
            &offsets_dataset,
            handle.metadata.num_vertices,
            "offsets",
            &offsets_uri,
        )
        .await?;
        validate_dataset_rows(
            &neighbors_dataset,
            handle.metadata.num_edges,
            "neighbors",
            &neighbors_uri,
        )
        .await?;

        let manifest = CsrIndexManifest::new(
            &handle.metadata,
            offsets_dataset.version().version,
            neighbors_dataset.version().version,
        )?;
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                format!("failed to serialize CSR manifest: {error}"),
            )
        })?;
        store
            .put(&manifest_path, &manifest_bytes)
            .await
            .map_err(|error| index_io_error(index_uri, error))?;

        Ok(PersistedCsrIndexDescriptor {
            index_uri: index_uri.into(),
            format_version: CSR_INDEX_FORMAT_VERSION,
            metadata: handle.metadata.clone(),
        })
    }

    pub async fn read_descriptor(index_uri: &str) -> Result<PersistedCsrIndexDescriptor> {
        let manifest = read_manifest(index_uri).await?;
        Ok(PersistedCsrIndexDescriptor {
            index_uri: index_uri.into(),
            format_version: manifest.format_version,
            metadata: manifest.metadata(),
        })
    }

    pub async fn load(
        descriptor: &PersistedCsrIndexDescriptor,
        options: CsrIndexLoadOptions,
    ) -> Result<CsrIndexHandle> {
        let manifest = read_manifest(&descriptor.index_uri).await?;
        let persisted_metadata = manifest.metadata();
        if descriptor.format_version != manifest.format_version
            || descriptor.metadata != persisted_metadata
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "persisted descriptor does not match its manifest",
            ));
        }
        validate_source(&persisted_metadata, &options.source_validation)?;
        validate_load_limits(&persisted_metadata, &options)?;

        let offsets_uri = component_uri(&descriptor.index_uri, &manifest.components.offsets.path);
        let neighbors_uri =
            component_uri(&descriptor.index_uri, &manifest.components.neighbors.path);
        let offsets_dataset = Dataset::open(&offsets_uri)
            .await
            .map_err(|error| index_io_error(&offsets_uri, error))?;
        let neighbors_dataset = Dataset::open(&neighbors_uri)
            .await
            .map_err(|error| index_io_error(&neighbors_uri, error))?;
        if offsets_dataset.version().version != manifest.components.offsets.dataset_version
            || neighbors_dataset.version().version != manifest.components.neighbors.dataset_version
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "component dataset version does not match manifest",
            ));
        }
        validate_dataset_rows(
            &offsets_dataset,
            manifest.num_vertices,
            "offsets",
            &offsets_uri,
        )
        .await?;
        validate_dataset_rows(
            &neighbors_dataset,
            manifest.num_edges,
            "neighbors",
            &neighbors_uri,
        )
        .await?;

        let offsets = load_offsets(&offsets_dataset, &manifest).await?;
        let neighbors = load_neighbors(&neighbors_dataset, &manifest).await?;
        let index = CsrIndex::try_from_parts(offsets, neighbors, manifest.num_vertices).map_err(
            |error| {
                index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!("persisted CSR invariants failed: {error}"),
                )
            },
        )?;
        Ok(CsrIndexHandle {
            index: Arc::new(index),
            metadata: persisted_metadata,
        })
    }

    /// Load and register one persisted generation according to query policy.
    /// Returns `true` when the requested generation was registered.
    pub async fn load_into_registry(
        descriptor: &PersistedCsrIndexDescriptor,
        options: CsrIndexLoadOptions,
        registry: &InMemoryGraphIndexRegistry,
        policy: IndexUsagePolicy,
    ) -> Result<bool> {
        if policy == IndexUsagePolicy::Disabled {
            return Ok(false);
        }
        let handle = match Self::load(descriptor, options).await {
            Ok(handle) => handle,
            Err(GraphError::IndexError { .. }) if policy == IndexUsagePolicy::Prefer => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        match registry.register_loaded_csr(handle) {
            Ok(()) => Ok(true),
            Err(GraphError::IndexError { .. }) if policy == IndexUsagePolicy::Prefer => Ok(false),
            Err(error) => Err(error),
        }
    }
}

fn validate_handle_dimensions(handle: &CsrIndexHandle) -> Result<()> {
    if handle.metadata.num_vertices != handle.index.num_vertices()
        || handle.metadata.num_edges != handle.index.num_edges()
    {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "CSR metadata does not match index dimensions",
        ));
    }
    PersistedVertexIdType::try_from(&handle.metadata.id_data_type)?;
    Ok(())
}

fn validate_source(
    metadata: &GraphIndexMetadata,
    validation: &IndexSourceValidation,
) -> Result<()> {
    if let IndexSourceValidation::RequireExact(expected) = validation {
        if metadata.source_uri.as_deref() != Some(expected.uri.as_str())
            || metadata.source_version != expected.version
        {
            return Err(index_error(
                GraphIndexErrorKind::Stale,
                format!(
                    "persisted CSR source {:?}@{:?} does not match expected {}@{:?}",
                    metadata.source_uri, metadata.source_version, expected.uri, expected.version
                ),
            ));
        }
    }
    Ok(())
}

fn validate_load_limits(
    metadata: &GraphIndexMetadata,
    options: &CsrIndexLoadOptions,
) -> Result<()> {
    if options
        .max_vertices
        .is_some_and(|limit| metadata.num_vertices > limit)
    {
        return Err(index_error(
            GraphIndexErrorKind::MemoryLimitExceeded,
            format!("CSR vertices {} exceed load limit", metadata.num_vertices),
        ));
    }
    if options
        .max_edges
        .is_some_and(|limit| metadata.num_edges > limit)
    {
        return Err(index_error(
            GraphIndexErrorKind::MemoryLimitExceeded,
            format!("CSR edges {} exceed load limit", metadata.num_edges),
        ));
    }
    let elements = metadata
        .num_vertices
        .checked_add(1)
        .and_then(|value| value.checked_add(metadata.num_edges))
        .ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::MemoryLimitExceeded,
                "CSR memory estimate overflows u64",
            )
        })?;
    let bytes = elements.checked_mul(8).ok_or_else(|| {
        index_error(
            GraphIndexErrorKind::MemoryLimitExceeded,
            "CSR memory estimate overflows u64",
        )
    })?;
    if options.max_memory_bytes.is_some_and(|limit| bytes > limit) {
        return Err(index_error(
            GraphIndexErrorKind::MemoryLimitExceeded,
            format!("CSR requires at least {bytes} bytes, exceeding load limit"),
        ));
    }
    Ok(())
}

async fn read_manifest(index_uri: &str) -> Result<CsrIndexManifest> {
    let (store, base_path) = ObjectStore::from_uri(index_uri)
        .await
        .map_err(|error| index_io_error(index_uri, error))?;
    let manifest_path = base_path.child(MANIFEST_FILE);
    if !store
        .inner
        .exists(&manifest_path)
        .await
        .map_err(|error| index_io_error(index_uri, error))?
    {
        return Err(index_error(
            GraphIndexErrorKind::Missing,
            format!("CSR manifest is missing at {index_uri}"),
        ));
    }
    let bytes = store
        .read_one_all(&manifest_path)
        .await
        .map_err(|error| index_io_error(index_uri, error))?;
    let manifest: CsrIndexManifest = serde_json::from_slice(&bytes).map_err(|error| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            format!("invalid CSR manifest at {index_uri}: {error}"),
        )
    })?;
    manifest.validate()?;
    Ok(manifest)
}

async fn validate_dataset_rows(
    dataset: &Dataset,
    expected: u64,
    component: &str,
    uri: &str,
) -> Result<()> {
    let expected_schema = match component {
        "offsets" => Schema::new(vec![
            Field::new("vertex_id", DataType::UInt64, false),
            Field::new("offset", DataType::UInt64, false),
            Field::new("degree", DataType::UInt64, false),
        ]),
        "neighbors" => Schema::new(vec![
            Field::new("position", DataType::UInt64, false),
            Field::new("dst_id", DataType::UInt64, false),
        ]),
        _ => {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                format!("unknown persisted CSR component {component}"),
            ));
        }
    };
    let actual_schema = Schema::from(dataset.schema());
    if actual_schema != expected_schema {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!(
                "{component} component schema {actual_schema:?} does not match {expected_schema:?} at {uri}"
            ),
        ));
    }
    let actual = dataset
        .count_rows(None)
        .await
        .map_err(|error| index_io_error(uri, error))? as u64;
    if actual != expected {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!("{component} component has {actual} rows; expected {expected} at {uri}"),
        ));
    }
    Ok(())
}

async fn load_offsets(dataset: &Dataset, manifest: &CsrIndexManifest) -> Result<Vec<u64>> {
    let capacity = usize::try_from(manifest.num_vertices)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::MemoryLimitExceeded,
                "offset vector length does not fit in usize",
            )
        })?;
    let mut offsets = Vec::with_capacity(capacity);
    let mut expected_vertex = 0u64;
    let mut expected_offset = 0u64;
    let mut stream = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|error| index_io_error(OFFSETS_DATASET, error))?;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| index_io_error(OFFSETS_DATASET, error))?
    {
        let vertices = required_u64_column(&batch, "vertex_id", OFFSETS_DATASET)?;
        let batch_offsets = required_u64_column(&batch, "offset", OFFSETS_DATASET)?;
        let degrees = required_u64_column(&batch, "degree", OFFSETS_DATASET)?;
        for row in 0..batch.num_rows() {
            if vertices.value(row) != expected_vertex || batch_offsets.value(row) != expected_offset
            {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!(
                        "invalid offset row {expected_vertex}: vertex={}, offset={}, expected offset={expected_offset}",
                        vertices.value(row),
                        batch_offsets.value(row)
                    ),
                ));
            }
            offsets.push(expected_offset);
            expected_offset = expected_offset
                .checked_add(degrees.value(row))
                .ok_or_else(|| {
                    index_error(GraphIndexErrorKind::Corrupt, "CSR degree sum overflows u64")
                })?;
            expected_vertex += 1;
        }
    }
    if expected_vertex != manifest.num_vertices || expected_offset != manifest.num_edges {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!(
                "offset component ended at vertex {expected_vertex}, edge {expected_offset}; expected {}, {}",
                manifest.num_vertices, manifest.num_edges
            ),
        ));
    }
    offsets.push(manifest.num_edges);
    Ok(offsets)
}

async fn load_neighbors(dataset: &Dataset, manifest: &CsrIndexManifest) -> Result<Vec<u64>> {
    let capacity = usize::try_from(manifest.num_edges).map_err(|_| {
        index_error(
            GraphIndexErrorKind::MemoryLimitExceeded,
            "neighbor vector length does not fit in usize",
        )
    })?;
    let mut neighbors = Vec::with_capacity(capacity);
    let mut expected_position = 0u64;
    let mut stream = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|error| index_io_error(NEIGHBORS_DATASET, error))?;
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| index_io_error(NEIGHBORS_DATASET, error))?
    {
        let positions = required_u64_column(&batch, "position", NEIGHBORS_DATASET)?;
        let destinations = required_u64_column(&batch, "dst_id", NEIGHBORS_DATASET)?;
        for row in 0..batch.num_rows() {
            if positions.value(row) != expected_position {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    format!(
                        "neighbor position {} is not expected position {expected_position}",
                        positions.value(row)
                    ),
                ));
            }
            neighbors.push(destinations.value(row));
            expected_position += 1;
        }
    }
    if expected_position != manifest.num_edges {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!(
                "neighbor component has {expected_position} rows; expected {}",
                manifest.num_edges
            ),
        ));
    }
    Ok(neighbors)
}

fn required_u64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    component: &str,
) -> Result<&'a UInt64Array> {
    let array = batch.column_by_name(name).ok_or_else(|| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            format!("{component} component is missing {name}"),
        )
    })?;
    let values = array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                format!("{component}.{name} must be UInt64"),
            )
        })?;
    if values.null_count() != 0 {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!("{component}.{name} must not contain nulls"),
        ));
    }
    Ok(values)
}

fn component_uri(index_uri: &str, component: &str) -> String {
    format!("{}/{}", index_uri.trim_end_matches('/'), component)
}

fn create_write_params() -> WriteParams {
    WriteParams {
        mode: WriteMode::Create,
        ..Default::default()
    }
}

fn index_error(kind: GraphIndexErrorKind, message: impl Into<String>) -> GraphError {
    GraphError::IndexError {
        kind,
        message: message.into(),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn index_io_error(uri: &str, error: impl std::fmt::Display) -> GraphError {
    index_error(
        GraphIndexErrorKind::Io,
        format!("persisted CSR I/O failed at {uri}: {error}"),
    )
}

struct OffsetsBatchReader {
    index: Arc<CsrIndex>,
    schema: SchemaRef,
    batch_size: usize,
    cursor: u64,
    offset: u64,
}

impl OffsetsBatchReader {
    fn new(index: Arc<CsrIndex>, batch_size: usize) -> Self {
        Self {
            index,
            schema: Arc::new(Schema::new(vec![
                Field::new("vertex_id", DataType::UInt64, false),
                Field::new("offset", DataType::UInt64, false),
                Field::new("degree", DataType::UInt64, false),
            ])),
            batch_size,
            cursor: 0,
            offset: 0,
        }
    }
}

impl Iterator for OffsetsBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.index.num_vertices() {
            return None;
        }
        let remaining = self.index.num_vertices() - self.cursor;
        let rows = remaining.min(self.batch_size as u64) as usize;
        let mut vertices = Vec::with_capacity(rows);
        let mut offsets = Vec::with_capacity(rows);
        let mut degrees = Vec::with_capacity(rows);
        for _ in 0..rows {
            let degree = self.index.neighbors(self.cursor).len() as u64;
            vertices.push(self.cursor);
            offsets.push(self.offset);
            degrees.push(degree);
            self.cursor += 1;
            self.offset += degree;
        }
        Some(RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vertices)),
                Arc::new(UInt64Array::from(offsets)),
                Arc::new(UInt64Array::from(degrees)),
            ],
        ))
    }
}

impl RecordBatchReader for OffsetsBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct NeighborsBatchReader {
    index: Arc<CsrIndex>,
    schema: SchemaRef,
    batch_size: usize,
    vertex: u64,
    within_vertex: usize,
    position: u64,
}

impl NeighborsBatchReader {
    fn new(index: Arc<CsrIndex>, batch_size: usize) -> Self {
        Self {
            index,
            schema: Arc::new(Schema::new(vec![
                Field::new("position", DataType::UInt64, false),
                Field::new("dst_id", DataType::UInt64, false),
            ])),
            batch_size,
            vertex: 0,
            within_vertex: 0,
            position: 0,
        }
    }
}

impl Iterator for NeighborsBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.index.num_edges() {
            return None;
        }
        let remaining = self.index.num_edges() - self.position;
        let rows = remaining.min(self.batch_size as u64) as usize;
        let mut positions = Vec::with_capacity(rows);
        let mut destinations = Vec::with_capacity(rows);
        while destinations.len() < rows {
            let vertex_neighbors = self.index.neighbors(self.vertex);
            if self.within_vertex == vertex_neighbors.len() {
                self.vertex += 1;
                self.within_vertex = 0;
                continue;
            }
            let take = (rows - destinations.len())
                .min(vertex_neighbors.len().saturating_sub(self.within_vertex));
            for &destination in &vertex_neighbors[self.within_vertex..self.within_vertex + take] {
                positions.push(self.position);
                destinations.push(destination);
                self.position += 1;
            }
            self.within_vertex += take;
        }
        Some(RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(UInt64Array::from(positions)),
                Arc::new(UInt64Array::from(destinations)),
            ],
        ))
    }
}

impl RecordBatchReader for NeighborsBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CsrIndexBuilder, CypherQuery, GraphConfig};
    use arrow_array::{Int64Array, StringArray};
    use datafusion::datasource::{DefaultTableSource, MemTable};
    use datafusion::execution::context::SessionContext;
    use lance_graph_catalog::InMemoryCatalog;
    use tempfile::TempDir;

    fn sample_handle(source_uri: Option<String>, source_version: Option<u64>) -> CsrIndexHandle {
        let index = CsrIndexBuilder::new()
            .with_num_vertices(6)
            .add_edge(0, 1)
            .add_edge(0, 1)
            .add_edge(1, 1)
            .add_edge(4, 5)
            .try_build()
            .unwrap();
        CsrIndexHandle {
            index: Arc::new(index),
            metadata: GraphIndexMetadata {
                key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
                source_id_field: "id".into(),
                target_id_field: "id".into(),
                id_data_type: DataType::Int64,
                num_vertices: 6,
                num_edges: 4,
                source_uri,
                source_version,
                generation: 7,
            },
        }
    }

    #[tokio::test]
    async fn persisted_csr_round_trip_survives_dropped_runtime_objects() {
        let temp_dir = TempDir::new().unwrap();
        let index_uri = temp_dir.path().join("generation-7");
        let index_uri = index_uri.to_str().unwrap().to_string();
        let expected_neighbors = {
            let handle = sample_handle(Some("memory://knows".into()), Some(12));
            let expected: Vec<Vec<u64>> = (0..handle.index.num_vertices())
                .map(|vertex| handle.index.neighbors(vertex).to_vec())
                .collect();
            let descriptor =
                CsrIndexStore::write(&index_uri, &handle, CsrIndexWriteOptions { batch_size: 2 })
                    .await
                    .unwrap();
            assert_eq!(descriptor.metadata, handle.metadata);
            expected
        };

        let descriptor = CsrIndexStore::read_descriptor(&index_uri).await.unwrap();
        let loaded = CsrIndexStore::load(
            &descriptor,
            CsrIndexLoadOptions {
                source_validation: IndexSourceValidation::RequireExact(GraphSourceIdentity {
                    uri: "memory://knows".into(),
                    version: Some(12),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(loaded.metadata, descriptor.metadata);
        for vertex in 0..loaded.index.num_vertices() {
            assert_eq!(
                loaded.index.neighbors(vertex),
                expected_neighbors[vertex as usize]
            );
        }
        assert!(temp_dir.path().join("generation-7/manifest.json").is_file());
        assert!(temp_dir.path().join("generation-7/offsets.lance").is_dir());
        assert!(temp_dir
            .path()
            .join("generation-7/neighbors.lance")
            .is_dir());
    }

    #[tokio::test]
    async fn persisted_csr_rejects_missing_stale_and_memory_limited_loads() {
        let temp_dir = TempDir::new().unwrap();
        let missing_uri = temp_dir.path().join("missing");
        let error = CsrIndexStore::read_descriptor(missing_uri.to_str().unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Missing,
                ..
            }
        ));

        let index_uri = temp_dir.path().join("generation-7");
        let index_uri = index_uri.to_str().unwrap().to_string();
        let handle = sample_handle(Some("memory://knows".into()), Some(12));
        let descriptor = CsrIndexStore::write(&index_uri, &handle, Default::default())
            .await
            .unwrap();

        let stale = CsrIndexStore::load(
            &descriptor,
            CsrIndexLoadOptions {
                source_validation: IndexSourceValidation::RequireExact(GraphSourceIdentity {
                    uri: "memory://knows".into(),
                    version: Some(13),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            stale,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Stale,
                ..
            }
        ));

        let registry = InMemoryGraphIndexRegistry::new();
        let loaded = CsrIndexStore::load_into_registry(
            &descriptor,
            CsrIndexLoadOptions {
                source_validation: IndexSourceValidation::RequireExact(GraphSourceIdentity {
                    uri: "memory://knows".into(),
                    version: Some(13),
                }),
                ..Default::default()
            },
            &registry,
            IndexUsagePolicy::Prefer,
        )
        .await
        .unwrap();
        assert!(!loaded);

        let limited = CsrIndexStore::load(
            &descriptor,
            CsrIndexLoadOptions {
                max_memory_bytes: Some(8),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            limited,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::MemoryLimitExceeded,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn persisted_generation_is_immutable() {
        let temp_dir = TempDir::new().unwrap();
        let index_uri = temp_dir.path().join("generation-7");
        let index_uri = index_uri.to_str().unwrap();
        let handle = sample_handle(None, None);
        CsrIndexStore::write(index_uri, &handle, Default::default())
            .await
            .unwrap();
        let error = CsrIndexStore::write(index_uri, &handle, Default::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::AlreadyExists,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn empty_persisted_csr_round_trips() {
        let temp_dir = TempDir::new().unwrap();
        let index_uri = temp_dir.path().join("empty-generation");
        let index_uri = index_uri.to_str().unwrap();
        let handle = CsrIndexHandle {
            index: Arc::new(
                CsrIndexBuilder::new()
                    .with_num_vertices(0)
                    .try_build()
                    .unwrap(),
            ),
            metadata: GraphIndexMetadata {
                key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
                source_id_field: "id".into(),
                target_id_field: "id".into(),
                id_data_type: DataType::UInt64,
                num_vertices: 0,
                num_edges: 0,
                source_uri: None,
                source_version: None,
                generation: 1,
            },
        };
        let descriptor = CsrIndexStore::write(index_uri, &handle, Default::default())
            .await
            .unwrap();
        let loaded = CsrIndexStore::load(&descriptor, Default::default())
            .await
            .unwrap();
        assert_eq!(loaded.index.num_vertices(), 0);
        assert_eq!(loaded.index.num_edges(), 0);
    }

    #[tokio::test]
    async fn corrupt_manifest_is_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let index_uri = temp_dir.path().join("generation-7");
        let index_uri_string = index_uri.to_str().unwrap().to_string();
        let handle = sample_handle(None, None);
        CsrIndexStore::write(&index_uri_string, &handle, Default::default())
            .await
            .unwrap();
        std::fs::write(index_uri.join(MANIFEST_FILE), b"not-json").unwrap();
        let error = CsrIndexStore::read_descriptor(&index_uri_string)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn incompatible_version_and_manifest_row_mismatch_are_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let index_uri = temp_dir.path().join("generation-7");
        let index_uri_string = index_uri.to_str().unwrap().to_string();
        let handle = sample_handle(None, None);
        CsrIndexStore::write(&index_uri_string, &handle, Default::default())
            .await
            .unwrap();
        let manifest_path = index_uri.join(MANIFEST_FILE);
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();

        manifest["format_version"] = serde_json::Value::from(2);
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let error = CsrIndexStore::read_descriptor(&index_uri_string)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Incompatible,
                ..
            }
        ));

        manifest["format_version"] = serde_json::Value::from(CSR_INDEX_FORMAT_VERSION);
        manifest["components"]["neighbors"]["rows"] = serde_json::Value::from(5);
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let error = CsrIndexStore::read_descriptor(&index_uri_string)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn incomplete_manifest_and_changed_component_are_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let incomplete_uri = temp_dir.path().join("incomplete-generation");
        let incomplete_uri_string = incomplete_uri.to_str().unwrap().to_string();
        let handle = sample_handle(None, None);
        CsrIndexStore::write(&incomplete_uri_string, &handle, Default::default())
            .await
            .unwrap();
        let manifest_path = incomplete_uri.join(MANIFEST_FILE);
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        manifest["state"] = serde_json::Value::String("writing".into());
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let error = CsrIndexStore::read_descriptor(&incomplete_uri_string)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Incomplete,
                ..
            }
        ));

        let changed_uri = temp_dir.path().join("changed-generation");
        let changed_uri_string = changed_uri.to_str().unwrap().to_string();
        let descriptor = CsrIndexStore::write(&changed_uri_string, &handle, Default::default())
            .await
            .unwrap();
        let offsets_uri = component_uri(&changed_uri_string, OFFSETS_DATASET);
        Dataset::write(
            OffsetsBatchReader::new(handle.index.clone(), 2),
            offsets_uri.as_str(),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let error = CsrIndexStore::load(&descriptor, Default::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn persisted_csr_executes_indexed_expand_after_runtime_reset() {
        let temp_dir = TempDir::new().unwrap();
        let index_uri = temp_dir.path().join("generation-1");
        let index_uri = index_uri.to_str().unwrap().to_string();

        let descriptor = {
            let index = CsrIndexBuilder::new()
                .with_num_vertices(3)
                .add_edge(0, 1)
                .add_edge(0, 2)
                .try_build()
                .unwrap();
            let handle = CsrIndexHandle {
                index: Arc::new(index),
                metadata: GraphIndexMetadata {
                    key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
                    source_id_field: "id".into(),
                    target_id_field: "id".into(),
                    id_data_type: DataType::Int64,
                    num_vertices: 3,
                    num_edges: 2,
                    source_uri: None,
                    source_version: None,
                    generation: 1,
                },
            };
            CsrIndexStore::write(&index_uri, &handle, Default::default())
                .await
                .unwrap()
        };

        // The builder, original CsrIndex, and handle are gone before loading.
        let registry = Arc::new(InMemoryGraphIndexRegistry::new());
        assert!(CsrIndexStore::load_into_registry(
            &descriptor,
            Default::default(),
            registry.as_ref(),
            IndexUsagePolicy::Require,
        )
        .await
        .unwrap());

        let node_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let nodes = RecordBatch::try_new(
            node_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            ],
        )
        .unwrap();
        let edge_schema = Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ]));
        let edges = RecordBatch::try_new(
            edge_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let node_table = Arc::new(MemTable::try_new(node_schema, vec![vec![nodes]]).unwrap());
        let edge_table = Arc::new(MemTable::try_new(edge_schema, vec![vec![edges]]).unwrap());
        let indexed_context = SessionContext::new();
        indexed_context
            .register_table("person", node_table.clone())
            .unwrap();
        indexed_context
            .register_table("knows", edge_table.clone())
            .unwrap();
        let join_context = SessionContext::new();
        join_context
            .register_table("person", node_table.clone())
            .unwrap();
        join_context
            .register_table("knows", edge_table.clone())
            .unwrap();
        let catalog = Arc::new(
            InMemoryCatalog::new()
                .with_node_source("Person", Arc::new(DefaultTableSource::new(node_table)))
                .with_relationship_source("KNOWS", Arc::new(DefaultTableSource::new(edge_table))),
        );
        let query = CypherQuery::new("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
            .unwrap()
            .with_config(
                GraphConfig::builder()
                    .with_node_label("Person", "id")
                    .with_relationship("KNOWS", "src_id", "dst_id")
                    .build()
                    .unwrap(),
            );

        let join = query
            .execute_with_catalog_and_context(catalog.clone(), join_context)
            .await
            .unwrap();
        let indexed = query
            .execute_with_catalog_context_and_indexes(
                catalog,
                indexed_context,
                registry,
                IndexUsagePolicy::Require,
            )
            .await
            .unwrap();
        assert_eq!(string_pairs(&join), string_pairs(&indexed));
    }

    fn string_pairs(batch: &RecordBatch) -> Vec<(String, String)> {
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut rows: Vec<_> = (0..batch.num_rows())
            .map(|row| (left.value(row).to_string(), right.value(row).to_string()))
            .collect();
        rows.sort();
        rows
    }
}
