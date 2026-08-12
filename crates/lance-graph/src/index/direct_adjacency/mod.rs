use super::{
    DirectAdjacencyIndexHandle, DirectAdjacencyMetadata, GraphIndexKey, IndexDirection,
    IndexSourceValidation,
};
use crate::error::{GraphError, GraphIndexErrorKind, Result};
use arrow_array::builder::{Int32Builder, Int64Builder, ListBuilder, UInt32Builder, UInt64Builder};
use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, RecordBatchIterator,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::index::DatasetIndexInternalExt;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};
use lance_io::object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

const DESCRIPTOR_FILE: &str = "descriptor.json";
const DATASET_NAME: &str = "adjacency.lance";
const FORMAT_NAME: &str = "lance-graph-direct-adjacency";

pub const DIRECT_ADJACENCY_INDEX_FORMAT_VERSION: u32 = 1;

mod bundle;

pub use bundle::{
    DirectAdjacencyComponentDescriptorRef, MultiTypeDirectAdjacencyIndexBuilder,
    MultiTypeDirectAdjacencyIndexStore, MultiTypeDirectAdjacencyLoadOptions,
    MultiTypeSourceValidation, PersistedMultiTypeDirectAdjacencyDescriptor,
    MULTI_TYPE_DIRECT_ADJACENCY_INDEX_FORMAT_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedDirectAdjacencyDescriptor {
    pub index_uri: String,
    pub format_version: u32,
    pub metadata: DirectAdjacencyMetadata,
}

#[derive(Debug, Clone, Copy)]
pub struct DirectAdjacencyWriteOptions {
    pub batch_size: usize,
    /// Optional Lance fragment size, useful for controlling locality in benchmarks.
    pub max_rows_per_file: Option<usize>,
}

impl Default for DirectAdjacencyWriteOptions {
    fn default() -> Self {
        Self {
            batch_size: 65_536,
            max_rows_per_file: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DirectAdjacencyLoadOptions {
    pub source_validation: IndexSourceValidation,
}

#[derive(Debug, Clone)]
enum EdgeBuffer {
    UInt32(Vec<(u32, u32)>),
    UInt64(Vec<(u64, u64)>),
    Int32(Vec<(i32, i32)>),
    Int64(Vec<(i64, i64)>),
}

impl EdgeBuffer {
    fn new(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::UInt32 => Ok(Self::UInt32(Vec::new())),
            DataType::UInt64 => Ok(Self::UInt64(Vec::new())),
            DataType::Int32 => Ok(Self::Int32(Vec::new())),
            DataType::Int64 => Ok(Self::Int64(Vec::new())),
            other => Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!("direct adjacency does not support ID type {other}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DirectAdjacencyIndexBuilder {
    metadata: DirectAdjacencyMetadata,
    edges: EdgeBuffer,
}

impl DirectAdjacencyIndexBuilder {
    pub fn new(metadata: DirectAdjacencyMetadata) -> Result<Self> {
        let edges = EdgeBuffer::new(&metadata.id_data_type)?;
        Ok(Self { metadata, edges })
    }

    pub fn add_edges_from_batch(mut self, batch: &RecordBatch) -> Result<Self> {
        let src = batch.column_by_name("src_id").ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::Incompatible,
                "edge batch is missing src_id",
            )
        })?;
        let dst = batch.column_by_name("dst_id").ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::Incompatible,
                "edge batch is missing dst_id",
            )
        })?;
        if src.data_type() != &self.metadata.id_data_type
            || dst.data_type() != &self.metadata.id_data_type
        {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!(
                    "edge endpoint types {:?}/{:?} do not match direct adjacency ID type {:?}",
                    src.data_type(),
                    dst.data_type(),
                    self.metadata.id_data_type
                ),
            ));
        }
        if src.null_count() != 0 || dst.null_count() != 0 {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "direct adjacency endpoints must be non-null",
            ));
        }

        macro_rules! append_edges {
            ($buffer:expr, $array_ty:ty) => {{
                let src = src.as_any().downcast_ref::<$array_ty>().unwrap();
                let dst = dst.as_any().downcast_ref::<$array_ty>().unwrap();
                $buffer.extend((0..batch.num_rows()).map(|row| (src.value(row), dst.value(row))));
            }};
        }
        match &mut self.edges {
            EdgeBuffer::UInt32(edges) => append_edges!(edges, UInt32Array),
            EdgeBuffer::UInt64(edges) => append_edges!(edges, UInt64Array),
            EdgeBuffer::Int32(edges) => append_edges!(edges, Int32Array),
            EdgeBuffer::Int64(edges) => append_edges!(edges, Int64Array),
        }
        Ok(self)
    }

    pub async fn build_and_persist(
        mut self,
        index_uri: &str,
        options: DirectAdjacencyWriteOptions,
    ) -> Result<PersistedDirectAdjacencyDescriptor> {
        if options.batch_size == 0 {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "direct adjacency batch size must be greater than zero",
            ));
        }

        macro_rules! build_batches {
            ($edges:expr, $builder_ty:ty, $array_ty:ty) => {{
                $edges.sort_by_key(|(src, _)| *src);
                group_edges::<$builder_ty, $array_ty>($edges, &self.metadata, options.batch_size)?
            }};
        }
        let (batches, num_sources, num_edges) = match &mut self.edges {
            EdgeBuffer::UInt32(edges) => build_batches!(edges, UInt32Builder, UInt32Array),
            EdgeBuffer::UInt64(edges) => build_batches!(edges, UInt64Builder, UInt64Array),
            EdgeBuffer::Int32(edges) => build_batches!(edges, Int32Builder, Int32Array),
            EdgeBuffer::Int64(edges) => build_batches!(edges, Int64Builder, Int64Array),
        };
        self.metadata.num_sources = num_sources;
        self.metadata.num_edges = num_edges;
        DirectAdjacencyIndexStore::write_batches(index_uri, self.metadata, batches, options).await
    }
}

trait DirectIdArray: Array + From<Vec<Self::Native>> + 'static {
    type Native: ArrowId;
}

trait ArrowId: Copy + Eq + Send + Sync + 'static {}
impl ArrowId for u32 {}
impl ArrowId for u64 {}
impl ArrowId for i32 {}
impl ArrowId for i64 {}

impl DirectIdArray for UInt32Array {
    type Native = u32;
}
impl DirectIdArray for UInt64Array {
    type Native = u64;
}
impl DirectIdArray for Int32Array {
    type Native = i32;
}
impl DirectIdArray for Int64Array {
    type Native = i64;
}

trait DirectIdBuilder: arrow_array::builder::ArrayBuilder + Default {
    type Native: ArrowId;
    fn append_id(&mut self, value: Self::Native);
}

macro_rules! impl_id_builder {
    ($builder:ty, $native:ty) => {
        impl DirectIdBuilder for $builder {
            type Native = $native;
            fn append_id(&mut self, value: Self::Native) {
                self.append_value(value);
            }
        }
    };
}
impl_id_builder!(UInt32Builder, u32);
impl_id_builder!(UInt64Builder, u64);
impl_id_builder!(Int32Builder, i32);
impl_id_builder!(Int64Builder, i64);

fn group_edges<B, A>(
    edges: &[(A::Native, A::Native)],
    metadata: &DirectAdjacencyMetadata,
    batch_size: usize,
) -> Result<(Vec<RecordBatch>, u64, u64)>
where
    B: DirectIdBuilder<Native = A::Native>,
    A: DirectIdArray,
{
    let mut batches = Vec::new();
    let mut source_ids = Vec::new();
    let mut lists = ListBuilder::new(B::default());
    let mut current_source = None;
    for &(src, dst) in edges {
        if current_source != Some(src) {
            if let Some(previous) = current_source {
                source_ids.push(previous);
                lists.append(true);
                if source_ids.len() >= batch_size {
                    batches.push(make_batch::<B, A>(&source_ids, &mut lists, metadata)?);
                    source_ids.clear();
                }
            }
            current_source = Some(src);
        }
        lists.values().append_id(dst);
    }
    if let Some(last) = current_source {
        source_ids.push(last);
        lists.append(true);
    }
    if !source_ids.is_empty() || batches.is_empty() {
        batches.push(make_batch::<B, A>(&source_ids, &mut lists, metadata)?);
    }
    Ok((batches, source_ids_count(edges), edges.len() as u64))
}

fn source_ids_count<T: Eq + Copy>(edges: &[(T, T)]) -> u64 {
    edges
        .iter()
        .enumerate()
        .filter(|(index, (src, _))| *index == 0 || *src != edges[*index - 1].0)
        .count() as u64
}

fn make_batch<B, A>(
    source_ids: &[A::Native],
    lists: &mut ListBuilder<B>,
    metadata: &DirectAdjacencyMetadata,
) -> Result<RecordBatch>
where
    B: DirectIdBuilder<Native = A::Native>,
    A: DirectIdArray,
{
    let values = lists.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            metadata.source_id_field.clone(),
            metadata.id_data_type.clone(),
            false,
        ),
        Field::new(
            metadata.adjacency_field.clone(),
            values.data_type().clone(),
            false,
        ),
    ]));
    let source: ArrayRef = Arc::new(A::from(source_ids.to_vec()));
    RecordBatch::try_new(schema, vec![source, Arc::new(values)]).map_err(Into::into)
}

pub struct DirectAdjacencyIndexStore;

impl DirectAdjacencyIndexStore {
    async fn write_batches(
        index_uri: &str,
        mut metadata: DirectAdjacencyMetadata,
        batches: Vec<RecordBatch>,
        options: DirectAdjacencyWriteOptions,
    ) -> Result<PersistedDirectAdjacencyDescriptor> {
        if index_uri.trim().is_empty() {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "direct adjacency index URI must not be empty",
            ));
        }
        let (actual_sources, actual_edges) = validate_batches(&metadata, &batches)?;
        if metadata.num_sources != actual_sources || metadata.num_edges != actual_edges {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                format!(
                    "direct adjacency metadata counts {}/{} do not match batches {actual_sources}/{actual_edges}",
                    metadata.num_sources, metadata.num_edges
                ),
            ));
        }

        let (store, base_path) = ObjectStore::from_uri(index_uri)
            .await
            .map_err(|error| index_io_error(index_uri, error))?;
        let descriptor_path = base_path.child(DESCRIPTOR_FILE);
        if store
            .inner
            .exists(&descriptor_path)
            .await
            .map_err(|error| index_io_error(index_uri, error))?
        {
            return Err(index_error(
                GraphIndexErrorKind::AlreadyExists,
                format!("direct adjacency generation already exists at {index_uri}"),
            ));
        }

        let dataset_uri = component_uri(index_uri, DATASET_NAME);
        let schema = batches[0].schema();
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let mut write_params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };
        if let Some(max_rows_per_file) = options.max_rows_per_file {
            if max_rows_per_file == 0 {
                return Err(index_error(
                    GraphIndexErrorKind::Incompatible,
                    "direct adjacency max_rows_per_file must be greater than zero",
                ));
            }
            write_params.max_rows_per_file = max_rows_per_file;
        }
        let mut dataset = Dataset::write(reader, &dataset_uri, Some(write_params))
            .await
            .map_err(|error| index_io_error(&dataset_uri, error))?;
        validate_dataset(&dataset, &metadata, actual_sources, actual_edges).await?;
        dataset
            .create_index(
                &[metadata.source_id_field.as_str()],
                IndexType::BTree,
                Some(metadata.scalar_index_name.clone()),
                &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
                false,
            )
            .await
            .map_err(|error| index_io_error(&dataset_uri, error))?;
        metadata.dataset_uri = dataset_uri;
        metadata.dataset_version = dataset.version().version;
        validate_scalar_index(&dataset, &metadata).await?;

        let persisted = PersistedDescriptor::new(&metadata)?;
        let bytes = serde_json::to_vec_pretty(&persisted).map_err(|error| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                format!("failed to serialize direct adjacency descriptor: {error}"),
            )
        })?;
        store
            .put(&descriptor_path, &bytes)
            .await
            .map_err(|error| index_io_error(index_uri, error))?;
        Ok(PersistedDirectAdjacencyDescriptor {
            index_uri: index_uri.into(),
            format_version: DIRECT_ADJACENCY_INDEX_FORMAT_VERSION,
            metadata,
        })
    }

    pub async fn write(
        index_uri: &str,
        metadata: DirectAdjacencyMetadata,
        batches: Vec<RecordBatch>,
    ) -> Result<PersistedDirectAdjacencyDescriptor> {
        Self::write_batches(
            index_uri,
            metadata,
            batches,
            DirectAdjacencyWriteOptions::default(),
        )
        .await
    }

    pub async fn read_descriptor(index_uri: &str) -> Result<PersistedDirectAdjacencyDescriptor> {
        let persisted = read_persisted_descriptor(index_uri).await?;
        Ok(PersistedDirectAdjacencyDescriptor {
            index_uri: index_uri.into(),
            format_version: persisted.format_version,
            metadata: persisted.metadata()?,
        })
    }

    pub async fn load(
        descriptor: &PersistedDirectAdjacencyDescriptor,
        options: DirectAdjacencyLoadOptions,
    ) -> Result<DirectAdjacencyIndexHandle> {
        let persisted = read_persisted_descriptor(&descriptor.index_uri).await?;
        let metadata = persisted.metadata()?;
        if descriptor.format_version != persisted.format_version || descriptor.metadata != metadata
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency descriptor does not match persisted metadata",
            ));
        }
        validate_source(&metadata, &options.source_validation)?;

        let latest = Dataset::open(&metadata.dataset_uri)
            .await
            .map_err(|error| index_io_error(&metadata.dataset_uri, error))?;
        let dataset = latest
            .checkout_version(metadata.dataset_version)
            .await
            .map_err(|error| index_io_error(&metadata.dataset_uri, error))?;
        // The immutable descriptor is write-time validated. Loading only checks
        // the pinned schema/row count and index coverage, avoiding a full List
        // scan on every process restart.
        validate_dataset_shape(&dataset, &metadata, metadata.num_sources).await?;
        let scalar_index = validate_scalar_index(&dataset, &metadata).await?;
        Ok(DirectAdjacencyIndexHandle {
            dataset: Arc::new(dataset),
            scalar_index,
            metadata,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedDescriptor {
    format: String,
    format_version: u32,
    index_kind: String,
    metadata: PersistedMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMetadata {
    key: PersistedKey,
    source_id_field: String,
    target_id_field: String,
    adjacency_field: String,
    id_data_type: PersistedIdType,
    num_sources: u64,
    num_edges: u64,
    dataset_uri: String,
    dataset_version: u64,
    scalar_index_name: String,
    source_uri: Option<String>,
    source_version: Option<u64>,
    generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedKey {
    relationship_type: String,
    source_label: String,
    target_label: String,
    direction: PersistedDirection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedIdType {
    UInt32,
    UInt64,
    Int32,
    Int64,
}

impl PersistedDescriptor {
    fn new(metadata: &DirectAdjacencyMetadata) -> Result<Self> {
        Ok(Self {
            format: FORMAT_NAME.into(),
            format_version: DIRECT_ADJACENCY_INDEX_FORMAT_VERSION,
            index_kind: "direct_adjacency".into(),
            metadata: PersistedMetadata {
                key: PersistedKey {
                    relationship_type: metadata.key.relationship_type.clone(),
                    source_label: metadata.key.source_label.clone(),
                    target_label: metadata.key.target_label.clone(),
                    direction: match metadata.key.direction {
                        IndexDirection::Outgoing => PersistedDirection::Outgoing,
                        IndexDirection::Incoming => PersistedDirection::Incoming,
                    },
                },
                source_id_field: metadata.source_id_field.clone(),
                target_id_field: metadata.target_id_field.clone(),
                adjacency_field: metadata.adjacency_field.clone(),
                id_data_type: PersistedIdType::try_from(&metadata.id_data_type)?,
                num_sources: metadata.num_sources,
                num_edges: metadata.num_edges,
                dataset_uri: metadata.dataset_uri.clone(),
                dataset_version: metadata.dataset_version,
                scalar_index_name: metadata.scalar_index_name.clone(),
                source_uri: metadata.source_uri.clone(),
                source_version: metadata.source_version,
                generation: metadata.generation,
            },
        })
    }

    fn metadata(&self) -> Result<DirectAdjacencyMetadata> {
        self.validate()?;
        Ok(DirectAdjacencyMetadata {
            key: GraphIndexKey::new(
                &self.metadata.key.relationship_type,
                &self.metadata.key.source_label,
                &self.metadata.key.target_label,
                match self.metadata.key.direction {
                    PersistedDirection::Outgoing => IndexDirection::Outgoing,
                    PersistedDirection::Incoming => IndexDirection::Incoming,
                },
            ),
            source_id_field: self.metadata.source_id_field.clone(),
            target_id_field: self.metadata.target_id_field.clone(),
            adjacency_field: self.metadata.adjacency_field.clone(),
            id_data_type: DataType::from(self.metadata.id_data_type),
            num_sources: self.metadata.num_sources,
            num_edges: self.metadata.num_edges,
            dataset_uri: self.metadata.dataset_uri.clone(),
            dataset_version: self.metadata.dataset_version,
            scalar_index_name: self.metadata.scalar_index_name.clone(),
            source_uri: self.metadata.source_uri.clone(),
            source_version: self.metadata.source_version,
            generation: self.metadata.generation,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.format != FORMAT_NAME
            || self.index_kind != "direct_adjacency"
            || self.format_version != DIRECT_ADJACENCY_INDEX_FORMAT_VERSION
        {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "unsupported direct adjacency descriptor format",
            ));
        }
        if self.metadata.source_id_field.is_empty()
            || self.metadata.target_id_field.is_empty()
            || self.metadata.adjacency_field.is_empty()
            || self.metadata.scalar_index_name.is_empty()
            || self.metadata.dataset_uri.is_empty()
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency descriptor contains an empty required field",
            ));
        }
        if self.metadata.source_version.is_some() && self.metadata.source_uri.is_none() {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency source_version requires source_uri",
            ));
        }
        Ok(())
    }
}

impl TryFrom<&DataType> for PersistedIdType {
    type Error = GraphError;
    fn try_from(value: &DataType) -> Result<Self> {
        match value {
            DataType::UInt32 => Ok(Self::UInt32),
            DataType::UInt64 => Ok(Self::UInt64),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            other => Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!("direct adjacency does not support ID type {other}"),
            )),
        }
    }
}

impl From<PersistedIdType> for DataType {
    fn from(value: PersistedIdType) -> Self {
        match value {
            PersistedIdType::UInt32 => Self::UInt32,
            PersistedIdType::UInt64 => Self::UInt64,
            PersistedIdType::Int32 => Self::Int32,
            PersistedIdType::Int64 => Self::Int64,
        }
    }
}

async fn read_persisted_descriptor(index_uri: &str) -> Result<PersistedDescriptor> {
    let (store, base_path) = ObjectStore::from_uri(index_uri)
        .await
        .map_err(|error| index_io_error(index_uri, error))?;
    let path = base_path.child(DESCRIPTOR_FILE);
    if !store
        .inner
        .exists(&path)
        .await
        .map_err(|error| index_io_error(index_uri, error))?
    {
        return Err(index_error(
            GraphIndexErrorKind::Missing,
            format!("direct adjacency descriptor is missing at {index_uri}"),
        ));
    }
    let bytes = store
        .read_one_all(&path)
        .await
        .map_err(|error| index_io_error(index_uri, error))?;
    let descriptor: PersistedDescriptor = serde_json::from_slice(&bytes).map_err(|error| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            format!("invalid direct adjacency descriptor at {index_uri}: {error}"),
        )
    })?;
    descriptor.validate()?;
    Ok(descriptor)
}

fn validate_batches(
    metadata: &DirectAdjacencyMetadata,
    batches: &[RecordBatch],
) -> Result<(u64, u64)> {
    if batches.is_empty() {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "direct adjacency requires at least one batch (which may be empty)",
        ));
    }
    let mut sources = HashSet::new();
    let mut edge_count = 0_u64;
    for batch in batches {
        validate_schema(batch.schema().as_ref(), metadata)?;
        let src = batch.column_by_name(&metadata.source_id_field).unwrap();
        let lists = batch
            .column_by_name(&metadata.adjacency_field)
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        if src.null_count() != 0 || lists.null_count() != 0 || lists.values().null_count() != 0 {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency columns and List items must be non-null",
            ));
        }
        for row in 0..batch.num_rows() {
            let key = source_key(src.as_ref(), row)?;
            if !sources.insert(key) {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    "direct adjacency source ID is not unique",
                ));
            }
            edge_count = edge_count
                .checked_add(lists.value_length(row) as u64)
                .ok_or_else(|| {
                    index_error(
                        GraphIndexErrorKind::Corrupt,
                        "direct adjacency edge count overflow",
                    )
                })?;
        }
    }
    Ok((sources.len() as u64, edge_count))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SourceKey {
    UInt32(u32),
    UInt64(u64),
    Int32(i32),
    Int64(i64),
}

fn source_key(array: &dyn Array, row: usize) -> Result<SourceKey> {
    match array.data_type() {
        DataType::UInt32 => Ok(SourceKey::UInt32(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row),
        )),
        DataType::UInt64 => Ok(SourceKey::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Int32 => Ok(SourceKey::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Int64 => Ok(SourceKey::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )),
        other => Err(index_error(
            GraphIndexErrorKind::Incompatible,
            format!("unsupported direct adjacency source type {other}"),
        )),
    }
}

fn validate_schema(schema: &Schema, metadata: &DirectAdjacencyMetadata) -> Result<()> {
    if schema.fields().len() != 2 {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "direct adjacency schema must contain exactly two fields",
        ));
    }
    let source = schema
        .field_with_name(&metadata.source_id_field)
        .map_err(|_| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency source field is missing",
            )
        })?;
    let adjacency = schema
        .field_with_name(&metadata.adjacency_field)
        .map_err(|_| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency List field is missing",
            )
        })?;
    if source.is_nullable() || source.data_type() != &metadata.id_data_type {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "direct adjacency source field has incompatible type or nullability",
        ));
    }
    match adjacency.data_type() {
        DataType::List(item) if item.data_type() == &metadata.id_data_type => {}
        _ => {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency List item type does not match metadata",
            ))
        }
    }
    if adjacency.is_nullable() {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "direct adjacency List field must be non-null",
        ));
    }
    Ok(())
}

async fn validate_dataset(
    dataset: &Dataset,
    metadata: &DirectAdjacencyMetadata,
    expected_sources: u64,
    expected_edges: u64,
) -> Result<()> {
    validate_schema(&Schema::from(dataset.schema()), metadata)?;
    let rows = dataset
        .count_rows(None)
        .await
        .map_err(|error| index_io_error(dataset.uri(), error))? as u64;
    if rows != expected_sources {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!("direct adjacency has {rows} source rows; expected {expected_sources}"),
        ));
    }
    let mut stream = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|error| index_io_error(dataset.uri(), error))?;
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|error| index_io_error(dataset.uri(), error))?
    {
        batches.push(batch);
    }
    let (sources, edges) = validate_batches(metadata, &batches)?;
    if sources != expected_sources || edges != expected_edges {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!(
                "direct adjacency data counts {sources}/{edges} do not match expected {expected_sources}/{expected_edges}"
            ),
        ));
    }
    Ok(())
}

async fn validate_dataset_shape(
    dataset: &Dataset,
    metadata: &DirectAdjacencyMetadata,
    expected_sources: u64,
) -> Result<()> {
    validate_schema(&Schema::from(dataset.schema()), metadata)?;
    let rows = dataset
        .count_rows(None)
        .await
        .map_err(|error| index_io_error(dataset.uri(), error))? as u64;
    if rows != expected_sources {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            format!("direct adjacency has {rows} source rows; expected {expected_sources}"),
        ));
    }
    Ok(())
}

async fn validate_scalar_index(
    dataset: &Dataset,
    metadata: &DirectAdjacencyMetadata,
) -> Result<Arc<dyn lance_index::scalar::ScalarIndex>> {
    let source_field = dataset
        .schema()
        .field(&metadata.source_id_field)
        .ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                "direct adjacency source field is missing",
            )
        })?;
    let indices = dataset
        .load_indices()
        .await
        .map_err(|error| index_io_error(dataset.uri(), error))?;
    let index = indices
        .iter()
        .find(|index| {
            index.name == metadata.scalar_index_name && index.fields.as_slice() == [source_field.id]
        })
        .ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::Missing,
                "direct adjacency scalar index is missing or bound to the wrong field",
            )
        })?;
    let current_fragments = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    if !index.fragment_bitmap.as_ref().is_some_and(|covered| {
        current_fragments
            .iter()
            .all(|fragment_id| covered.contains(*fragment_id))
    }) {
        return Err(index_error(
            GraphIndexErrorKind::Stale,
            "direct adjacency scalar index does not cover all dataset fragments",
        ));
    }
    dataset
        .open_scalar_index(
            &metadata.source_id_field,
            &index.uuid.to_string(),
            &NoOpMetricsCollector,
        )
        .await
        .map_err(|error| index_io_error(dataset.uri(), error))
}

fn validate_source(
    metadata: &DirectAdjacencyMetadata,
    validation: &IndexSourceValidation,
) -> Result<()> {
    if let IndexSourceValidation::RequireExact(expected) = validation {
        if metadata.source_uri.as_deref() != Some(expected.uri.as_str())
            || metadata.source_version != expected.version
        {
            return Err(index_error(
                GraphIndexErrorKind::Stale,
                format!(
                    "direct adjacency source {:?}@{:?} does not match expected {}@{:?}",
                    metadata.source_uri, metadata.source_version, expected.uri, expected.version
                ),
            ));
        }
    }
    Ok(())
}

fn component_uri(index_uri: &str, component: &str) -> String {
    format!("{}/{}", index_uri.trim_end_matches('/'), component)
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
        format!("direct adjacency I/O failed at {uri}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use lance_index::scalar::{SargableQuery, SearchResult};

    fn metadata(id_data_type: DataType) -> DirectAdjacencyMetadata {
        DirectAdjacencyMetadata {
            key: GraphIndexKey::new("FRIEND_OF", "Person", "Person", IndexDirection::Outgoing),
            source_id_field: "src_id".into(),
            target_id_field: "person_id".into(),
            adjacency_field: "dst_ids".into(),
            id_data_type,
            num_sources: 0,
            num_edges: 0,
            dataset_uri: String::new(),
            dataset_version: 0,
            scalar_index_name: "src_id_btree".into(),
            source_uri: Some("memory://edges".into()),
            source_version: Some(1),
            generation: 1,
        }
    }

    fn edge_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("src_id", DataType::Int64, false),
                Field::new("dst_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![8, 7, 7, 9])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_read_load_round_trips_nested_adjacency() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("generation-1");
        let descriptor = DirectAdjacencyIndexBuilder::new(metadata(DataType::Int64))
            .unwrap()
            .add_edges_from_batch(&edge_batch())
            .unwrap()
            .build_and_persist(uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();

        assert_eq!(descriptor.metadata.num_sources, 2);
        assert_eq!(descriptor.metadata.num_edges, 4);
        let read = DirectAdjacencyIndexStore::read_descriptor(uri.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(read, descriptor);
        let handle = DirectAdjacencyIndexStore::load(&read, Default::default())
            .await
            .unwrap();
        let result = handle
            .scalar_index
            .search(
                &SargableQuery::IsIn(vec![
                    datafusion::common::ScalarValue::Int64(Some(1)),
                    datafusion::common::ScalarValue::Int64(Some(2)),
                ]),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let row_ids = match result {
            SearchResult::Exact(ids) | SearchResult::AtMost(ids) => {
                ids.row_ids().unwrap().map(u64::from).collect::<Vec<_>>()
            }
            SearchResult::AtLeast(_) => panic!("unexpected incomplete scalar result"),
        };
        let rows = handle
            .dataset
            .take_rows(&row_ids, handle.dataset.schema().clone())
            .await
            .unwrap();
        assert_eq!(rows.num_rows(), 2);
        let lists = rows.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        let list_values = lists.value(0);
        let values = list_values.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(values.values().as_ref() == [7, 7, 9] || values.values().as_ref() == [8]);
    }

    #[tokio::test]
    async fn multi_fragment_generation_validates_scalar_index_coverage() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("generation-1");
        let descriptor = DirectAdjacencyIndexBuilder::new(metadata(DataType::Int64))
            .unwrap()
            .add_edges_from_batch(&edge_batch())
            .unwrap()
            .build_and_persist(
                uri.to_str().unwrap(),
                DirectAdjacencyWriteOptions {
                    batch_size: 1,
                    max_rows_per_file: Some(1),
                },
            )
            .await
            .unwrap();
        let loaded = DirectAdjacencyIndexStore::load(&descriptor, Default::default())
            .await
            .unwrap();
        assert!(loaded.dataset.get_fragments().len() > 1);
        assert_eq!(loaded.metadata.num_sources, 2);
        assert_eq!(loaded.metadata.num_edges, 4);
    }

    #[test]
    fn builder_accepts_all_supported_integer_types() {
        for data_type in [
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
        ] {
            assert!(DirectAdjacencyIndexBuilder::new(metadata(data_type)).is_ok());
        }
    }
}
