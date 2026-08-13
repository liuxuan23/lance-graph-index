use super::{
    CoveringAdjacencyCompression, CoveringAdjacencyIndexHandle, CoveringAdjacencyMetadata,
    GraphIndexKey, IndexDirection, IndexSourceValidation,
};
use crate::error::{GraphError, GraphIndexErrorKind, Result};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::DataType;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use lance_io::object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;

mod bundle;

pub use bundle::{
    CoveringComponentDescriptorRef, MultiTypeCoveringAdjacencyIndexBuilder,
    MultiTypeCoveringAdjacencyIndexStore, MultiTypeCoveringAdjacencyLoadOptions,
    PersistedMultiTypeCoveringAdjacencyDescriptor,
    MULTI_TYPE_COVERING_ADJACENCY_INDEX_FORMAT_VERSION,
};

const DESCRIPTOR_FILE: &str = "descriptor.json";
const ENTRY_DIRECTORY_FILE: &str = "entry-directory.json";
const POSTING_DIRECTORY_FILE: &str = "posting-directory.json";
const ENTRY_PAGES_FILE: &str = "entry-pages.idx";
const POSTING_PAGES_FILE: &str = "posting-pages.idx";
const FORMAT_NAME: &str = "lance-graph-covering-adjacency";
const ENTRY_MAGIC: &[u8; 4] = b"CAE1";
const POSTING_MAGIC: &[u8; 4] = b"CAP1";
const MAX_CACHED_ENTRY_PAGES: usize = 128;
const MAX_CACHED_POSTING_PAGES: usize = 32;

pub const COVERING_ADJACENCY_INDEX_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct IdValue(i128);

impl IdValue {
    fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0.to_le_bytes());
    }

    fn read_from(input: &[u8], cursor: &mut usize) -> Result<Self> {
        let bytes = take_bytes(input, cursor, 16)?;
        Ok(Self(i128::from_le_bytes(bytes.try_into().unwrap())))
    }
}

#[derive(Debug, Clone)]
pub struct AdjacencyChunk {
    pub source_id: datafusion::common::ScalarValue,
    pub chunk_ordinal: u32,
    pub dst_ids: ArrayRef,
    pub is_last: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct AdjacencyLookupOptions {
    pub max_output_chunk_edges: usize,
}

impl Default for AdjacencyLookupOptions {
    fn default() -> Self {
        Self {
            max_output_chunk_edges: 8_192,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CoveringAdjacencyWriteOptions {
    pub entry_page_target_bytes: usize,
    pub inline_posting_threshold_bytes: usize,
    pub posting_page_target_bytes: usize,
}

impl Default for CoveringAdjacencyWriteOptions {
    fn default() -> Self {
        Self {
            entry_page_target_bytes: 64 * 1024,
            inline_posting_threshold_bytes: 8 * 1024,
            posting_page_target_bytes: 256 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CoveringAdjacencyLoadOptions {
    pub source_validation: IndexSourceValidation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedCoveringAdjacencyDescriptor {
    pub index_uri: String,
    pub format_version: u32,
    pub metadata: CoveringAdjacencyMetadata,
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
                format!("covering adjacency does not support ID type {other}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoveringAdjacencyIndexBuilder {
    metadata: CoveringAdjacencyMetadata,
    edges: EdgeBuffer,
}

impl CoveringAdjacencyIndexBuilder {
    pub fn new(metadata: CoveringAdjacencyMetadata) -> Result<Self> {
        validate_metadata_contract(&metadata)?;
        Ok(Self {
            edges: EdgeBuffer::new(&metadata.source_id_data_type)?,
            metadata,
        })
    }

    pub fn add_edges_from_batch(mut self, batch: &RecordBatch) -> Result<Self> {
        let (src, dst) = validate_edge_batch(batch, &self.metadata)?;
        macro_rules! append {
            ($values:expr, $array:ty) => {{
                let src = src.as_any().downcast_ref::<$array>().unwrap();
                let dst = dst.as_any().downcast_ref::<$array>().unwrap();
                $values.extend((0..batch.num_rows()).map(|row| (src.value(row), dst.value(row))));
            }};
        }
        match &mut self.edges {
            EdgeBuffer::UInt32(values) => append!(values, UInt32Array),
            EdgeBuffer::UInt64(values) => append!(values, UInt64Array),
            EdgeBuffer::Int32(values) => append!(values, Int32Array),
            EdgeBuffer::Int64(values) => append!(values, Int64Array),
        }
        Ok(self)
    }

    pub async fn build_and_persist(
        self,
        index_uri: &str,
        options: CoveringAdjacencyWriteOptions,
    ) -> Result<PersistedCoveringAdjacencyDescriptor> {
        validate_write_options(options)?;
        let groups = match self.edges {
            EdgeBuffer::UInt32(values) => group_sorted(values, |value| IdValue(value as i128)),
            EdgeBuffer::UInt64(values) => group_sorted(values, |value| IdValue(value as i128)),
            EdgeBuffer::Int32(values) => group_sorted(values, |value| IdValue(value as i128)),
            EdgeBuffer::Int64(values) => group_sorted(values, |value| IdValue(value as i128)),
        };
        CoveringAdjacencyIndexStore::write_groups(index_uri, self.metadata, groups, options).await
    }

    /// Build from a batch already sorted by source ID. Unlike
    /// [`Self::add_edges_from_batch`], this does not copy or sort every edge;
    /// only the current source adjacency and current page are buffered.
    pub async fn build_sorted_batch_and_persist(
        self,
        batch: &RecordBatch,
        index_uri: &str,
        options: CoveringAdjacencyWriteOptions,
    ) -> Result<PersistedCoveringAdjacencyDescriptor> {
        self.build_sorted_batches_and_persist(std::iter::once(batch.clone()), index_uri, options)
            .await
    }

    /// Build from a source-sorted iterator of batches. A source may continue
    /// into the next batch; only that source's adjacency is retained between
    /// iterator pulls.
    pub async fn build_sorted_batches_and_persist(
        self,
        batches: impl IntoIterator<Item = RecordBatch>,
        index_uri: &str,
        options: CoveringAdjacencyWriteOptions,
    ) -> Result<PersistedCoveringAdjacencyDescriptor> {
        validate_write_options(options)?;
        let groups = SortedBatchGroups::new(batches.into_iter(), self.metadata.clone());
        CoveringAdjacencyIndexStore::write_group_iter(index_uri, self.metadata, groups, options)
            .await
    }
}

fn validate_edge_batch<'a>(
    batch: &'a RecordBatch,
    metadata: &CoveringAdjacencyMetadata,
) -> Result<(&'a ArrayRef, &'a ArrayRef)> {
    let source = batch.column_by_name("src_id").ok_or_else(|| {
        index_error(
            GraphIndexErrorKind::Incompatible,
            "edge batch is missing src_id",
        )
    })?;
    let target = batch.column_by_name("dst_id").ok_or_else(|| {
        index_error(
            GraphIndexErrorKind::Incompatible,
            "edge batch is missing dst_id",
        )
    })?;
    if source.data_type() != &metadata.source_id_data_type
        || target.data_type() != &metadata.target_id_data_type
    {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "edge endpoint types do not match covering adjacency metadata",
        ));
    }
    if source.null_count() != 0 || target.null_count() != 0 {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "covering adjacency endpoints must be non-null",
        ));
    }
    Ok((source, target))
}

struct SortedBatchGroups<I: Iterator<Item = RecordBatch>> {
    batches: I,
    metadata: CoveringAdjacencyMetadata,
    source: Option<ArrayRef>,
    target: Option<ArrayRef>,
    position: usize,
    finished: bool,
}

impl<I: Iterator<Item = RecordBatch>> SortedBatchGroups<I> {
    fn new(batches: I, metadata: CoveringAdjacencyMetadata) -> Self {
        Self {
            batches,
            metadata,
            source: None,
            target: None,
            position: 0,
            finished: false,
        }
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        loop {
            let Some(batch) = self.batches.next() else {
                self.source = None;
                self.target = None;
                return Ok(false);
            };
            let (source, target) = validate_edge_batch(&batch, &self.metadata)?;
            if source.is_empty() {
                continue;
            }
            self.source = Some(source.clone());
            self.target = Some(target.clone());
            self.position = 0;
            return Ok(true);
        }
    }
}

impl<I: Iterator<Item = RecordBatch>> Iterator for SortedBatchGroups<I> {
    type Item = Result<(IdValue, Vec<IdValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if self
            .source
            .as_ref()
            .is_none_or(|source| self.position == source.len())
        {
            match self.load_next_batch() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
        let source_array = self.source.as_ref().unwrap();
        let source = match id_at(source_array.as_ref(), self.position) {
            Ok(source) => source,
            Err(error) => {
                self.finished = true;
                return Some(Err(error));
            }
        };
        let mut targets = Vec::new();
        loop {
            if self
                .source
                .as_ref()
                .is_some_and(|source| self.position == source.len())
            {
                match self.load_next_batch() {
                    Ok(true) => {}
                    Ok(false) => return Some(Ok((source, targets))),
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
            }
            let source_array = self.source.as_ref().unwrap();
            let current = match id_at(source_array.as_ref(), self.position) {
                Ok(current) => current,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            if current < source {
                self.finished = true;
                return Some(Err(index_error(
                    GraphIndexErrorKind::Incompatible,
                    "covering adjacency streaming builder requires source-sorted edges",
                )));
            }
            if current != source {
                break;
            }
            let target_array = self.target.as_ref().unwrap();
            match id_at(target_array.as_ref(), self.position) {
                Ok(target) => targets.push(target),
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
            self.position += 1;
        }
        Some(Ok((source, targets)))
    }
}

fn group_sorted<T: Copy + Ord>(
    mut edges: Vec<(T, T)>,
    convert: impl Fn(T) -> IdValue,
) -> Vec<(IdValue, Vec<IdValue>)> {
    edges.sort_by_key(|(source, _)| *source);
    let mut groups: Vec<(IdValue, Vec<IdValue>)> = Vec::new();
    for (source, target) in edges {
        let source = convert(source);
        let target = convert(target);
        if groups.last().is_some_and(|(last, _)| *last == source) {
            groups.last_mut().unwrap().1.push(target);
        } else {
            groups.push((source, vec![target]));
        }
    }
    groups
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageDescriptor {
    page_id: u64,
    first_source: IdValue,
    last_source: IdValue,
    offset: u64,
    length: u64,
    checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostingPageDescriptor {
    page_id: u64,
    source: IdValue,
    chunk_ordinal: u32,
    offset: u64,
    length: u64,
    checksum: u64,
    is_last: bool,
}

#[derive(Debug, Clone)]
enum EntryPayload {
    Inline(Vec<IdValue>),
    Posting { first_page: u64, num_pages: u32 },
}

#[derive(Debug, Clone)]
struct Entry {
    source: IdValue,
    degree: u64,
    payload: EntryPayload,
}

#[derive(Debug, Default)]
pub struct CoveringAdjacencyMetrics {
    pub range_requests: AtomicU64,
    pub entry_page_reads: AtomicU64,
    pub entry_page_cache_hits: AtomicU64,
    pub posting_page_reads: AtomicU64,
    pub posting_page_cache_hits: AtomicU64,
    pub entry_bytes_read: AtomicU64,
    pub posting_bytes_read: AtomicU64,
    pub inline_posting_hits: AtomicU64,
    pub posting_tree_hits: AtomicU64,
    pub checksums_verified: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoveringAdjacencyMetricsSnapshot {
    pub range_requests: u64,
    pub entry_page_reads: u64,
    pub entry_page_cache_hits: u64,
    pub posting_page_reads: u64,
    pub posting_page_cache_hits: u64,
    pub entry_bytes_read: u64,
    pub posting_bytes_read: u64,
    pub inline_posting_hits: u64,
    pub posting_tree_hits: u64,
    pub checksums_verified: u64,
}

impl CoveringAdjacencyMetrics {
    pub fn snapshot(&self) -> CoveringAdjacencyMetricsSnapshot {
        CoveringAdjacencyMetricsSnapshot {
            range_requests: self.range_requests.load(AtomicOrdering::Relaxed),
            entry_page_reads: self.entry_page_reads.load(AtomicOrdering::Relaxed),
            entry_page_cache_hits: self.entry_page_cache_hits.load(AtomicOrdering::Relaxed),
            posting_page_reads: self.posting_page_reads.load(AtomicOrdering::Relaxed),
            posting_page_cache_hits: self.posting_page_cache_hits.load(AtomicOrdering::Relaxed),
            entry_bytes_read: self.entry_bytes_read.load(AtomicOrdering::Relaxed),
            posting_bytes_read: self.posting_bytes_read.load(AtomicOrdering::Relaxed),
            inline_posting_hits: self.inline_posting_hits.load(AtomicOrdering::Relaxed),
            posting_tree_hits: self.posting_tree_hits.load(AtomicOrdering::Relaxed),
            checksums_verified: self.checksums_verified.load(AtomicOrdering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct CoveringAdjacencyIndex {
    metadata: CoveringAdjacencyMetadata,
    entry_directory: Vec<PageDescriptor>,
    posting_directory: Vec<PostingPageDescriptor>,
    entry_cache: Mutex<BTreeMap<u64, Arc<Vec<Entry>>>>,
    posting_cache: Mutex<BTreeMap<u64, Arc<Vec<IdValue>>>>,
    metrics: CoveringAdjacencyMetrics,
}

impl CoveringAdjacencyIndex {
    pub fn metadata(&self) -> &CoveringAdjacencyMetadata {
        &self.metadata
    }

    pub fn metrics(&self) -> &CoveringAdjacencyMetrics {
        &self.metrics
    }

    pub async fn lookup(
        self: &Arc<Self>,
        source_ids: ArrayRef,
        options: AdjacencyLookupOptions,
    ) -> Result<Vec<AdjacencyChunk>> {
        self.lookup_stream(source_ids, options)?.try_collect().await
    }

    pub fn lookup_stream(
        self: &Arc<Self>,
        source_ids: ArrayRef,
        options: AdjacencyLookupOptions,
    ) -> Result<BoxStream<'static, Result<AdjacencyChunk>>> {
        if source_ids.data_type() != &self.metadata.source_id_data_type {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "covering adjacency lookup source ID type mismatch",
            ));
        }
        if options.max_output_chunk_edges == 0 {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "covering adjacency lookup limits must be greater than zero",
            ));
        }
        let mut unique = Vec::new();
        let mut seen = HashSet::new();
        for row in 0..source_ids.len() {
            if source_ids.is_null(row) {
                continue;
            }
            let id = id_at(source_ids.as_ref(), row)?;
            if seen.insert(id) {
                unique.push(id);
            }
        }
        let state = LookupStreamState {
            index: self.clone(),
            sources: unique,
            next_source: 0,
            active_posting: None,
            pending: VecDeque::new(),
            options,
        };
        Ok(futures::stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some(chunk) = state.pending.pop_front() {
                    return Ok(Some((chunk, state)));
                }
                if let Some(active) = state.active_posting.as_mut() {
                    let descriptor = state
                        .index
                        .posting_directory
                        .get(active.next_page as usize)
                        .cloned()
                        .ok_or_else(|| {
                            index_error(
                                GraphIndexErrorKind::Corrupt,
                                "posting page reference is out of bounds",
                            )
                        })?;
                    if descriptor.source != active.source {
                        return Err(index_error(
                            GraphIndexErrorKind::Corrupt,
                            "posting page source does not match entry",
                        ));
                    }
                    let values = state.index.read_posting_page(&descriptor).await?;
                    active.output_ordinal = append_chunks(
                        &mut state.pending,
                        active.source,
                        values.as_ref().clone(),
                        active.output_ordinal,
                        state.options.max_output_chunk_edges,
                        &state.index.metadata.target_id_data_type,
                        descriptor.is_last,
                    )?;
                    active.next_page += 1;
                    if active.next_page == active.end_page {
                        state.active_posting = None;
                    }
                    continue;
                }
                let Some(source) = state.sources.get(state.next_source).copied() else {
                    return Ok(None);
                };
                state.next_source += 1;
                let Some(entry) = state.index.find_entry(source).await? else {
                    continue;
                };
                match entry.payload {
                    EntryPayload::Inline(values) => {
                        state
                            .index
                            .metrics
                            .inline_posting_hits
                            .fetch_add(1, AtomicOrdering::Relaxed);
                        append_chunks(
                            &mut state.pending,
                            source,
                            values,
                            0,
                            state.options.max_output_chunk_edges,
                            &state.index.metadata.target_id_data_type,
                            true,
                        )?;
                    }
                    EntryPayload::Posting {
                        first_page,
                        num_pages,
                    } => {
                        state
                            .index
                            .metrics
                            .posting_tree_hits
                            .fetch_add(1, AtomicOrdering::Relaxed);
                        state.active_posting = Some(ActivePosting {
                            source,
                            next_page: first_page,
                            end_page: first_page + u64::from(num_pages),
                            output_ordinal: 0,
                        });
                    }
                }
            }
        })
        .boxed())
    }

    async fn find_entry(&self, source: IdValue) -> Result<Option<Entry>> {
        let position = self.entry_directory.binary_search_by(|page| {
            if source < page.first_source {
                Ordering::Greater
            } else if source > page.last_source {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        });
        let Ok(position) = position else {
            return Ok(None);
        };
        let descriptor = &self.entry_directory[position];
        let entries = self.read_entry_page(descriptor).await?;
        Ok(entries
            .binary_search_by_key(&source, |entry| entry.source)
            .ok()
            .map(|position| entries[position].clone()))
    }

    async fn read_entry_page(&self, descriptor: &PageDescriptor) -> Result<Arc<Vec<Entry>>> {
        if let Some(page) = self
            .entry_cache
            .lock()
            .unwrap()
            .get(&descriptor.page_id)
            .cloned()
        {
            self.metrics
                .entry_page_cache_hits
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(page);
        }
        let bytes = read_range(
            &self.metadata.entry_pages_uri,
            descriptor.offset,
            descriptor.length,
        )
        .await?;
        self.metrics
            .range_requests
            .fetch_add(1, AtomicOrdering::Relaxed);
        verify_checksum(&bytes, descriptor.checksum)?;
        let page = Arc::new(decode_entry_page(&bytes)?);
        self.metrics
            .entry_page_reads
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.metrics
            .entry_bytes_read
            .fetch_add(bytes.len() as u64, AtomicOrdering::Relaxed);
        self.metrics
            .checksums_verified
            .fetch_add(1, AtomicOrdering::Relaxed);
        insert_bounded(
            &mut self.entry_cache.lock().unwrap(),
            descriptor.page_id,
            page.clone(),
            MAX_CACHED_ENTRY_PAGES,
        );
        Ok(page)
    }

    async fn read_posting_page(
        &self,
        descriptor: &PostingPageDescriptor,
    ) -> Result<Arc<Vec<IdValue>>> {
        if let Some(page) = self
            .posting_cache
            .lock()
            .unwrap()
            .get(&descriptor.page_id)
            .cloned()
        {
            self.metrics
                .posting_page_cache_hits
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(page);
        }
        let bytes = read_range(
            &self.metadata.posting_pages_uri,
            descriptor.offset,
            descriptor.length,
        )
        .await?;
        self.metrics
            .range_requests
            .fetch_add(1, AtomicOrdering::Relaxed);
        verify_checksum(&bytes, descriptor.checksum)?;
        let (source, ordinal, is_last, values) = decode_posting_page(&bytes)?;
        if source != descriptor.source
            || ordinal != descriptor.chunk_ordinal
            || is_last != descriptor.is_last
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "posting page identity mismatch",
            ));
        }
        let page = Arc::new(values);
        self.metrics
            .posting_page_reads
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.metrics
            .posting_bytes_read
            .fetch_add(bytes.len() as u64, AtomicOrdering::Relaxed);
        self.metrics
            .checksums_verified
            .fetch_add(1, AtomicOrdering::Relaxed);
        insert_bounded(
            &mut self.posting_cache.lock().unwrap(),
            descriptor.page_id,
            page.clone(),
            MAX_CACHED_POSTING_PAGES,
        );
        Ok(page)
    }
}

struct LookupStreamState {
    index: Arc<CoveringAdjacencyIndex>,
    sources: Vec<IdValue>,
    next_source: usize,
    active_posting: Option<ActivePosting>,
    pending: VecDeque<AdjacencyChunk>,
    options: AdjacencyLookupOptions,
}

struct ActivePosting {
    source: IdValue,
    next_page: u64,
    end_page: u64,
    output_ordinal: u32,
}

fn insert_bounded<T>(cache: &mut BTreeMap<u64, Arc<T>>, key: u64, value: Arc<T>, capacity: usize) {
    if cache.len() >= capacity && !cache.contains_key(&key) {
        if let Some(oldest) = cache.keys().next().copied() {
            cache.remove(&oldest);
        }
    }
    cache.insert(key, value);
}

fn append_chunks(
    output: &mut VecDeque<AdjacencyChunk>,
    source: IdValue,
    values: Vec<IdValue>,
    base_ordinal: u32,
    max_edges: usize,
    data_type: &DataType,
    source_is_last: bool,
) -> Result<u32> {
    let num_chunks = values.len().div_ceil(max_edges);
    for (offset, chunk) in values.chunks(max_edges).enumerate() {
        let ordinal = base_ordinal.checked_add(offset as u32).ok_or_else(|| {
            index_error(
                GraphIndexErrorKind::Corrupt,
                "adjacency chunk ordinal overflow",
            )
        })?;
        output.push_back(AdjacencyChunk {
            source_id: scalar_from_id(source, data_type)?,
            chunk_ordinal: ordinal,
            dst_ids: ids_to_array(chunk, data_type)?,
            is_last: source_is_last && offset + 1 == num_chunks,
        });
    }
    base_ordinal.checked_add(num_chunks as u32).ok_or_else(|| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            "adjacency chunk ordinal overflow",
        )
    })
}

pub struct CoveringAdjacencyIndexStore;

impl CoveringAdjacencyIndexStore {
    async fn write_groups(
        index_uri: &str,
        metadata: CoveringAdjacencyMetadata,
        groups: Vec<(IdValue, Vec<IdValue>)>,
        options: CoveringAdjacencyWriteOptions,
    ) -> Result<PersistedCoveringAdjacencyDescriptor> {
        Self::write_group_iter(index_uri, metadata, groups.into_iter().map(Ok), options).await
    }

    async fn write_group_iter(
        index_uri: &str,
        mut metadata: CoveringAdjacencyMetadata,
        groups: impl Iterator<Item = Result<(IdValue, Vec<IdValue>)>>,
        options: CoveringAdjacencyWriteOptions,
    ) -> Result<PersistedCoveringAdjacencyDescriptor> {
        if index_uri.trim().is_empty() {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "covering adjacency index URI must not be empty",
            ));
        }
        let (store, base) = ObjectStore::from_uri(index_uri)
            .await
            .map_err(|error| index_io_error(index_uri, error))?;
        let descriptor_path = base.child(DESCRIPTOR_FILE);
        if store
            .inner
            .exists(&descriptor_path)
            .await
            .map_err(|error| index_io_error(index_uri, error))?
        {
            return Err(index_error(
                GraphIndexErrorKind::AlreadyExists,
                format!("covering adjacency generation already exists at {index_uri}"),
            ));
        }

        let (entry_store, entry_path) =
            ObjectStore::from_uri(&component_uri(index_uri, ENTRY_PAGES_FILE))
                .await
                .map_err(|error| index_io_error(index_uri, error))?;
        let (posting_store, posting_path) =
            ObjectStore::from_uri(&component_uri(index_uri, POSTING_PAGES_FILE))
                .await
                .map_err(|error| index_io_error(index_uri, error))?;
        let mut entry_writer = entry_store
            .create(&entry_path)
            .await
            .map_err(|error| index_io_error(index_uri, error))?;
        let mut posting_writer = posting_store
            .create(&posting_path)
            .await
            .map_err(|error| index_io_error(index_uri, error))?;
        let mut entry_offset = 0_u64;
        let mut posting_offset = 0_u64;
        let mut entry_directory = Vec::new();
        let mut posting_directory = Vec::new();
        let mut entries = Vec::new();
        let mut entry_estimate = 8_usize;
        let mut num_inline = 0_u64;
        let mut num_posting_sources = 0_u64;
        let mut max_degree = 0_u64;
        let mut num_edges = 0_u64;

        for group in groups {
            let (source, targets) = group?;
            let degree = targets.len() as u64;
            max_degree = max_degree.max(targets.len() as u64);
            num_edges = num_edges.checked_add(targets.len() as u64).ok_or_else(|| {
                index_error(
                    GraphIndexErrorKind::Corrupt,
                    "covering adjacency edge count overflow",
                )
            })?;
            let inline_bytes = targets.len().saturating_mul(16);
            let payload = if inline_bytes <= options.inline_posting_threshold_bytes {
                num_inline += 1;
                EntryPayload::Inline(targets)
            } else {
                num_posting_sources += 1;
                let first_page = posting_directory.len() as u64;
                let per_page = ((options.posting_page_target_bytes.saturating_sub(32)) / 16).max(1);
                let chunks = targets.chunks(per_page).collect::<Vec<_>>();
                for (ordinal, chunk) in chunks.iter().enumerate() {
                    let bytes = encode_posting_page(
                        source,
                        ordinal as u32,
                        ordinal + 1 == chunks.len(),
                        chunk,
                    );
                    let offset = posting_offset;
                    posting_writer
                        .write_all(&bytes)
                        .await
                        .map_err(|error| index_io_error(index_uri, error))?;
                    posting_offset += bytes.len() as u64;
                    posting_directory.push(PostingPageDescriptor {
                        page_id: posting_directory.len() as u64,
                        source,
                        chunk_ordinal: ordinal as u32,
                        offset,
                        length: bytes.len() as u64,
                        checksum: checksum(&bytes),
                        is_last: ordinal + 1 == chunks.len(),
                    });
                }
                EntryPayload::Posting {
                    first_page,
                    num_pages: (posting_directory.len() as u64 - first_page) as u32,
                }
            };
            let entry = Entry {
                source,
                degree,
                payload,
            };
            let size = entry_encoded_len(&entry);
            if !entries.is_empty() && entry_estimate + size > options.entry_page_target_bytes {
                flush_entry_page_to_writer(
                    &mut entries,
                    &mut entry_writer,
                    &mut entry_offset,
                    &mut entry_directory,
                    index_uri,
                )
                .await?;
                entry_estimate = 8;
            }
            entry_estimate += size;
            entries.push(entry);
        }
        if !entries.is_empty() {
            flush_entry_page_to_writer(
                &mut entries,
                &mut entry_writer,
                &mut entry_offset,
                &mut entry_directory,
                index_uri,
            )
            .await?;
        }

        entry_writer
            .shutdown()
            .await
            .map_err(|error| index_io_error(index_uri, error))?;
        posting_writer
            .shutdown()
            .await
            .map_err(|error| index_io_error(index_uri, error))?;

        metadata.num_sources = num_inline + num_posting_sources;
        metadata.num_edges = num_edges;
        metadata.max_degree = max_degree;
        metadata.format_version = COVERING_ADJACENCY_INDEX_FORMAT_VERSION;
        metadata.index_uri = index_uri.to_string();
        metadata.entry_directory_uri = component_uri(index_uri, ENTRY_DIRECTORY_FILE);
        metadata.posting_directory_uri = component_uri(index_uri, POSTING_DIRECTORY_FILE);
        metadata.entry_pages_uri = component_uri(index_uri, ENTRY_PAGES_FILE);
        metadata.posting_pages_uri = component_uri(index_uri, POSTING_PAGES_FILE);
        metadata.entry_page_target_bytes = options.entry_page_target_bytes as u64;
        metadata.inline_posting_threshold_bytes = options.inline_posting_threshold_bytes as u64;
        metadata.posting_page_target_bytes = options.posting_page_target_bytes as u64;
        metadata.compression = CoveringAdjacencyCompression::None;
        metadata.num_entry_pages = entry_directory.len() as u64;
        metadata.num_inline_sources = num_inline;
        metadata.num_posting_tree_sources = num_posting_sources;
        metadata.num_posting_pages = posting_directory.len() as u64;

        put_json_uri(
            &component_uri(index_uri, POSTING_DIRECTORY_FILE),
            &posting_directory,
        )
        .await?;
        put_json_uri(
            &component_uri(index_uri, ENTRY_DIRECTORY_FILE),
            &entry_directory,
        )
        .await?;

        // A descriptor is the generation's publication marker. Reopen and
        // validate every page before making the generation visible.
        validate_directories(&metadata, &entry_directory, &posting_directory).await?;
        validate_all_pages(&metadata, &entry_directory, &posting_directory).await?;
        let persisted = PersistedDescriptor::from_metadata(&metadata)?;
        put_json_uri(&component_uri(index_uri, DESCRIPTOR_FILE), &persisted).await?;
        Ok(PersistedCoveringAdjacencyDescriptor {
            index_uri: index_uri.into(),
            format_version: COVERING_ADJACENCY_INDEX_FORMAT_VERSION,
            metadata,
        })
    }

    pub async fn read_descriptor(index_uri: &str) -> Result<PersistedCoveringAdjacencyDescriptor> {
        let persisted = read_persisted_descriptor(index_uri).await?;
        Ok(PersistedCoveringAdjacencyDescriptor {
            index_uri: index_uri.into(),
            format_version: persisted.format_version,
            metadata: persisted.metadata(index_uri)?,
        })
    }

    pub async fn load(
        descriptor: &PersistedCoveringAdjacencyDescriptor,
        options: CoveringAdjacencyLoadOptions,
    ) -> Result<CoveringAdjacencyIndexHandle> {
        let actual = Self::read_descriptor(&descriptor.index_uri).await?;
        if &actual != descriptor {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "covering adjacency descriptor does not match persisted metadata",
            ));
        }
        validate_source(&descriptor.metadata, &options.source_validation)?;
        let entry_directory: Vec<PageDescriptor> =
            read_json(&component_uri(&descriptor.index_uri, ENTRY_DIRECTORY_FILE)).await?;
        let posting_directory: Vec<PostingPageDescriptor> = read_json(&component_uri(
            &descriptor.index_uri,
            POSTING_DIRECTORY_FILE,
        ))
        .await?;
        validate_directories(&descriptor.metadata, &entry_directory, &posting_directory).await?;
        let index = Arc::new(CoveringAdjacencyIndex {
            metadata: descriptor.metadata.clone(),
            entry_directory,
            posting_directory,
            entry_cache: Mutex::new(BTreeMap::new()),
            posting_cache: Mutex::new(BTreeMap::new()),
            metrics: CoveringAdjacencyMetrics::default(),
        });
        Ok(CoveringAdjacencyIndexHandle {
            index,
            metadata: descriptor.metadata.clone(),
        })
    }
}

async fn flush_entry_page_to_writer(
    entries: &mut Vec<Entry>,
    writer: &mut lance_io::object_writer::ObjectWriter,
    offset: &mut u64,
    directory: &mut Vec<PageDescriptor>,
    index_uri: &str,
) -> Result<()> {
    let first_source = entries.first().unwrap().source;
    let last_source = entries.last().unwrap().source;
    let bytes = encode_entry_page(entries)?;
    let page_offset = *offset;
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| index_io_error(index_uri, error))?;
    *offset += bytes.len() as u64;
    directory.push(PageDescriptor {
        page_id: directory.len() as u64,
        first_source,
        last_source,
        offset: page_offset,
        length: bytes.len() as u64,
        checksum: checksum(&bytes),
    });
    entries.clear();
    Ok(())
}

fn entry_encoded_len(entry: &Entry) -> usize {
    16 + 1
        + 8
        + match &entry.payload {
            EntryPayload::Inline(values) => 4 + values.len() * 16,
            EntryPayload::Posting { .. } => 8 + 4,
        }
}

fn encode_entry_page(entries: &[Entry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(ENTRY_MAGIC);
    write_u32(&mut out, entries.len() as u32);
    for entry in entries {
        entry.source.write_to(&mut out);
        write_u64(&mut out, entry.degree);
        match &entry.payload {
            EntryPayload::Inline(values) => {
                out.push(0);
                write_u32(&mut out, values.len() as u32);
                for value in values {
                    value.write_to(&mut out);
                }
            }
            EntryPayload::Posting {
                first_page,
                num_pages,
            } => {
                out.push(1);
                write_u64(&mut out, *first_page);
                write_u32(&mut out, *num_pages);
            }
        }
    }
    Ok(out)
}

fn decode_entry_page(input: &[u8]) -> Result<Vec<Entry>> {
    if input.get(..4) != Some(ENTRY_MAGIC) {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "invalid entry page magic",
        ));
    }
    let mut cursor = 4;
    let count = read_u32(input, &mut cursor)? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let source = IdValue::read_from(input, &mut cursor)?;
        let degree = read_u64(input, &mut cursor)?;
        let kind = *take_bytes(input, &mut cursor, 1)?.first().unwrap();
        let payload = match kind {
            0 => {
                let count = read_u32(input, &mut cursor)? as usize;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(IdValue::read_from(input, &mut cursor)?);
                }
                EntryPayload::Inline(values)
            }
            1 => EntryPayload::Posting {
                first_page: read_u64(input, &mut cursor)?,
                num_pages: read_u32(input, &mut cursor)?,
            },
            _ => {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    "invalid entry payload kind",
                ))
            }
        };
        entries.push(Entry {
            source,
            degree,
            payload,
        });
    }
    if cursor != input.len() {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "entry page contains trailing bytes",
        ));
    }
    Ok(entries)
}

fn encode_posting_page(
    source: IdValue,
    ordinal: u32,
    is_last: bool,
    values: &[IdValue],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(POSTING_MAGIC);
    source.write_to(&mut out);
    write_u32(&mut out, ordinal);
    out.push(u8::from(is_last));
    write_u32(&mut out, values.len() as u32);
    for value in values {
        value.write_to(&mut out);
    }
    out
}

fn decode_posting_page(input: &[u8]) -> Result<(IdValue, u32, bool, Vec<IdValue>)> {
    if input.get(..4) != Some(POSTING_MAGIC) {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "invalid posting page magic",
        ));
    }
    let mut cursor = 4;
    let source = IdValue::read_from(input, &mut cursor)?;
    let ordinal = read_u32(input, &mut cursor)?;
    let is_last = match *take_bytes(input, &mut cursor, 1)?.first().unwrap() {
        0 => false,
        1 => true,
        _ => {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "invalid posting page last flag",
            ))
        }
    };
    let count = read_u32(input, &mut cursor)? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(IdValue::read_from(input, &mut cursor)?);
    }
    if cursor != input.len() {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "posting page contains trailing bytes",
        ));
    }
    Ok((source, ordinal, is_last, values))
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
    relationship_type: String,
    source_label: String,
    target_label: String,
    direction: String,
    source_id_field: String,
    target_id_field: String,
    id_data_type: String,
    num_sources: u64,
    num_edges: u64,
    max_degree: u64,
    generation: u64,
    entry_page_target_bytes: u64,
    inline_posting_threshold_bytes: u64,
    posting_page_target_bytes: u64,
    num_entry_pages: u64,
    num_inline_sources: u64,
    num_posting_tree_sources: u64,
    num_posting_pages: u64,
    source_uri: Option<String>,
    source_version: Option<u64>,
}

impl PersistedDescriptor {
    fn from_metadata(metadata: &CoveringAdjacencyMetadata) -> Result<Self> {
        validate_metadata_contract(metadata)?;
        Ok(Self {
            format: FORMAT_NAME.into(),
            format_version: COVERING_ADJACENCY_INDEX_FORMAT_VERSION,
            index_kind: "gin_style_covering_adjacency".into(),
            metadata: PersistedMetadata {
                relationship_type: metadata.key.relationship_type.clone(),
                source_label: metadata.key.source_label.clone(),
                target_label: metadata.key.target_label.clone(),
                direction: match metadata.key.direction {
                    IndexDirection::Outgoing => "outgoing",
                    IndexDirection::Incoming => "incoming",
                }
                .into(),
                source_id_field: metadata.source_id_field.clone(),
                target_id_field: metadata.target_id_field.clone(),
                id_data_type: data_type_name(&metadata.source_id_data_type)?.into(),
                num_sources: metadata.num_sources,
                num_edges: metadata.num_edges,
                max_degree: metadata.max_degree,
                generation: metadata.generation,
                entry_page_target_bytes: metadata.entry_page_target_bytes,
                inline_posting_threshold_bytes: metadata.inline_posting_threshold_bytes,
                posting_page_target_bytes: metadata.posting_page_target_bytes,
                num_entry_pages: metadata.num_entry_pages,
                num_inline_sources: metadata.num_inline_sources,
                num_posting_tree_sources: metadata.num_posting_tree_sources,
                num_posting_pages: metadata.num_posting_pages,
                source_uri: metadata.source_uri.clone(),
                source_version: metadata.source_version,
            },
        })
    }

    fn metadata(&self, index_uri: &str) -> Result<CoveringAdjacencyMetadata> {
        if self.format != FORMAT_NAME
            || self.index_kind != "gin_style_covering_adjacency"
            || self.format_version != COVERING_ADJACENCY_INDEX_FORMAT_VERSION
        {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                "unsupported covering adjacency descriptor format",
            ));
        }
        let data_type = parse_data_type(&self.metadata.id_data_type)?;
        let direction = match self.metadata.direction.as_str() {
            "outgoing" => IndexDirection::Outgoing,
            "incoming" => IndexDirection::Incoming,
            _ => {
                return Err(index_error(
                    GraphIndexErrorKind::Corrupt,
                    "invalid covering adjacency direction",
                ))
            }
        };
        Ok(CoveringAdjacencyMetadata {
            key: GraphIndexKey::new(
                &self.metadata.relationship_type,
                &self.metadata.source_label,
                &self.metadata.target_label,
                direction,
            ),
            source_id_field: self.metadata.source_id_field.clone(),
            target_id_field: self.metadata.target_id_field.clone(),
            source_id_data_type: data_type.clone(),
            target_id_data_type: data_type,
            num_sources: self.metadata.num_sources,
            num_edges: self.metadata.num_edges,
            max_degree: self.metadata.max_degree,
            generation: self.metadata.generation,
            format_version: self.format_version,
            index_uri: index_uri.into(),
            entry_directory_uri: component_uri(index_uri, ENTRY_DIRECTORY_FILE),
            posting_directory_uri: component_uri(index_uri, POSTING_DIRECTORY_FILE),
            entry_pages_uri: component_uri(index_uri, ENTRY_PAGES_FILE),
            posting_pages_uri: component_uri(index_uri, POSTING_PAGES_FILE),
            entry_page_target_bytes: self.metadata.entry_page_target_bytes,
            inline_posting_threshold_bytes: self.metadata.inline_posting_threshold_bytes,
            posting_page_target_bytes: self.metadata.posting_page_target_bytes,
            compression: CoveringAdjacencyCompression::None,
            num_entry_pages: self.metadata.num_entry_pages,
            num_inline_sources: self.metadata.num_inline_sources,
            num_posting_tree_sources: self.metadata.num_posting_tree_sources,
            num_posting_pages: self.metadata.num_posting_pages,
            source_uri: self.metadata.source_uri.clone(),
            source_version: self.metadata.source_version,
        })
    }
}

async fn read_persisted_descriptor(index_uri: &str) -> Result<PersistedDescriptor> {
    read_json(&component_uri(index_uri, DESCRIPTOR_FILE)).await
}

async fn read_json<T: for<'de> Deserialize<'de>>(uri: &str) -> Result<T> {
    let (store, path) = ObjectStore::from_uri(uri)
        .await
        .map_err(|error| index_io_error(uri, error))?;
    if !store
        .inner
        .exists(&path)
        .await
        .map_err(|error| index_io_error(uri, error))?
    {
        return Err(index_error(
            GraphIndexErrorKind::Missing,
            format!("covering adjacency file is missing at {uri}"),
        ));
    }
    let bytes = store
        .read_one_all(&path)
        .await
        .map_err(|error| index_io_error(uri, error))?;
    serde_json::from_slice(&bytes).map_err(|error| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            format!("invalid covering adjacency JSON at {uri}: {error}"),
        )
    })
}

async fn read_range(uri: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    let (store, path) = ObjectStore::from_uri(uri)
        .await
        .map_err(|error| index_io_error(uri, error))?;
    let start = usize::try_from(offset)
        .map_err(|_| index_error(GraphIndexErrorKind::Corrupt, "page offset overflow"))?;
    let length = usize::try_from(length)
        .map_err(|_| index_error(GraphIndexErrorKind::Corrupt, "page length overflow"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| index_error(GraphIndexErrorKind::Corrupt, "page range overflow"))?;
    store
        .read_one_range(&path, start..end)
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|error| index_io_error(uri, error))
}

async fn put_json_uri<T: Serialize>(uri: &str, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            format!("failed to serialize covering adjacency JSON: {error}"),
        )
    })?;
    put_bytes_uri(uri, &bytes).await
}

async fn put_bytes_uri(uri: &str, bytes: &[u8]) -> Result<()> {
    let (store, path) = ObjectStore::from_uri(uri)
        .await
        .map_err(|error| index_io_error(uri, error))?;
    store
        .put(&path, bytes)
        .await
        .map(|_| ())
        .map_err(|error| index_io_error(uri, error))
}

async fn validate_directories(
    metadata: &CoveringAdjacencyMetadata,
    entries: &[PageDescriptor],
    postings: &[PostingPageDescriptor],
) -> Result<()> {
    if metadata.num_entry_pages != entries.len() as u64
        || metadata.num_posting_pages != postings.len() as u64
    {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "covering adjacency directory counts do not match metadata",
        ));
    }
    let entry_size = object_size(&metadata.entry_pages_uri).await?;
    let posting_size = object_size(&metadata.posting_pages_uri).await?;
    validate_ranges(
        entries
            .iter()
            .map(|page| (page.page_id, page.offset, page.length)),
        entry_size,
        "entry",
    )?;
    validate_ranges(
        postings
            .iter()
            .map(|page| (page.page_id, page.offset, page.length)),
        posting_size,
        "posting",
    )?;
    for pair in entries.windows(2) {
        if pair[0].last_source >= pair[1].first_source {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "entry directory fences overlap",
            ));
        }
    }
    Ok(())
}

async fn validate_all_pages(
    metadata: &CoveringAdjacencyMetadata,
    entries: &[PageDescriptor],
    postings: &[PostingPageDescriptor],
) -> Result<()> {
    let mut source_count = 0_u64;
    let mut edge_count = 0_u64;
    let mut inline_sources = 0_u64;
    let mut posting_sources = 0_u64;
    let mut max_degree = 0_u64;
    for descriptor in entries {
        let bytes = read_range(
            &metadata.entry_pages_uri,
            descriptor.offset,
            descriptor.length,
        )
        .await?;
        verify_checksum(&bytes, descriptor.checksum)?;
        let page = decode_entry_page(&bytes)?;
        if page.first().map(|entry| entry.source) != Some(descriptor.first_source)
            || page.last().map(|entry| entry.source) != Some(descriptor.last_source)
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "entry page fence does not match directory",
            ));
        }
        source_count += page.len() as u64;
        for entry in page {
            edge_count = edge_count.checked_add(entry.degree).ok_or_else(|| {
                index_error(GraphIndexErrorKind::Corrupt, "covering edge count overflow")
            })?;
            max_degree = max_degree.max(entry.degree);
            match entry.payload {
                EntryPayload::Inline(values) => {
                    inline_sources += 1;
                    if values.len() as u64 != entry.degree {
                        return Err(index_error(
                            GraphIndexErrorKind::Corrupt,
                            "inline posting degree mismatch",
                        ));
                    }
                }
                EntryPayload::Posting {
                    first_page,
                    num_pages,
                } => {
                    posting_sources += 1;
                    let end = first_page
                        .checked_add(u64::from(num_pages))
                        .ok_or_else(|| {
                            index_error(GraphIndexErrorKind::Corrupt, "posting extent overflow")
                        })?;
                    if num_pages == 0 || end > postings.len() as u64 {
                        return Err(index_error(
                            GraphIndexErrorKind::Corrupt,
                            "posting extent is outside posting directory",
                        ));
                    }
                }
            }
        }
    }
    for descriptor in postings {
        let bytes = read_range(
            &metadata.posting_pages_uri,
            descriptor.offset,
            descriptor.length,
        )
        .await?;
        verify_checksum(&bytes, descriptor.checksum)?;
        let (source, ordinal, is_last, _) = decode_posting_page(&bytes)?;
        if source != descriptor.source
            || ordinal != descriptor.chunk_ordinal
            || is_last != descriptor.is_last
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                "posting page identity does not match directory",
            ));
        }
    }
    if source_count != metadata.num_sources
        || edge_count != metadata.num_edges
        || inline_sources != metadata.num_inline_sources
        || posting_sources != metadata.num_posting_tree_sources
        || max_degree != metadata.max_degree
    {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "covering adjacency aggregate counts do not match page contents",
        ));
    }
    Ok(())
}

async fn object_size(uri: &str) -> Result<u64> {
    let (store, path) = ObjectStore::from_uri(uri)
        .await
        .map_err(|error| index_io_error(uri, error))?;
    store
        .size(&path)
        .await
        .map_err(|error| index_io_error(uri, error))
}

fn validate_ranges(
    ranges: impl Iterator<Item = (u64, u64, u64)>,
    file_size: u64,
    kind: &str,
) -> Result<()> {
    let mut previous_end = 0;
    for (expected_id, (page_id, offset, length)) in ranges.enumerate() {
        if page_id != expected_id as u64
            || length == 0
            || offset < previous_end
            || offset.checked_add(length).is_none_or(|end| end > file_size)
        {
            return Err(index_error(
                GraphIndexErrorKind::Corrupt,
                format!("invalid covering adjacency {kind} page directory"),
            ));
        }
        previous_end = offset + length;
    }
    Ok(())
}

fn validate_metadata_contract(metadata: &CoveringAdjacencyMetadata) -> Result<()> {
    if metadata.source_id_data_type != metadata.target_id_data_type {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "covering adjacency source and target ID types must match",
        ));
    }
    data_type_name(&metadata.source_id_data_type)?;
    if metadata.source_id_field.trim().is_empty() || metadata.target_id_field.trim().is_empty() {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "covering adjacency ID field names must not be empty",
        ));
    }
    if metadata.source_version.is_some() && metadata.source_uri.is_none() {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "covering adjacency source_version requires source_uri",
        ));
    }
    Ok(())
}

fn validate_write_options(options: CoveringAdjacencyWriteOptions) -> Result<()> {
    if options.entry_page_target_bytes < 64
        || options.posting_page_target_bytes < 64
        || options.inline_posting_threshold_bytes == 0
    {
        return Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "covering adjacency page sizes are invalid",
        ));
    }
    Ok(())
}

fn validate_source(
    metadata: &CoveringAdjacencyMetadata,
    validation: &IndexSourceValidation,
) -> Result<()> {
    if let IndexSourceValidation::RequireExact(expected) = validation {
        if metadata.source_uri.as_deref() != Some(expected.uri.as_str())
            || metadata.source_version != expected.version
        {
            return Err(index_error(
                GraphIndexErrorKind::Stale,
                "covering adjacency source snapshot does not match expected source",
            ));
        }
    }
    Ok(())
}

fn id_at(array: &dyn Array, row: usize) -> Result<IdValue> {
    Ok(match array.data_type() {
        DataType::UInt32 => IdValue(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row) as i128,
        ),
        DataType::UInt64 => IdValue(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row) as i128,
        ),
        DataType::Int32 => IdValue(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i128,
        ),
        DataType::Int64 => IdValue(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row) as i128,
        ),
        other => {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!("unsupported covering adjacency ID type {other}"),
            ))
        }
    })
}

fn scalar_from_id(value: IdValue, data_type: &DataType) -> Result<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue;
    Ok(match data_type {
        DataType::UInt32 => {
            ScalarValue::UInt32(Some(u32::try_from(value.0).map_err(|_| {
                index_error(GraphIndexErrorKind::Corrupt, "UInt32 ID overflow")
            })?))
        }
        DataType::UInt64 => {
            ScalarValue::UInt64(Some(u64::try_from(value.0).map_err(|_| {
                index_error(GraphIndexErrorKind::Corrupt, "UInt64 ID overflow")
            })?))
        }
        DataType::Int32 => {
            ScalarValue::Int32(Some(i32::try_from(value.0).map_err(|_| {
                index_error(GraphIndexErrorKind::Corrupt, "Int32 ID overflow")
            })?))
        }
        DataType::Int64 => {
            ScalarValue::Int64(Some(i64::try_from(value.0).map_err(|_| {
                index_error(GraphIndexErrorKind::Corrupt, "Int64 ID overflow")
            })?))
        }
        other => {
            return Err(index_error(
                GraphIndexErrorKind::Incompatible,
                format!("unsupported covering adjacency ID type {other}"),
            ))
        }
    })
}

fn ids_to_array(values: &[IdValue], data_type: &DataType) -> Result<ArrayRef> {
    macro_rules! convert {
        ($native:ty, $array:ty, $name:literal) => {{
            let values = values
                .iter()
                .map(|value| {
                    <$native>::try_from(value.0).map_err(|_| {
                        index_error(GraphIndexErrorKind::Corrupt, concat!($name, " ID overflow"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(<$array>::from(values)) as ArrayRef)
        }};
    }
    match data_type {
        DataType::UInt32 => convert!(u32, UInt32Array, "UInt32"),
        DataType::UInt64 => convert!(u64, UInt64Array, "UInt64"),
        DataType::Int32 => convert!(i32, Int32Array, "Int32"),
        DataType::Int64 => convert!(i64, Int64Array, "Int64"),
        other => Err(index_error(
            GraphIndexErrorKind::Incompatible,
            format!("unsupported covering adjacency ID type {other}"),
        )),
    }
}

fn data_type_name(data_type: &DataType) -> Result<&'static str> {
    match data_type {
        DataType::UInt32 => Ok("uint32"),
        DataType::UInt64 => Ok("uint64"),
        DataType::Int32 => Ok("int32"),
        DataType::Int64 => Ok("int64"),
        other => Err(index_error(
            GraphIndexErrorKind::Incompatible,
            format!("unsupported covering adjacency ID type {other}"),
        )),
    }
}

fn parse_data_type(value: &str) -> Result<DataType> {
    match value {
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        _ => Err(index_error(
            GraphIndexErrorKind::Incompatible,
            "unsupported persisted covering adjacency ID type",
        )),
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    hash
}

fn verify_checksum(bytes: &[u8], expected: u64) -> Result<()> {
    if checksum(bytes) != expected {
        return Err(index_error(
            GraphIndexErrorKind::Corrupt,
            "covering adjacency page checksum mismatch",
        ));
    }
    Ok(())
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn read_u32(input: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        take_bytes(input, cursor, 4)?.try_into().unwrap(),
    ))
}
fn read_u64(input: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        take_bytes(input, cursor, 8)?.try_into().unwrap(),
    ))
}
fn take_bytes<'a>(input: &'a [u8], cursor: &mut usize, count: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(count)
        .ok_or_else(|| index_error(GraphIndexErrorKind::Corrupt, "page cursor overflow"))?;
    let bytes = input.get(*cursor..end).ok_or_else(|| {
        index_error(
            GraphIndexErrorKind::Corrupt,
            "truncated covering adjacency page",
        )
    })?;
    *cursor = end;
    Ok(bytes)
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
        format!("covering adjacency I/O failed at {uri}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};

    fn metadata() -> CoveringAdjacencyMetadata {
        CoveringAdjacencyMetadata {
            key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
            source_id_field: "person_id".into(),
            target_id_field: "person_id".into(),
            source_id_data_type: DataType::Int64,
            target_id_data_type: DataType::Int64,
            num_sources: 0,
            num_edges: 0,
            max_degree: 0,
            generation: 1,
            format_version: COVERING_ADJACENCY_INDEX_FORMAT_VERSION,
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
            source_uri: Some("memory://edges".into()),
            source_version: Some(7),
        }
    }

    #[tokio::test]
    async fn inline_and_posting_lookup_preserve_bag_semantics_after_reload() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("generation-1");
        let edges = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("src_id", DataType::Int64, false),
                Field::new("dst_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![7, 7, 9, 8, 9, 10, 11, 12])),
            ],
        )
        .unwrap();
        let descriptor = CoveringAdjacencyIndexBuilder::new(metadata())
            .unwrap()
            .add_edges_from_batch(&edges)
            .unwrap()
            .build_and_persist(
                uri.to_str().unwrap(),
                CoveringAdjacencyWriteOptions {
                    entry_page_target_bytes: 128,
                    inline_posting_threshold_bytes: 48,
                    posting_page_target_bytes: 64,
                },
            )
            .await
            .unwrap();
        assert_eq!(descriptor.metadata.num_inline_sources, 1);
        assert_eq!(descriptor.metadata.num_posting_tree_sources, 1);
        let handle = CoveringAdjacencyIndexStore::load(
            &descriptor,
            CoveringAdjacencyLoadOptions {
                source_validation: IndexSourceValidation::RequireExact(
                    super::super::GraphSourceIdentity {
                        uri: "memory://edges".into(),
                        version: Some(7),
                    },
                ),
            },
        )
        .await
        .unwrap();
        let chunks = handle
            .index
            .lookup(
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                AdjacencyLookupOptions {
                    max_output_chunk_edges: 2,
                },
            )
            .await
            .unwrap();
        let values = chunks
            .iter()
            .flat_map(|chunk| {
                chunk
                    .dst_ids
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![7, 7, 9, 8, 9, 10, 11, 12]);
        assert_eq!(chunks.iter().filter(|chunk| chunk.is_last).count(), 2);
        assert!(
            handle
                .index
                .metrics()
                .entry_page_reads
                .load(AtomicOrdering::Relaxed)
                > 0
        );
        assert!(
            handle
                .index
                .metrics()
                .posting_page_reads
                .load(AtomicOrdering::Relaxed)
                > 0
        );
    }

    #[test]
    fn page_codec_rejects_corruption() {
        let entry = Entry {
            source: IdValue(1),
            degree: 2,
            payload: EntryPayload::Inline(vec![IdValue(7), IdValue(9)]),
        };
        let mut bytes = encode_entry_page(&[entry]).unwrap();
        let expected = checksum(&bytes);
        bytes[5] ^= 0xff;
        assert!(verify_checksum(&bytes, expected).is_err());
    }

    #[tokio::test]
    async fn stale_source_and_corrupt_page_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("generation-1");
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
        let descriptor = CoveringAdjacencyIndexBuilder::new(metadata())
            .unwrap()
            .add_edges_from_batch(&edges)
            .unwrap()
            .build_and_persist(uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        let stale = CoveringAdjacencyIndexStore::load(
            &descriptor,
            CoveringAdjacencyLoadOptions {
                source_validation: IndexSourceValidation::RequireExact(
                    super::super::GraphSourceIdentity {
                        uri: "memory://different".into(),
                        version: Some(7),
                    },
                ),
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

        let handle = CoveringAdjacencyIndexStore::load(&descriptor, Default::default())
            .await
            .unwrap();
        let entry_path = uri.join(ENTRY_PAGES_FILE);
        let mut bytes = std::fs::read(&entry_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(entry_path, bytes).unwrap();
        let corrupt = handle
            .index
            .lookup(
                Arc::new(Int64Array::from(vec![1])),
                AdjacencyLookupOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            corrupt,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Corrupt,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn missing_descriptor_is_not_treated_as_an_empty_index() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing-generation");
        let error = CoveringAdjacencyIndexStore::read_descriptor(missing.to_str().unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Missing,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn streaming_builder_requires_source_sorted_input() {
        let directory = tempfile::tempdir().unwrap();
        let edges = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("src_id", DataType::Int64, false),
                Field::new("dst_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2, 1])),
                Arc::new(Int64Array::from(vec![3, 4])),
            ],
        )
        .unwrap();
        let error = CoveringAdjacencyIndexBuilder::new(metadata())
            .unwrap()
            .build_sorted_batch_and_persist(
                &edges,
                directory.path().join("generation-1").to_str().unwrap(),
                Default::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GraphError::IndexError {
                kind: GraphIndexErrorKind::Incompatible,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn streaming_builder_groups_a_source_across_batch_boundaries() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![7, 8])),
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![9, 10])),
            ],
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let descriptor = CoveringAdjacencyIndexBuilder::new(metadata())
            .unwrap()
            .build_sorted_batches_and_persist(
                vec![first, second],
                directory.path().join("generation-1").to_str().unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(descriptor.metadata.num_sources, 2);
        assert_eq!(descriptor.metadata.num_edges, 4);
        let handle = CoveringAdjacencyIndexStore::load(&descriptor, Default::default())
            .await
            .unwrap();
        let chunks = handle
            .index
            .lookup(
                Arc::new(Int64Array::from(vec![1])),
                AdjacencyLookupOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            chunks[0]
                .dst_ids
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 8, 9]
        );
    }
}
