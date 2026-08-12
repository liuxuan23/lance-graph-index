use crate::csr_index::CsrIndex;
use crate::index::DirectAdjacencyIndexHandle;
use arrow::compute::take;
use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, EvaluationType, PlanProperties,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{SargableQuery, SearchResult};
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub struct IndexedExpandExec {
    input: Arc<dyn ExecutionPlan>,
    index: Arc<CsrIndex>,
    source_column_index: usize,
    output_target_field: Arc<Field>,
    schema: SchemaRef,
    properties: PlanProperties,
    max_output_batch_rows: usize,
    metrics: ExecutionPlanMetricsSet,
}
impl IndexedExpandExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        index: Arc<CsrIndex>,
        source_column: &str,
        output_target_field: Arc<Field>,
        max_output_batch_rows: usize,
    ) -> Result<Self> {
        let source_column_index = input.schema().index_of(source_column).map_err(|_| {
            DataFusionError::Plan(format!(
                "IndexedExpand source column '{}' is missing",
                source_column
            ))
        })?;
        if max_output_batch_rows == 0 {
            return Err(DataFusionError::Plan(
                "IndexedExpand batch size must be greater than zero".into(),
            ));
        }
        let mut fields = input.schema().fields().to_vec();
        fields.push(output_target_field.clone());
        let schema = Arc::new(Schema::new(fields));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.properties().partitioning.clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_evaluation_type(EvaluationType::Lazy);
        Ok(Self {
            input,
            index,
            source_column_index,
            output_target_field,
            schema,
            properties,
            max_output_batch_rows,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

fn expand_batch(
    batch: &RecordBatch,
    index: &CsrIndex,
    source_idx: usize,
    target_field: &Arc<Field>,
    max_rows: usize,
) -> Result<(Vec<RecordBatch>, ExpansionStats)> {
    let source = batch.column(source_idx);
    let mut rows = Vec::new();
    let mut ids = Vec::new();
    let mut output = Vec::new();
    let mut stats = ExpansionStats {
        input_rows: batch.num_rows(),
        ..Default::default()
    };
    for row in 0..batch.num_rows() {
        if source.is_null(row) {
            stats.null_source_ids += 1;
            continue;
        }
        let id = match source.data_type() {
            DataType::UInt32 => source
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row) as u64,
            DataType::UInt64 => source
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
            DataType::Int32 => i64::from(
                source
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(row),
            )
            .try_into()
            .map_err(|_| DataFusionError::Execution("negative CSR source ID".into()))?,
            DataType::Int64 => source
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
                .try_into()
                .map_err(|_| DataFusionError::Execution("negative CSR source ID".into()))?,
            dt => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported CSR source ID type {dt:?}"
                )))
            }
        };
        if id >= index.num_vertices() {
            return Err(DataFusionError::Execution(format!(
                "CSR source ID {id} is outside index with {} vertices",
                index.num_vertices()
            )));
        }
        stats.index_lookups += 1;
        for target in index.neighbors(id) {
            stats.neighbors_emitted += 1;
            rows.push(row as u32);
            ids.push(*target);
            if rows.len() == max_rows {
                output.push(make_output_batch(batch, target_field, &rows, &ids)?);
                rows.clear();
                ids.clear();
            }
        }
    }
    if !rows.is_empty() {
        output.push(make_output_batch(batch, target_field, &rows, &ids)?);
    }
    stats.output_batches = output.len();
    stats.output_rows = output.iter().map(|b| b.num_rows()).sum();
    Ok((output, stats))
}

#[derive(Default)]
struct ExpansionStats {
    input_rows: usize,
    index_lookups: usize,
    null_source_ids: usize,
    neighbors_emitted: usize,
    output_batches: usize,
    output_rows: usize,
}

fn make_output_batch(
    batch: &RecordBatch,
    target_field: &Arc<Field>,
    rows: &[u32],
    ids: &[u64],
) -> Result<RecordBatch> {
    let indices = UInt32Array::from(rows.to_vec());
    let mut columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<_, _>>()?;
    let target: ArrayRef = match target_field.data_type() {
        DataType::UInt32 => Arc::new(UInt32Array::from(
            ids.iter()
                .map(|v| u32::try_from(*v))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| DataFusionError::Execution("target ID overflow".into()))?,
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(ids.to_vec())),
        DataType::Int32 => Arc::new(Int32Array::from(
            ids.iter()
                .map(|v| i32::try_from(*v))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| DataFusionError::Execution("target ID overflow".into()))?,
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            ids.iter()
                .map(|v| i64::try_from(*v))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| DataFusionError::Execution("target ID overflow".into()))?,
        )),
        dt => {
            return Err(DataFusionError::Plan(format!(
                "unsupported target ID type {dt:?}"
            )))
        }
    };
    columns.push(target);
    let mut fields = batch.schema().fields().to_vec();
    fields.push(target_field.clone());
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}
impl DisplayAs for IndexedExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IndexedExpandExec: source_column_index={}, target={}",
            self.source_column_index,
            self.output_target_field.name()
        )
    }
}
impl ExecutionPlan for IndexedExpandExec {
    fn name(&self) -> &str {
        "IndexedExpandExec"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "IndexedExpand expects one child".into(),
            ));
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.index.clone(),
            self.input.schema().field(self.source_column_index).name(),
            self.output_target_field.clone(),
            self.max_output_batch_rows,
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let index = self.index.clone();
        let source_idx = self.source_column_index;
        let max_rows = self.max_output_batch_rows;
        let schema = self.schema.clone();
        let target_field = self.output_target_field.clone();
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let input_batches = MetricBuilder::new(&self.metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(&self.metrics).counter("input_rows", partition);
        let index_lookups = MetricBuilder::new(&self.metrics).counter("index_lookups", partition);
        let null_source_ids =
            MetricBuilder::new(&self.metrics).counter("null_source_ids", partition);
        let neighbors_emitted =
            MetricBuilder::new(&self.metrics).counter("neighbors_emitted", partition);
        let output_batches = MetricBuilder::new(&self.metrics).counter("output_batches", partition);
        let stream = input
            .map(move |batch| {
                let batch = batch?;
                input_batches.add(1);
                let (batches, stats) =
                    expand_batch(&batch, &index, source_idx, &target_field, max_rows)?;
                input_rows.add(stats.input_rows);
                index_lookups.add(stats.index_lookups);
                null_source_ids.add(stats.null_source_ids);
                neighbors_emitted.add(stats.neighbors_emitted);
                output_batches.add(stats.output_batches);
                output_rows.add(stats.output_rows);
                Ok::<_, DataFusionError>(futures::stream::iter(batches.into_iter().map(Ok)))
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

#[derive(Debug)]
pub struct DirectAdjacencyExpandExec {
    input: Arc<dyn ExecutionPlan>,
    handle: Arc<DirectAdjacencyIndexHandle>,
    index_name: Option<String>,
    bundle_generation: Option<u64>,
    source_column_index: usize,
    output_target_field: Arc<Field>,
    schema: SchemaRef,
    properties: PlanProperties,
    max_output_batch_rows: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl DirectAdjacencyExpandExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        handle: Arc<DirectAdjacencyIndexHandle>,
        source_column: &str,
        output_target_field: Arc<Field>,
        max_output_batch_rows: usize,
    ) -> Result<Self> {
        let source_column_index = input.schema().index_of(source_column).map_err(|_| {
            DataFusionError::Plan(format!(
                "DirectAdjacency source column '{}' is missing",
                source_column
            ))
        })?;
        if max_output_batch_rows == 0 {
            return Err(DataFusionError::Plan(
                "DirectAdjacency batch size must be greater than zero".into(),
            ));
        }
        if handle.metadata.id_data_type != *output_target_field.data_type() {
            return Err(DataFusionError::Plan(
                "DirectAdjacency source and target ID types must match".into(),
            ));
        }
        let mut fields = input.schema().fields().to_vec();
        fields.push(output_target_field.clone());
        let schema = Arc::new(Schema::new(fields));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.properties().partitioning.clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            handle,
            index_name: None,
            bundle_generation: None,
            source_column_index,
            output_target_field,
            schema,
            properties,
            max_output_batch_rows,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn with_bundle_identity(
        mut self,
        index_name: impl Into<String>,
        bundle_generation: u64,
    ) -> Self {
        self.index_name = Some(index_name.into());
        self.bundle_generation = Some(bundle_generation);
        self
    }
}

impl DisplayAs for DirectAdjacencyExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DirectAdjacencyExpandExec: source_column_index={}, target={}, index_name={}, bundle_generation={}, relationship_type={}, source_label={}, target_label={}, direction={:?}, component_generation={}, dataset_version={}", self.source_column_index, self.output_target_field.name(), self.index_name.as_deref().unwrap_or("unregistered"), self.bundle_generation.map(|value| value.to_string()).unwrap_or_else(|| "unknown".into()), self.handle.metadata.key.relationship_type, self.handle.metadata.key.source_label, self.handle.metadata.key.target_label, self.handle.metadata.key.direction, self.handle.metadata.generation, self.handle.metadata.dataset_version)
    }
}

impl ExecutionPlan for DirectAdjacencyExpandExec {
    fn name(&self) -> &str {
        "DirectAdjacencyExpandExec"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "DirectAdjacency expects one child".into(),
            ));
        }
        let mut rebuilt = Self::try_new(
            children[0].clone(),
            self.handle.clone(),
            self.input.schema().field(self.source_column_index).name(),
            self.output_target_field.clone(),
            self.max_output_batch_rows,
        )?;
        rebuilt.index_name = self.index_name.clone();
        rebuilt.bundle_generation = self.bundle_generation;
        Ok(Arc::new(rebuilt))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let handle = self.handle.clone();
        let source_index = self.source_column_index;
        let target_field = self.output_target_field.clone();
        let schema = self.schema.clone();
        let max_rows = self.max_output_batch_rows;
        let stream = input
            .then(move |batch| {
                let handle = handle.clone();
                let target_field = target_field.clone();
                let schema = schema.clone();
                async move {
                    let batch = batch?;
                    expand_direct_batch(
                        &batch,
                        &handle,
                        source_index,
                        &target_field,
                        &schema,
                        max_rows,
                    )
                    .await
                }
            })
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

async fn expand_direct_batch(
    batch: &RecordBatch,
    handle: &DirectAdjacencyIndexHandle,
    source_index: usize,
    target_field: &Arc<Field>,
    schema: &SchemaRef,
    max_rows: usize,
) -> Result<Vec<RecordBatch>> {
    let sources = batch.column(source_index);
    let mut unique = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for row in 0..sources.len() {
        if sources.is_null(row) {
            continue;
        }
        let id = direct_scalar_value(sources, row)?;
        if seen.insert(id.clone()) {
            unique.push(id);
        }
    }
    if unique.is_empty() {
        return Ok(vec![]);
    }
    let query = unique;
    let result = handle
        .scalar_index
        .search(&SargableQuery::IsIn(query), &NoOpMetricsCollector)
        .await
        .map_err(DataFusionError::from)?;
    let row_ids = match result {
        SearchResult::Exact(ids) | SearchResult::AtMost(ids) => ids,
        SearchResult::AtLeast(_) => {
            return Err(DataFusionError::Execution(
                "DirectAdjacency scalar index returned incomplete result".into(),
            ))
        }
    };
    let row_ids = row_ids
        .row_ids()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "DirectAdjacency scalar index returned non-enumerable row IDs".into(),
            )
        })?
        .map(u64::from)
        .collect::<Vec<_>>();
    if row_ids.is_empty() {
        return Ok(vec![]);
    }
    let fetched = handle
        .dataset
        .take_rows(&row_ids, handle.dataset.schema().clone())
        .await
        .map_err(DataFusionError::from)?;
    let source_col = fetched
        .column_by_name(&handle.metadata.source_id_field)
        .ok_or_else(|| {
            DataFusionError::Execution("DirectAdjacency fetched source column is missing".into())
        })?
        .as_ref();
    let lists = fetched
        .column_by_name(&handle.metadata.adjacency_field)
        .ok_or_else(|| {
            DataFusionError::Execution("DirectAdjacency adjacency column is missing".into())
        })?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("DirectAdjacency adjacency column must be List".into())
        })?;
    let mut adjacency = std::collections::HashMap::new();
    for row in 0..fetched.num_rows() {
        if adjacency
            .insert(direct_scalar_value(source_col, row)?, lists.value(row))
            .is_some()
        {
            return Err(DataFusionError::Execution(
                "DirectAdjacency source ID is not unique".into(),
            ));
        }
    }
    let mut output = Vec::new();
    let mut positions = Vec::new();
    let mut targets = Vec::new();
    for row in 0..batch.num_rows() {
        if sources.is_null(row) {
            continue;
        }
        let Some(values) = adjacency.get(&direct_scalar_value(sources, row)?) else {
            continue;
        };
        for target in 0..values.len() {
            positions.push(row as u32);
            targets.push(array_value_at(values.as_ref(), target)?);
            if positions.len() == max_rows {
                output.push(make_direct_output_batch(
                    batch,
                    &positions,
                    &targets,
                    target_field,
                    schema,
                )?);
                positions.clear();
                targets.clear();
            }
        }
    }
    if !positions.is_empty() {
        output.push(make_direct_output_batch(
            batch,
            &positions,
            &targets,
            target_field,
            schema,
        )?);
    }
    Ok(output)
}

fn make_direct_output_batch(
    batch: &RecordBatch,
    positions: &[u32],
    targets: &[datafusion::common::ScalarValue],
    target_field: &Arc<Field>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let indices = UInt32Array::from(positions.to_vec());
    let mut columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    columns.push(scalar_values_to_array(targets, target_field.data_type())?);
    RecordBatch::try_new(schema.clone(), columns).map_err(Into::into)
}

fn direct_scalar_value(array: &dyn Array, row: usize) -> Result<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue;
    Ok(match array.data_type() {
        DataType::UInt32 => ScalarValue::UInt32(Some(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row),
        )),
        DataType::UInt64 => ScalarValue::UInt64(Some(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Int32 => ScalarValue::Int32(Some(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Int64 => ScalarValue::Int64(Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )),
        other => {
            return Err(DataFusionError::Plan(format!(
                "DirectAdjacency source ID type {other} is unsupported"
            )))
        }
    })
}

fn array_value_at(array: &dyn Array, row: usize) -> Result<datafusion::common::ScalarValue> {
    direct_scalar_value(array, row)
}

fn scalar_values_to_array(
    values: &[datafusion::common::ScalarValue],
    data_type: &DataType,
) -> Result<ArrayRef> {
    use datafusion::common::ScalarValue;
    macro_rules! values_array {
        ($variant:ident, $array:ty) => {{
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    ScalarValue::$variant(Some(value)) => out.push(*value),
                    _ => {
                        return Err(DataFusionError::Execution(
                            "DirectAdjacency target ID type mismatch".into(),
                        ))
                    }
                }
            }
            Ok(Arc::new(<$array>::from(out)) as ArrayRef)
        }};
    }
    match data_type {
        DataType::UInt32 => values_array!(UInt32, UInt32Array),
        DataType::UInt64 => values_array!(UInt64, UInt64Array),
        DataType::Int32 => values_array!(Int32, Int32Array),
        DataType::Int64 => values_array!(Int64, Int64Array),
        other => Err(DataFusionError::Plan(format!(
            "DirectAdjacency target ID type {other} is unsupported"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CsrIndexBuilder, DirectAdjacencyIndexBuilder, DirectAdjacencyIndexStore,
        DirectAdjacencyMetadata, GraphIndexKey, IndexDirection,
    };
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use datafusion::physical_plan::test::TestMemoryExec;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn one_to_many_expansion_preserves_input_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a__id", DataType::Int64, false),
            Field::new("a__name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let index = Arc::new(
            CsrIndexBuilder::new()
                .with_num_vertices(3)
                .add_edge(0, 1)
                .add_edge(0, 2)
                .add_edge(1, 1)
                .try_build()
                .unwrap(),
        );
        let target = Arc::new(Field::new("knows__dst_id", DataType::Int64, false));
        let exec = IndexedExpandExec::try_new(input, index, "a__id", target, 2).unwrap();
        let batches = exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
    }

    #[tokio::test]
    async fn direct_expansion_preserves_bag_semantics_and_bounds_batches() {
        let directory = tempfile::tempdir().unwrap();
        let index_uri = directory.path().join("generation-1");
        let edge_schema = Arc::new(Schema::new(vec![
            Field::new("src_id", DataType::Int64, false),
            Field::new("dst_id", DataType::Int64, false),
        ]));
        let edges = RecordBatch::try_new(
            edge_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![7, 7, 9])),
            ],
        )
        .unwrap();
        let metadata = DirectAdjacencyMetadata {
            key: GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing),
            source_id_field: "src_id".into(),
            target_id_field: "person_id".into(),
            adjacency_field: "dst_ids".into(),
            id_data_type: DataType::Int64,
            num_sources: 0,
            num_edges: 0,
            dataset_uri: String::new(),
            dataset_version: 0,
            scalar_index_name: "src_id_btree".into(),
            source_uri: None,
            source_version: None,
            generation: 1,
        };
        let descriptor = DirectAdjacencyIndexBuilder::new(metadata)
            .unwrap()
            .add_edges_from_batch(&edges)
            .unwrap()
            .build_and_persist(index_uri.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        let handle = Arc::new(
            DirectAdjacencyIndexStore::load(&descriptor, Default::default())
                .await
                .unwrap(),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("a__id", DataType::Int64, true),
            Field::new("a__name", DataType::Utf8, false),
        ]));
        let input_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])),
                Arc::new(StringArray::from(vec!["a", "duplicate", "empty", "null"])),
            ],
        )
        .unwrap();
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[vec![input_batch]], schema, None).unwrap();
        let target = Arc::new(Field::new("knows__dst_id", DataType::Int64, false));
        let exec = DirectAdjacencyExpandExec::try_new(input, handle, "a__id", target, 2).unwrap();
        let batches = exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
        let targets = arrow::compute::concat(
            &batches
                .iter()
                .map(|batch| batch.column(2).as_ref())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(
            targets
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 7, 9, 7, 7, 9]
        );
        let names = arrow::compute::concat(
            &batches
                .iter()
                .map(|batch| batch.column(1).as_ref())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let names = names.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            names.iter().collect::<Vec<_>>(),
            vec![
                Some("a"),
                Some("a"),
                Some("a"),
                Some("duplicate"),
                Some("duplicate"),
                Some("duplicate"),
            ]
        );
    }
}
