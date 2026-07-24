use crate::csr_index::CsrIndex;
use arrow::compute::take;
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CsrIndexBuilder;
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
}
