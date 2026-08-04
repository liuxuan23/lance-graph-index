use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow_schema::{Schema, SchemaRef};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue, Statistics};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{col, Expr};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::{create_physical_expr, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, EvaluationType, PlanProperties,
};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{SargableQuery, ScalarIndex, SearchResult};

use crate::case_insensitive::qualify_column;

#[derive(Clone)]
struct GetVMetrics {
    input_batches: Count,
    input_rows: Count,
    lookup_batches: Count,
    lookup_keys: Count,
    unique_lookup_keys: Count,
    duplicate_lookup_keys: Count,
    target_rows_fetched: Count,
    target_ids_not_found: Count,
    output_batches: Count,
    output_rows: Count,
    scalar_lookup_time: Time,
    target_fetch_time: Time,
    attach_time: Time,
}

#[derive(Debug)]
pub struct LanceGetVByIdExec {
    input: Arc<dyn ExecutionPlan>,
    dataset: Arc<Dataset>,
    scalar_index: Option<Arc<dyn ScalarIndex>>,
    input_id_column: String,
    input_id_column_index: usize,
    target_id_field: String,
    target_id_column_index: usize,
    target_variable: String,
    target_schema: SchemaRef,
    target_predicates: Vec<Expr>,
    target_filter: Option<Arc<dyn PhysicalExpr>>,
    scalar_index_name: String,
    dataset_version: u64,
    max_lookup_keys: usize,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl LanceGetVByIdExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        input_id_column: impl Into<String>,
        target_id_field: impl Into<String>,
        target_variable: impl Into<String>,
        target_schema: SchemaRef,
        target_predicates: Vec<Expr>,
        scalar_index_name: impl Into<String>,
        dataset_version: u64,
        max_lookup_keys: usize,
    ) -> Result<Self> {
        Self::try_new_inner(
            input,
            dataset,
            None,
            input_id_column,
            target_id_field,
            target_variable,
            target_schema,
            target_predicates,
            scalar_index_name,
            dataset_version,
            max_lookup_keys,
        )
    }

    /// Create a GetV operator that reuses an already-opened Lance scalar
    /// index. This is the production path selected by node-lookup discovery.
    /// `try_new` remains available as a scanner-backed compatibility path for
    /// focused operator tests and callers that do not own an index handle.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_scalar_index(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        scalar_index: Arc<dyn ScalarIndex>,
        input_id_column: impl Into<String>,
        target_id_field: impl Into<String>,
        target_variable: impl Into<String>,
        target_schema: SchemaRef,
        target_predicates: Vec<Expr>,
        scalar_index_name: impl Into<String>,
        dataset_version: u64,
        max_lookup_keys: usize,
    ) -> Result<Self> {
        Self::try_new_inner(
            input,
            dataset,
            Some(scalar_index),
            input_id_column,
            target_id_field,
            target_variable,
            target_schema,
            target_predicates,
            scalar_index_name,
            dataset_version,
            max_lookup_keys,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_inner(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        scalar_index: Option<Arc<dyn ScalarIndex>>,
        input_id_column: impl Into<String>,
        target_id_field: impl Into<String>,
        target_variable: impl Into<String>,
        target_schema: SchemaRef,
        target_predicates: Vec<Expr>,
        scalar_index_name: impl Into<String>,
        dataset_version: u64,
        max_lookup_keys: usize,
    ) -> Result<Self> {
        let input_id_column = input_id_column.into();
        let target_id_field = target_id_field.into();
        let target_variable = target_variable.into().to_lowercase();
        if max_lookup_keys == 0 {
            return Err(DataFusionError::Plan(
                "GetV lookup batch size must be greater than zero".into(),
            ));
        }
        if dataset.version().version != dataset_version {
            return Err(DataFusionError::Plan(format!(
                "GetV target dataset version changed from {} to {}",
                dataset_version,
                dataset.version().version
            )));
        }
        let input_id_column_index = input.schema().index_of(&input_id_column).map_err(|_| {
            DataFusionError::Plan(format!(
                "GetV input ID column '{}' is missing",
                input_id_column
            ))
        })?;
        let target_id_column_index = target_schema.index_of(&target_id_field).map_err(|_| {
            DataFusionError::Plan(format!(
                "GetV target ID field '{}' is missing",
                target_id_field
            ))
        })?;
        if input.schema().field(input_id_column_index).data_type()
            != target_schema.field(target_id_column_index).data_type()
        {
            return Err(DataFusionError::Plan(format!(
                "GetV ID type mismatch: input {:?}, target {:?}",
                input.schema().field(input_id_column_index).data_type(),
                target_schema.field(target_id_column_index).data_type()
            )));
        }

        let target_filter = target_predicates
            .iter()
            .cloned()
            .reduce(Expr::and)
            .map(|predicate| {
                let target_df_schema = DFSchema::try_from(target_schema.as_ref().clone())?;
                create_physical_expr(&predicate, &target_df_schema, &ExecutionProps::new())
            })
            .transpose()?;

        let mut fields = input.schema().fields().to_vec();
        fields.extend(target_schema.fields().iter().map(|field| {
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(qualify_column(&target_variable, field.name())),
            )
        }));
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
            dataset,
            scalar_index,
            input_id_column,
            input_id_column_index,
            target_id_field,
            target_id_column_index,
            target_variable,
            target_schema,
            target_predicates,
            target_filter,
            scalar_index_name: scalar_index_name.into(),
            dataset_version,
            max_lookup_keys,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

fn apply_target_filter(
    batch: RecordBatch,
    target_filter: Option<&Arc<dyn PhysicalExpr>>,
) -> Result<RecordBatch> {
    let Some(target_filter) = target_filter else {
        return Ok(batch);
    };
    let mask = target_filter
        .evaluate(&batch)?
        .into_array(batch.num_rows())?;
    let mask = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "GetV target predicate returned {:?}, expected Boolean",
                mask.data_type()
            ))
        })?;
    Ok(filter_record_batch(&batch, mask)?)
}

fn read_lookup_ids(batch: &RecordBatch, column_index: usize) -> Result<Vec<Option<ScalarValue>>> {
    let array = batch.column(column_index);
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                Ok(None)
            } else {
                ScalarValue::try_from_array(array, row).map(Some)
            }
        })
        .collect()
}

fn stable_unique(ids: &[Option<ScalarValue>]) -> Vec<ScalarValue> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for id in ids.iter().flatten() {
        if seen.insert(id.clone()) {
            unique.push(id.clone());
        }
    }
    unique
}

fn make_lookup_filter(
    target_id_field: &str,
    ids: &[ScalarValue],
    target_predicates: &[Expr],
) -> Expr {
    let id_filter = col(target_id_field).in_list(
        ids.iter()
            .cloned()
            .map(|value| Expr::Literal(value, None))
            .collect(),
        false,
    );
    target_predicates.iter().cloned().fold(id_filter, Expr::and)
}

#[allow(clippy::too_many_arguments)]
async fn lookup_and_attach_batch(
    input: RecordBatch,
    dataset: Arc<Dataset>,
    scalar_index: Option<Arc<dyn ScalarIndex>>,
    input_id_column_index: usize,
    target_id_field: String,
    target_id_column_index: usize,
    target_schema: SchemaRef,
    target_predicates: Vec<Expr>,
    target_filter: Option<Arc<dyn PhysicalExpr>>,
    max_lookup_keys: usize,
    output_schema: SchemaRef,
    task_context: Arc<TaskContext>,
    metrics: GetVMetrics,
) -> Result<Vec<RecordBatch>> {
    metrics.input_batches.add(1);
    metrics.input_rows.add(input.num_rows());
    if input.num_rows() == 0 {
        return Ok(vec![]);
    }

    let ids = read_lookup_ids(&input, input_id_column_index)?;
    let unique_ids = stable_unique(&ids);
    metrics.lookup_keys.add(ids.iter().flatten().count());
    metrics.unique_lookup_keys.add(unique_ids.len());
    metrics.duplicate_lookup_keys.add(
        ids.iter()
            .flatten()
            .count()
            .saturating_sub(unique_ids.len()),
    );
    if unique_ids.is_empty() {
        metrics.target_ids_not_found.add(input.num_rows());
        return Ok(vec![]);
    }
    let requested_ids = unique_ids.iter().cloned().collect::<HashSet<_>>();

    let target_columns = target_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    let mut fetched_batches = Vec::new();
    for chunk in unique_ids.chunks(max_lookup_keys) {
        metrics.lookup_batches.add(1);
        if let Some(scalar_index) = &scalar_index {
            let lookup_start = Instant::now();
            let search_result = scalar_index
                .search(&SargableQuery::IsIn(chunk.to_vec()), &NoOpMetricsCollector)
                .await
                .map_err(DataFusionError::from)?;
            metrics
                .scalar_lookup_time
                .add_duration(lookup_start.elapsed());

            let row_id_map = match search_result {
                SearchResult::Exact(row_ids) | SearchResult::AtMost(row_ids) => row_ids,
                SearchResult::AtLeast(_) => {
                    return Err(DataFusionError::Execution(
                        "GetV scalar index returned an incomplete AtLeast result".into(),
                    ));
                }
            };
            let row_ids = row_id_map
                .row_ids()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "GetV scalar index returned non-enumerable full-fragment row IDs".into(),
                    )
                })?
                .map(u64::from)
                .collect::<Vec<_>>();
            if row_ids.is_empty() {
                continue;
            }

            let fetch_start = Instant::now();
            let batch = dataset
                .take_rows(&row_ids, dataset.schema().clone())
                .await
                .map_err(DataFusionError::from)?;
            metrics
                .target_fetch_time
                .add_duration(fetch_start.elapsed());
            metrics.target_rows_fetched.add(batch.num_rows());
            let batch = apply_target_filter(batch, target_filter.as_ref())?;
            if batch.num_rows() != 0 {
                fetched_batches.push(batch);
            }
        } else {
            let mut scanner = dataset.scan();
            scanner
                .project(&target_columns)
                .map_err(DataFusionError::from)?;
            scanner.filter_expr(make_lookup_filter(
                &target_id_field,
                chunk,
                &target_predicates,
            ));

            let lookup_start = Instant::now();
            let plan = scanner.create_plan().await.map_err(DataFusionError::from)?;
            metrics
                .scalar_lookup_time
                .add_duration(lookup_start.elapsed());

            let fetch_start = Instant::now();
            let batches = collect(plan, task_context.clone()).await?;
            metrics
                .target_fetch_time
                .add_duration(fetch_start.elapsed());
            metrics
                .target_rows_fetched
                .add(batches.iter().map(RecordBatch::num_rows).sum());
            fetched_batches.extend(batches);
        }
    }

    if fetched_batches.is_empty() {
        metrics.target_ids_not_found.add(input.num_rows());
        return Ok(vec![]);
    }

    let fetched = concat_batches(&target_schema, &fetched_batches)?;
    let attach_start = Instant::now();
    let target_ids = fetched.column(target_id_column_index);
    let mut target_rows = HashMap::with_capacity(fetched.num_rows());
    for row in 0..fetched.num_rows() {
        let id = ScalarValue::try_from_array(target_ids, row)?;
        if !requested_ids.contains(&id) {
            continue;
        }
        if target_rows.insert(id.clone(), row as u32).is_some() {
            return Err(DataFusionError::Execution(format!(
                "GetV target ID '{}' is not unique",
                id
            )));
        }
    }

    let mut input_indices = Vec::new();
    let mut target_indices = Vec::new();
    for (input_row, id) in ids.iter().enumerate() {
        let Some(id) = id else {
            continue;
        };
        if let Some(&target_row) = target_rows.get(id) {
            input_indices.push(input_row as u32);
            target_indices.push(target_row);
        } else {
            metrics.target_ids_not_found.add(1);
        }
    }
    if input_indices.is_empty() {
        metrics.attach_time.add_duration(attach_start.elapsed());
        return Ok(vec![]);
    }

    let input_indices = UInt32Array::from(input_indices);
    let target_indices = UInt32Array::from(target_indices);
    let mut columns = input
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &input_indices, None))
        .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
    columns.extend(
        fetched
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &target_indices, None))
            .collect::<std::result::Result<Vec<ArrayRef>, _>>()?,
    );
    let output = RecordBatch::try_new(output_schema, columns)?;
    metrics.output_batches.add(1);
    metrics.output_rows.add(output.num_rows());
    metrics.attach_time.add_duration(attach_start.elapsed());
    Ok(vec![output])
}

impl DisplayAs for LanceGetVByIdExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LanceGetVByIdExec: target={} AS {}, input_id={}, target_id={}, scalar_index={}, dataset_version={}, max_lookup_keys={}, preserve_multiplicity=true",
            self.dataset.uri(),
            self.target_variable,
            self.input_id_column,
            self.target_id_field,
            self.scalar_index_name,
            self.dataset_version,
            self.max_lookup_keys
        )
    }
}

impl ExecutionPlan for LanceGetVByIdExec {
    fn name(&self) -> &str {
        "LanceGetVByIdExec"
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
            return Err(DataFusionError::Plan("GetV expects one child".into()));
        }
        let rebuilt = if let Some(scalar_index) = &self.scalar_index {
            Self::try_new_with_scalar_index(
                children[0].clone(),
                self.dataset.clone(),
                scalar_index.clone(),
                self.input_id_column.clone(),
                self.target_id_field.clone(),
                self.target_variable.clone(),
                self.target_schema.clone(),
                self.target_predicates.clone(),
                self.scalar_index_name.clone(),
                self.dataset_version,
                self.max_lookup_keys,
            )?
        } else {
            Self::try_new(
                children[0].clone(),
                self.dataset.clone(),
                self.input_id_column.clone(),
                self.target_id_field.clone(),
                self.target_variable.clone(),
                self.target_schema.clone(),
                self.target_predicates.clone(),
                self.scalar_index_name.clone(),
                self.dataset_version,
                self.max_lookup_keys,
            )?
        };
        Ok(Arc::new(rebuilt))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;
        let metrics = GetVMetrics {
            input_batches: MetricBuilder::new(&self.metrics).counter("input_batches", partition),
            input_rows: MetricBuilder::new(&self.metrics).counter("input_rows", partition),
            lookup_batches: MetricBuilder::new(&self.metrics).counter("lookup_batches", partition),
            lookup_keys: MetricBuilder::new(&self.metrics).counter("lookup_keys", partition),
            unique_lookup_keys: MetricBuilder::new(&self.metrics)
                .counter("unique_lookup_keys", partition),
            duplicate_lookup_keys: MetricBuilder::new(&self.metrics)
                .counter("duplicate_lookup_keys", partition),
            target_rows_fetched: MetricBuilder::new(&self.metrics)
                .counter("target_rows_fetched", partition),
            target_ids_not_found: MetricBuilder::new(&self.metrics)
                .counter("target_ids_not_found", partition),
            output_batches: MetricBuilder::new(&self.metrics).counter("output_batches", partition),
            output_rows: MetricBuilder::new(&self.metrics).output_rows(partition),
            scalar_lookup_time: MetricBuilder::new(&self.metrics)
                .subset_time("scalar_lookup_time", partition),
            target_fetch_time: MetricBuilder::new(&self.metrics)
                .subset_time("target_fetch_time", partition),
            attach_time: MetricBuilder::new(&self.metrics).subset_time("attach_time", partition),
        };
        let dataset = self.dataset.clone();
        let scalar_index = self.scalar_index.clone();
        let input_id_column_index = self.input_id_column_index;
        let target_id_field = self.target_id_field.clone();
        let target_id_column_index = self.target_id_column_index;
        let target_schema = self.target_schema.clone();
        let target_predicates = self.target_predicates.clone();
        let target_filter = self.target_filter.clone();
        let target_description = format!("{} AS {}", self.dataset.uri(), self.target_variable);
        let scalar_index_name = self.scalar_index_name.clone();
        let dataset_version = self.dataset_version;
        let max_lookup_keys = self.max_lookup_keys;
        let output_schema = self.schema.clone();
        let stream = input
            .then(move |batch| {
                let dataset = dataset.clone();
                let scalar_index = scalar_index.clone();
                let target_id_field = target_id_field.clone();
                let target_schema = target_schema.clone();
                let target_predicates = target_predicates.clone();
                let target_filter = target_filter.clone();
                let target_description = target_description.clone();
                let scalar_index_name = scalar_index_name.clone();
                let output_schema = output_schema.clone();
                let context = context.clone();
                let metrics = metrics.clone();
                async move {
                    let batch = batch?;
                    let lookup_batch_key_count = batch.num_rows();
                    let error_target_id_field = target_id_field.clone();
                    lookup_and_attach_batch(
                        batch,
                        dataset,
                        scalar_index,
                        input_id_column_index,
                        target_id_field,
                        target_id_column_index,
                        target_schema,
                        target_predicates,
                        target_filter,
                        max_lookup_keys,
                        output_schema,
                        context,
                        metrics,
                    )
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "GetV lookup failed: target={}, target_id_field={}, scalar_index={}, dataset_version={}, lookup_batch_key_count={}: {}",
                            target_description,
                            error_target_id_field,
                            scalar_index_name,
                            dataset_version,
                            lookup_batch_key_count,
                            error
                        ))
                    })
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
