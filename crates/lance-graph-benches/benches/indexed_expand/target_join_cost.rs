// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Isolates the remaining target-node Join after CSR indexed expansion.
//!
//! The selected source always has ten neighbors. Only the target node table
//! grows, so CSR lookup and expansion work stay constant while the current
//! target Join must probe a progressively larger node-table scan.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::catalog::Session;
use datafusion::common::NullEquality;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::{context::SessionContext, context::SessionState, TaskContext};
use datafusion::logical_expr::{Expr, JoinType, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use lance::datafusion::LanceTableProvider;
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexInternalExt;
use lance_graph::datafusion_planner::get_v::LanceGetVByIdExec;
use lance_graph::datafusion_planner::indexed_expand::IndexedExpandExec;
use lance_graph::{CsrIndex, CsrIndexBuilder};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};

const QUERY_SOURCE_ID: u64 = 42;
const DEGREE: usize = 10;

struct TargetJoinCase {
    _target_dir: tempfile::TempDir,
    index: Arc<CsrIndex>,
    expand_plan: Arc<dyn ExecutionPlan>,
    expand_task_context: Arc<TaskContext>,
    mem_target_join_plan: Arc<dyn ExecutionPlan>,
    mem_target_join_task_context: Arc<TaskContext>,
    full_mem_target_join_plan: Arc<dyn ExecutionPlan>,
    full_mem_target_join_task_context: Arc<TaskContext>,
    target_join: FreshTargetJoin,
    full_target_join: FreshTargetJoin,
    fresh_join_task_context: Arc<TaskContext>,
    get_v_plan: Arc<dyn ExecutionPlan>,
    get_v_task_context: Arc<TaskContext>,
    full_get_v_plan: Arc<dyn ExecutionPlan>,
    full_get_v_task_context: Arc<TaskContext>,
}

/// Rebuilds the HashJoinExec for every iteration so DataFusion cannot reuse
/// its build-side OnceAsync state. The Lance scan plan itself is reusable and
/// opens a fresh execution stream each time.
#[derive(Clone)]
struct FreshTargetJoin {
    target: Arc<dyn TableProvider>,
    scan_state: SessionState,
    expanded: Arc<dyn ExecutionPlan>,
    expanded_id_index: usize,
}

impl FreshTargetJoin {
    fn build(&self, rt: &tokio::runtime::Runtime) -> Arc<dyn ExecutionPlan> {
        let target = rt
            .block_on(self.target.scan(&self.scan_state, None, &[], None))
            .unwrap();
        let target_id = Arc::new(Column::new("person_id", 0)) as Arc<dyn PhysicalExpr>;
        let expanded_id =
            Arc::new(Column::new("dst_id", self.expanded_id_index)) as Arc<dyn PhysicalExpr>;
        Arc::new(
            HashJoinExec::try_new(
                target,
                self.expanded.clone(),
                vec![(target_id, expanded_id)],
                None,
                &JoinType::Inner,
                Some(vec![1]),
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        )
    }
}

#[derive(Debug)]
struct ExecTableProvider {
    plan: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl TableProvider for ExecTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let Some(projection) = projection else {
            return Ok(self.plan.clone());
        };
        let input_schema = self.plan.schema();
        let expressions = projection.iter().map(|&index| ProjectionExpr {
            expr: Arc::new(Column::new(input_schema.field(index).name(), index)),
            alias: input_schema.field(index).name().clone(),
        });
        Ok(Arc::new(ProjectionExec::try_new(
            expressions,
            self.plan.clone(),
        )?))
    }
}

fn make_nodes(target_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..target_rows as i64).collect();
    let names: Vec<String> = (0..target_rows).map(|id| format!("person_{id}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn make_expanded_targets(degree: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dst_id",
        DataType::Int64,
        false,
    )]));
    let ids: Vec<i64> = (1..=degree)
        .map(|offset| (QUERY_SOURCE_ID as usize + offset) as i64)
        .collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap()
}

fn make_index(target_rows: usize, degree: usize) -> Arc<CsrIndex> {
    let mut builder = CsrIndexBuilder::new().with_num_vertices(target_rows as u64);
    for offset in 1..=degree {
        builder = builder.add_edge(QUERY_SOURCE_ID, QUERY_SOURCE_ID + offset as u64);
    }
    Arc::new(builder.try_build().unwrap())
}

fn memory_plan(rt: &tokio::runtime::Runtime, batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    let state = SessionContext::new().state();
    rt.block_on(table.scan(&state, None, &[], None)).unwrap()
}

fn make_expand_plan(
    rt: &tokio::runtime::Runtime,
    index: Arc<CsrIndex>,
) -> (Arc<dyn ExecutionPlan>, Arc<TaskContext>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a__person_id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![QUERY_SOURCE_ID as i64]))],
    )
    .unwrap();
    let input = memory_plan(rt, batch);
    let plan = IndexedExpandExec::try_new(
        input,
        index,
        "a__person_id",
        Arc::new(Field::new("dst_id", DataType::Int64, false)),
        8_192,
    )
    .unwrap();
    (Arc::new(plan), Arc::new(TaskContext::default()))
}

fn make_expanded_input(rt: &tokio::runtime::Runtime, degree: usize) -> Arc<dyn ExecutionPlan> {
    let batch = make_expanded_targets(degree);
    memory_plan(rt, batch)
}

fn plan_target_join(
    rt: &tokio::runtime::Runtime,
    expanded: Arc<dyn TableProvider>,
    person: Arc<dyn TableProvider>,
) -> (Arc<dyn ExecutionPlan>, Arc<TaskContext>) {
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    context.register_table("expanded", expanded).unwrap();
    context.register_table("person", person).unwrap();
    let df = rt
        .block_on(context.sql(
            "SELECT person.name \
             FROM expanded \
             JOIN person ON expanded.dst_id = person.person_id",
        ))
        .unwrap();
    let plan = rt.block_on(df.create_physical_plan()).unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("HashJoinExec"),
        "expected a physical HashJoinExec, got:\n{plan_text}"
    );
    (plan, context.task_ctx())
}

fn setup_case(rt: &tokio::runtime::Runtime, target_rows: usize, degree: usize) -> TargetJoinCase {
    assert!(
        target_rows > QUERY_SOURCE_ID as usize + degree,
        "all benchmark target IDs must exist"
    );

    let nodes = make_nodes(target_rows);
    let expanded_targets = make_expanded_targets(degree);
    let node_table =
        Arc::new(MemTable::try_new(nodes.schema(), vec![vec![nodes.clone()]]).unwrap());
    let expanded_table = Arc::new(
        MemTable::try_new(expanded_targets.schema(), vec![vec![expanded_targets]]).unwrap(),
    );
    let index = make_index(target_rows, degree);
    let (expand_plan, expand_task_context) = make_expand_plan(rt, index.clone());

    // Build and open the indexed Lance target outside all timed iterations.
    let target_dir = tempfile::tempdir().unwrap();
    let target_uri = target_dir.path().join("person.lance");
    let reader = RecordBatchIterator::new(vec![Ok(nodes.clone())], nodes.schema());
    let mut target_dataset = rt
        .block_on(Dataset::write(
            reader,
            target_uri.to_str().unwrap(),
            Some(WriteParams::default()),
        ))
        .unwrap();
    rt.block_on(target_dataset.create_index(
        &["person_id"],
        IndexType::BTree,
        Some("person_id_btree".into()),
        &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
        false,
    ))
    .unwrap();
    let target_dataset = Arc::new(
        rt.block_on(Dataset::open(target_uri.to_str().unwrap()))
            .unwrap(),
    );
    let target_version = target_dataset.version().version;
    let index_metadata = rt.block_on(target_dataset.load_indices()).unwrap();
    let index_uuid = index_metadata
        .iter()
        .find(|index| index.name == "person_id_btree")
        .unwrap()
        .uuid
        .to_string();
    let scalar_index = rt
        .block_on(target_dataset.open_scalar_index("person_id", &index_uuid, &NoOpMetricsCollector))
        .unwrap();
    let target_schema: Schema = target_dataset.schema().into();
    let target_schema = Arc::new(target_schema);
    let lance_target_provider: Arc<dyn TableProvider> = Arc::new(LanceTableProvider::new(
        target_dataset.clone(),
        false,
        false,
    ));
    let lance_scan_state =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)).state();
    let get_v_plan: Arc<dyn ExecutionPlan> = Arc::new(
        LanceGetVByIdExec::try_new_with_scalar_index(
            make_expanded_input(rt, degree),
            target_dataset.clone(),
            scalar_index.clone(),
            "dst_id",
            "person_id",
            "b",
            target_schema.clone(),
            vec![],
            "person_id_btree",
            target_version,
            8_192,
        )
        .unwrap(),
    );
    let get_v_task_context = Arc::new(TaskContext::default());
    let (full_expand_plan, _) = make_expand_plan(rt, index.clone());
    let full_get_v_plan: Arc<dyn ExecutionPlan> = Arc::new(
        LanceGetVByIdExec::try_new_with_scalar_index(
            full_expand_plan,
            target_dataset.clone(),
            scalar_index,
            "dst_id",
            "person_id",
            "b",
            target_schema,
            vec![],
            "person_id_btree",
            target_version,
            8_192,
        )
        .unwrap(),
    );
    let full_get_v_task_context = Arc::new(TaskContext::default());

    // Keep a MemTable Join as a best-case in-memory lower bound. The production
    // comparison scans the same Lance target dataset as GetV and rebuilds only
    // HashJoinExec per iteration, avoiding build-side cache reuse.
    let (mem_target_join_plan, mem_target_join_task_context) =
        plan_target_join(rt, expanded_table, node_table.clone());
    let (mem_target_join_expand, _) = make_expand_plan(rt, index.clone());
    let (full_mem_target_join_plan, full_mem_target_join_task_context) = plan_target_join(
        rt,
        Arc::new(ExecTableProvider {
            plan: mem_target_join_expand,
        }),
        node_table,
    );
    let target_join = FreshTargetJoin {
        target: lance_target_provider.clone(),
        scan_state: lance_scan_state.clone(),
        expanded: make_expanded_input(rt, degree),
        expanded_id_index: 0,
    };
    let full_target_join = FreshTargetJoin {
        target: lance_target_provider,
        scan_state: lance_scan_state,
        expanded: make_expand_plan(rt, index.clone()).0,
        expanded_id_index: 1,
    };

    TargetJoinCase {
        _target_dir: target_dir,
        index,
        expand_plan,
        expand_task_context,
        mem_target_join_plan,
        mem_target_join_task_context,
        full_mem_target_join_plan,
        full_mem_target_join_task_context,
        target_join,
        full_target_join,
        fresh_join_task_context: Arc::new(TaskContext::default()),
        get_v_plan,
        get_v_task_context,
        full_get_v_plan,
        full_get_v_task_context,
    }
}

fn run_plan(
    rt: &tokio::runtime::Runtime,
    plan: &Arc<dyn ExecutionPlan>,
    task_context: &Arc<TaskContext>,
) -> usize {
    rt.block_on(collect(plan.clone(), task_context.clone()))
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

fn run_fresh_target_join(
    rt: &tokio::runtime::Runtime,
    join: &FreshTargetJoin,
    task_context: &Arc<TaskContext>,
) -> usize {
    let plan = join.build(rt);
    run_plan(rt, &plan, task_context)
}

fn bench_target_join_cost(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("indexed_expand_target_join_cost");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for target_rows in [1_000usize, 10_000, 100_000, 1_000_000] {
        let case = setup_case(&rt, target_rows, DEGREE);

        assert_eq!(case.index.neighbors(QUERY_SOURCE_ID).len(), DEGREE);
        assert_eq!(
            run_plan(&rt, &case.expand_plan, &case.expand_task_context),
            DEGREE
        );
        assert_eq!(
            run_plan(
                &rt,
                &case.mem_target_join_plan,
                &case.mem_target_join_task_context,
            ),
            DEGREE
        );
        assert_eq!(
            run_plan(
                &rt,
                &case.full_mem_target_join_plan,
                &case.full_mem_target_join_task_context,
            ),
            DEGREE
        );
        assert_eq!(
            run_fresh_target_join(&rt, &case.target_join, &case.fresh_join_task_context,),
            DEGREE
        );
        assert_eq!(
            run_fresh_target_join(&rt, &case.full_target_join, &case.fresh_join_task_context,),
            DEGREE
        );
        assert_eq!(
            run_plan(&rt, &case.get_v_plan, &case.get_v_task_context),
            DEGREE
        );
        assert_eq!(
            run_plan(&rt, &case.full_get_v_plan, &case.full_get_v_task_context,),
            DEGREE
        );

        group.bench_with_input(
            BenchmarkId::new("csr_neighbors_only", target_rows),
            &target_rows,
            |b, _| {
                b.iter(|| {
                    let neighbors = case.index.neighbors(black_box(QUERY_SOURCE_ID));
                    black_box(neighbors.len())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("indexed_expand_only", target_rows),
            &target_rows,
            |b, _| {
                b.iter(|| black_box(run_plan(&rt, &case.expand_plan, &case.expand_task_context)))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("get_v_by_id_only", target_rows),
            &target_rows,
            |b, _| b.iter(|| black_box(run_plan(&rt, &case.get_v_plan, &case.get_v_task_context))),
        );
        group.bench_with_input(
            BenchmarkId::new("mem_target_join_lower_bound", target_rows),
            &target_rows,
            |b, _| {
                b.iter(|| {
                    black_box(run_plan(
                        &rt,
                        &case.mem_target_join_plan,
                        &case.mem_target_join_task_context,
                    ))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("target_join_only", target_rows),
            &target_rows,
            |b, _| {
                b.iter(|| {
                    black_box(run_fresh_target_join(
                        &rt,
                        &case.target_join,
                        &case.fresh_join_task_context,
                    ))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("full_mem_indexed_query_lower_bound", target_rows),
            &target_rows,
            |b, _| {
                b.iter(|| {
                    black_box(run_plan(
                        &rt,
                        &case.full_mem_target_join_plan,
                        &case.full_mem_target_join_task_context,
                    ))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("full_indexed_query", target_rows),
            &target_rows,
            |b, _| {
                b.iter(|| {
                    black_box(run_fresh_target_join(
                        &rt,
                        &case.full_target_join,
                        &case.fresh_join_task_context,
                    ))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("full_indexed_get_v", target_rows),
            &target_rows,
            |b, _| {
                b.iter(|| {
                    black_box(run_plan(
                        &rt,
                        &case.full_get_v_plan,
                        &case.full_get_v_task_context,
                    ))
                })
            },
        );
    }
    group.finish();
}

fn bench_get_v_degree(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let target_rows = 1_000_000usize;
    let mut group = c.benchmark_group("indexed_get_v_degree_cost");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for degree in [1usize, 10, 100, 1_000, 10_000] {
        let case = setup_case(&rt, target_rows, degree);
        assert_eq!(case.index.neighbors(QUERY_SOURCE_ID).len(), degree);
        assert_eq!(
            run_plan(&rt, &case.expand_plan, &case.expand_task_context),
            degree
        );
        assert_eq!(
            run_plan(&rt, &case.get_v_plan, &case.get_v_task_context),
            degree
        );
        assert_eq!(
            run_fresh_target_join(&rt, &case.target_join, &case.fresh_join_task_context,),
            degree
        );
        assert_eq!(
            run_plan(&rt, &case.full_get_v_plan, &case.full_get_v_task_context,),
            degree
        );

        group.bench_with_input(
            BenchmarkId::new("indexed_expand_only", degree),
            &degree,
            |b, _| {
                b.iter(|| black_box(run_plan(&rt, &case.expand_plan, &case.expand_task_context)))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("get_v_by_id_only", degree),
            &degree,
            |b, _| b.iter(|| black_box(run_plan(&rt, &case.get_v_plan, &case.get_v_task_context))),
        );
        group.bench_with_input(
            BenchmarkId::new("target_join_only", degree),
            &degree,
            |b, _| {
                b.iter(|| {
                    black_box(run_fresh_target_join(
                        &rt,
                        &case.target_join,
                        &case.fresh_join_task_context,
                    ))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("full_indexed_get_v", degree),
            &degree,
            |b, _| {
                b.iter(|| {
                    black_box(run_plan(
                        &rt,
                        &case.full_get_v_plan,
                        &case.full_get_v_task_context,
                    ))
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_target_join_cost, bench_get_v_degree);
criterion_main!(benches);
