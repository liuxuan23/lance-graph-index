# 基于节点语义 ID 的 Indexed GetV 第一版实施计划

## 1. 文档状态

- 状态：Implemented and Verified
- 完成日期：2026-08-04
- 目标分支：`research/graph-index`
- 基线提交：`b1253c4 feat(graph): persist CSR indexes in Lance`
- 前置能力：内存/持久化 CSR、`IndexedExpandNode`、`IndexedExpandExec`
- 目标阶段：消除 CSR 路径中与 target 节点表的全表 Scan + Hash Join
- 主要依赖：DataFusion 50.3、Arrow 56.2、Lance 1.0.4

本文档规划第一版 `IndexedExpand → GetV` 查询路径。第一版不修改 CSR 的持久化格式，
CSR neighbor 继续保存节点语义 ID；新增的 GetV 算子使用 target 节点语义 ID 上的 Lance
scalar index，将一批 neighbor ID 转换为目标节点行，并把目标节点属性重新附着到
`IndexedExpand` 输出。

本文档中的 `GetV` 表示图执行计划中的“根据节点标识物化节点记录”，不是 GraphAr
内部按 vertex ordinal 直接访问属性列的原样实现。

## 2. 背景与验证结论

当前 CSR 查询路径为：

```text
Source Scan
  → IndexedExpand(CSR)
  → Target Scan
  → Endpoint-Target Hash Join
  → Projection
```

现有 `IndexedExpand` 已经消除：

- relationship table scan；
- source 与 relationship table 的第一次 Join。

它仍然输出关系 target endpoint 的语义 ID：

```text
input source columns
+
friend_of_0__dst_id
```

随后当前 planner 调用 `join_relationship_to_target()`，构造：

```text
friend_of_0__dst_id = b__person_id
```

的 Inner Join。

`indexed_expand_target_join_cost` benchmark 已经隔离验证：当命中固定为 10 个 target、
target 表从 1 千行增长到 100 万行时：

```text
CSR neighbors lookup:       约 3 ns，基本不变
IndexedExpand only:         约 4 µs，基本不变
Target Join only:           约 57 µs → 7.37 ms
Full indexed query:         约 3.00 ms → 23.45 ms
```

100 万 target 行时，单独 target Join 比 `IndexedExpandExec` 约慢 1,800 倍。Target Join
在 10 万到 100 万行之间呈接近线性增长，说明当前 target 全表 Scan/Join 是大规模查询的
主要扩展性问题之一。

## 3. 第一版设计决策摘要

第一版采用以下明确决策：

1. 不修改 CSR neighbor 内容，neighbor 仍是 target 节点语义 ID；
2. 增加独立的 `GetVNode` DataFusion 扩展逻辑节点；
3. 增加 `LanceGetVByIdExec` 物理算子；
4. GetV 只在 target provider 是 `LanceTableProvider`，且 target ID 列存在可用 scalar
   index 时启用；
5. 无 Lance provider 或无 scalar index 时，保留当前 `IndexedExpand → Target Join`；
6. 第一版在 capability discovery 阶段打开并缓存 target ID scalar-index handle，执行时直接
   调用 `ScalarIndex::search(SargableQuery::IsIn)`，再用返回的 row address 调用
   `Dataset::take_rows`；不为每个 Expand batch 动态创建 Lance scanner physical plan；
7. `IN` 查询前只对 lookup key 去重，最终输出必须恢复 source 上下文、输入顺序和重复边；
8. GetV 内部允许使用 batch-local ID map，但不再扫描 N 行 target 表做 Hash Join；
9. 第一版按 `IndexedExpand` output batch 顺序串行 lookup，不增加跨 batch 并发和 cache；
10. target ID 必须满足节点 ID 唯一性契约；如果 lookup 返回重复 target ID，第一版明确报错；
11. `IndexUsagePolicy::Require` 第一版仍只表示“CSR 必须可用”，不表示 target scalar
    index 必须可用；target lookup 不可用时仍允许回退 target Join；
12. 第一版优先完成正确性和成本曲线闭环，不同时引入 row-id CSR、late materialization、
    动态 mask scan 或可变长度路径优化。

## 4. 目标

完成后，以下查询：

```cypher
MATCH (a:Person {person_id: 42})
      -[:FRIEND_OF]->
      (b:Person)
RETURN a.name, b.name
```

在同时存在以下索引时：

```text
FRIEND_OF outgoing CSR
Person.person_id Lance scalar index
```

应产生：

```text
Source Scan/Filter
  → IndexedExpand(CSR)
  → GetVBySemanticId(Person.person_id)
  → Projection
```

而不是：

```text
Source Scan/Filter
  → IndexedExpand(CSR)
  → Target full Scan
  → Hash Join
  → Projection
```

必须满足以下结果：

1. 物理计划中存在 `IndexedExpandExec` 和 `LanceGetVByIdExec`；
2. 物理计划中不存在 endpoint-target `HashJoinExec`；
3. target lookup 使用 `person_id IN (...)`，并在测试计划/metrics 中证明使用 scalar
   index；
4. target 表从 10 万增至 100 万行、命中固定为 10 行时，GetV 时间不再随 target 总行数
   近似线性增长；
5. 输出结果与当前 target Join 路径一致；
6. self-loop、parallel edges、多个 source 指向同一 target 和 dangling target 保持当前
   Inner Join 语义；
7. MemTable、非 Lance provider 和没有 target scalar index 的查询继续使用当前 target Join；
8. 现有非索引执行 API 行为不变；
9. CSR 持久化格式和已有 generation 不发生变化；
10. benchmark 分别计时 Expand、GetV、旧 Target Join 和完整查询。

## 5. 非目标

第一版不包含：

- 将 Lance row ID 写入 CSR；
- 外部 ID 到内部稠密 ordinal 的 `VertexIdMap`；
- 直接调用 `Dataset::take_rows(CSR neighbors)`；
- target row-id bitmap / dynamic mask scan；
- 基于 frontier 大小在 point lookup、mask scan 和 Hash Join 之间做成本选择；
- target 属性 late materialization；
- 跨 lookup batch 的 LRU node cache；
- 多个 lookup batch 的并发执行；
- incoming、undirected、多关系类型和 variable-length path 的新能力；
- 关系变量、关系属性过滤和关系属性返回；
- target variable reuse；
- String/UUID CSR ID；
- 节点 ID 唯一索引的自动创建和维护；
- 修改 `GraphSourceCatalog` crate 使其直接依赖 Lance；
- 用 GetV 替换非 CSR Join 路径；
- 在查询只返回 `b.id` 时跳过 GetV；
- 自动修复 stale CSR 或 stale node scalar index。

## 6. ID 与快照契约

### 6.1 当前 CSR 契约保持不变

第一版继续使用：

```text
source node semantic ID == CSR source vertex ID
CSR neighbor == target node semantic ID
```

例如：

```text
Person.person_id = 42
CSR.neighbors(42) = [43, 44, 45]
```

CSR 不保存：

```text
Lance row ID
Lance row address
target dataset row offset
```

### 6.2 Target ID 唯一性

`GraphConfig` 中配置的 node ID field 必须是节点 label 内的唯一标识：

```text
(target_label, target_id_field, target_id_value)
→ at most one live target row
```

第一版 GetV 在单次 lookup 结果中发现重复 ID 时返回明确错误，不静默选择其中一行。
后续可以在节点表注册或索引创建阶段提前验证唯一性。

### 6.3 数据类型

第一版支持与当前 CSR 相同的类型：

```text
UInt32
UInt64
Int32
Int64
```

以下三处类型必须完全匹配：

```text
IndexedExpand output ID type
GraphConfig target node ID type
Lance target ID scalar index value type
```

不在 GetV 中做可能丢失信息的隐式 cast。

### 6.4 快照一致性

GetV 使用查询时已经注册的 target `Dataset` 快照。CSR 继续使用已注册 generation。
第一版必须在 explain/metadata 中暴露：

```text
CSR generation
target dataset version
target scalar index name
```

如果 CSR 包含已删除 target 的 ID，GetV 查不到该节点时丢弃对应 Expand 行，保持当前
Inner Join 对 dangling endpoint 的行为。

第一版不新增跨 relationship/target dataset 的事务快照协议，但测试必须覆盖 target 缺失
节点。

## 7. 总体架构

```text
                         ┌────────────────────────────┐
                         │ GraphIndexRegistry         │
                         │ FRIEND_OF/outgoing → CSR   │
                         └──────────────┬─────────────┘
                                        │
Cypher                                  │
  ↓                                     │
LogicalOperator::Expand                 │
  ↓                                     │
DataFusionPlanner                       │
  ↓                                     │
IndexedExpandNode ──────────────────────┘
  ↓ target semantic IDs
GetVNode
  ↓
GraphQueryPlanner
  ├─ IndexedExpandExtensionPlanner
  └─ GetVExtensionPlanner
       ↓ resolve
  NodeLookupRegistry
       ↓
  LanceGetVByIdExec
       ↓
  Person.person_id scalar index
       ↓
  Lance target rows
```

建议新增目录：

```text
crates/lance-graph/src/
├── node_lookup/
│   ├── mod.rs
│   ├── metadata.rs
│   ├── registry.rs
│   └── discovery.rs
│
└── datafusion_planner/
    └── get_v/
        ├── mod.rs
        ├── logical.rs
        ├── physical.rs
        └── planner.rs
```

`node_lookup` 只描述 target semantic ID lookup 资源，不修改 `lance-graph-catalog` 的依赖
方向。

## 8. 优化前后的逻辑计划

### 8.1 当前索引逻辑计划

```text
Projection: a__name, b__name
  HashJoin:
    type=Inner
    on=friend_of_0__dst_id = b__person_id

    IndexedExpand:
      relationship_type=friend_of
      direction=Outgoing
      source=a__person_id
      target=friend_of_0__dst_id
      index_generation=1
      Filter: a__person_id = 42
        TableScan: Person AS a

    TableScan: Person AS b
```

### 8.2 第一版目标逻辑计划

```text
Projection: a__name, b__name
  GetV:
    target_label=person
    target_variable=b
    input_id=friend_of_0__dst_id
    target_id_field=person_id
    lookup=SemanticIdScalarIndex
    target_predicates=[]
    lookup_batch_size=8192

    IndexedExpand:
      relationship_type=friend_of
      direction=Outgoing
      source=a__person_id
      target=friend_of_0__dst_id
      index_generation=1

      Filter: a__person_id = 42
        TableScan: Person AS a
```

target 节点表不再作为 `GetVNode` 的第二个普通 `LogicalPlan` input。否则默认物理 planner
会提前把它变成一个 target Scan，无法在执行时使用 Expand 产生的动态 ID filter。

`GetVNode` 只保存可比较、可 Hash 的 `NodeLookupReference`。真实 `Arc<Dataset>` 由
extension planner 从 registry 解析。

## 9. 优化后的物理执行计划

### 9.1 Explain 中的目标计划

```text
ProjectionExec: expr=[a__name, b__name]
  LanceGetVByIdExec:
    target=person
    input_id=friend_of_0__dst_id
    target_id_field=person_id
    scalar_index=person_id_idx
    target_dataset_version=7
    lookup_batch_size=8192
    preserve_multiplicity=true

    IndexedExpandExec:
      source_column_index=0
      target=friend_of_0__dst_id

      FilterExec: a__person_id = 42
        DataSourceExec: Person AS a
```

该计划不应包含：

```text
HashJoinExec(friend_of_0__dst_id = b__person_id)
```

### 9.2 `LanceGetVByIdExec` 内部阶段

```text
IndexedExpand output batch
  ↓
Extract input target semantic IDs
  ↓
Deduplicate lookup keys only
  ↓
Chunk unique IDs by max_lookup_keys
  ↓
ScalarIndex::search(IsIn(...))
  ↓ SearchResult::Exact / AtMost
Enumerate Lance row addresses
  ↓
Dataset::take_rows(row_addresses, target schema)
  ↓
Residual target predicate on fetched rows
  ↓
matching target rows
  ↓
Build semantic ID → target row map
  ↓
Replay original expanded ID sequence
  ↓
Arrow take input rows + Arrow take target rows
  ↓
input columns + aliased target columns
```

GetV 内部形成：

```text
ScalarIndex::search(SargableQuery::IsIn)
  → SearchResult row-address set
  → Dataset::take_rows
  → DataFusion PhysicalExpr residual filter
```

scalar-index search 是 `LanceGetVByIdExec` 内部调用，不会作为独立 DataFusion child node
出现在 explain 中。验收通过 lookup discovery、算子 metadata、direct-path 测试与 metrics
共同证明使用 scalar index，并断言不执行 target 全表 Join。

## 10. 为什么不能只返回 Lance 过滤结果

假设 Expand 输出：

```text
source_id | dst_id
----------+-------
42        | 43
42        | 44
42        | 43
```

去重后的 lookup key 是：

```text
[43, 44]
```

Lance 返回：

```text
person_id | name
----------+------
43        | Bob
44        | Carol
```

最终图查询必须恢复：

```text
source_id | dst_id | b__name
----------+--------+--------
42        | 43     | Bob
42        | 44     | Carol
42        | 43     | Bob
```

因此去重只用于降低 scalar index lookup key 数量。GetV 必须保留原始 Expand batch，并在
target rows 返回后按 semantic ID 重新附着属性。

该过程是 batch-local `O(K + U)` attach：

```text
K = Expand output rows
U = unique target IDs, U <= K
```

它不再执行：

```text
K-row input JOIN N-row target scan
```

## 11. `GetVNode` 逻辑类型

建议类型：

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GetVNode {
    input: LogicalPlan,
    target_variable: String,
    input_id_column: String,
    target_id_field: String,
    lookup_ref: NodeLookupReference,
    target_predicates: Vec<Expr>,
    max_lookup_keys: usize,
    schema: DFSchemaRef,
}
```

`NodeLookupReference`：

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeLookupReference {
    pub target_label: String,
    pub target_id_field: String,
    pub scalar_index_name: String,
    pub dataset_version: u64,
}
```

`GetVNode` 输出 schema 第一版保持与当前 target Join 相同：

```text
input schema
+
target node schema with target variable aliases
```

例如：

```text
a__person_id
a__name
friend_of_0__dst_id
b__person_id
b__name
b__age
```

第一版为了缩小范围，可以读取 target node schema 的全部普通列，先确保与当前 target Join
具有相同输出 schema。Target projection pruning 和属性 late materialization 留到后续阶段。

内部必须始终读取原始 target ID field，即使最终 projection 不返回它，因为 attach 需要按
semantic ID 建立映射。

`fmt_for_explain()` 至少输出：

```text
GetV:
  target=person AS b
  input_id=friend_of_0__dst_id
  target_id=person_id
  scalar_index=person_id_idx
  dataset_version=7
  max_lookup_keys=8192
```

不在逻辑 plan 中打印动态 ID 内容。

## 12. Node lookup registry

### 12.1 Key 和 metadata

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeLookupKey {
    pub target_label: String,
    pub target_id_field: String,
}
```

```rust
pub struct NodeLookupMetadata {
    pub key: NodeLookupKey,
    pub id_data_type: DataType,
    pub scalar_index_name: String,
    pub dataset_uri: String,
    pub dataset_version: u64,
}
```

```rust
pub struct LanceNodeLookupHandle {
    pub dataset: Arc<lance::Dataset>,
    pub scalar_index: Arc<dyn lance_index::scalar::ScalarIndex>,
    pub metadata: NodeLookupMetadata,
}
```

### 12.2 Registry

```rust
pub trait NodeLookupRegistry: Send + Sync {
    fn get(
        &self,
        key: &NodeLookupKey,
    ) -> Result<Option<Arc<LanceNodeLookupHandle>>>;
}
```

第一版提供：

```rust
pub struct InMemoryNodeLookupRegistry {
    lookups: RwLock<HashMap<NodeLookupKey, Arc<LanceNodeLookupHandle>>>,
}
```

逻辑节点只保存 `NodeLookupReference`，物理 planner 验证 registry 当前 handle 的：

```text
label
ID field
ID type
scalar index name
dataset version
```

与逻辑引用一致，防止逻辑规划和物理规划之间资源被替换。

## 13. Lookup capability discovery

当前 `GraphSourceCatalog` 只返回 `TableSource`，第一版通过以下只读 downcast 发现 Lance
lookup capability：

```text
GraphSourceCatalog::node_source(label)
  ↓
DefaultTableSource
  ↓ source_as_provider()
LanceTableProvider
  ↓ dataset()
Arc<Dataset>
```

随后异步读取 dataset index metadata，确认 target ID field 上存在可用且覆盖当前所有 target
fragment 的 scalar index，并在 discovery 阶段打开 index handle。若 dataset append 产生了索引
未覆盖的新 fragment，则不启用 direct GetV，回退 target Join，避免静默漏行。

发现流程：

```text
for each node label referenced by GraphConfig/query:
  resolve node source
  if source is not DefaultTableSource:
      no GetV capability
  resolve TableProvider
  if provider is not LanceTableProvider:
      no GetV capability
  obtain Dataset
  load index metadata
  find scalar index whose indexed field == configured node ID field
  validate ID data type
  verify index fragment coverage
  open ScalarIndex handle
  register LanceNodeLookupHandle
```

发现失败的行为：

| 情况 | 第一版行为 |
|---|---|
| target provider 不是 Lance |不注册，使用 target Join |
| target ID 没有 scalar index |不注册，使用 target Join |
| scalar index metadata 无法读取 |返回明确错误，不静默忽略损坏 |
|存在多个同字段可用 scalar index |选择 Lance 标记为当前/可用的索引；无法唯一选择则报错 |
| ID type 不匹配 |不注册并记录 fallback reason |
| dataset version 在规划间变化 |物理规划报 stale lookup reference |

该 discovery 在逻辑规划之前完成，使 `DataFusionPlanner` 可以同步选择 GetV 或 target
Join，避免在物理规划阶段才发现无索引而无法恢复原逻辑计划。

## 14. `LanceGetVByIdExec`

建议结构：

```rust
pub struct LanceGetVByIdExec {
    input: Arc<dyn ExecutionPlan>,
    dataset: Arc<lance::Dataset>,
    scalar_index: Arc<dyn ScalarIndex>,
    input_id_column_index: usize,
    target_id_field: String,
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
```

第一版约束：

- 一个 input partition 对应一个 output partition；
-每个 input RecordBatch 独立 lookup；
- lookup batch 串行执行，保持输入 batch 顺序；
- unique ID 数量超过 `max_lookup_keys` 时分块查询；
-分块结果合并为一个 ID → target row map；
- target 表返回顺序不作为语义；
- output 按原 input 行顺序恢复；
-未命中的 target ID 丢弃；
-重复 input target ID 重复输出；
- null input ID 不产生 target 行；
-负数和溢出 ID 按现有 IndexedExpand ID 错误规则处理；
-不在 lookup 中自动 fallback 到 full scan。

### 14.1 Stream 算法

伪代码：

```rust
let input_stream = input.execute(partition, context)?;

input_stream
    .then(move |batch| async move {
        let batch = batch?;
        lookup_and_attach_batch(
            batch,
            dataset,
            scalar_index,
            target_id_field,
            target_filter,
            max_lookup_keys,
        )
        .await
    })
    .try_flatten()
```

`lookup_and_attach_batch()`：

```rust
async fn lookup_and_attach_batch(...) -> Result<Vec<RecordBatch>> {
    if input.num_rows() == 0 {
        return Ok(vec![]);
    }

    let original_ids = read_checked_ids(&input)?;
    let unique_ids = stable_deduplicate(&original_ids);

    let mut fetched = Vec<RecordBatch>::new();
    for id_chunk in unique_ids.chunks(max_lookup_keys) {
        let result = scalar_index.search(
            &SargableQuery::IsIn(id_chunk.to_vec()),
            &NoOpMetricsCollector,
        ).await?;
        let row_addresses = require_enumerable_exact_or_at_most(result)?;
        let rows = dataset.take_rows(
            &row_addresses,
            dataset.schema().clone(),
        ).await?;
        fetched.push(apply_target_filter(rows, target_filter)?);
    }

    let target_map = build_unique_target_map(&fetched)?;
    attach_target_rows(&input, &original_ids, &target_map)
}
```

### 14.2 Direct scalar-index lookup

lookup key 直接构造成 Lance sargable query，不拼 SQL 字符串，也不为每个 input batch 创建
新的 scanner physical plan：

```rust
let search_result = scalar_index
    .search(
        &SargableQuery::IsIn(ids.to_vec()),
        &NoOpMetricsCollector,
    )
    .await?;
```

结果处理：

```rust
let row_addresses = match search_result {
    SearchResult::Exact(ids) | SearchResult::AtMost(ids) => {
        ids.row_ids().ok_or(non_enumerable_error)?
            .map(u64::from)
            .collect::<Vec<_>>()
    }
    SearchResult::AtLeast(_) => return Err(incomplete_result_error),
};

let rows = dataset
    .take_rows(&row_addresses, dataset.schema().clone())
    .await?;
```

`AtMost` 可能包含不满足 lookup 条件的候选行，因此 attach map 只接纳本批 requested semantic
ID；`AtLeast` 不能保证结果完整，必须报错。`take_rows` 会按当前 Dataset 快照处理删除行。

P0 仍通过 scanner `InList` 的 physical plan 证明 Lance BTree 能处理
`SargableQuery::IsIn`。正式 benchmark 发现 per-batch scanner plan 存在约 23–30 ms 固定开销，
因此第一版生产执行改为复用 discovery 已打开的 scalar-index handle。

### 14.3 Target predicate

查询：

```cypher
MATCH (a)-[:FRIEND_OF]->(b:Person {age: 30})
RETURN b.name
```

内部执行为：

```text
person_id scalar-index IsIn(expanded IDs)
  → take candidate target rows
  → age = 30 residual filter
```

至少利用 `person_id` scalar index 将候选集缩小，再对候选 target rows 执行 `age = 30`
residual filter。第一版不要求 `age` 自身存在 scalar index。

### 14.4 Batch-local attach

构建：

```text
target semantic ID → fetched batch index + row index
```

然后为原 input 中每个命中 ID 生成：

```text
input_row_indices
target_row_indices grouped by fetched batch
```

最终使用 Arrow `take()`：

```text
take(input columns, input_row_indices)
+
take(target columns, target_row_indices)
```

target 字段按现有命名约定别名为：

```text
{target_variable}__{field_name}
```

第一版可以先把 fetched target batches concat 成一个 batch，再完成 attach，减少跨 batch
index 处理代码。`max_lookup_keys = 8192` 限制单次 target 集合规模；如果后续发现 concat
内存成本明显，再实现分批输出。

## 15. Metrics

`LanceGetVByIdExec` 至少暴露：

```text
input_batches
input_rows
lookup_batches
lookup_keys
unique_lookup_keys
duplicate_lookup_keys
target_rows_fetched
target_ids_not_found
output_batches
output_rows
scalar_lookup_time
target_fetch_time
attach_time
```

Explain/metrics 验收需要回答：

```text
一次查询产生多少 Expand rows？
实际查了多少 unique target IDs？
是否使用 scalar index？
Lance 返回多少 target rows？
有多少 dangling IDs？
lookup、fetch 和 attach 分别耗时多少？
```

第一版不把 Lance scalar index 和 `take_rows` 内部所有 I/O metrics 重新包装一遍；算子先
分别记录 `scalar_lookup_time`、`target_fetch_time` 和 `attach_time`，更细的 cache/I/O metrics
留到后续增强。

## 16. Planner 改造

### 16.1 `DataFusionPlanner` 资源

增加可选 registry：

```rust
pub struct DataFusionPlanner {
    config: GraphConfig,
    catalog: Option<Arc<dyn GraphSourceCatalog>>,
    indexes: Option<Arc<dyn GraphIndexRegistry>>,
    node_lookups: Option<Arc<dyn NodeLookupRegistry>>,
    index_policy: IndexUsagePolicy,
}
```

增加 builder：

```rust
pub fn with_node_lookups(
    mut self,
    node_lookups: Arc<dyn NodeLookupRegistry>,
) -> Self;
```

### 16.2 Target access decision

新增结构化决策：

```rust
pub enum TargetAccessDecision {
    GetV(NodeLookupReference),
    Join(TargetAccessFallbackReason),
}
```

fallback reason 至少包括：

```text
NodeLookupRegistryMissing
TargetProviderNotLance
TargetScalarIndexNotFound
TargetIdTypeMismatch
TargetVariableReused
UnsupportedTargetPredicate
StaleTargetDataset
```

`IndexUsagePolicy::Require` 不改变 target fallback 行为：只要 CSR 可用，缺少 target scalar
index 仍可选择当前 target Join。后续如需强制完整 native path，再单独增加 target access
policy，不复用现有语义。

### 16.3 `build_expand()` 新结构

当前索引分支：

```text
IndexedExpand
  → join_relationship_to_target()
```

目标结构：

```rust
match self.select_expand_index(...) {
    IndexDecision::Use(index_ref) => {
        let indexed_plan = self.build_indexed_expand(...)?;

        match self.select_target_access(
            target_label,
            &target_node_map.id_field,
            &target_id_type,
            target_reused,
            target_properties,
        )? {
            TargetAccessDecision::GetV(lookup_ref) => {
                self.build_get_v(
                    indexed_plan,
                    target_variable,
                    output_target_column,
                    target_node_map,
                    target_properties,
                    lookup_ref,
                )
            }
            TargetAccessDecision::Join(reason) => {
                self.record_target_fallback(reason);
                self.join_relationship_to_target(...)
            }
        }
    }
    IndexDecision::Fallback(reason) => {
        self.build_relationship_join_path(...)
    }
}
```

选择顺序必须是：

```text
先选择 relationship access：CSR 或 relationship Join
再选择 target access：GetV 或 target Join
```

不要把两种决策合并成一个巨大枚举。

## 17. Extension physical planner

当前 `GraphQueryPlanner` 只携带 CSR registry。目标结构：

```rust
pub struct GraphQueryPlanner {
    pub indexes: Arc<dyn GraphIndexRegistry>,
    pub node_lookups: Arc<dyn NodeLookupRegistry>,
}
```

使用统一 extension planner 或两个 planner：

```text
IndexedExpandNode
  → IndexedExpandExec

GetVNode
  → LanceGetVByIdExec
```

第一版建议保留两个职责单一的 planner：

```rust
DefaultPhysicalPlanner::with_extension_planners(vec![
    Arc::new(IndexedExpandExtensionPlanner { indexes }),
    Arc::new(GetVExtensionPlanner { node_lookups }),
])
```

`GetVExtensionPlanner`：

1. downcast `GetVNode`；
2. 从逻辑节点读取 `NodeLookupReference`；
3. 从 registry 获取相同 label、ID field、index name 和 dataset version 的 handle；
4. 验证 target schema 和 ID type；
5. 获取唯一 child physical plan；
6. 创建 `LanceGetVByIdExec`；
7. handle missing/stale 时返回明确 physical planning error，不在此阶段 fallback。

fallback 必须在 DataFusion 逻辑规划阶段完成。

## 18. Query 入口接入

现有：

```rust
execute_with_catalog_context_and_indexes(
    catalog,
    ctx,
    indexes,
    index_policy,
)
```

第一版不增加新的公开参数。该异步方法内部增加：

```text
discover Lance node lookup capabilities
  ↓
build InMemoryNodeLookupRegistry
  ↓
create DataFusion logical plan with indexes + node lookups
  ↓
install GraphQueryPlanner { indexes, node_lookups }
  ↓
execute
```

建议增加私有异步 helper：

```rust
async fn discover_node_lookups(
    &self,
    catalog: Arc<dyn GraphSourceCatalog>,
) -> Result<Arc<dyn NodeLookupRegistry>>;
```

并将同步 helper 扩展为：

```rust
fn create_logical_plans_with_resources(
    &self,
    catalog: Arc<dyn GraphSourceCatalog>,
    indexes: Option<Arc<dyn GraphIndexRegistry>>,
    node_lookups: Option<Arc<dyn NodeLookupRegistry>>,
    index_policy: IndexUsagePolicy,
) -> Result<(LogicalOperator, LogicalPlan)>;
```

现有非索引 API 继续传：

```text
indexes=None
node_lookups=None
index_policy=Disabled
```

因此行为不变。

## 19. Fallback 和错误边界

### 19.1 允许 fallback 的规划期情况

```text
CSR 可用 + target Lance scalar index 可用
  → IndexedExpand + GetV

CSR 可用 + target lookup 不可用
  → IndexedExpand + existing target Join

CSR 不可用且 policy=Prefer
  → existing relationship Join + target Join

CSR 不可用且 policy=Require
  → planning error
```

### 19.2 不允许运行时 fallback

一旦 `LanceGetVByIdExec` 已经开始输出 batch，以下错误不能切换到 target Join：

- scalar index 读取失败；
- target dataset version 改变；
- target ID 类型错误；
- lookup 返回重复节点 ID；
- Lance I/O 错误；
- Arrow attach 错误。

必须终止查询并返回明确错误，避免产生部分 GetV、部分 Join 的混合结果。

### 19.3 错误类型

第一版可以先映射到现有 `GraphError::PlanError` / `ExecutionError`，错误文本必须包含：

```text
target label
target ID field
scalar index name
dataset version
lookup batch key count
```

不在第一版扩展大量新错误枚举。

## 20. 正确性测试

### 20.1 `GetVNode` 单元测试

必须覆盖：

1. input ID column 不存在；
2. target ID field 不存在；
3. ID 类型不匹配；
4. output target alias 与 input 列冲突；
5. `max_lookup_keys == 0`；
6. `with_exprs_and_inputs()` 输入数量错误；
7. explain 文本包含 lookup reference；
8. schema 为 input + target aliases。

### 20.2 `LanceGetVByIdExec` 单元测试

使用临时 Lance dataset，并在 ID 列创建 scalar index。覆盖：

| 输入 |预期 |
|---|---|
|一个 source，多个 target |返回所有 target 属性 |
|重复 target ID |保持重复输出 |
|两个 source 指向同一 target |保留两个 source 上下文 |
|parallel edges |保留 multiplicity |
|self-loop |正常返回 |
|target ID 不存在 |该行被丢弃 |
|所有 target 都不存在 |返回空 stream/schema 正确 |
|空 input batch |不执行 lookup |
|lookup keys 超过 chunk size |多次 lookup、结果正确 |
|target inline predicate |只返回满足条件 target |
|target ID 重复节点行 |明确执行错误 |
|Int32/Int64/UInt32/UInt64 |类型正确 |
|负 signed ID |明确错误或按上游契约不可达 |
|多个 input partitions |每个 partition 独立正确 |

### 20.3 Planner 测试

必须断言：

```text
Lance target + scalar index:
  logical plan contains GetV
  physical plan contains LanceGetVByIdExec
  physical plan does not contain target HashJoinExec

Lance target without scalar index:
  logical plan contains existing target Join

MemTable target:
  logical plan contains existing target Join

CSR missing + Prefer:
  existing two-Join path

CSR missing + Require:
  error

CSR present + target scalar index missing + Require:
  IndexedExpand + target Join
```

### 20.4 端到端结果对比

同一数据集执行：

```text
join baseline
indexed + target join
indexed + GetV
```

对结果做无序多重集比较，不能只比较 row count。至少覆盖：

- source property 返回；
- target property 返回；
- target inline filter；
-多个 source；
-多个 source 指向同一 target；
-parallel edges；
-dangling target；
-0 行结果。

## 21. P0 Lance API spike

在实现完整算子前，先写一个最小、可删除或转成测试的 spike，验证 Lance 1.0.4：

1. 创建临时 `Person.lance`；
2. `person_id` 为 0..N；
3. 创建 BTree scalar index；
4. 构造 DataFusion `Expr::InList`，例如 `[43, 44, 45]`；
5. 通过 `Dataset::scan().filter_expr()` 创建 physical plan；
6. explain/metrics 证明使用 scalar index；
7. projection 只读取 `person_id` 和 `name`；
8. 返回结果允许与 IN list 顺序不同；
9. IN list 中重复值不会自动恢复重复语义；
10. target predicate 与 ID filter 组合正确；
11. 无 scalar index 时验证计划退化行为，确认 planner 必须提前 fallback；
12. 记录 10、100、1,000 个 lookup key 的 setup 和执行耗时。

P0 通过标准：

```text
target rows = 1,000,000
lookup keys = 10
physical plan uses scalar index
result rows correct
执行时间不随 target rows 近似线性增长
```

P0 实际结论分两层：

```text
正确性：scanner InList physical plan 使用 ScalarIndexQuery，结果正确
性能：per-batch scanner planning/execute 存在约 23–30 ms 固定开销
```

因此保留 P0 作为 scalar `IsIn` capability 证明，但正式 GetV 执行采用直接
`ScalarIndex::search + Dataset::take_rows`。如果 direct search 无法返回可枚举的完整候选集，
不得退回 full scan，必须报错或在规划期回退 target Join。

## 22. Benchmark 计划

扩展当前：

```text
crates/lance-graph-benches/benches/indexed_expand/target_join_cost.rs
```

增加：

```text
get_v_by_id_only
full_indexed_get_v
```

保留：

```text
csr_neighbors_only
indexed_expand_only
mem_target_join_lower_bound
target_join_only（每次 fresh Lance scan + fresh HashJoinExec）
full_mem_indexed_query_lower_bound
full_indexed_query（IndexedExpand + fresh Lance target Join）
```

### 22.1 固定命中数，增长 target 表

```text
target rows:
1k / 10k / 100k / 1m

degree:
10

query source:
42

result rows:
10
```

成功标准：

```text
target_join_only:
继续随 target rows 增长

get_v_by_id_only:
1k → 1m 不出现数量级线性增长

full_indexed_get_v:
在 100k 和 1m 明显快于 full indexed target-join path
```

### 22.2 固定 target 表，增长 degree

```text
target rows:
1m

degree:
1 / 10 / 100 / 1k / 10k
```

用于观察：

```text
IsIn query 构造
scalar index search
target fetch
batch-local attach
Arrow take
```

随 frontier 大小的增长。第一版不要求自动切换 mask scan，但必须找到 point lookup 明显
退化的规模，为下一阶段成本模型提供数据。

### 22.3 Benchmark 公平性

- target scalar index 构建在 timing 外；
- CSR 构建/加载在 timing 外；
- catalog、Dataset open 和 SessionContext setup 在 timing 外；
- Lance target Join 每次 iteration 重新创建 scan physical plan 和 HashJoinExec，避免复用
  HashJoin build-side `OnceAsync` 或一次性 scan 状态；
- MemTable Join 只作为纯内存最佳情况下界，不替代真实 Lance 旧路径；
-每个 case 先 warm 并断言结果；
-所有路径返回相同多重集；
-正式测试至少 1 秒 warm-up、3 秒 measurement、30 samples；
-单独报告 GetV lookup、full query，不用 `full - expand` 猜测算子时间；
-记录 target dataset fragment 数和 Lance file version；
- warm 和 cold disk 结果分开，第一版验收以 warm steady-state 为主。

## 23. 实施阶段

### P0：验证 Lance `IN` scalar index 路径

工作：

-创建临时 Lance node dataset 和 ID scalar index；
-用 DataFusion `InList` 证明 Lance scalar index capability；
-确认 physical plan 和 metrics；
-短 benchmark 目标规模曲线。

验证：

```bash
cargo test -p lance-graph <lance_get_v_spike_test>
```

完成条件：满足第 21 节 P0 标准。

### P1：Node lookup metadata、registry 和 discovery

工作：

-增加 `NodeLookupKey`、metadata、reference、handle；
-增加 in-memory registry；
-实现 catalog/provider/dataset/scalar-index discovery；
-验证 dataset version、ID field 和 ID type。

测试：

- Lance provider + index 能发现；
- Lance provider 无 index 不注册；
- MemTable 不注册；
- stale/mismatched metadata 报错。

### P2：`GetVNode`

工作：

-增加 logical extension node；
-实现 schema、Eq/Hash、expressions、with inputs 和 explain；
-输出 schema 与现有 target Join 对齐。

测试：第 20.1 节全部通过。

### P3：`LanceGetVByIdExec`

工作：

-实现 ExecutionPlan 合约；
-实现 ID 提取、稳定去重和分块；
-实现 `ScalarIndex::search(SargableQuery::IsIn)`；
-实现 SearchResult 完整性校验和 row-address 枚举；
-实现 `Dataset::take_rows`；
-实现 target predicate；
-实现 unique target map；
-实现 Arrow attach；
-实现 metrics。

测试：第 20.2 节全部通过。

### P4：Extension planner 和 `build_expand()` 接入

工作：

-增加 GetV extension planner；
-让 `GraphQueryPlanner` 同时持有两个 registry；
-增加 `TargetAccessDecision`；
- CSR 分支在 GetV/target Join 中选择；
-保持关系 Join fallback 不变。

测试：第 20.3 节全部通过。

### P5：查询入口和 explain

工作：

- indexed execute API 内异步发现 node lookup；
- logical planner 和 physical planner 使用同一个 lookup registry；
- explain 输出 logical GetV、physical GetV 和关键 metadata；
-现有普通 execute API 不变。

测试：端到端 explain 和结果对比。

### P6：Benchmark 与性能验收

工作：

-增加 ID scalar index setup；
-增加 `get_v_by_id_only`；
-增加 `full_indexed_get_v`；
-运行 target-size 和 degree 两组正式 benchmark；
-记录结果和下一阶段阈值。

完成条件：满足第 22 节成功标准。

## 24. Definition of Done

第一版只有在以下全部满足时才完成：

### 功能

- `IndexedExpand → GetV` 单跳 outgoing 查询结果正确；
- source 和 target 属性可同时返回；
- target inline predicate 正确；
-parallel edges 和重复 target 保留；
-dangling target 按 Inner Join 语义丢弃；
-空结果 schema 正确。

### 计划

- logical explain 包含 `GetV`；
- physical explain 包含 `LanceGetVByIdExec`；
- GetV 路径不存在 endpoint-target `HashJoinExec`；
- target ID scalar index 不存在时仍显示当前 target Join；
- relationship table 不被 CSR 路径扫描。

### 性能

- target 1m、degree 10 时，GetV 明显快于 isolated target Join；
- target 100k → 1m 时，GetV 不出现与 target 总行数一致的约 10 倍增长；
- full indexed GetV 在大规模 case 快于 full indexed target Join；
- CSR 构建、节点 scalar index 构建和 Dataset open 不计入 steady-state query timing。

### 兼容性

-现有普通 Join 查询测试通过；
-现有 IndexedExpand + target Join fallback 测试通过；
-现有 CSR persistence 测试通过；
- MemTable benchmark/test 不被强制切换到 Lance GetV；
-现有持久化 CSR format version 不变；
-无运行时部分 fallback。

### 验证命令

```bash
cargo fmt --all -- --check
cargo check -p lance-graph-benches --benches
cargo test -p lance-graph
cargo bench -p lance-graph-benches --bench indexed_expand_target_join -- --test
cargo bench -p lance-graph-benches --bench indexed_expand_target_join -- --noplot
```

## 25. 后续阶段

第一版完成后，再根据 degree benchmark 选择后续方向：

### 25.1 Direct lookup 的进一步流水化

第一版已经消除 per-batch scanner plan，使用：

```text
ScalarIndex::search
  → Dataset::take_rows
```

后续可评估把 search、take、residual filter 和 attach 进一步做成可分段输出的流水线，避免大
frontier 下先收集并 concat 全部 fetched batches，同时暴露更细的 index/cache/I/O metrics。

### 25.2 Target projection pruning 和 late materialization

```text
先 Take ID + filter columns
  → Filter
  →再 Take output columns
```

### 25.3 大 frontier 的动态 mask scan

```text
small K:
scalar index + Take

large K:
row-id mask + target column scan
```

### 25.4 多跳延迟 GetV

```text
IndexedExpand R
  → semantic b IDs
  → IndexedExpand S
  → semantic c IDs
  → GetV(c)
```

中间节点不读取属性时不执行 GetV。

### 25.5 CSR 保存内部 NodeRef

长期再评估：

```text
semantic ID
  → internal vertex ordinal / stable Lance row ID
```

届时 GetV 可以从 semantic-ID scalar lookup 演进为直接 row-ID Take，但这不属于第一版。

## 26. 最终第一版执行链路

```text
Cypher MATCH
  ↓
Graph Logical Expand
  ↓
DataFusion IndexedExpandNode
  ↓
IndexedExpandExec
  ↓ target semantic IDs
GetVNode
  ↓
LanceGetVByIdExec
  ├─ stable deduplicate lookup keys
  ├─ ScalarIndex::search(IsIn(...))
  ├─ SearchResult → row addresses
  ├─ Dataset::take_rows
  ├─ target predicate
  └─ batch-local semantic-ID attach
  ↓
source columns + target node columns
  ↓
Projection / Aggregate / next operator
```

这条路径保留当前 CSR 和持久化格式，把原先与 target 节点表的全表 Scan + Hash Join
替换为按一批 target 语义 ID 的索引过滤和节点属性物化，是当前架构下消除第二次 Join 的
最小闭环。

## 27. 第一版实施与 benchmark 结果

P0 首先证明 Lance scanner 的 `InList` 确实生成 `ScalarIndexQuery`。第一次完整实现沿用
scanner 动态计划，但正式 benchmark 发现每个 lookup batch 有约 23–30 ms 固定开销。第一版
最终实现因此复用 discovery 阶段打开的 `ScalarIndex` handle，改为：

```text
ScalarIndex::search(IsIn)
  → SearchResult row addresses
  → Dataset::take_rows
  → residual target predicate
  → semantic-ID attach/replay
```

2026-08-04 的 warm steady-state benchmark，target 命中固定为 10 行：

| target rows | GetV only | fresh Lance target Join | full Indexed GetV | full Indexed + Lance Join | MemTable Join 下界 |
|---:|---:|---:|---:|---:|---:|
| 1k | 2.28–2.37 ms | 10.95–11.59 ms | 2.37–2.51 ms | 11.47–12.10 ms | 16.5–17.2 µs |
| 10k | 2.73–2.85 ms | 10.34–10.90 ms | 2.54–2.67 ms | 12.16–12.93 ms | 62.6–64.8 µs |
| 100k | 2.33–2.51 ms | 15.19–15.85 ms | 2.54–2.68 ms | 15.29–16.00 ms | 0.66–0.69 ms |
| 1m | 2.29–2.47 ms | 76.28–79.97 ms | 2.40–2.51 ms | 77.23–81.70 ms | 7.02–7.27 ms |

结论：

- direct GetV 从 1k 到 1m 没有随 target 总行数近似线性增长；
- 100k 时 full GetV 比真实 Lance target Join 约快 6 倍；
- 1m 时 full GetV 比真实 Lance target Join 约快 32 倍；
- 1m 时 direct GetV 也快于纯内存 MemTable Join 下界约 3 倍；
- 小 target 表上 MemTable Join 仍明显更快，说明后续成本模型不应无条件把 GetV 推广到所有
  provider/规模；第一版只对 Lance + scalar index 启用 GetV。

固定 target 1m，增长 degree 的 GetV-only 结果：

| degree | GetV only | fresh Lance target Join |
|---:|---:|---:|
| 1 | 2.38–2.53 ms | 77.47–84.48 ms |
| 10 | 2.45–2.65 ms | 75.98–78.79 ms |
| 100 | 2.47–2.55 ms | 81.86–87.29 ms |
| 1k | 3.32–3.45 ms | 76.65–80.53 ms |
| 10k | 28.54–29.93 ms | 77.85–83.67 ms |

degree 1–100 基本由约 2.5 ms 的固定 lookup/take 成本主导；1k 开始增长，10k 出现明显退化，
但仍快于当前 Lance 全表 target Join。后续 point lookup / mask scan 成本切换应重点考察
1k–10k frontier 区间。

最终验证：

```text
cargo test -p lance-graph -j 1
  → 343 unit tests passed
  → all integration tests passed
  → 6 doc tests passed, 9 ignored

cargo bench -j 1 -p lance-graph-benches \
  --bench indexed_expand_target_join -- --test
  → all target-size and degree cases passed

cargo bench -j 1 -p lance-graph-benches \
  --bench indexed_expand_target_join -- --noplot
  → formal target-size and degree measurements completed

cargo fmt --all -- --check
cargo check -p lance-graph
cargo check -p lance-graph-benches --benches
git diff --check
  → passed
```
