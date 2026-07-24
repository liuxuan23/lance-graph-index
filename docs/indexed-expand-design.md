# CSR-backed IndexedExpand 设计与执行计划

> 状态：Draft
>
> 目标分支：`research/graph-index`
>
> 基线提交：`adfb9a1 bench(graph): add warm and cold disk execution benchmarks`
>
> 适用版本：DataFusion 50.3、Arrow 56.2
>
> 设计目标：将现有 `CsrIndex` 接入 Cypher 查询执行，以索引邻接查找替换关系表扫描及第一次 Join。

## 1. 摘要

本阶段在现有 DataFusion 查询路径中加入一个基于 `CsrIndex` 的
`IndexedExpand` 逻辑扩展节点和物理执行算子。

用户层的 Cypher、AST 和 lance-graph 逻辑计划仍然使用普通 `Expand`。
`DataFusionPlanner` 在降低 `LogicalOperator::Expand` 时，根据索引策略、查询语义和
索引元数据选择以下两条路径之一：

```text
Join 路径：
Source Scan
  → Relationship Scan
  → Source-Relationship Join
  → Target Scan
  → Relationship-Target Join

CSR 索引路径：
Source Scan
  → IndexedExpand(CSR)
  → Target Scan
  → Endpoint-Target Join
```

`IndexedExpand` 消除：

- 查询时的关系表扫描；
- 源节点与关系表之间的第一次 Join。

第一阶段仍然保留：

- 源节点表的 Lance 扫描；
- 目标节点表的 Lance 扫描；
- CSR 输出目标 ID 后，与目标节点表进行的第二次 Join。

因此，本阶段的准确定位是：

> DataFusion 查询计划中的 CSR-backed physical expansion operator，而不是完整的图原生执行器。

## 2. 当前基线

### 2.1 当前查询路径

```text
Cypher
  ↓
Parser / AST
  ↓
Semantic Analysis
  ↓
lance-graph LogicalOperator::Expand
  ↓
DataFusion LogicalPlan: Scan + Join
  ↓
DataFusion PhysicalPlan
  ↓
RecordBatch
```

当前 `GraphPhysicalPlanner` 虽然名称中包含 `Physical`，实际返回的是 DataFusion
`LogicalPlan`：

```rust
pub trait GraphPhysicalPlanner {
    fn plan(&self, logical_plan: &LogicalOperator) -> Result<LogicalPlan>;
}
```

因此真实分层是：

```text
lance-graph LogicalPlan
  ↓ GraphPhysicalPlanner
DataFusion LogicalPlan
  ↓ DataFusion QueryPlanner
DataFusion PhysicalPlan
```

本阶段不顺带重命名该 trait，避免扩大改动范围。

### 2.2 相关源码

- [`csr_index.rs`](../crates/lance-graph/src/csr_index.rs)：当前 CSR、Builder、BFS 和最短路径。
- [`logical_plan.rs`](../crates/lance-graph/src/logical_plan.rs)：`LogicalOperator::Expand`。
- [`datafusion_planner/mod.rs`](../crates/lance-graph/src/datafusion_planner/mod.rs)：`GraphPhysicalPlanner` 和 `DataFusionPlanner`。
- [`builder/expand_ops.rs`](../crates/lance-graph/src/datafusion_planner/builder/expand_ops.rs)：当前 `Expand` 到 Scan + Join 的转换。
- [`join_ops.rs`](../crates/lance-graph/src/datafusion_planner/join_ops.rs)：源端 Join 和目标端 Join。
- [`query.rs`](../crates/lance-graph/src/query.rs)：`SessionContext` 创建和最终执行入口。
- [`source_catalog.rs`](../crates/lance-graph-catalog/src/source_catalog.rs)：节点与关系 `TableSource` Catalog。
- [`graph_execution_disk.rs`](../crates/lance-graph-benches/benches/graph_execution_disk.rs)：当前 Lance 磁盘 warm/cold benchmark。

### 2.3 当前单跳执行

查询：

```cypher
MATCH (a:Person {name: "Alice"})-[:FRIEND_OF]->(b:Person)
RETURN a.name, b.name
```

当前执行结构：

```text
Scan Person AS a
  ↓ Filter a.name = "Alice"
Scan FRIEND_OF
  ↓ Join a.person_id = rel.person1_id
Scan Person AS b
  ↓ Join rel.person2_id = b.person_id
Project a.name, b.name
```

即使 source filter 最终只得到一个节点，查询仍要处理关系表扫描及 Join。现有
`CsrIndex` 已经可以通过 source ID 查到邻居，但当前没有任何 planner 或 executor
调用它。

## 3. 阶段目标与非目标

### 3.1 必须完成

1. 加固现有 `CsrIndex` 的构造和 ID 校验。
2. 增加图索引元数据、注册表和索引使用策略。
3. 增加 DataFusion `IndexedExpandNode`。
4. 增加 DataFusion `IndexedExpandExec`。
5. 在 `DataFusionPlanner::build_expand()` 中选择索引或 Join 路径。
6. 通过 DataFusion `ExtensionPlanner` 将逻辑扩展节点转换为物理算子。
7. 提供显式传入索引的查询执行 API。
8. 在计划和 metrics 中证明查询确实使用了索引。
9. 增加单元测试、端到端测试和 Lance 磁盘测试。
10. 增加 Join 与 CSR 路径的对比 benchmark。
11. 保证没有索引时现有行为不变。

### 3.2 本阶段不做

- 不重写 `LanceNativePlanner`；
- 不新增 Cypher 语法；
- 不修改 AST；
- 不在 lance-graph 语义逻辑计划中增加 `IndexedExpand`；
- 不实现 variable-length path 的 CSR 执行；
- 不支持关系属性返回或关系属性过滤；
- 不支持 String/UUID 节点 ID；
- 不实现稀疏外部 ID 到稠密 ordinal 的映射；
- 不消除目标节点 Join；
- 不实现索引增量维护；
- 不在本阶段接入向量索引。

## 4. 设计原则

### 4.1 `Expand` 是语义，`IndexedExpand` 是实现

现有 `LogicalOperator::Expand` 表达的是“从 source 沿 relationship 扩展到
target”。使用关系表 Join、CSR 或其他邻接索引属于物理实现选择。因此索引选择发生在
lance-graph 逻辑计划转换为 DataFusion 计划时，不向 AST 和语义层泄漏索引细节。

### 4.2 第一阶段只替换第一次 Join

当前路径：

```text
source scan
  → relationship scan
  → source/relationship Join
  → target scan
  → relationship/target Join
```

索引路径：

```text
source scan
  → IndexedExpand(CSR)
  → target scan
  → endpoint/target Join
```

CSR 只提供 target ID。`b.name`、`b.age` 等属性仍需从目标节点表获得，因此第二个
Join 必须保留。后续可增加 `IndexedNodeLookup`，再研究消除目标节点全表 Scan + Join。

### 4.3 索引注册与 Source Catalog 分离

`GraphSourceCatalog` 位于独立的 `lance-graph-catalog` crate，只负责解析
`TableSource`。`CsrIndex` 定义在主 `lance-graph` crate。让 catalog crate 反向依赖
主 crate 会形成不合理依赖，因此索引注册表应放在主 crate 中。

## 5. MVP 支持矩阵

| 查询能力 | 第一阶段 | 说明 |
|---|---:|---|
| 单个固定一跳 `Expand` | 支持 | 核心目标 |
| 单一关系类型 | 支持 | 如 `[:FRIEND_OF]` |
| `Outgoing` | 支持 | CSR 保存 source → target |
| source inline property filter | 支持 | 在扩展前执行 |
| target inline property filter | 支持 | 在目标节点 scan 中执行 |
| 普通 `WHERE` | 语义支持 | 下推效果需通过计划验证 |
| 无出边节点 | 支持 | 产生 0 行 |
| self-loop | 支持 | 保留一行匹配 |
| parallel edges | 支持 | 保留重复结果 |
| 多 input partition | 支持 | 每个 child partition 独立展开 |
| null source ID | 支持 | 该输入行产生 0 行 |
| `Incoming` | 暂不支持 | 后续注册反向 CSR |
| `Undirected` | 暂不支持 | 后续组合正向和反向 CSR |
| 多关系类型 | 暂不支持 | 如 `[:A\|B]` |
| 关系变量 | 暂不支持 | 如 `-[r:KNOWS]->` |
| 关系属性过滤/返回 | 暂不支持 | CSR 不保存 edge payload |
| variable-length path | 暂不支持 | 如 `*1..3` |
| 目标变量复用 | 暂不支持 | 第一版避免复杂约束语义 |
| String/UUID ID | 暂不支持 | 当前 CSR 使用 `u64` |
| 负数 ID | 不支持 | 明确报错 |
| 稀疏大整数 ID | 受限 | 会造成巨大 offsets |

显式链式查询 `(a)-[:R]->(b)-[:R]->(c)` 在逻辑计划中是两个嵌套的固定
`Expand`，理论上可以逐个选择索引。实施时先验收单个 `Expand`，再验收显式两跳；
`VariableLengthExpand` 不进入本阶段。

## 6. 目标架构

```text
                         ┌──────────────────────────┐
                         │ GraphIndexRegistry       │
                         │ FRIEND_OF / Outgoing     │
                         │   → Arc<CsrIndexHandle>  │
                         └────────────┬─────────────┘
                                      │ resolve
Cypher                                │
  ↓                                   │
LogicalOperator::Expand               │
  ↓                                   │
DataFusionPlanner                     │
  ├── Disabled → Scan + Join          │
  ├── Prefer                          │
  │     ├── eligible ─────────────────┤
  │     └── ineligible → Scan + Join  │
  └── Require                         │
        ├── eligible ─────────────────┤
        └── ineligible → Error        │
                                      ↓
                           IndexedExpandNode
                                      ↓
                           GraphQueryPlanner
                                      ↓
                    IndexedExpandExtensionPlanner
                                      ↓
                           IndexedExpandExec
                                      ↓
                           RecordBatch Stream
```

建议新增结构：

```text
crates/lance-graph/src/
├── index/
│   ├── mod.rs
│   ├── metadata.rs
│   ├── registry.rs
│   └── policy.rs
│
└── datafusion_planner/
    └── indexed_expand/
        ├── mod.rs
        ├── logical.rs
        ├── physical.rs
        ├── planner.rs
        └── stream.rs
```

## 7. CSR 正确性和 ID 契约

### 7.1 第一阶段 ID 契约

当前 `CsrIndex::neighbors(vertex_id)` 直接以 `vertex_id` 访问 offsets。因此第一阶段
必须明确：

```text
节点外部 ID == CSR vertex ordinal
0 <= vertex_id < num_vertices
```

ID 可以有少量空洞，但 offsets 的大小由最大 ordinal 决定。类似
`1_000_000_000` 的稀疏 ID 会产生巨大 offsets；String、UUID 和负数 ID 不可用。

后续正确方向是：

```text
External Node ID
  ↕ VertexIdMap
Dense Vertex Ordinal
  ↓
CSR offsets / neighbors
```

### 7.2 Builder 加固

当前 `CsrIndexBuilder::build()` 不返回错误。建议增加：

```rust
pub fn try_build(self) -> Result<CsrIndex>;
```

必须验证：

- `num_vertices` 和 `num_vertices + 1` 可安全转换到当前平台的 `usize`；
- 每个 source/target ID 小于 `num_vertices`；
- 输入 ID 列无 null；
- signed integer 无负数；
- 整数转换到 `u64` 不溢出；
- offsets 单调不减；
- 最后一个 offset 等于 `neighbors.len()`；
- 每个 offset 不超过 `neighbors.len()`。

执行引擎只能注册经过验证的索引。旧 `build()` 可在迁移期保留，但不应进入查询执行
API。

### 7.3 Arrow ID 类型

当前 `add_edges_from_batch()` 只接受 `UInt64`，而现有 Lance 数据和测试常使用
`Int64`。第一阶段至少支持：

```text
UInt32 / UInt64 / Int32 / Int64
```

所有转换必须经过范围检查，不能静默使用 `as u64` 或 `as i64`。

## 8. 索引元数据与注册表

### 8.1 索引 Key

```rust
pub struct GraphIndexKey {
    pub relationship_type: String,
    pub source_label: String,
    pub target_label: String,
    pub direction: IndexDirection,
}

pub enum IndexDirection {
    Outgoing,
    Incoming,
}
```

所有字符串在构造时转成 lowercase。Key 包含两端 label，避免同名关系在不同节点
类型之间使用时发生错误匹配。

### 8.2 元数据

```rust
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
```

`source_version` 对 Lance 数据集记录建索引时的版本；`generation` 防止逻辑规划和
物理规划之间索引被替换。

### 8.3 Handle 和 Registry

```rust
pub struct CsrIndexHandle {
    pub index: Arc<CsrIndex>,
    pub metadata: GraphIndexMetadata,
}

pub trait GraphIndexRegistry: Send + Sync {
    fn get_csr(
        &self,
        key: &GraphIndexKey,
    ) -> Result<Option<Arc<CsrIndexHandle>>>;
}
```

提供线程安全的：

```rust
pub struct InMemoryGraphIndexRegistry {
    indexes: RwLock<HashMap<GraphIndexKey, Arc<CsrIndexHandle>>>,
}
```

逻辑节点不直接持有 `Arc<CsrIndex>`。它只保存可比较、可 Hash 的
`IndexReference { key, generation }`，物理 extension planner 再从 registry 解析
真实索引。

## 9. 索引使用策略与 eligibility

不要复用 `ExecutionStrategy::LanceNative`，因为本方案仍由 DataFusion 执行。

```rust
pub enum IndexUsagePolicy {
    Disabled,
    Prefer,
    Require,
}
```

- `Disabled`：始终使用现有 Scan + Join，作为回归和 benchmark baseline。
- `Prefer`：索引存在且语义符合时使用 CSR，否则回退 Join。
- `Require`：不能使用索引时明确报错，用于测试和 benchmark。

建议使用结构化判断结果：

```rust
pub enum IndexDecision {
    Use(IndexReference),
    Fallback(IndexFallbackReason),
}
```

第一阶段 eligibility：

```text
relationship_types.len() == 1
AND direction == Outgoing
AND relationship_variable == None
AND relationship properties empty
AND target variable not reused
AND source/target IDs are supported integers
AND index metadata matches GraphConfig
AND index exists and is current
```

fallback 原因至少包括：

```text
PolicyDisabled
IndexNotFound
UnsupportedDirection
MultipleRelationshipTypes
RelationshipVariableUsed
RelationshipPropertyRequired
TargetVariableReused
UnsupportedIdType
SchemaMismatch
StaleIndex
```

策略行为：

| 情况 | Disabled | Prefer | Require |
|---|---|---|---|
| 索引不存在 | Join | Join | Error |
| 查询方向不支持 | Join | Join | Error |
| 关系属性被使用 | Join | Join | Error |
| 索引版本过期 | Join | Join，并记录原因 | Error |
| schema 不匹配 | Join | Join，并记录原因 | Error |
| 索引内部损坏 | Error | Error | Error |
| 运行时负 ID | Error | Error | Error |

运行时已经输出部分 batch 后不能动态 fallback。

当前 `LogicalOperator::Expand` 包含 `relationship_variable`，但
`builder/mod.rs` 调用 `build_expand()` 时忽略了它。本阶段必须将该字段显式传给
`build_expand()`，否则 planner 无法判断查询是否需要关系变量数据。

## 10. `IndexedExpandNode`

建议字段：

```rust
pub struct IndexedExpandNode {
    pub input: LogicalPlan,
    pub source_column: String,
    pub output_target_column: String,
    pub index_ref: IndexReference,
    pub output_id_type: DataType,
    pub schema: DFSchemaRef,
    pub max_output_batch_rows: usize,
}
```

输出 schema 是：

```text
input schema + one endpoint ID field
```

例如输入：

```text
a__person_id | a__name
```

输出：

```text
a__person_id | a__name | friend_of__0__person2_id
```

临时目标 ID 必须使用现有关系实例 alias 和 `qualify_column()` 命名约定：

```text
{relationship_instance_alias}__{target_id_field}
```

不要直接输出 `b__person_id`，否则会与目标节点 scan 生成的列冲突。保持与当前关系
scan 的目标端点列同名、同类型后，现有 `join_relationship_to_target()` 可以直接复用。

逻辑计划显示至少包含：

```text
IndexedExpand:
  relationship_type=friend_of
  direction=Outgoing
  source=a__person_id
  target=friend_of__0__person2_id
  index_generation=3
```

不要将 offsets/neighbors 内容写入计划文本。

## 11. `IndexedExpandExec`

### 11.1 数据契约

输入必须包含 source ID 列：

```text
a__person_id | a__name
1            | Alice
2            | Bob
```

CSR：

```text
1 → [2, 3]
2 → [4]
```

输出：

```text
a__person_id | a__name | friend_of__0__person2_id
1            | Alice   | 2
1            | Alice   | 3
2            | Bob     | 4
```

算子对每个邻居复制一次完整输入行，再追加邻居 ID。degree 为 0 的 source 产生 0 行，
对应 INNER JOIN 语义。

### 11.2 物理结构

```rust
pub struct IndexedExpandExec {
    input: Arc<dyn ExecutionPlan>,
    index: Arc<CsrIndex>,
    source_column_index: usize,
    output_target_field: FieldRef,
    schema: SchemaRef,
    properties: PlanProperties,
    max_output_batch_rows: usize,
    metrics: ExecutionPlanMetricsSet,
}
```

实现 DataFusion 50.3 的 `ExecutionPlan` 合约，包括 `properties`、`children`、
`with_new_children`、`execute`、`metrics` 和 `statistics`。第一阶段 statistics 返回
Unknown，不伪造基数。

### 11.3 流式算法

```text
execute child partition
  ↓
读取 input RecordBatch
  ↓
逐行进行 csr.neighbors(source_id)
  ↓
累积 source_row_indices 和 neighbor_ids
  ↓ 达到 max_output_batch_rows
Arrow take(input columns, source_row_indices)
  ↓
追加 target ID array
  ↓
yield RecordBatch
```

伪代码：

```rust
for batch in child_stream {
    for source_row in 0..batch.num_rows() {
        let Some(source_id) = checked_vertex_id(&batch, source_row)? else {
            continue;
        };

        for target_id in csr.neighbors(source_id) {
            row_indices.push(source_row);
            target_ids.push(*target_id);

            if row_indices.len() == max_output_batch_rows {
                yield build_output_batch(&batch, &row_indices, &target_ids)?;
                row_indices.clear();
                target_ids.clear();
            }
        }
    }

    if !row_indices.is_empty() {
        yield build_output_batch(&batch, &row_indices, &target_ids)?;
    }
}
```

复制原始输入列时使用 Arrow `take`，不逐值复制。默认
`max_output_batch_rows = 8192`，并允许配置。一个 hub 节点的邻居必须被拆成多个
output batch，不能一次 materialize 全部结果。

### 11.4 边界行为

- null source ID：产生 0 行；
- 负数或无法转换到 `u64`：`InvalidVertexId`；
- source ID 超出 `num_vertices`：视为过期或不完整索引并明确报错；
- self-loop：正常输出；
- parallel edges：保留重复 target ID，不做去重；
- 一个 input partition 对应一个 output partition；
- 不承诺全局输出顺序。

## 12. DataFusion 物理规划接入

默认 DataFusion physical planner 不认识用户扩展节点，需要：

```text
IndexedExpandNode
  ↓
IndexedExpandExtensionPlanner
  ↓
IndexedExpandExec
```

```rust
pub struct IndexedExpandExtensionPlanner {
    indexes: Arc<dyn GraphIndexRegistry>,
}
```

Extension planner：

1. 从逻辑节点获取 `IndexReference`；
2. 从 registry 获取相同 key 和 generation 的 handle；
3. 验证 metadata；
4. 接收 child physical plan；
5. 创建 `IndexedExpandExec`。

增加：

```rust
pub struct GraphQueryPlanner {
    indexes: Arc<dyn GraphIndexRegistry>,
}
```

其内部使用：

```text
DefaultPhysicalPlanner
  + IndexedExpandExtensionPlanner
```

具体 trait 签名以 DataFusion 50.3 为准。

## 13. `build_expand()` 改造

目标结构：

```rust
let left_plan = self.build_operator(ctx, input)?;
let rel_instance = ctx.next_relationship_instance(rel_type)?;
let decision = self.select_expand_implementation(...)?;

match decision {
    IndexDecision::Use(index_ref) => {
        let indexed_plan = self.build_indexed_expand(
            left_plan,
            index_ref,
            ...,
        )?;

        self.join_relationship_to_target(
            LogicalPlanBuilder::from(indexed_plan),
            ...,
        )
    }
    IndexDecision::Fallback(reason) => {
        let rel_scan = self.build_relationship_scan(...)?;
        let builder = self.join_source_to_relationship(
            left_plan,
            rel_scan,
            ...,
        )?;
        self.join_relationship_to_target(builder, ...)
    }
}
```

eligibility 必须在 `catalog.relationship_source()` 之前完成。只有 fallback 分支才构造
relationship scan，保证索引路径不会无意义地解析关系表 provider。

当前代码中部分缺失配置会 `return Ok(left_plan)`，可能让 Expand 静默消失。索引实现
不得复制这一行为；尤其 `Require` 下，缺失映射、catalog 或索引必须明确报错。

## 14. Query 和 SessionContext 接入

当前 `execute_with_catalog_and_context()` 直接调用：

```rust
ctx.execute_logical_plan(df_logical_plan)
```

包含 `IndexedExpandNode` 时，`SessionContext` 必须安装 `GraphQueryPlanner`。第一版推荐
提供显式 API：

```rust
pub async fn execute_with_catalog_context_and_indexes(
    &self,
    catalog: Arc<dyn GraphSourceCatalog>,
    ctx: SessionContext,
    indexes: Arc<dyn GraphIndexRegistry>,
    index_policy: IndexUsagePolicy,
) -> Result<RecordBatch>;
```

不应悄悄覆盖用户传入 context 上已有的自定义 QueryPlanner。现有执行 API 第一阶段保持
原 Join 行为；等功能稳定后，再讨论是否将 `Prefer` 设为默认。

自动构建 Catalog 时：

- `Prefer` 仍需关系 provider，以便 fallback；
- `Require` 在索引元数据完整时，可以允许不注册关系 provider；
- source 和 target 节点 provider 始终需要存在。

第一版也可以先保留关系 provider 注册，只要求计划不扫描关系表；随后再放宽 provider
存在性要求。

## 15. 完整执行示例

节点表 `Person.lance`：

```text
person_id | name  | age
0         | Alice | 34
1         | Bob   | 40
2         | Carol | 28
3         | David | 50
```

关系表 `FRIEND_OF.lance`：

```text
person1_id | person2_id
0          | 1
0          | 2
1          | 3
```

CSR：

```text
offsets   = [0, 2, 3, 3, 3]
neighbors = [1, 2, 3]

0 → [1, 2]
1 → [3]
2 → []
3 → []
```

查询：

```cypher
MATCH (a:Person {name: "Alice"})-[:FRIEND_OF]->(b:Person)
WHERE b.age > 30
RETURN a.name, b.name
```

lance-graph 逻辑计划仍然是：

```text
Project
└── Filter b.age > 30
    └── Expand
        ├── source_variable: a
        ├── target_variable: b
        ├── relationship_types: ["FRIEND_OF"]
        ├── direction: Outgoing
        └── ScanByLabel a:Person {name: "Alice"}
```

DataFusion 索引逻辑计划：

```text
Projection: a__name, b__name
  Filter: b__age > 30
    Inner Join:
      friend_of__0__person2_id = b__person_id
      Left:
        IndexedExpand:
          source=a__person_id
          output=friend_of__0__person2_id
          Input:
            Filter name = "Alice"
              TableScan person
      Right:
        TableScan person AS b
```

物理计划：

```text
ProjectionExec
└── FilterExec
    └── HashJoinExec
        ├── IndexedExpandExec
        │   └── LanceScanExec(Person AS a)
        └── LanceScanExec(Person AS b)
```

计划中必须看不到：

```text
LanceScanExec(FRIEND_OF)
HashJoinExec(a.person_id = relationship.person1_id)
```

运行时：

```text
source scan:
0 | Alice

neighbors(0):
[1, 2]

IndexedExpand output:
Alice | 1
Alice | 2

target Join:
Alice | Bob   | 40
Alice | Carol | 28

Filter b.age > 30:
Alice | Bob
```

## 16. Filter 下推注意事项

CSR 对“source 过滤后只剩小 frontier”的查询最有价值。inline property：

```cypher
MATCH (a:Person {person_id: 42})-[:R]->(b)
```

天然位于 source scan 内，适合第一阶段 benchmark。

普通 `WHERE a.age > 30` 在 lance-graph 逻辑计划中可能位于 `Expand` 之上。自定义
Extension 可能形成优化边界，不能预设 DataFusion 会自动将 filter 下推到
`IndexedExpand` child。

第一阶段要求：

1. 保证查询语义正确；
2. benchmark 优先使用 inline source property；
3. 用计划测试确认 source filter 的实际位置；
4. 未下推时，后续增加 source-only predicate placement 或 extension 优化规则。

## 17. 索引生命周期与一致性

正确的查询路径：

```text
应用启动或显式建索引
  ↓ 扫描 relationship table 一次
构建并验证 CSR
  ↓
注册 Arc<CsrIndexHandle>
  ↓
多次查询复用
```

不能在每次 timed query 内重建 CSR。benchmark 将 index build 与 query execution
分别计时。

Lance 关系表更新后，旧 CSR 可能过期。metadata 至少记录 source URI 和 dataset
version：

```text
Require + stale → Error
Prefer + stale  → fallback Join，并记录原因
```

逻辑节点保存 key + generation，物理 planner 只接受同一 generation，避免查询规划期间
索引被替换。

## 18. Metrics、EXPLAIN 和错误处理

`IndexedExpandExec` 至少记录：

```text
input_batches
input_rows
index_lookups
null_source_ids
out_of_range_source_ids
neighbors_emitted
output_batches
output_rows
elapsed_compute
```

可选记录：

```text
max_degree_seen
zero_degree_rows
id_conversion_time
arrow_take_time
```

逻辑计划必须显示 `IndexedExpand`，物理计划必须显示 `IndexedExpandExec`。索引路径中
不能出现关系表 scan，但目标节点 scan 和 target Join 应继续存在。

建议错误类型或明确错误分类：

```text
IndexNotFound
IndexNotEligible
IndexSchemaMismatch
StaleIndex
InvalidVertexId
CorruptIndex
UnsupportedIndexDirection
```

错误消息至少包含 relationship type、direction、ID fields、index key、generation、
policy 和失败原因。`Prefer` 的 planner fallback 应通过 tracing 记录结构化原因。

## 19. 实施里程碑

### M0：冻结 baseline

- 保存当前单跳 Join 的 logical/physical plan；
- 保存结果集和现有 benchmark；
- 运行现有测试。

完成条件：现有测试通过，disk benchmark 可运行，并保存一个单跳 baseline plan。

### M1：CSR hardening

- 增加 `try_build()` 和 validated constructor；
- 支持受控整数类型；
- 验证 null、负数、越界和溢出；
- 保留 parallel edge 和 self-loop。

完成条件：无效 CSR 无法注册，Int64 ID 可安全转换，现有 CSR 测试继续通过。

### M2：Metadata、Registry 和 Policy

- `GraphIndexKey`；
- `GraphIndexMetadata`；
- `CsrIndexHandle`；
- `GraphIndexRegistry`；
- `InMemoryGraphIndexRegistry`；
- `IndexUsagePolicy`；
- generation/version 检查。

完成条件：能显式注册并解析 Outgoing CSR，schema 或版本不匹配可明确失败。

### M3：`IndexedExpandNode`

- 实现 DataFusion logical extension；
- 确定 schema 和临时列命名；
- 实现 display、相等性和 input 替换。

完成条件：logical plan 可构造 `IndexedExpand`，EXPLAIN 可见，schema 与目标 Join 兼容。

### M4：`IndexedExpandExec`

- 实现物理算子和 Arrow `take`；
- 实现 checked ID codec；
- 实现 output batch chunking；
- 实现 metrics 和多 partition 支持。

完成条件：算子单元测试通过，高 degree 不生成无限大 batch。

### M5：Planner integration

- 将 `relationship_variable` 传入 `build_expand()`；
- 增加 eligibility 和三种 policy；
- indexed 分支生成 extension node；
- fallback 保留当前逻辑；
- 复用 target Join。

完成条件：`Require + index` 使用 `IndexedExpand`，`Require + no index` 报错，
`Prefer + no index` 使用原 Join。

### M6：QueryPlanner / SessionContext integration

- `IndexedExpandExtensionPlanner`；
- `GraphQueryPlanner`；
- index-aware execute API；
- 端到端执行。

完成条件：Cypher 能真正执行 `IndexedExpandExec`，而不只是构造 logical plan。

### M7：计划、metrics 和 diagnostics

- logical/physical display；
- fallback reason；
- execution metrics。

完成条件：可从计划和 metrics 判断是否使用索引及实际扩展规模。

### M8：端到端测试

- Join 与 indexed 结果一致；
- MemTable 和 Lance 磁盘；
- policy 和边界数据测试。

### M9：Benchmark

- index build；
- isolated operator；
- Lance disk end-to-end；
- source selectivity、degree 和 topology；
- warm/cold。

## 20. 测试计划

### 20.1 CSR

```text
try_build_empty_graph
try_build_rejects_source_out_of_range
try_build_rejects_target_out_of_range
try_build_rejects_negative_int64
try_build_rejects_null_id
try_build_accepts_int64
try_build_accepts_uint64
parallel_edges_are_preserved
self_loop_is_preserved
```

### 20.2 Registry

```text
register_and_get_csr
lookup_is_case_insensitive
same_relationship_different_direction
generation_mismatch
reject_metadata_edge_count_mismatch
reject_metadata_vertex_count_mismatch
replace_index_increments_generation
```

### 20.3 Logical node

```text
output schema = input schema + target ID
source column must exist
output column cannot collide
display contains key information
with_new_inputs preserves configuration
equality includes index generation
```

### 20.4 Physical exec

```text
one_to_many_expansion
zero_degree_produces_no_rows
null_source_produces_no_rows
self_loop
parallel_edges
empty_input
multiple_input_batches
multiple_partitions
hub_node_is_split_into_batches
input_columns_are_duplicated_correctly
target_id_output_type_matches_schema
negative_id_returns_error
out_of_range_id_returns_stale_index_error
metrics_match_output
```

### 20.5 Planner policy

- `Disabled`：包含 relationship scan 和两个 Join，不包含 `IndexedExpand`；
- `Prefer + index`：包含 `IndexedExpand`，无 relationship scan，保留 target Join；
- `Prefer + no index`：回退现有 Join；
- `Require + no index`：明确 `IndexNotFound`；
- `Require + relationship property`：明确 `RelationshipPropertyRequired`。

### 20.6 端到端一致性

同一数据和查询分别使用 `Disabled` 与 `Require`，按 multiset 比较结果，覆盖：

```text
single/multiple source
isolated node
self-loop
parallel edge
source/target filter
empty result
LIMIT / ORDER BY
COUNT
```

parallel edge 的 `COUNT(*)` 用于防止 CSR 路径意外去重。结果比较不依赖默认行顺序。

Lance 磁盘测试必须断言 physical plan 有 `IndexedExpandExec`、没有关系表 scan，并保留
目标节点 scan 和 target Join。

## 21. Benchmark 计划

建议新增：

```text
crates/lance-graph-benches/benches/
├── graph_index_build.rs
├── indexed_expand.rs
└── graph_execution_indexed.rs
```

### 21.1 Index build

测量 build time、edges/s、offset memory、neighbor memory 和临时构建内存。维度：

```text
规模：1K / 10K / 100K / 1M
平均度：1 / 4 / 16 / 64
拓扑：ring / uniform random / power-law hub
```

CSR 基础内存约为：

```text
8 × (num_vertices + 1) + 8 × num_edges
```

还需报告 `Vec<(u64, u64)>` 等构建期临时内存。

### 21.2 Isolated `IndexedExpandExec`

只测：

```text
MemTable source → IndexedExpandExec → consume output
```

不包含 parser、Lance scan、target Join 和 index build。维度包括 source 行数、平均度、
孤立节点比例和 hub topology。

### 21.3 End-to-end

比较：

```text
A. MemTable + Scan/Join
B. LanceTableProvider + Scan/Join
C. LanceTableProvider + CSR IndexedExpand + target Join
```

同时测试高选择性 source 和全图 source。不能预设全图扫描时 CSR 一定快于 DataFusion
向量化 Hash Join；CSR 的主要优势通常来自小 frontier 查询。

索引 build 必须放在 timed query 外，并单独报告。所有 indexed benchmark 使用
`IndexUsagePolicy::Require`，开始计时前检查计划包含 `IndexedExpandExec`。

磁盘 cold benchmark 中 CSR 仍驻留内存，cold 只影响节点 Lance 页面，应准确描述为：

```text
cold node-table pages + warm in-memory CSR
```

推荐指标：query latency、rows/s、index lookups、neighbors emitted、relationship/node
bytes read、peak memory、index memory 和 output rows。核心 I/O 验证是 indexed path 的
relationship bytes read 为 0。

## 22. 推荐 PR 拆分

1. `feat(graph-index): validate CSR construction and integer vertex IDs`
2. `feat(graph-index): add CSR index registry and usage policy`
3. `feat(datafusion): add IndexedExpand logical and physical operators`
4. `feat(planner): select CSR-backed IndexedExpand for eligible traversals`
5. `feat(query): execute IndexedExpand through index-aware SessionContext`
6. `bench(graph): compare join and CSR-backed expansion`

每个 PR 分别隔离 CSR、registry、算子、planner、SessionContext 和 benchmark 风险。

## 23. 风险与后续路线

主要风险：

1. 节点 ID 不是稠密整数：第一阶段限制 ID contract，后续实现 `VertexIdMap`。
2. CSR 不保存 edge payload：关系属性查询 fallback，后续增加 `edge_ids` 或 payload
   index。
3. 索引过期：记录 Lance version，`Prefer` fallback，`Require` error。
4. 目标节点 Join 仍可能扫描整张节点表：后续实现 `IndexedNodeLookup`。
5. Extension 阻断 filter/projection 下推：通过计划测试确认，再增加优化规则。
6. hub 输出爆炸：流式输出、限制 output batch、记录 max degree。

后续推荐顺序：

```text
Incoming CSR
  → Undirected Expand
  → 显式多跳 IndexedExpand
  → VariableLengthExpandExec
  → VertexIdMap
  → Edge payload index
  → IndexedNodeLookup
  → Persisted CSR
  → Incremental maintenance
  → 图索引与向量索引联合查询
```

## 24. 验收标准

### 24.1 功能

- Join 与 indexed 路径对同一查询的结果 multiset 完全一致；
- isolated node、self-loop、parallel edge 正确；
- source/target filter 正确；
- null、负数和越界 ID 行为明确；
- `Require` 模式确实使用索引；
- logical plan 出现 `IndexedExpand`；
- physical plan 出现 `IndexedExpandExec`；
- indexed plan 不出现 relationship scan；
- indexed plan 不出现 source-to-relationship Join；
- target node scan 和 target Join 仍然存在；
- unsupported query 明确 fallback 或报错；
- 无索引时现有行为不变。

### 24.2 回归

```bash
cargo fmt --all --check
cargo test --all
cargo clippy --all-targets --all-features
```

### 24.3 性能

本阶段不预设未经验证的固定加速比例。必须证明：

1. indexed plan 不扫描关系表；
2. selective source 查询减少关系数据 I/O；
3. index build 时间和内存单独报告；
4. selective 与 full-scan 分别报告；
5. benchmark 使用 `Require`，不存在隐式 fallback；
6. warm/cold 条件定义清楚；
7. 两条路径结果行数一致。

## 25. Definition of Done

只有下面的端到端链路真正跑通，本阶段才算完成：

```text
Person.lance + FRIEND_OF.lance
  ↓ build once
CsrIndex registered in GraphIndexRegistry
  ↓
CypherQuery
  ↓
LogicalOperator::Expand
  ↓ policy=Require
IndexedExpandNode
  ↓ custom DataFusion QueryPlanner
IndexedExpandExec
  ↓
target Person Lance scan
  ↓
target Join
  ↓
RecordBatch
```

同时必须满足：结果与原 Join 路径一致，EXPLAIN 明确出现 `IndexedExpandExec`，关系表
没有出现在物理扫描计划中，benchmark 将索引构建与查询执行分开，且不支持的语义不会
被静默执行或静默 fallback。
