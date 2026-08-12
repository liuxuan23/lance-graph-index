# Lance 原生直接邻接索引第一版实施计划

## 1. 文档状态

- 状态：Implementation in progress
- 创建日期：2026-08-06
- 目标分支：`research/graph-index`
- 基线提交：`7ad67f0 bench(graph): add end-to-end indexed GetV star cases`
- 前置能力：持久化 CSR、`IndexedExpandExec`、`LanceGetVByIdExec`、节点语义 ID scalar lookup
- 目标阶段：验证以 Lance Dataset 直接保存嵌套邻接列表的图访问路径
- 建议名称：Direct Adjacency Index；中文统一称“直接邻接索引”

本文档规划一种与当前 CSR 互补的图索引：不再把邻接关系编码为需要整体加载到内存的
`offsets + neighbors` 数组，而是在一个独立的 Lance Dataset 中按 source 节点保存嵌套的
neighbor 列表，并在 `src_id` 上建立 Lance scalar index。查询时只读取当前 source 所对应的
邻接行，再将 `List<dst_id>` 展开为现有 GetV 路径可以消费的 endpoint ID。

第一版的核心问题不是证明嵌套列表一定比 CSR 更快，而是验证以下假设：

1. 邻接信息可以作为普通 Lance 数据集完成持久化、版本管理和按需访问；
2. 小 frontier 查询不需要将整个图索引加载到内存；
3. 邻接查询可以复用 Lance 的 scalar index、fragment、对象存储和 `take_rows` 能力；
4. 新路径能够无缝接入已经实现的 `LanceGetVByIdExec`；
5. CSR 和直接邻接索引可以共享逻辑语义，但保留各自最合适的物理执行方式。

## 2. 背景

当前完整的 CSR 查询路径为：

```text
LanceScanExec(Person AS a)
  → IndexedExpandExec(CSR)
  → LanceGetVByIdExec(Person AS b)
  → FilterExec(b.age > 30)
  → ProjectionExec
```

CSR 已经解决了 relationship table scan 以及 source 与 relationship table 的 Hash Join，
`LanceGetVByIdExec` 又解决了 endpoint 与 target 节点表的全表 Hash Join。因此，对一跳查询
而言，当前链路已经完整。

但 CSR 的物理形态仍有以下特征：

- 索引加载后常驻内存；
- 查询前需要读取完整 `offsets` 和 `neighbors`；
- 索引格式、generation、加载和缓存需要由 lance-graph 自己维护；
- 全量重建简单，但增量更新和关系属性扩展不自然；
- 超大图即使单次查询只访问极少 source，也需要承担完整索引的内存成本。

直接邻接索引采用另一种布局：

```text
src_id | dst_ids
-------+---------------------
42     | [7, 19, 31, 88]
43     | [3, 9]
44     | []
```

其中每个 `src_id` 最多对应一行，`dst_ids` 是 Arrow/Lance 的嵌套 List 列。查询 source 42
时，先通过 `src_id` scalar index 定位这一行，再通过 `Dataset::take_rows` 读取并展开列表。

这是一种“Lance 原生、按 source 读取”的邻接表，而不是把 CSR 文件换一种方式存储。

## 3. 设计结论摘要

第一版采用以下明确决策：

1. 索引数据集只包含 `src_id + List<dst_id>`；
2. 一个 `GraphIndexKey` 对应一个独立的直接邻接 Dataset；
3. `src_id` 在索引数据集内唯一，并建立 Lance BTree scalar index；
4. `dst_ids` 保留原关系表的边重复度和顺序，不做集合化；
5. 查询只批量读取当前 input batch 涉及的 source 邻接行；
6. 新增 `DirectAdjacencyExpandExec`，不把异步 Lance I/O 塞进现有内存
   `IndexedExpandExec`；
7. `DirectAdjacencyExpandExec` 输出与 `IndexedExpandExec` 相同的 endpoint ID 形态；
8. target 节点物化继续使用现有 `LanceGetVByIdExec`；
9. Expand 后端由调用方显式指定：`Join`、`Csr` 或 `DirectAdjacency`；
10. benchmark 通过只注册目标索引类型，分别强制测量 CSR 和直接邻接路径；
11. 指定的索引不存在、过期或不兼容时直接在规划阶段报错，不静默切换到另一种索引；
12. 第一版索引 generation 是不可变快照，更新通过构建并发布新 generation 完成；
13. 第一版不保存关系属性，不支持关系变量，也不引入 `List<Struct<...>>`；
14. 第一版不抽象统一的“邻接读取 trait”来包住 CSR 与 Lance I/O；
15. 第一版先证明正确性、可执行计划和成本曲线，再决定是否扩展为可更新邻接表。

## 4. 目标

以查询为例：

```cypher
MATCH (a:Person {name: "Alice"})-[:FRIEND_OF]->(b:Person)
WHERE b.age > 30
RETURN a.name, b.name
```

当存在：

```text
FRIEND_OF(Person → Person, Outgoing) direct adjacency index
Person.person_id Lance scalar index
```

且 planner 选择直接邻接路径时，期望物理计划为：

```text
ProjectionExec(a.name, b.name)
└── FilterExec(b.age > 30)
    └── LanceGetVByIdExec(Person.person_id AS b)
        └── DirectAdjacencyExpandExec(FRIEND_OF, outgoing)
            └── LanceScanExec(Person AS a, name = "Alice")
```

必须满足：

1. 物理计划包含 `DirectAdjacencyExpandExec`；
2. 不扫描原始 relationship table；
3. 不出现 source-to-relationship `HashJoinExec`；
4. target 表有可用 scalar index 时继续使用 `LanceGetVByIdExec`；
5. 输出结果与原 Join 路径、CSR 路径按 multiset 比较完全一致；
6. 查询只读取命中 source 的邻接行，而不是打开后加载完整邻接数组；
7. CSR 路径不发生行为和性能回归；
8. `Join`、`Csr`、`DirectAdjacency` 三种显式模式有明确且可测试的行为；
9. benchmark 能同时报告构建、持久化体积、打开和查询阶段的成本。

## 5. 非目标

第一版不包含：

- `Map<src_id, neighbors>` 单行全图布局；
- `List<Struct<dst_id, edge_properties...>>`；
- relationship property 过滤或返回；
- relationship variable；
- 多种 relationship type 混放在同一个 Dataset；
- `(src_id, relationship_type)` 复合索引；
- variable-length path、BFS 或多跳算子；
- 将 target Lance row address 直接保存在邻接列表中；
- 用 row ID 替换节点语义 ID；
- 对邻接 List 内部的 `dst_id` 建立倒排索引；
- 多个邻接 generation 的在线 compaction；
- 细粒度边增删的事务 API；
- 自动从 catalog 发现所有远端邻接索引；
- 根据 frontier、degree 和缓存命中率进行完整成本优化；
- 统一 CSR 和直接邻接算子的底层存储 trait；
- Python 公共 API。

这些能力可以在第一版 benchmark 证明直接邻接布局有价值后逐步加入。

## 6. 数据布局

### 6.1 第一版 Lance schema

逻辑 schema：

```text
src_id:  Int64, non-null
dst_ids: List<Int64>, non-null
```

Arrow 表达可写为：

```rust
Schema::new(vec![
    Field::new("src_id", DataType::Int64, false),
    Field::new(
        "dst_ids",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
        false,
    ),
])
```

真实实现不能写死 `Int64`，应继续支持当前图索引路径已有的整数语义 ID：

```text
UInt32
UInt64
Int32
Int64
```

同一个邻接 Dataset 中，`src_id` 与 `dst_ids.item` 第一版要求是相同类型。若未来支持
不同 source/target label 使用不同 ID 类型，再分别记录两种类型。

### 6.2 一行一个 source

索引的唯一性契约为：

```text
(GraphIndexKey, src_id) → at most one adjacency row
```

构建器默认只写入 degree 大于 0 的 source；没有邻接行和存在空 List 在 Expand 语义上都
表示零个 neighbor。如果构建时提供完整 source universe，也允许显式写入 `[]`，但不能
再写第二行相同 `src_id`。

选择稀疏布局的原因是：关系表本身通常只能提供有边的 source，强制为所有无边节点创建
空行会增加存储和构建成本，却不改变一跳 Expand 结果。

### 6.3 relationship type 与方向

第一版每个 `GraphIndexKey` 使用独立 Dataset：

```text
FRIEND_OF/person/person/outgoing/generation-1/adjacency.lance
FRIEND_OF/person/person/incoming/generation-1/adjacency.lance
WORKS_AT/person/company/outgoing/generation-1/adjacency.lance
```

这使一次邻接访问只需要对 `src_id` 做 scalar lookup，避免：

- 在一行中维护 relationship type 字典；
- 查询后再过滤其他关系类型；
- 对 `(relationship_type, src_id)` 引入复合索引依赖；
- 不同类型更新时重写共享的超大邻接行。

incoming 索引使用原 relationship 的 `dst_id` 作为邻接 Dataset 的 `src_id`，List 中保存
原 relationship 的 `src_id`。undirected 查询第一版仍按当前 planner 能力决定是否需要同时
读取 outgoing 与 incoming，不在单个 Dataset 内混合方向。

### 6.4 边重复度与顺序

直接邻接索引必须保留 parallel edges：

```text
edge rows:
42 → 7
42 → 7
42 → 9

adjacency row:
42 → [7, 7, 9]
```

Expand 后仍输出三行。不能对 `dst_ids` 去重，否则会改变 Cypher 的 bag semantics。

第一版将同一 source 的 neighbor 顺序定义为构建输入经过稳定 source 分组后的原始顺序。
查询正确性不依赖这个顺序，测试比较使用 multiset；但单次 generation 内应保持确定性，
便于复现 benchmark 和排查问题。

### 6.5 List 大小限制

第一版使用 Arrow `List`，单个 source 的 List offset 受 32 位 offset 限制。星型 benchmark
最大 degree 为 10,000，远低于该限制。构建器必须检查单个 source degree 和单 batch value
数量，不允许发生静默截断或 offset 溢出。

如果后续需要支持超高 degree hub，可评估：

- 切换为 `LargeList`；
- 将一个 source 拆成多行并增加 `chunk_id`；
- 单独为超级节点保存分块邻接 Dataset。

第一版不提前引入这些复杂度。

## 7. 元数据与索引句柄

### 7.1 元数据

建议新增独立元数据，而不是把 Lance Dataset 专属字段继续塞入当前
`GraphIndexMetadata`：

```rust
pub struct DirectAdjacencyMetadata {
    pub key: GraphIndexKey,
    pub source_id_field: String,
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
```

字段语义：

- `key`：关系类型、source label、target label 和方向；
- `source_id_field`：邻接 Dataset 的 source key，第一版固定为 `src_id` 但仍记录；
- `adjacency_field`：嵌套 neighbor 列，第一版固定为 `dst_ids`；
- `id_data_type`：source 和 neighbor 的整数类型；
- `num_sources`：实际写入的邻接行数，不等于节点表总行数；
- `num_edges`：所有 List 长度之和；
- `dataset_uri`、`dataset_version`：固定到不可变 Lance 快照；
- `scalar_index_name`：`src_id` 上的索引名称；
- `source_uri`、`source_version`：构建索引时 relationship 数据源身份；
- `generation`：图索引的发布 generation。

### 7.2 运行时句柄

建议新增：

```rust
pub struct DirectAdjacencyIndexHandle {
    pub dataset: Arc<Dataset>,
    pub scalar_index: Arc<dyn ScalarIndex>,
    pub metadata: DirectAdjacencyMetadata,
}
```

Dataset 和 scalar-index handle 在索引加载/注册阶段打开，查询执行时不重复做 capability
discovery。与 `LanceGetVByIdExec` 相同，物理算子直接持有可执行的 handle。

### 7.3 Registry 扩展

当前 registry 只暴露 CSR：

```rust
fn get_csr(&self, key: &GraphIndexKey) -> Result<Option<Arc<CsrIndexHandle>>>;
```

第一版建议并列增加：

```rust
fn get_direct_adjacency(
    &self,
    key: &GraphIndexKey,
) -> Result<Option<Arc<DirectAdjacencyIndexHandle>>>;
```

`InMemoryGraphIndexRegistry` 分别维护：

```text
csr_indexes: HashMap<GraphIndexKey, Arc<CsrIndexHandle>>
direct_adjacency_indexes:
    HashMap<GraphIndexKey, Arc<DirectAdjacencyIndexHandle>>
```

不建议第一版定义一个返回动态 trait object 的统一 `get_adjacency()`：CSR lookup 是同步的
内存 slice，直接邻接 lookup 是异步 scalar search + Lance I/O，强行统一容易把 I/O、生命周期
和错误语义隐藏到一个过宽的抽象中。

## 8. 持久化与 generation

### 8.1 目录布局

建议沿用持久化 CSR 的 immutable generation 思路：

```text
<index-root>/
└── generation-000001/
    ├── adjacency.lance/
    └── descriptor.json
```

`descriptor.json` 至少包含：

```text
format_version
index_kind = "direct_adjacency"
metadata
dataset_uri
dataset_version
scalar_index_name
created_at
```

虽然 Lance Dataset 已经保存 schema、version 和 scalar-index metadata，独立 descriptor
仍有价值：

- 不打开 Dataset 即可判断索引类型和 GraphIndexKey；
- 与现有 CSR store 保持一致的发现和发布模式；
- 记录 relationship source 快照和 generation；
- 为格式版本升级提供稳定入口。

### 8.2 发布顺序

构建和发布顺序必须是：

```text
write adjacency Dataset
→ validate schema/counts/uniqueness
→ create src_id scalar index
→ reopen pinned dataset version
→ validate scalar-index fragment coverage
→ write descriptor last
→ register/publish generation
```

只有 descriptor 完成后，该 generation 才对查询可见。失败的临时 Dataset 不得注册。

### 8.3 快照一致性

句柄必须固定 `dataset_version`，不能在每次查询时隐式打开 URI 的 latest version。否则：

- descriptor 的 `num_edges` 可能与查询数据不一致；
- scalar index 可能只覆盖旧 fragments；
- 同一 query 的多个 batch 可能观察到不同邻接快照。

第一版使用 immutable generation：关系表更新后，旧索引通过 `source_version` 判断 stale，
然后全量生成新 generation。原地 append、delete 和 merge-update 留到后续版本。

### 8.4 加载验证

`DirectAdjacencyIndexStore::load` 至少验证：

1. descriptor 格式版本受支持；
2. Dataset URI 和 pinned version 可打开；
3. schema 恰好包含兼容的 `src_id` 和 `dst_ids`；
4. `src_id` 非空且类型与 metadata 一致；
5. `dst_ids.item` 类型与 metadata 一致；
6. scalar index 存在并绑定到 `src_id`；
7. scalar index 覆盖 pinned version 的全部 fragments；
8. Dataset 不存在重复 `src_id`；
9. `num_sources`、`num_edges` 与 descriptor 一致；
10. relationship source identity 满足调用方的 validation policy。

其中全量唯一性和边数检查可以在 write 后完成并写入 descriptor；常规 load 不应每次重新
扫描整个 Dataset，但必须信任一个经过校验且带格式版本的 descriptor。

## 9. 构建路径

### 9.1 输入

构建器输入仍是关系表 endpoint 列：

```text
src_id
dst_id
```

outgoing 索引按 `src_id` 分组，incoming 索引交换 endpoint 后按原 `dst_id` 分组。

建议 API：

```rust
DirectAdjacencyIndexBuilder::new(metadata)
    .with_output_uri(uri)
    .with_batch_size(8_192)
    .build_from_stream(edge_stream)
    .await?;
```

### 9.2 可扩展构建算法

生产实现不能把 1,000 万条甚至更大的边全部放入一个 `HashMap<src_id, Vec<dst_id>>`。
建议流程为：

```text
relationship endpoint scan
→ project(src_id, dst_id)
→ stable sort by src_id（允许 DataFusion external sort/spill）
→ streaming group-by src_id
→ append dst_id to ListBuilder
→ emit bounded RecordBatch
→ write Lance Dataset
→ create BTree scalar index on src_id
```

流式分组只需要在内存中保存：

- 当前 source ID；
- 当前 source 的 neighbor List；
- 当前输出 RecordBatch 的 builders。

唯一无法严格受普通 batch size 限制的是单个超级节点的 neighbor List，这由第 6.5 节的
degree 检查处理。

### 9.3 构建期校验

构建完成后必须校验：

```text
sum(length(dst_ids)) == input relationship row count
count(distinct src_id) == adjacency row count
src_id is unique and non-null
dst_ids is non-null
dst_ids items are non-null
metadata.num_edges == actual edge multiplicity
```

不能对 edge endpoint 做去重，也不能因为 scalar-index search 返回集合语义而改变 List 中的
重复 neighbor。

## 10. 逻辑计划与索引选择

### 10.1 逻辑语义保持统一

Cypher builder 仍把单跳关系匹配识别为一个图 Expand。CSR 与直接邻接索引的逻辑语义相同：

```text
input source row
→ lookup neighbors(source semantic ID)
→ replicate input row once per relationship
→ append relationship target semantic ID
```

不应为两种索引复制完整的 Cypher 构建逻辑。建议扩展当前索引引用，使 planner 能明确记录
所选物理后端：

```rust
pub enum ExpandIndexReference {
    Csr(IndexReference),
    DirectAdjacency(DirectAdjacencyReference),
}
```

现有 `IndexedExpandNode` 直接重命名为更中性的 `AdjacencyExpandNode`，内部保存
`ExpandIndexReference`。本次开发同步修改全部调用点、测试和 EXPLAIN 断言，不保留 CSR
专用逻辑节点名称。物理算子仍按实现区分为 `IndexedExpandExec` 和
`DirectAdjacencyExpandExec`。

### 10.2 显式指定 Expand 后端

不采用“自动在 CSR 和 Direct Adjacency 之间择优”的策略。对于图查询，调用方在创建
`DataFusionPlanner` 或执行查询时直接指定 Expand 后端：

```rust
pub enum ExpandExecutionMode {
    Join,
    Csr,
    DirectAdjacency,
}
```

语义非常直接：

```text
Join
    → 禁用图邻接索引，使用原始 relationship Scan + Join

Csr
    → 必须找到与 GraphIndexKey、generation、ID 类型和 snapshot 匹配的 CSR
    → 找不到或不兼容时，规划阶段返回错误

DirectAdjacency
    → 必须找到与 GraphIndexKey、generation、schema、scalar index 和 snapshot 匹配的
      Direct Adjacency
    → 找不到或不兼容时，规划阶段返回错误
```

这样，planner 不需要回答“CSR 是否比 Direct 更快”，也不会因为某个索引意外缺失而悄悄
改变执行计划。索引选择责任在调用方、benchmark 配置或上层成本模型，而不是隐藏在
`select_expand_index()` 内部。

建议执行入口显式接受该模式：

```rust
query.execute_with_catalog_context_and_indexes(
    catalog,
    ctx,
    indexes,
    ExpandExecutionMode::DirectAdjacency,
)
```

本次开发直接删除 `IndexUsagePolicy` 以及 `Disabled / Prefer / Require` 语义，所有现有调用点
统一迁移到 `ExpandExecutionMode`。不新增兼容入口，不保留两套 planner 配置。

`Csr` 和 `DirectAdjacency` 本身就表示“必须使用该后端”，不再增加额外 `Require` 层。
如果未来确实需要自动选择或 fallback，应作为新的显式 mode 设计，而不是改变这三个 mode
已经确定的严格语义。

需要同步调整的主要 API：

```rust
DataFusionPlanner {
    expand_mode: ExpandExecutionMode,
    // 删除 index_policy
}

DataFusionPlanner::with_indexes(
    indexes: Arc<dyn GraphIndexRegistry>,
    expand_mode: ExpandExecutionMode,
)

CypherQuery::execute_with_catalog_context_and_indexes(
    catalog,
    ctx,
    indexes,
    expand_mode,
)

CypherQuery::explain_with_catalog_context_and_indexes(
    catalog,
    ctx,
    indexes,
    expand_mode,
)
```

常规的不带 indexes 执行入口明确使用 `ExpandExecutionMode::Join`。传入 `Csr` 或
`DirectAdjacency` 时 registry 缺失本身就是 planning error。

这个 mode 只选择“source 到 relationship endpoint”的 Expand 后端。后续 target GetV 是否
使用节点 scalar index 是另一层访问决策，第一版继续沿用现有规则：target scalar index 可用
则使用 `LanceGetVByIdExec`，不可用则回退 target Scan + HashJoin。若以后也要求 target 路径
完全可控，应再增加独立的 `TargetAccessMode`，不要把它混入 `ExpandExecutionMode`。

### 10.3 规划流程

显式模式下 planner 的步骤为：

```text
1. 检查查询 eligibility：关系类型、方向、关系变量、关系属性、ID 类型等。
2. 若 mode = Join：直接构造原始 relationship Scan + Join。
3. 若 mode = Csr：按 GraphIndexKey 查询 CSR，并验证 generation/metadata。
4. 若 mode = DirectAdjacency：按 GraphIndexKey 查询 Direct，并验证
   descriptor/schema/scalar-index coverage/version。
5. 指定后端验证通过：将带类型的 index reference 写入 logical Expand node。
6. physical planner 按 reference 类型创建 `IndexedExpandExec` 或
   `DirectAdjacencyExpandExec`。
7. 索引缺失、过期或不兼容：立即返回带具体原因的 planning error。
```

当前 `IndexDecision::Use/Fallback` 和 `IndexFallbackReason` 不再适合显式模式，直接替换为：

```rust
pub enum ExpandPlanDecision {
    Join,
    Indexed(ExpandIndexReference),
}
```

`Join` 只可能由 `ExpandExecutionMode::Join` 产生；`Csr` 或 `DirectAdjacency` 选择失败时返回
`Err`，不能产生 `ExpandPlanDecision::Join`。

当 CSR 和 Direct 同时注册时，结果仍然完全由 `mode` 决定：

```text
Csr             → IndexedExpandExec
DirectAdjacency → DirectAdjacencyExpandExec
Join            → relationship Scan + Join
```

这使 EXPLAIN、benchmark 和线上故障排查都能从调用参数直接推导预期物理计划。

### 10.4 eligibility 条件

直接邻接路径第一版沿用 CSR Expand 的限制：

- 单一 relationship type；
- 明确的 source/target label；
- 支持的方向；
- relationship variable 未被返回或引用；
- 不需要 relationship property；
- source ID field 和 index metadata 匹配；
- target ID field 和 neighbor ID 语义匹配；
- 整数 ID 类型兼容；
- relationship source snapshot 未过期。

## 11. 物理执行计划

### 11.1 完整计划

对本文查询，source predicate 尽可能下推到 Lance scan，target predicate 在 GetV 物化属性后
执行：

```text
ProjectionExec(a.name, b.name)
└── FilterExec(b.age > 30)
    └── LanceGetVByIdExec
        │ target = Person
        │ lookup_field = person_id
        └── DirectAdjacencyExpandExec
            │ key = FRIEND_OF(Person → Person, Outgoing)
            │ source_field = a.person_id
            │ endpoint_field = friend_of_0__dst_id
            └── LanceScanExec(Person AS a)
                filter = a.name = "Alice"
```

`src_id` scalar-index search 和 adjacency `take_rows` 是
`DirectAdjacencyExpandExec` 的内部步骤，不作为独立 DataFusion child node。算子只有一个
DataFusion input child，即 source 计划。

### 11.2 与现有路径的关系

三条路径为：

```text
Join baseline:
Source Scan → Relationship Scan → HashJoin → Target Scan → HashJoin

CSR:
Source Scan → IndexedExpandExec → LanceGetVByIdExec

Direct adjacency:
Source Scan → DirectAdjacencyExpandExec → LanceGetVByIdExec
```

直接邻接路径只替换 CSR Expand 部分，不复制或修改 GetV 的 target lookup 语义。

## 12. `DirectAdjacencyExpandExec`

### 12.1 建议结构

```rust
pub struct DirectAdjacencyExpandExec {
    input: Arc<dyn ExecutionPlan>,
    handle: Arc<DirectAdjacencyIndexHandle>,
    source_id_column: String,
    endpoint_field: Field,
    output_schema: SchemaRef,
    output_batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
}
```

它与 `IndexedExpandExec` 保持相同的外部 schema 契约：

```text
all input columns
+ relationship endpoint semantic ID column
```

因此现有 `GetVNode` 和 `LanceGetVByIdExec` 不需要知道 endpoint 来自 CSR 还是 Lance List。

### 12.2 每个 input batch 的执行算法

```text
1. 读取 input batch 的 source ID 列
2. 跳过 null source ID，但保留原 row position
3. 按首次出现顺序生成 stable unique lookup keys
4. 对 unique keys 执行 ScalarIndex::search(IsIn)
5. 将搜索结果转换为 adjacency row addresses
6. Dataset::take_rows 只读取 src_id 和 dst_ids
7. 验证返回 src_id 唯一，并建立 src_id → List slice 映射
8. 按原 input row 顺序重放：
   - 找到 neighbor List
   - 每个 neighbor 复制一次 input row
   - 追加 endpoint ID
9. 达到 output_batch_size 时输出 RecordBatch
10. 继续处理剩余 input rows 和 neighbors
```

伪代码：

```text
for input_batch in input:
    source_ids = read_source_ids(input_batch)
    unique_keys = stable_unique(non_null(source_ids))

    row_addresses = scalar_search_is_in(unique_keys)
    adjacency_batch = take_rows(row_addresses, [src_id, dst_ids])
    adjacency = validate_and_build_map(adjacency_batch)

    for (input_row, source_id) in input_batch.rows():
        if source_id is null:
            continue
        for dst_id in adjacency.get(source_id).unwrap_or(empty):
            output.append(copy(input_row), dst_id)
            if output.is_full():
                yield output.finish()
```

### 12.3 为什么必须返回中间 endpoint 表

与 CSR `IndexedExpandExec` 一样，新算子仍输出“input source 上下文 + dst_id”，而不是直接
返回纯 `dst_id`，原因是：

- 一个 input batch 可能有多个 source；
- 同一个 source 节点可能因为上游匹配出现多次；
- 后续 projection 仍可能返回 `a.name`；
- parallel edges 必须保留多重性；
- target GetV 必须把每个目标节点行重新关联到对应的 source 上下文；
- 后续多跳 Expand 需要完整的当前绑定表。

这不是 relationship Join 的残留，而是 Cypher 中间绑定表的物理表达。可以优化列物化和
row replication，但不能只返回一个失去 source row identity 的 `dst_id` 集合。

### 12.4 正确性语义

算子必须覆盖：

- 多个 input source rows；
- 重复 input source ID；
- input row 按 neighbor 数复制；
- parallel edges；
- self-loop；
- 空 List；
- source 在邻接 Dataset 中不存在；
- null source ID；
- 多个 source 指向同一 target；
- signed/unsigned 整数边界；
- 多个 Lance fragments；
- output batch 跨单个大 List 分割；
- input batch 为空；
- scalar search 返回顺序与 input key 顺序不同。

missing source、空 List 和 null source 在 Inner Expand 中都产生零行。不能输出一行 null
endpoint，除非未来实现 OPTIONAL MATCH 的外连接语义。

### 12.5 scalar-index 覆盖与精确性

直接调用 `ScalarIndex::search` 时必须处理 index coverage。现有 GetV/node lookup 已经面对
相同问题，第一版应提取或复用一个小型的 Lance scalar lookup helper，统一完成：

- `IsIn` 查询构造；
- `SearchResult` 解析；
- fragment coverage 检查；
- row-address 解码；
- 必要的 residual equality validation；
- lookup metrics。

这里建议复用窄 helper，而不是让 `DirectAdjacencyExpandExec` 依赖完整
`LanceGetVByIdExec`。

由于邻接 generation 是 immutable 且 scalar index 在数据写完后创建，正常句柄应覆盖全部
fragments。若运行时发现不完整 coverage：

- 查询尚未输出任何 batch：返回明确错误；指定 Direct Adjacency 后不切换 CSR 或 Join；
- 查询已经输出 batch：必须报错，不能中途切换路径，否则会重复或遗漏结果。

最简单且可靠的第一版做法是：在 load/register 阶段拒绝不完整 coverage，使执行阶段不再
需要处理后端切换。

### 12.6 内存控制

算子不能一次构建完整查询结果。需要限制：

- 每次 scalar lookup 的 unique source keys；
- 每次 `take_rows` 的 row-address 数量；
- adjacency fetch batch 大小；
- output batch 行数；
- 单个超大 List 展开时的游标状态。

如果 input batch 的 unique source 数超过 lookup limit，应分块查询；但重放 output 时仍按
原 input row 顺序进行。可以将 input rows 按 key chunk 建立 position list，再按原 position
合并，或在第一版将 DataFusion input batch size 限制在 lookup limit 内。

## 13. Planner 接入

### 13.1 逻辑 planner

在当前 Expand 索引决策位置增加显式 mode 分支：

```text
analyze relationship pattern
→ build GraphIndexKey
→ validate common indexed-expand eligibility
→ mode = Join: build relationship Scan + Join
→ mode = Csr: query and validate the exact CSR
→ mode = DirectAdjacency: query and validate the exact Direct Adjacency index
→ attach typed index reference to logical Expand node
→ requested index absent/incompatible: planning error
```

公共的 eligibility 检查只做图语义判断；CSR 稠密 vertex range 等特有校验留在 CSR 分支，
direct adjacency 的 Dataset/schema/scalar-index 校验留在 direct adjacency 分支。

### 13.2 physical extension planner

扩展节点物理化规则：

```text
ExpandIndexReference::Csr
    → IndexedExpandExec

ExpandIndexReference::DirectAdjacency
    → DirectAdjacencyExpandExec
```

物理 planner 必须断言：

- registry 中 generation 与 logical reference 一致；
- source column 存在且类型匹配；
- output endpoint field 与 GetV 所需 target ID 类型一致；
- Dataset handle 已固定到 metadata 的 version；
- index key 与查询 relationship pattern 一致。

### 13.3 GetV 复用

`DirectAdjacencyExpandExec` 输出 endpoint 列后，调用当前已有的 target access decision：

```text
target is Lance provider + target ID scalar index available
    → GetVNode → LanceGetVByIdExec

otherwise
    → current target Scan + HashJoin fallback
```

因此第一版可以独立验证两层优化：

```text
Direct adjacency + target Join
Direct adjacency + indexed GetV
```

正式 benchmark 重点比较完整的第二种路径，但测试必须确认 target scalar index 缺失时仍能
保持现有 fallback 语义。

## 14. 显式后端与错误处理

### 14.1 规划期行为

显式模式下的行为如下：

| `ExpandExecutionMode` | 指定索引存在且兼容 | 指定索引不存在/失效 |
|---|---|---|
| `Join` | Join | Join |
| `Csr` | CSR | Planning error |
| `DirectAdjacency` | Direct Adjacency | Planning error |

索引存在但验证失败时，必须将其视为“失效索引”，并返回具体原因：

```text
Direct descriptor 损坏
Direct source snapshot stale
Direct scalar-index coverage 不完整
CSR generation mismatch
CSR metadata/schema mismatch
```

查询需要关系属性、关系变量或不支持的方向时，不是“索引坏了”，而是查询本身不满足
Direct/CSR Expand 的 eligibility：`Join` 走 Join，`Csr` 和 `DirectAdjacency` 返回规划错误。

这里不允许自动切换后端。例如 `DirectAdjacency` 下 Direct 索引缺失，即使 CSR
存在也必须报错。否则调用方无法确定 benchmark 或生产查询实际使用了哪种索引。

### 14.2 执行期行为

以下问题必须返回带 `GraphIndexErrorKind` 的明确错误：

- Dataset/version 不存在；
- schema 与 metadata 不兼容；
- scalar index 不存在；
- scalar index coverage 不完整；
- duplicate `src_id`；
- returned `src_id` 不在 lookup keys 中；
- List item 类型错误或包含 null；
- row address 无效；
- List offset 溢出；
- generation conflict。

第一版不做执行期后端 fallback。无论 stream 是否已经产生结果，任何索引错误都必须终止
查询；区别只在于错误发生前是否已经产生了可丢弃的中间 batch，调用方不能消费部分结果。

## 15. 指标与可观测性

`DirectAdjacencyExpandExec` 至少记录：

```text
input_batches
input_rows
lookup_batches
lookup_keys
unique_lookup_keys
scalar_lookup_time
row_addresses
adjacency_rows_fetched
adjacency_bytes_fetched
fetch_time
neighbors_emitted
missing_sources
empty_adjacency_rows
flatten_time
output_batches
output_rows
```

EXPLAIN/Display 输出建议包含：

```text
DirectAdjacencyExpandExec:
  relationship_type=friend_of,
  direction=outgoing,
  source_id=a.person_id,
  endpoint=friend_of_0__dst_id,
  generation=1,
  dataset_version=3
```

不要输出完整 URI 中可能存在的凭证或 query parameters。

## 16. 测试计划

### 16.1 Lance 嵌套列能力验证

在接入 planner 前先增加最小 integration spike：

1. 写入多个 batch 的 `src_id + List<Int64>`；
2. 关闭并重新打开 Dataset；
3. 在 `src_id` 创建 BTree scalar index；
4. 使用 `ScalarIndex::search(SargableQuery::IsIn)` 查询多个 source；
5. 使用返回 row address 调用 `Dataset::take_rows`；
6. 正确 downcast 并解析 `ListArray`；
7. 覆盖多个 fragments；
8. 证明 duplicate neighbors 和 empty List round trip 不变。

该阶段的结论决定真实 Lance 1.0.4 API 用法；在 spike 通过前不开始大范围 planner 改动。

### 16.2 Builder/Store 单元测试

- unsorted edge input 能正确分组；
- parallel edges 被保留；
- empty input；
- signed/unsigned 四种 ID 类型；
- null endpoint 被拒绝；
- direction swap 正确；
- metadata counts 正确；
- write/read descriptor round trip；
- pinned dataset version；
- scalar index 不存在；
- scalar index coverage 不完整；
- stale source version；
- corrupt schema；
- duplicate source rows；
- generation conflict。

### 16.3 `DirectAdjacencyExpandExec` 单元测试

使用可控 input plan 和临时 Lance Dataset 验证：

- 单 source、多 neighbor；
- 多 source；
- duplicate input source；
- parallel edges；
- self-loop；
- missing source；
- empty List；
- null source；
- scalar result 顺序被打乱；
- 多 fragments；
- 小 output batch size 导致一个 List 跨多个 batch；
- 输入列完整复制；
- 输出 schema 与 `IndexedExpandExec` 一致；
- metrics counts 正确。

### 16.4 Planner 与端到端测试

至少增加以下端到端查询：

```cypher
MATCH (a:Person {person_id: 42})-[:FRIEND_OF]->(b:Person)
WHERE b.age > 30
RETURN a.name, b.name
```

断言：

```text
physical plan contains DirectAdjacencyExpandExec
physical plan contains LanceGetVByIdExec
physical plan does not contain relationship-table scan
physical plan does not contain source-relationship HashJoinExec
physical plan does not contain endpoint-target HashJoinExec
```

结果与以下路径按 multiset 比较：

```text
Join baseline
CSR + indexed GetV
Direct adjacency + indexed GetV
```

还要覆盖：

- `Join` 明确构造原始 Join；
- `Csr` 只选择 CSR，缺失时报错；
- `DirectAdjacency` 只选择 Direct，缺失时报错；
- registry 只有 Direct 时指定 `Csr` 必须报错；
- registry 只有 CSR 时指定 `DirectAdjacency` 必须报错；
- CSR 与 Direct 同时存在时仍严格遵循显式 mode；
- 指定任一索引时不得静默切换另一种索引或 Join；
- 常规不带 indexes 的执行入口固定生成 Join 路径；
- target scalar index 缺失时回退 target Join；
- relationship property 查询不误用直接邻接索引。

## 17. Benchmark 计划

### 17.1 目录

在现有：

```text
crates/lance-graph-benches/benches/indexed_expand/
```

新增：

```text
direct_adjacency.rs
```

如文件继续增大，再整理为：

```text
benches/indexed_expand/direct_adjacency/
├── build.rs
├── lookup.rs
└── star.rs
```

第一版优先复用现有 `star.rs` 图生成器和查询定义，避免两套 benchmark 数据语义漂移。

### 17.2 固定数据规模

沿用现有星型设计：

```text
source_count = 1,000,000
total_edges  = 10,000,000
queried source count = 1
queried hub degree = 10 / 100 / 1,000 / 10,000
```

为保持 total edges 固定，非查询 source 的 degree 分布随 queried hub degree 做相应调整。
三条查询路径必须使用同一组节点和关系数据。

### 17.3 查询对比

至少报告：

```text
join_baseline
csr_indexed_getv
direct_adjacency_indexed_getv_warm
direct_adjacency_indexed_getv_cold
```

其中：

- warm：Dataset、scalar-index handle 和相关页面已打开/预热；
- cold：重新打开索引 Dataset，明确包含 open/load 成本；
- steady-state query 不得把索引构建时间计入；
- cold 与 warm 必须分组，不能混在同一统计值中。

### 17.4 构建与存储对比

单独测量：

```text
CSR build time
CSR persisted write time
CSR persisted bytes
CSR load-to-memory time
CSR resident bytes

Direct adjacency group/write time
Direct adjacency scalar-index build time
Direct adjacency persisted bytes
Direct adjacency open time
Direct adjacency resident bytes before query
```

### 17.5 结果记录

每组 benchmark 输出或附带记录：

- source count；
- total edge count；
- queried degree；
- input source rows；
- output neighbor rows；
- target rows；
- Lance fragment count；
- adjacency Dataset version；
- warm/cold 状态；
- median、p95 或 Criterion estimate；
- 算子 metrics 中 lookup/fetch/flatten 的分项时间。

### 17.6 预期，不作为硬编码断言

预期趋势：

- 热内存 CSR 在纯 neighbor lookup 上最快；
- direct adjacency warm 查询多出 scalar lookup 和 Lance fetch；
- direct adjacency 的查询延迟随 queried degree 增长，而不应随全图总边数线性增长；
- direct adjacency 初始 resident memory 显著低于完整 CSR；
- cold direct adjacency 受对象存储、本地缓存和 fragment 数影响更大；
- 当 frontier 扩大到大量 source 时，逐行嵌套访问可能不如 CSR 或顺序 relationship scan。

计划的目标是测出边界，而不是预设 direct adjacency 必须在所有场景获胜。

## 18. 分阶段实施

### P0：Lance nested-list spike

工作：

- 实现临时测试 Dataset；
- 验证 List round trip；
- 验证 `src_id` scalar search；
- 验证 `take_rows` 和多个 fragments；
- 记录 Lance 1.0.4 的实际 API 与 coverage 行为。

完成条件：能够输入 source IDs，稳定返回对应 `List<dst_id>`，并保留重复项和空 List。

### P1：Metadata、Descriptor 与 Store

工作：

- 新增 `DirectAdjacencyMetadata`；
- 新增 descriptor 格式；
- 实现 Dataset write/open/load；
- 创建和验证 `src_id` scalar index；
- 实现 pinned version 和 source snapshot validation。

完成条件：索引可跨进程写入、关闭、重新打开和校验。

### P2：可扩展 Builder

工作：

- endpoint projection；
- source stable sort；
- streaming group；
- bounded List RecordBatch 构建；
- counts、null、duplicate source 校验；
- outgoing/incoming 构建。

完成条件：10M edge 星型数据能够在受控内存下构建，并满足 edge-count 不变量。

### P3：Registry 与索引发现

工作：

- registry 保存 direct adjacency handle；
- generation conflict 处理；
- common eligibility 与 backend-specific validation 分离；
- `get_csr()` 和 `get_direct_adjacency()` 只返回调用方指定类型的索引；
- 不在 registry 或 planner 内实现 CSR/Direct 自动优先级。

完成条件：同一 `GraphIndexKey` 可同时注册 CSR 和 direct adjacency；查询使用哪一个完全由
`ExpandExecutionMode` 决定。

### P4：`DirectAdjacencyExpandExec`

工作：

- 实现单 child physical operator；
- stable unique source lookup；
- scalar search 与 `take_rows`；
- List flatten 和 input row replication；
- bounded output；
- metrics 与 Display；
- 完整单元测试。

完成条件：算子输出与 `IndexedExpandExec` 对等，并覆盖 duplicate、missing、null、multi-fragment。

### P5：Planner 接入与 GetV 复用

工作：

- 删除 `IndexUsagePolicy`、`IndexDecision`、`IndexFallbackReason` 及相关 re-export；
- 新增 `ExpandExecutionMode`、`ExpandPlanDecision` 和 `ExpandIndexReference`；
- `DataFusionPlanner.index_policy` 替换为 `expand_mode`；
- 修改 `with_indexes`、execute、explain 和内部 logical-plan 创建函数的参数；
- 将 `IndexedExpandNode` 重命名为 `AdjacencyExpandNode`；
- logical index reference 支持 direct adjacency；
- physical extension planner 创建新算子；
- 复用现有 target access decision；
- 增加 `Join/Csr/DirectAdjacency` 显式模式；
- 指定索引缺失、失效或不兼容时返回规划错误；
- 迁移当前 CSR 单元测试、持久化测试和 benchmark 的全部调用点；
- EXPLAIN 和端到端结果断言。

完成条件：Cypher 查询真实执行
`DirectAdjacencyExpandExec → LanceGetVByIdExec`，而不是只手工构造物理计划。

### P6：Benchmark

工作：

- 复用 star workload；
- 加入 Join、CSR、Direct 三路径；
- 分开 warm/cold；
- 加入构建、体积、打开、resident memory；
- 记录 degree 曲线和算子分项 metrics。

完成条件：1M source、10M edge、degree 10/100/1,000/10,000 的正式结果可复现。

### P7：文档与后续决策

工作：

- 更新 crate README 的索引使用示例；
- 记录适用场景和限制；
- 根据 benchmark 决定下一步是增量更新、关系属性还是成本模型。

完成条件：使用者可以完成 build、persist、load、register、query 全流程，并知道何时选择 CSR
或直接邻接索引。

## 19. 建议模块布局

基于当前代码结构，建议：

```text
crates/lance-graph/src/
├── index/
│   ├── metadata.rs                 # common key + CSR metadata
│   ├── registry.rs
│   ├── selection.rs                # ExpandExecutionMode + typed reference
│   └── direct_adjacency/
│       ├── mod.rs
│       ├── metadata.rs
│       ├── builder.rs
│       └── persistence.rs
├── datafusion_planner/
│   ├── indexed_expand/             # existing CSR physical path
│   ├── direct_adjacency_expand/
│   │   ├── mod.rs
│   │   ├── physical.rs
│   │   └── planner.rs
│   └── get_v/                      # existing target path
└── node_lookup/                    # reusable narrow scalar lookup helper
```

如果 direct adjacency 与 CSR 共用现有 logical Expand node，则不需要在
`direct_adjacency_expand/` 再放一份 `logical.rs`。

Benchmark：

```text
crates/lance-graph-benches/benches/indexed_expand/
├── star.rs
├── graph_index_build.rs
├── persisted_index.rs
├── target_join_cost.rs
└── direct_adjacency.rs
```

## 20. 风险与缓解

### 20.1 小 List 的随机读取放大

风险：每个 source 一行虽然逻辑自然，但 Lance page/fragment 解码可能为了读取一个小 List
访问远多于实际 neighbor bytes。

缓解：

- 对 input batch 做 `IsIn` 批量 lookup；
- 使用 `take_rows` 批量读取；
- benchmark 多种 fragment 大小；
- 记录 fetched bytes / emitted neighbor；
- 后续评估 source-range clustering。

### 20.2 超级节点 List 过大

风险：单个 List 必须整体解码，可能造成尖峰内存。

缓解：第一版显式 degree/offset 检查并记录限制；后续增加 chunked adjacency rows。

### 20.3 scalar index coverage

风险：Dataset 更新后 scalar index 没有覆盖新 fragments，直接调用 index search 会漏数据。

缓解：immutable pinned generation；数据写完后再建索引；load/register 阶段验证完整 coverage。

### 20.4 多 source 输出顺序

风险：scalar search 和 `take_rows` 返回顺序不等于 input source 顺序。

缓解：始终根据返回的 `src_id` 建 map，再按原 input rows 重放，绝不依赖 row-address 顺序。

### 20.5 与 CSR 抽象过早统一

风险：为统一 API 引入 async trait、boxed streams 和额外 copy，反而损害 CSR 热路径。

缓解：共享 logical semantics、metadata key 和 planner decision；物理算子保持独立。

### 20.6 更新成本未必更低

风险：List 直接存储意味着给某个 source 增删边时需要重写该 List，未必天然适合高频更新。

缓解：第一版只承诺 immutable generation；后续用真实更新 workload 比较 merge-insert、delta
adjacency 和完整重建，不在本阶段宣称更新优势。

### 20.7 与 target GetV 的双重随机 I/O

风险：一次查询先随机读取 adjacency Dataset，再随机读取 target Dataset，冷查询可能出现两次
I/O 延迟。

缓解：分开记录 adjacency lookup 和 target GetV metrics；评估 cache、prefetch，以及未来将
常用 target 属性嵌入 `List<Struct>` 的收益，但不在第一版实施。

## 21. 待 benchmark 回答的问题

1. direct adjacency 在 warm 和 cold 条件下分别比 CSR 慢多少？
2. 不加载完整 CSR 节省了多少 resident memory？
3. `src_id` scalar search、`take_rows` 和 List flatten 各占多少时间？
4. fragment 数量和行组大小如何影响单 source lookup？
5. degree 从 10 增长到 10,000 时，延迟增长主要来自 I/O、解码还是 output materialization？
6. frontier 从 1 个 source 扩大到 10、100、1,000 时，何时应该切换为 CSR 或 scan？
7. direct adjacency Dataset 的持久化体积与 CSR 的 offsets/neighbors Dataset 相比如何？
8. BTree scalar index 自身的构建时间和体积是多少？
9. 对象存储环境下，邻接行 clustering、fragment size 和本地 cache 的影响有多大？
10. 若未来加入 edge properties，`List<Struct>` 是否能减少 relationship table 二次访问？

## 22. 第一版完成标准

以下条件全部满足后，第一版才算完成：

- [x] `src_id + List<dst_id>` 可在 Lance 中持久化并跨进程重新打开；
- [x] `src_id` BTree scalar index 能批量定位邻接行；
- [x] Dataset 固定到 descriptor 记录的 immutable version；
- [ ] builder 在 10M edges 下不需要全图 `HashMap<src_id, Vec<dst_id>>`；
- [x] registry 能按 `GraphIndexKey` 注册和发现直接邻接索引；
- [x] `IndexUsagePolicy`、`IndexDecision::Fallback` 和 `IndexFallbackReason` 已从 Expand 路径删除；
- [x] 所有 execute、explain、测试和 benchmark 调用点已迁移到 `ExpandExecutionMode`；
- [x] physical explain 包含 `DirectAdjacencyExpandExec`；
- [x] 索引路径不扫描 relationship table；
- [x] 索引路径不存在 source-relationship `HashJoinExec`；
- [x] target scalar index 可用时继续使用 `LanceGetVByIdExec`；
- [x] 结果与 Join、CSR 路径按 multiset 一致；
- [x] duplicate edges、missing source、null source、empty List 语义正确；
- [x] multi-fragment 与 scalar-index coverage 有测试；
- [x] `Join`、`Csr`、`DirectAdjacency` 的行为明确；
- [x] 指定索引缺失或失效时不会静默切换到另一种后端；
- [x] CSR 原路径的单元测试和 benchmark 不回归；
- [x] star benchmark 覆盖 1M source、10M edge、degree 10/100/1,000/10,000；
- [x] benchmark 分别报告 build、size、open/load、warm query 和 cold query；
- [x] README 说明直接邻接索引与 CSR 的适用场景和限制。

当前实现的已知限制：builder 仍先把 endpoint 边缓存在内存中再按 source 稳定排序；后续需要接入 DataFusion external sort/streaming group-by，才能满足超大关系表的严格流式构建目标。

## 23. 后续演进方向

第一版完成并取得 benchmark 数据后，按收益决定后续优先级：

1. `List<Struct<dst_id, edge_id, properties...>>`，支持关系属性下推；
2. source adjacency 分块，支持超级节点和流式 List 读取；
3. delta adjacency Dataset，降低增量边更新的 List 重写成本；
4. 基于 frontier/degree/cache 的 CSR、Direct Adjacency、Join 成本选择；
5. 多跳算子内批量 frontier lookup；
6. incoming/outgoing 双向索引的联合构建；
7. 常用 target 属性的邻接内嵌与 late materialization；
8. 对象存储下的预取、cache 和 fragment/layout 调优。

第一版应保持足够窄：先确认“Lance Dataset 中的一行一个 source、嵌套 List 保存 neighbors”
能够形成正确、可观测、可比较的端到端查询链路，再决定是否将它发展成比 CSR 更通用的图
存储格式。
