# 多类型直接邻接索引实施计划

## 1. 文档状态

- 状态：Implemented and verified
- 创建日期：2026-08-12
- 完成日期：2026-08-12
- 目标分支：`research/graph-index`
- 当前代码基线：Direct Adjacency 第一版工作区实现
- 前置能力：
  - 单个 `GraphIndexKey` 对应一个 `src_id + List<dst_id>` Lance Dataset；
  - `src_id` BTree scalar index；
  - immutable Dataset version 和持久化 component descriptor；
  - `DirectAdjacencyExpandExec`；
  - `LanceGetVByIdExec`；
  - `Join / Csr / DirectAdjacency` 显式 Expand 模式。
- 目标能力：一个具名的逻辑 Direct Adjacency Index 同时管理多个关系类型，但每个
  `GraphIndexKey` 仍使用独立的物理邻接 component。

本文档是 [direct-adjacency-index-plan.md](direct-adjacency-index-plan.md) 的下一阶段计划。
第一版 Direct Adjacency 已经验证了以下访问路径：

```text
GraphIndexKey
  → src_id scalar lookup
  → Dataset::take_rows
  → List<dst_id>
  → DirectAdjacencyExpandExec
  → LanceGetVByIdExec
```

本阶段不改变单个 component 的邻接行布局，而是在其上增加逻辑多类型索引、bundle
descriptor、原子加载/注册和具名选择能力。

## 2. 背景与问题定义

当前 Direct Adjacency 的物理和注册粒度都是一个 `GraphIndexKey`：

```rust
GraphIndexKey {
    relationship_type,
    source_label,
    target_label,
    direction,
}
```

例如以下关系会分别构建和注册三个互不关联的索引：

```text
FRIEND_OF(Person → Person, Outgoing)
FOLLOWS(Person → Person, Outgoing)
WORKS_AT(Person → Company, Outgoing)
```

每个索引对应一个独立 Dataset：

```text
src_id | dst_ids
-------+-----------------
42     | [7, 19, 31]
43     | [8, 11]
```

该布局对单类型查询是正确的，但缺少“这些 components 共同组成同一个图邻接索引”的上层
对象，导致：

1. 多个关系类型需要分别 build、persist、load 和 register；
2. 没有一个 manifest 描述某一索引 generation 包含哪些关系类型；
3. 多个 component 不能作为一个原子快照发布；
4. planner 只能指定 `DirectAdjacency` 后端，不能指定要使用哪个具名逻辑索引；
5. registry 对同一 `GraphIndexKey` 只能保存一个 Direct component，无法同时注册两个用途
   不同的 Direct Adjacency 索引；
6. 某个 component 缺失、损坏或 stale 时，没有 bundle 级一致性语义；
7. 关系类型数量增加后，构建、加载、替换、观测和 benchmark 缺少统一入口。

本阶段需要解决的核心问题是：

> 如何让一个 Direct Adjacency Index 在逻辑上直接支持多个关系类型，同时保持精确类型查询
> 的完整裁剪能力，并最大程度复用当前已经验证的单类型 Dataset 和物理算子？

## 3. 外部系统给出的设计约束

现有图系统虽然物理结构不同，但关系类型都在读取邻居集合之前参与定位：

| 系统 | 邻接定位方式 | 关系类型位置 |
|---|---|---|
| Dgraph | `(subject, predicate) → Posting List` | Posting List key / predicate tablet |
| LiveGraph | `(vertex, label) → TEL` | per-vertex label index |
| JanusGraph | vertex wide row + column range | column key 前缀 |
| NebulaGraph | `(src, edge_type, rank, dst)` KV range | KV key 前缀 |
| Neo4j | node → relationship group → edge chain | relationship group |
| FalkorDB | relation matrix → row/column | 每种 relation 一个矩阵 |
| GraphAr/Graphflow/Kùzu | edge triplet/label → CSR | 每种 edge type 一个 CSR/component |

这些系统没有采用以下布局作为通用 n-n 关系的主要访问路径：

```text
src_id
  → List<Struct<relationship_type, dst_id>>
  → 读取全部类型后再过滤
```

因此，本计划把以下原则作为硬约束：

1. 关系类型必须在邻接 Dataset 读取前完成选择；
2. 查询一种关系类型时不能解码同一 source 的其他类型邻居；
3. `relationship_type` 不在每个 neighbor element 中重复保存；
4. 一个 component 只对应一个完整 `GraphIndexKey`；
5. 多类型支持首先是 descriptor、生命周期和选择层的能力，不是混合 List schema；
6. 精确单类型查询继续使用现有的单 component 物理热路径。

## 4. 设计结论摘要

第一版多类型 Direct Adjacency 采用以下明确决策：

1. 对外提供一个具名的逻辑多类型索引，例如 `social_adjacency`；
2. 一个逻辑索引包含多个类型专属的 Direct Adjacency components；
3. 每个 component 继续使用当前 `src_id + List<dst_id>` schema；
4. `GraphIndexKey` 保持 component 唯一键，不新增全局 `relationship_type_id` 字典；
5. bundle 使用独立 descriptor，component descriptor 格式继续复用当前实现；
6. bundle descriptor 只组合 component descriptor，不复制邻接数据；
7. bundle generation 是原子发布和原子替换的单位；
8. 第一版加载时 eager open 并验证全部 components，但不会把全部邻接数据加载到内存；
9. registry 按逻辑索引名称保存 bundle，并按 `(index_name, GraphIndexKey)` 查找 component；
10. Direct 模式必须显式指定逻辑索引名称；
11. 指定 bundle 或 component 不存在、损坏、过期或不兼容时直接报错，不回退到 CSR 或 Join；
12. 精确单类型查询继续生成一个 `DirectAdjacencyExpandExec`，不新增多类型热路径算子；
13. 一个 bundle 可以包含不同 source/target label、不同方向和不同整数 ID 类型的 components；
14. 第一版不支持 `[:A|B]` 多类型关系模式；该能力作为后续阶段使用多个 components 做
    bag-preserving union；
15. 第一版不采用单 Dataset 复合键 `(relationship_type, src_id)`，但 benchmark 会记录
    component 数量增加后的 open/load 固定成本，作为是否需要该布局的后续依据；
16. 当前所有单类型 Direct Adjacency 调用点迁移为“只包含一个 component 的 bundle”，不保留
    两套并行公共 API。

## 5. 目标

### 5.1 索引生命周期目标

使用者可以把多个关系类型作为一个逻辑索引完成：

```text
build components
→ persist component descriptors
→ build bundle descriptor
→ validate all components
→ publish bundle descriptor last
→ load bundle
→ atomically register bundle
→ query a selected relationship type
```

示例逻辑索引：

```text
social_adjacency, generation 3
  ├── FRIEND_OF(Person → Person, Outgoing)
  ├── FRIEND_OF(Person → Person, Incoming)
  ├── FOLLOWS(Person → Person, Outgoing)
  ├── FOLLOWS(Person → Person, Incoming)
  └── WORKS_AT(Person → Company, Outgoing)
```

### 5.2 查询目标

对查询：

```cypher
MATCH (a:Person {name: "Alice"})-[:FRIEND_OF]->(b:Person)
WHERE b.age > 30
RETURN a.name, b.name
```

执行时显式指定：

```text
ExpandExecutionMode::DirectAdjacency {
    index_name: "social_adjacency"
}
```

planner 根据查询构造：

```text
GraphIndexKey(
  relationship_type = FRIEND_OF,
  source_label = Person,
  target_label = Person,
  direction = Outgoing,
)
```

然后执行严格查找：

```text
(social_adjacency, GraphIndexKey)
  → exact component
  → DirectAdjacencyExpandExec
```

期望物理计划保持：

```text
ProjectionExec(a.name, b.name)
└── FilterExec(b.age > 30)
    └── LanceGetVByIdExec(Person.person_id AS b)
        └── DirectAdjacencyExpandExec
            │ index_name = social_adjacency
            │ bundle_generation = 3
            │ component = FRIEND_OF(Person → Person, Outgoing)
            │ component_generation = 3
            └── LanceScanExec(Person AS a, name = "Alice")
```

必须满足：

1. 查询执行阶段只对 `FRIEND_OF` component 发起 scalar lookup 和邻接行读取；bundle load
   阶段允许按第一版 eager-load 规则打开其他 component handles；
2. 不读取 `FOLLOWS`、`WORKS_AT` 等其他 component 的邻接数据；
3. 不扫描原 relationship table；
4. 不出现 source-to-relationship Hash Join；
5. target 节点仍由 `LanceGetVByIdExec` 物化；
6. 结果与 Join、CSR 和原单类型 Direct 路径按 multiset 一致；
7. 指定索引或指定类型缺失时，在规划阶段返回可诊断错误。

## 6. 非目标

第一版不包含：

- `src_id → List<Struct<relationship_type, dst_id>>` 混合邻接列表；
- 单个 Dataset 上的 `(relationship_type, src_id)` 复合 BTree；
- 一个 source 一行、每种关系类型一个 List 列；
- 运行时在 CSR、Direct 和 Join 之间自动择优；
- 指定 Direct bundle 失败后回退到其他 bundle、CSR 或 Join；
- `MATCH (a)-[:FRIEND_OF|FOLLOWS]->(b)` 多类型关系模式；
- 无类型关系模式 `MATCH (a)-[]->(b)`；
- relationship variable、relationship property 过滤或返回；
- 在邻接 List 内嵌 `edge_id`、`edge_row_id` 或关系属性；
- component 的细粒度增删边事务；
- delta adjacency、rollup 或跨 generation compaction；
- lazy component open；
- component 自动按热度装卸；
- 远端对象存储的 `LATEST` 指针或 catalog 自动发现；
- Python 公共 API；
- 同一个查询中混用两个不同的 Direct bundles；
- 为 CSR 同时引入具名 bundle；
- 为了统一 CSR 和 Direct 而引入新的异步邻接 trait。

## 7. 术语与对象层次

本计划统一使用以下术语：

```text
Direct Adjacency Component
    一个 GraphIndexKey 对应的物理邻接 Dataset、scalar index 和 component descriptor。

Multi-Type Direct Adjacency Index / Bundle
    一个具名逻辑索引，由一个 bundle descriptor 和多个 components 组成。

Bundle Generation
    一组 component 引用的不可变原子快照。

Component Generation
    单个物理邻接 component 的 generation，沿用当前 metadata.generation。
```

对象关系：

```text
MultiTypeDirectAdjacencyIndexHandle
  ├── metadata(index_name, bundle_generation, counts)
  └── components: BTreeMap<GraphIndexKey, Arc<DirectAdjacencyIndexHandle>>
        ├── FRIEND_OF/outgoing → Dataset handle + scalar index handle
        ├── FRIEND_OF/incoming → Dataset handle + scalar index handle
        ├── FOLLOWS/outgoing   → Dataset handle + scalar index handle
        └── WORKS_AT/outgoing  → Dataset handle + scalar index handle
```

使用 `BTreeMap` 的主要目的是 descriptor、Display 和测试输出稳定；查询复杂度并不是本阶段的
瓶颈。若 registry 热路径需要，可以在外层同时维护 HashMap，但不应复制 handle 所有权。

## 8. 物理布局

### 8.1 component schema 保持不变

每个 component 继续使用当前 schema：

```text
src_id:  ID, non-null
dst_ids: List<ID>, non-null
```

例如：

```text
FRIEND_OF component
src_id | dst_ids
-------+----------------
42     | [7, 8, 9]

FOLLOWS component
src_id | dst_ids
-------+----------------
42     | [17, 21]
```

查询 `FRIEND_OF` 时，不会读取 `FOLLOWS` Dataset。

### 8.2 component 的唯一性

同一个 bundle generation 内必须满足：

```text
GraphIndexKey → exactly one component descriptor
```

以下情况构建时直接报错：

- 重复的 `GraphIndexKey`；
- 同一个 key 指向两个 Dataset versions；
- component metadata 中的 key 与 bundle entry key 不一致；
- component descriptor generation、URI 或格式版本不合法；
- `source_version` 存在但 `source_uri` 缺失。

一个关系类型的 outgoing 和 incoming 是不同 `GraphIndexKey`，因此可以同时存在：

```text
FRIEND_OF(Person → Person, Outgoing)
FRIEND_OF(Person → Person, Incoming)
```

### 8.3 不要求 bundle 级统一 ID 类型

以下 components 可以位于同一个 bundle：

```text
FRIEND_OF(Person[Int64] → Person[Int64])
WORKS_AT(Employee[UInt64] → Company[UInt64])
```

第一版现有 component 仍要求其 `src_id` 与 `dst_ids.item` 使用相同 ID 类型。bundle 不再增加
全局 `id_data_type` 字段，而是由每个 component 独立记录和校验。

如果后续允许 source/target 使用不同 ID 类型，应扩展 component metadata，而不是在 bundle
层加入一个无法准确表达所有 components 的公共类型。

### 8.4 边重复度和顺序

每个 component 继续保留关系表的 bag semantics：

```text
42 → 7
42 → 7
42 → 9
```

持久化为：

```text
42 → [7, 7, 9]
```

bundle 不能对 component 内部或跨 component 的边做去重。未来多类型模式若合并多个
components，也必须执行 bag-preserving union，而不是集合 union。

## 9. 持久化格式

### 9.1 保留 component descriptor

当前单类型 Direct Adjacency descriptor 继续作为 component 的权威描述：

```text
component-root/
├── descriptor.json
└── adjacency.lance/
```

本阶段不把 component metadata 全部复制到 bundle descriptor。bundle entry 保存 component
descriptor URI 和用于快速校验/展示的最小 identity，加载时仍读取并核对 component
descriptor。

这样可以：

- 直接复用 `DirectAdjacencyIndexStore::load`；
- 独立测试和诊断某个 component；
- 避免维护两份容易漂移的完整 metadata；
- 为后续新 bundle generation 复用已有 immutable component 留出空间。

### 9.2 建议目录布局

第一版由 bundle builder 构建全部 components 时，建议布局为：

```text
<bundle-generation-uri>/
├── bundle-descriptor.json
└── components/
    ├── friend-of-person-person-outgoing-<stable-hash>/
    │   ├── descriptor.json
    │   └── adjacency.lance/
    ├── follows-person-person-outgoing-<stable-hash>/
    │   ├── descriptor.json
    │   └── adjacency.lance/
    └── works-at-person-company-outgoing-<stable-hash>/
        ├── descriptor.json
        └── adjacency.lance/
```

component 路径不能只使用 relationship type，因为 source/target label 和 direction 也属于
key。建议使用可读 slug 加稳定 hash，避免大小写、特殊字符和超长名称造成冲突。

### 9.3 bundle descriptor

建议新增公开 descriptor：

```rust
pub struct PersistedMultiTypeDirectAdjacencyDescriptor {
    pub index_uri: String,
    pub format_version: u32,
    pub metadata: MultiTypeDirectAdjacencyMetadata,
    pub components: Vec<DirectAdjacencyComponentDescriptorRef>,
}
```

建议持久化 JSON：

```json
{
  "format": "lance_graph_multi_type_direct_adjacency",
  "format_version": 1,
  "index_kind": "direct_adjacency_bundle",
  "index_name": "social_adjacency",
  "bundle_generation": 3,
  "num_components": 3,
  "num_edges": 10000000,
  "components": [
    {
      "key": {
        "relationship_type": "friend_of",
        "source_label": "person",
        "target_label": "person",
        "direction": "outgoing"
      },
      "component_descriptor_uri": "components/friend-of-person-person-outgoing-a1b2/",
      "component_generation": 3
    },
    {
      "key": {
        "relationship_type": "follows",
        "source_label": "person",
        "target_label": "person",
        "direction": "outgoing"
      },
      "component_descriptor_uri": "components/follows-person-person-outgoing-c3d4/",
      "component_generation": 3
    }
  ]
}
```

相对 URI 按 bundle descriptor 所在 URI 解析；绝对 URI 原样使用。第一版 writer 默认写相对
URI，使整个本地 bundle generation 可以整体移动。对象存储下是否允许移动不在第一版承诺
范围内。

### 9.4 bundle metadata

建议新增：

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTypeDirectAdjacencyMetadata {
    pub index_name: String,
    pub bundle_generation: u64,
    pub num_components: u64,
    pub num_sources: u64,
    pub num_edges: u64,
}
```

字段语义：

- `index_name`：调用方显式选择的逻辑索引名称；
- `bundle_generation`：组件映射的原子 generation；
- `num_components`：bundle entry 数量；
- `num_sources`：各 component `num_sources` 之和，仅用于统计，不能解释为去重节点数；
- `num_edges`：各 component `num_edges` 之和，保留 parallel edge multiplicity。

`index_name` 通过构造函数统一规范化。建议沿用 `GraphIndexKey` 的大小写无关语义，内部保存
lowercase；空名称和只包含空白的名称必须拒绝。

### 9.5 发布顺序

bundle generation 的写入顺序必须是：

```text
1. 为每个 GraphIndexKey 写 component Dataset
2. 创建 component scalar index
3. 写 component descriptor
4. 重新 load 并验证每个 component
5. 检查 bundle key 唯一性和聚合 counts
6. 写 bundle-descriptor.json last
7. 返回可注册 descriptor
```

只有 `bundle-descriptor.json` 存在时，该 bundle generation 才被视为已发布。任何 component
构建失败都不得产生可加载的 bundle descriptor。

第一版不自动删除失败构建留下的 component 目录，避免在对象存储和用户指定 URI 上执行
隐式破坏性操作；由调用方或后续 GC 工具清理未发布数据。

## 10. 运行时句柄与加载语义

### 10.1 bundle handle

建议新增：

```rust
#[derive(Debug)]
pub struct MultiTypeDirectAdjacencyIndexHandle {
    pub metadata: MultiTypeDirectAdjacencyMetadata,
    pub components:
        BTreeMap<GraphIndexKey, Arc<DirectAdjacencyIndexHandle>>,
}
```

辅助接口：

```rust
impl MultiTypeDirectAdjacencyIndexHandle {
    pub fn get(
        &self,
        key: &GraphIndexKey,
    ) -> Option<Arc<DirectAdjacencyIndexHandle>>;

    pub fn keys(&self) -> impl Iterator<Item = &GraphIndexKey>;
}
```

不要向查询算子暴露可变 component map。bundle 和 component handles 都是 immutable，并通过
`Arc` 被正在执行的查询固定。

### 10.2 第一版 eager load

`MultiTypeDirectAdjacencyIndexStore::load` 第一版按以下流程加载所有 components：

```text
read bundle descriptor
→ validate format/name/generation/counts
→ resolve component descriptor URIs
→ reject duplicate GraphIndexKey
→ load and validate every component
→ verify entry key == component metadata key
→ verify entry generation == component metadata generation
→ aggregate counts
→ construct bundle handle
```

需要明确：eager load 打开的主要是：

- Lance Dataset handle；
- pinned Dataset metadata；
- scalar-index handle/top-level metadata；
- component descriptor。

它不会像 CSR 一样把所有 `dst_ids` 加载到内存。邻接 List 仍然在查询时通过
`Dataset::take_rows` 按 source 读取。

选择 eager load 的原因是：

- registry 和当前 planner 查找接口是同步的；
- bundle 可以在注册前完成全量一致性验证；
- 查询规划不需要临时执行异步对象存储 I/O；
- 第一版实现简单，能够先测量 component 数量带来的实际固定成本。

若 benchmark 表明 50/100+ components 的打开成本不可接受，再设计 lazy component state，
不在第一版提前引入 async once-cell 和失败缓存语义。

### 10.3 多 source snapshot 校验选项

当前 `DirectAdjacencyLoadOptions` 只携带一个 `IndexSourceValidation`，不能准确表达多个
components 分别来自不同 relationship tables。bundle load 建议新增：

```rust
#[derive(Debug, Clone, Default)]
pub struct MultiTypeDirectAdjacencyLoadOptions {
    pub source_validation: MultiTypeSourceValidation,
}

#[derive(Debug, Clone, Default)]
pub enum MultiTypeSourceValidation {
    #[default]
    AllowUnknown,
    RequireExact(BTreeMap<GraphIndexKey, GraphSourceIdentity>),
}
```

语义：

```text
AllowUnknown
    → 仍验证 component descriptor 内部 source_uri/source_version 自洽，
      但不与调用方外部快照比较。

RequireExact(expected)
    → expected key 集合必须与 bundle component key 集合完全一致；
    → 每个 component 的 source_uri/source_version 必须与对应 expected identity 一致；
    → 缺少 expected key、出现额外 key 或任一版本不匹配都返回 Stale/Incompatible。
```

加载单个 component 时，将对应 identity 转换为当前
`DirectAdjacencyLoadOptions::source_validation`，继续复用已有 source validation 代码。

不允许把一个 `GraphSourceIdentity` 应用到所有 components，因为不同关系类型通常来自不同
relationship table URI/version。

### 10.4 all-or-nothing 加载

以下任意一个 component 失败，整个 bundle load 失败：

- descriptor 缺失或损坏；
- Dataset URI/version 不可打开；
- schema 不兼容；
- scalar index 缺失或 fragment coverage 不完整；
- source snapshot stale；
- component key/generation 与 bundle entry 不一致；
- counts 与 bundle descriptor 不一致。

不返回只包含部分类型的 handle。否则同一个 bundle generation 会因机器缓存、权限或对象
存储故障不同而表现为不同的关系类型集合，破坏可诊断性和快照语义。

## 11. Registry 设计

### 11.1 以 bundle 为注册单位

当前 registry 保存：

```rust
HashMap<GraphIndexKey, Arc<DirectAdjacencyIndexHandle>>
```

本阶段建议替换为：

```rust
HashMap<String, Arc<MultiTypeDirectAdjacencyIndexHandle>>
```

`String` 是规范化后的 `index_name`。

`GraphIndexRegistry` 的 Direct 查找接口调整为：

```rust
fn get_direct_adjacency(
    &self,
    index_name: &str,
    key: &GraphIndexKey,
) -> Result<Option<Arc<DirectAdjacencyIndexHandle>>>;
```

必要时增加诊断接口：

```rust
fn get_direct_adjacency_bundle(
    &self,
    index_name: &str,
) -> Result<Option<Arc<MultiTypeDirectAdjacencyIndexHandle>>>;
```

planner 热路径只需要第一个接口；bundle 接口主要用于测试、管理和 EXPLAIN 诊断。

### 11.2 原子注册和替换

新增：

```rust
pub fn register_direct_adjacency_bundle(
    &self,
    handle: MultiTypeDirectAdjacencyIndexHandle,
) -> Result<()>;
```

注册规则：

```text
index name 不存在
    → 注册

相同 name，新 bundle_generation 更大
    → 原子替换整个 Arc<BundleHandle>

相同 name，相同 generation，metadata 和 component identities 完全一致
    → 幂等成功

相同 name，相同 generation，但内容不同
    → GenerationConflict

相同 name，更旧 generation
    → GenerationConflict
```

替换 bundle 时不能逐个覆盖 component map。必须先构造并验证完整新 handle，再在 registry 的
一次 write-lock 临界区内替换 bundle Arc。已经开始执行的查询继续持有旧 Arc，不受替换影响。

### 11.3 不同 bundle 可包含相同 GraphIndexKey

以下两个 bundle 可以同时注册：

```text
social_adjacency
  └── FRIEND_OF(Person → Person, Outgoing)

experimental_adjacency
  └── FRIEND_OF(Person → Person, Outgoing)
```

planner 通过 `index_name` 明确选择，因此不存在当前全局 key 冲突。这也是引入具名 bundle 的
主要价值之一。

## 12. 构建 API

### 12.1 先组合 component，再发布 bundle

为了最大程度复用当前实现，第一版 builder 采用组合式 API：

```rust
let friend_of = DirectAdjacencyIndexBuilder::new(friend_metadata)?
    .add_edges_from_batch(&friend_edges)?
    .build_and_persist(friend_uri, options.clone())
    .await?;

let follows = DirectAdjacencyIndexBuilder::new(follows_metadata)?
    .add_edges_from_batch(&follows_edges)?
    .build_and_persist(follows_uri, options.clone())
    .await?;

let bundle = MultiTypeDirectAdjacencyIndexBuilder::new(
        "social_adjacency",
        1,
    )?
    .add_component(friend_of)?
    .add_component(follows)?
    .build_and_persist(bundle_uri)
    .await?;
```

`add_component` 至少验证：

- descriptor 格式受支持；
- key 尚未出现；
- generation 合法；
- descriptor URI 非空；
- source identity 字段自洽。

真正的 Dataset/schema/scalar-index validation 在 bundle publish 前通过 component store load
完成，不能只信任调用方传入的内存 descriptor。

### 12.2 便利型 orchestration API

在组合式 API 稳定后，可以增加：

```rust
MultiTypeDirectAdjacencyIndexBuilder::new(name, generation)?
    .add_component_build(component_spec, edge_batches)?
    .add_component_build(other_spec, other_edge_batches)?
    .build_all_and_persist(bundle_uri, options)
    .await?;
```

内部仍逐个调用当前 component builder。第一版默认顺序构建，或使用很小的有界并发；不能对
所有关系类型同时复制 10M 级 edge buffers，造成峰值内存随 component 数线性增长。

### 12.3 不引入混合 typed edge stream

第一版不要求输入：

```text
relationship_type | src_id | dst_id
```

然后在 builder 内部按 type 分区。原因是当前每种关系类型通常已经来自独立 relationship
table，而内部全类型分桶会重新引入大规模 `type → source → neighbors` 内存结构。

如果未来 catalog 暴露一个混合边流，应使用外部 sort/partition：

```text
project(type, src, dst)
→ external sort by (type, src)
→ streaming group
→ per-type component writers
```

而不是全量 HashMap。该能力不阻塞第一版 bundle。

## 13. 索引选择 API

### 13.1 Direct 模式携带索引名称

当前：

```rust
pub enum ExpandExecutionMode {
    Join,
    Csr,
    DirectAdjacency,
}
```

建议调整为：

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandExecutionMode {
    Join,
    Csr,
    DirectAdjacency { index_name: String },
}
```

`ExpandExecutionMode` 不再实现 `Copy`；所有 execute、explain、planner、测试和 benchmark
调用点相应改为借用或 clone。这里不保留无名称的 `DirectAdjacency` 兼容 variant，因为它会
重新引入“registry 中有多个 Direct index 时选择哪个”的隐式行为。

建议提供便利构造函数：

```rust
impl ExpandExecutionMode {
    pub fn direct_adjacency(index_name: impl Into<String>) -> Result<Self>;
}
```

名称在构造时规范化并校验，避免错误延迟到 planner。

### 13.2 严格选择语义

行为表：

| 模式 | 行为 |
|---|---|
| `Join` | 使用原 relationship scan + join，不查询图索引 registry |
| `Csr` | 沿用当前严格 CSR 语义 |
| `DirectAdjacency { index_name }` | 必须在指定 bundle 中找到精确 `GraphIndexKey` component |

Direct 模式错误语义：

| 情况 | 结果 |
|---|---|
| bundle 未注册 | planning error：包含 index name |
| bundle 存在，但 component key 缺失 | planning error：列出缺失的 relationship type/labels/direction |
| component ID 类型不兼容 | planning error |
| component source snapshot stale | load/register error；若状态在注册后变化则 planning/execution error |
| component descriptor 损坏 | bundle load error |
| 查询需要关系属性 | planning error，不回退 Join |
| 查询需要不支持的多类型模式 | planning error |

### 13.3 logical reference

当前 `DirectAdjacencyReference` 建议扩展为：

```rust
pub struct DirectAdjacencyReference {
    pub index_name: String,
    pub bundle_generation: u64,
    pub key: GraphIndexKey,
    pub component_generation: u64,
    pub dataset_version: u64,
}
```

不要继续只使用一个含义模糊的 `generation`。logical plan 和 EXPLAIN 应能区分：

- 哪个逻辑索引；
- 哪个 bundle generation；
- 哪个 component；
- component generation；
- pinned Lance Dataset version。

## 14. Planner 与物理执行

### 14.1 精确单类型查询

规划过程：

```text
1. Cypher analysis 得到单一 relationship type、labels 和 direction
2. 构造 GraphIndexKey
3. 读取 DirectAdjacency { index_name }
4. registry.get_direct_adjacency(index_name, key)
5. 缺失或不兼容则返回 planning error
6. 创建带 bundle/component identity 的 DirectAdjacencyReference
7. logical AdjacencyExpandNode 保存 reference
8. physical extension planner 取得 exact component handle
9. 创建现有 DirectAdjacencyExpandExec
```

不需要修改 `DirectAdjacencyExpandExec` 的核心查找算法：

```text
source IDs
→ component src_id BTree
→ component Dataset::take_rows
→ List<dst_id>
→ replicate input rows
→ append endpoint ID
```

需要修改的仅是 handle 获取、identity 校验和 Display/metrics 标签。

### 14.2 physical plan identity

`DirectAdjacencyExpandExec::fmt_as` 至少输出：

```text
DirectAdjacencyExpandExec:
  index_name=social_adjacency,
  bundle_generation=3,
  relationship_type=friend_of,
  source_label=person,
  target_label=person,
  direction=outgoing,
  component_generation=3,
  dataset_version=2
```

这使 benchmark 和线上 EXPLAIN 可以确认不仅使用了 Direct 后端，还使用了预期 bundle 和
关系类型 component。

### 14.3 多类型关系模式的后续计划

未来支持：

```cypher
MATCH (a:Person)-[:FRIEND_OF|FOLLOWS]->(b:Person)
```

建议执行方式：

```text
source input
  ├── FRIEND_OF component lookup
  └── FOLLOWS component lookup
          ↓
bag-preserving union
          ↓
LanceGetVByIdExec
```

可以实现为一个接收多个 handles 的 `MultiTypeDirectAdjacencyExpandExec`，也可以先在物理层
构造多个分支后 `UnionExec`。在正式设计前必须处理 source input 被多个分支消费、重复边、
输出顺序和共享 lookup batch 的问题。

该能力不应通过读取一个混合 `List<Struct<type,dst>>` 实现。

## 15. 快照与 generation 语义

### 15.1 bundle generation 是选择映射快照

bundle generation 固定以下映射：

```text
GraphIndexKey → component descriptor → Dataset version
```

运行中的查询通过 `Arc<MultiTypeDirectAdjacencyIndexHandle>` 固定该映射。registry 替换为新
generation 后，旧查询仍然访问旧 components。

### 15.2 component source snapshot 独立验证

每种关系类型可以来自不同 relationship table，因此 source identity 必须保留在 component：

```text
FRIEND_OF → source_uri/version A/7
FOLLOWS   → source_uri/version B/11
WORKS_AT  → source_uri/version C/4
```

bundle 不提供单一 `source_uri/source_version`。load 时对每个 component 使用现有
`IndexSourceValidation` 校验。

### 15.3 第一版更新策略

第一版采用 immutable bundle generation：

```text
relationship data changes
→ build replacement component(s)
→ produce a complete new bundle descriptor
→ validate complete bundle
→ atomically replace registered bundle
```

最简单的第一版可以重建全部 components。descriptor 格式允许未来引用旧的 immutable
component descriptor，从而只重建变更类型，但以下能力留到后续：

- component 引用共享的生命周期管理；
- 跨 bundle generation 的引用计数；
- orphan component GC；
- 删除旧 generation 时防止误删仍被引用的 component。

在这些语义完成前，自动清理不能跨 generation 递归删除共享 components。

## 16. 关系属性的后续兼容方向

本阶段 component 只保存：

```text
src_id
dst_ids: List<ID>
```

如果后续需要：

```cypher
MATCH (a)-[r:FRIEND_OF]->(b)
WHERE r.since > 2020
RETURN r.weight
```

更自然的 component 演进为：

```text
src_id
neighbors: List<Struct<
    dst_id,
    edge_row_id
>>
```

或对齐的多个 List：

```text
dst_ids:      List<ID>
edge_row_ids: List<UInt64>
```

关系类型仍由 bundle component 选择，不应在每个 neighbor element 中重复存储。bundle
descriptor 应允许不同 components 采用不同 component format versions，但第一版要求全部是
当前 `src_id + List<dst_id>` 格式。

## 17. 错误处理与一致性要求

建议复用当前 `GraphIndexErrorKind`，并保证错误消息包含以下 identity：

```text
index_name
bundle_generation
GraphIndexKey
component_descriptor_uri
component_generation
dataset_uri/version（若已知）
```

关键错误场景：

1. `Missing`：bundle descriptor、component descriptor、Dataset 或指定 key 缺失；
2. `Corrupt`：descriptor 不可解析、counts/key 不一致、重复 component key；
3. `Incompatible`：格式版本、schema、ID 类型、查询能力不兼容；
4. `Stale`：source snapshot 或 pinned Dataset version 不匹配；
5. `GenerationConflict`：同名 bundle 的 generation 替换不合法；
6. `AlreadyExists`：目标 bundle generation 已发布。

错误不能触发以下隐式行为：

- 从指定 bundle 切换到另一个 bundle；
- 从 Direct 切换到 CSR；
- 从 Direct 切换到 Join；
- 忽略损坏 component 后注册 partial bundle；
- 打开 Dataset latest version 替代 descriptor pinned version。

## 18. 可观测性

### 18.1 bundle load metrics

至少记录：

```text
bundle_descriptor_read_time
component_count
component_descriptor_read_time
component_dataset_open_time
component_scalar_index_open_time
bundle_validation_time
bundle_total_load_time
```

按 component 记录：

```text
GraphIndexKey
dataset_uri/version
num_sources
num_edges
load/open latency
```

### 18.2 查询 metrics

现有 Direct query metrics 增加静态标签：

```text
index_name
bundle_generation
relationship_type
direction
component_generation
```

单次查询的邻接 lookup metrics 仍归属于 selected component：

```text
input_source_rows
unique_source_ids
scalar_search_time
matched_adjacency_rows
take_rows_time
list_decode_time
emitted_neighbors
output_batches
```

不得把未被查询的其他 components 的 counts 计入查询吞吐。

## 19. 测试计划

### 19.1 descriptor 和 metadata 单元测试

覆盖：

- 两种及以上关系类型 round trip；
- outgoing/incoming 同时存在；
- 不同 source/target labels；
- 不同 component ID 类型；
- one-component bundle 合法；
- empty bundle 非法；
- duplicate `GraphIndexKey` 拒绝；
- 空 index name 拒绝；
- 相对/绝对 component descriptor URI；
- bundle counts 聚合；
- entry key 与 component key 不一致；
- entry generation 与 component generation 不一致；
- format version 不受支持；
- bundle descriptor 缺失和损坏。

### 19.2 store/load 测试

覆盖：

- build multiple components → persist bundle → drop handles → reload；
- 所有 Dataset 固定到 descriptor version；
- 全部 scalar indexes 和 fragment coverage 验证；
- 一个 component 损坏导致整个 load 失败；
- 一个 component stale 导致整个 load 失败；
- load 失败不返回 partial handle；
- descriptor 在 components 完成后才发布；
- 同一 bundle 可以包含本地和对象存储 URI（若测试环境支持）。

### 19.3 registry 测试

覆盖：

- 按 `(index_name, GraphIndexKey)` 精确查找；
- 同一 bundle 中多个类型查找；
- 两个 bundle 包含相同 key，按名称隔离；
- 相同 generation 幂等注册；
- 相同 generation 不同内容冲突；
- 旧 generation 拒绝；
- 新 generation 原子替换；
- 替换后旧 Arc 仍可用于已开始查询；
- bundle 缺失与 component 缺失错误可区分。

### 19.4 planner 测试

至少使用以下 queries：

```cypher
MATCH (a:Person)-[:FRIEND_OF]->(b:Person) RETURN b.person_id
MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN b.person_id
MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.company_id
```

断言：

- 三个查询都使用同一个 `index_name`；
- 每个查询选择正确 component；
- physical plan 包含预期 relationship type 和 bundle generation；
- plan 不包含 relationship scan/source join；
- 指定错误 bundle name 时 planning error；
- 正确 bundle 缺少关系类型时 planning error；
- Direct mode 不对缺失类型回退 Join；
- Join 和 CSR 模式不受具名 Direct bundle 影响；
- 关系属性查询在 Direct mode 下明确报错。

### 19.5 端到端正确性

构造同时包含：

```text
FRIEND_OF
FOLLOWS
BLOCKS
WORKS_AT
```

的测试图，并覆盖：

- 同一个 source 在多个类型中均有邻居；
- 某个 source 只在一种类型中有邻居；
- 某类型 source 缺失；
- parallel edges；
- self-loop；
- incoming/outgoing；
- Person → Person 和 Person → Company；
- null source 上游行；
- 多 source input batch。

每种类型分别与 Join baseline 做 multiset 比较，防止错误读取其他类型 adjacency。

## 20. Benchmark 计划

### 20.1 数据规模

固定：

```text
source count: 1,000,000
total edges:  10,000,000
```

类型分布：

```text
FRIEND_OF: 7,000,000 edges
FOLLOWS:   2,000,000 edges
BLOCKS:    1,000,000 edges
```

每种关系类型都保证 query source 42 存在，并分别设置可控 degree。除三类型主 workload 外，再
生成 1/3/10/50 个 components 的 metadata/load workload，用于测量 bundle component 数量
带来的固定成本。

### 20.2 查询 cases

第一版 benchmark 测量精确单类型查询：

```text
one source + FRIEND_OF
one source + FOLLOWS
one source + BLOCKS
many sources + FRIEND_OF
alternating relationship types across repeated queries
```

对比：

```text
Join
CSR（对应类型）
原单类型 Direct component 基线
具名 multi-type bundle 中的同一 Direct component
```

“原单类型 Direct component 基线”可以通过实施前结果或内部直接构造物理算子的 benchmark
保留，不需要为其继续保留公共 API。

### 20.3 构建与加载 metrics

测量：

```text
component build time per type
bundle descriptor build/publish time
total persisted bytes
bundle descriptor bytes
component descriptor bytes
warm bundle load
cold local bundle load
component count vs load latency
component count vs open object-store requests
process resident memory after load
```

### 20.4 查询 metrics

测量：

```text
warm exact-type query latency
cold exact-type query latency
scalar lookup
take_rows
List decode
target GetV
emitted rows
bytes read if available
```

### 20.5 成功判据

多类型 bundle 查询的热路径应与相同 component 的当前 Direct 路径基本一致：

```text
bundle lookup overhead 应为 registry/HashMap 级固定成本，
不应引入其他 relationship type Dataset 的读取或 List 解码。
```

不对纳秒级差异设置脆弱断言。必须通过 physical plan、component metrics 和 I/O 观测确认类型
裁剪生效。

需要重点回答：

1. 3/10/50 components 的 eager open 成本是否可接受？
2. bundle load 后 resident memory 是否随 component 数量明显增长？
3. 精确类型查询是否与单 component Direct 基线等价？
4. 对象存储下 N 个 component descriptors/Dataset opens 是否成为主要延迟？
5. 何时值得实现 lazy component open？
6. 是否存在足够大的 Lance 特有收益，值得重新评估单 Dataset 复合键方案？

## 21. 分阶段实施

### P0：冻结术语和 descriptor 契约

工作：

- 确认 `bundle`、`component`、bundle generation 和 component generation 的命名；
- 定义 `MultiTypeDirectAdjacencyMetadata`；
- 定义 component descriptor reference；
- 定义 bundle JSON 格式和 format version；
- 明确相对 URI 解析和名称规范化。

完成条件：可以仅从 bundle descriptor 确定 index name、generation 和完整 component key
集合。

### P1：Bundle Store 与 Handle

工作：

- 新增 `MultiTypeDirectAdjacencyIndexHandle`；
- 新增 `PersistedMultiTypeDirectAdjacencyDescriptor`；
- 实现 bundle descriptor write/read；
- 复用 `DirectAdjacencyIndexStore::load` eager load components；
- 实现 all-or-nothing validation；
- 聚合 counts；
- descriptor-last publish。

完成条件：多个现有 Direct components 可以组成一个 bundle，跨进程关闭并重新加载。

### P2：Builder Orchestration

工作：

- 实现组合式 `add_component`；
- 生成稳定无冲突 component 路径；
- 可选实现顺序 `build_all_and_persist`；
- 限制构建并发和峰值内存；
- 对 duplicate key、generation 和 source identity 做校验。

完成条件：FRIEND_OF/FOLLOWS/BLOCKS 三类 10M-edge workload 可以生成一个已发布 bundle。

### P3：Registry 原子 bundle 注册

工作：

- Direct registry 从 key map 改为 named bundle map；
- 实现 `(index_name, GraphIndexKey)` 查找；
- 实现 bundle generation conflict 规则；
- 实现完整 handle 的原子替换；
- 删除或收敛当前单 component 公共注册入口；
- 迁移现有 registry tests。

完成条件：两个具名 bundles 可以包含同一个 `GraphIndexKey`，并被严格区分。

### P4：显式选择 API 与 Planner

工作：

- `ExpandExecutionMode::DirectAdjacency` 携带 `index_name`；
- 移除其 `Copy` 假设并迁移调用点；
- 扩展 `DirectAdjacencyReference` identity；
- planner 使用具名 bundle 查找 exact component；
- 更新 physical planner handle resolution；
- EXPLAIN 显示 bundle/component identity；
- 缺失和不兼容严格报错。

完成条件：同一个查询通过不同 `index_name` 可以选择不同 Direct component generation，且不会
发生隐式 fallback。

### P5：端到端测试迁移

工作：

- 把当前单类型 Direct tests 包装为 one-component bundle；
- 增加多类型 bundle correctness tests；
- 增加跨 label/direction/ID type cases；
- 增加 corrupt/stale/partial failure；
- 更新 execute/explain tests；
- 更新 README 使用示例。

完成条件：Join、CSR、Direct bundle 三条路径结果一致，Direct 精确选择类型的 plan 断言稳定。

### P6：Benchmark

工作：

- 在 `crates/lance-graph-benches/benches/indexed_expand/` 增加多类型 workload；
- 复用 star 的 1M source/10M edge 规模；
- 测量 1/3/10/50 components load；
- 对比单 component 与 bundle exact-type query；
- 记录 persisted bytes、open latency、RSS 和对象存储请求；
- 输出可复现参数和 Criterion 结果。

完成条件：能够判断 bundle 层是否只增加可接受的管理固定成本，以及 eager load 是否需要后续
改为 lazy。

### P7：后续决策

根据 benchmark 在以下方向中选择下一步，而不是同时实施：

1. lazy component open/cache；
2. 多类型关系模式和 bag-preserving union；
3. component reuse 与 bundle 增量 generation；
4. `edge_row_id`/关系属性支持；
5. 单 Dataset `(type, source)` 复合键实验。

## 22. 建议模块布局

在当前代码结构上进行最小扩展：

```text
crates/lance-graph/src/index/
├── metadata.rs
├── registry.rs
├── selection.rs
└── direct_adjacency/
    ├── mod.rs                    # 现有 component builder/store
    └── bundle.rs                 # bundle metadata/descriptor/builder/store
```

如果 `mod.rs` 在拆分后仍过大，再做机械重构：

```text
direct_adjacency/
├── mod.rs
├── component.rs
├── bundle.rs
├── descriptor.rs
└── tests.rs
```

不应为了本计划先进行无关的大规模模块重命名。

Planner 继续复用：

```text
crates/lance-graph/src/datafusion_planner/indexed_expand/
├── logical.rs
├── planner.rs
└── physical.rs
```

第一版不需要新增 `multi_type_direct_adjacency_expand/` 物理模块，因为单类型查询仍使用现有
`DirectAdjacencyExpandExec`。

Benchmark 建议：

```text
crates/lance-graph-benches/benches/indexed_expand/
├── star.rs
├── direct_adjacency.rs
└── multi_type_direct_adjacency.rs
```

## 23. 风险与缓解

### 23.1 component 数量导致加载放大

风险：eager load 需要读取 N 个 descriptors、打开 N 个 Datasets 和 N 个 scalar indexes。

缓解：

- 第一版明确测量 1/3/10/50 components；
- 不加载邻接 List 本体；
- bundle descriptor 提供完整 key 集合，未来可做 lazy open；
- 若远端 metadata 请求成为瓶颈，再设计 bounded concurrent open 或 lazy cache。

### 23.2 bundle 与 component generation 混淆

风险：一个 `generation` 字段同时表达映射快照和物理 component 版本，导致 stale/冲突判断
错误。

缓解：类型和 EXPLAIN 中明确使用 `bundle_generation` 与 `component_generation`，不复用含义
模糊的字段名。

### 23.3 partial bundle

风险：构建过程中部分 components 成功，调用方误把不完整索引注册。

缓解：bundle descriptor 最后写；load all-or-nothing；registry 只接受完整 bundle handle。

### 23.4 类型错误串读

风险：planner 查找只按 relationship type，忽略 label 或 direction，错误读取另一个 component。

缓解：始终使用完整 `GraphIndexKey`；测试覆盖同类型不同 labels/directions；EXPLAIN 输出完整
key。

### 23.5 同名 bundle 冲突

风险：并发发布两个相同 generation、内容不同的 bundle。

缓解：registry 严格 generation conflict；持久化目标 descriptor 已存在时返回
`AlreadyExists`；第一版不实现覆盖写。

### 23.6 相对 URI 可移动性

风险：本地相对 URI 可以随目录移动，对象存储中的拷贝、权限和跨 bucket 引用语义不同。

缓解：descriptor 明确 URI 解析规则；writer 默认同 root 相对引用；外部绝对 URI 只作为显式
高级用法，不承诺整体移动。

### 23.7 component reuse 与删除

风险：未来新 bundle generation 引用旧 component 后，清理旧 bundle 时误删仍被引用的数据。

缓解：第一版不自动递归 GC 跨 generation components；在引入 reuse 前先定义引用和保留策略。

### 23.8 过早选择单 Dataset 复合键

风险：为了减少 open 数，把所有类型放入一个 Dataset，引入复合键、共享 schema 和更新耦合，
但没有实测收益。

缓解：先测量 bundle components 的真实固定成本。只有当 Lance 特有的 Dataset open/cache 成本
足够大时，才以独立实验比较 `(type, source)` 方案。

## 24. 第一版完成标准

以下条件全部满足后，多类型 Direct Adjacency 第一版才算完成：

- [x] 一个 bundle descriptor 可以引用至少两个不同 `GraphIndexKey` components；
- [x] component 继续使用当前 `src_id + List<dst_id>` Dataset 格式；
- [x] bundle descriptor 拥有独立 format version；
- [x] bundle descriptor 最后发布；
- [x] bundle load 对所有 components 执行完整验证；
- [x] 任一 component 失败时不返回 partial handle；
- [x] registry 以具名 bundle 为原子注册/替换单位；
- [x] registry 支持 `(index_name, GraphIndexKey)` 精确查找；
- [x] 两个 bundle 可以包含相同 `GraphIndexKey`；
- [x] `ExpandExecutionMode::DirectAdjacency` 必须携带 index name；
- [x] 指定 bundle 不存在时 planning error；
- [x] 指定 bundle 缺少查询关系类型时 planning error；
- [x] Direct 失败时不回退 CSR 或 Join；
- [x] exact-type 查询继续使用现有 `DirectAdjacencyExpandExec`；
- [x] physical EXPLAIN 显示 index name、bundle generation 和完整 component identity；
- [x] 查询只读取被选 component，不读取其他关系类型邻接数据；
- [x] FRIEND_OF/FOLLOWS/BLOCKS 结果分别与 Join baseline multiset 一致；
- [x] outgoing/incoming、cross-label 和 parallel edge cases 正确；
- [x] 当前 one-type Direct tests 已迁移为 one-component bundle；
- [x] multi-type benchmark 覆盖 1M sources、10M total edges、三种关系类型；
- [x] benchmark 覆盖 1/3/10/50 components 的 load 成本；
- [x] benchmark 通过 exact component handle 和 physical plan 证明查询热路径不会打开其他类型；
- [x] README 给出 build、persist、load、register、query 的完整多类型示例。

验证命令：

```text
CARGO_BUILD_JOBS=1 cargo test -p lance-graph --tests --no-fail-fast
CARGO_BUILD_JOBS=1 cargo check --workspace --all-targets
CARGO_BUILD_JOBS=1 cargo bench -p lance-graph-benches \
  --bench multi_type_direct_adjacency --no-run
```

本机 15 GiB 内存环境下还使用正式的 1M-source/10M-edge 数据运行了短 Criterion smoke。
FRIEND_OF/FOLLOWS/BLOCKS 的 Direct exact-type 查询均完成并通过 Join/CSR 结果断言；
1/3/10/50 components 的 warm eager-load case 也全部完成。详细时延受短采样和本机缓存影响，
仅用于验证 benchmark 可运行，不作为稳定性能结论。

## 25. 后续演进方向

完成本计划并取得 benchmark 数据后，再按收益选择：

1. lazy component open 和有界 component handle cache；
2. `[:A|B]` 多类型关系模式；
3. 一个 bundle generation 复用未变化 components；
4. component reference counting 和 orphan GC；
5. delta adjacency + rollup，降低单类型更新成本；
6. `List<Struct<dst_id, edge_row_id>>` 支持关系属性访问；
7. source adjacency chunking，支持超级节点；
8. bundle catalog 和远端自动发现；
9. 单 Dataset `(relationship_type, src_id)` 复合键原型；
10. 基于 component count、frontier、degree、cache 的显式成本建议工具。

第一版应保持核心边界：

> 多类型能力属于逻辑索引、descriptor、生命周期和选择层；每个关系类型的查询热路径仍然是
> 类型专属 component 上的 `src_id → List<dst_id>` 精确邻接读取。
