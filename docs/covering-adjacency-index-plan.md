# GIN 风格 Covering Adjacency Index 研究与实施计划

## 1. 文档状态

- 状态：Implemented (v1 research prototype)
- 创建日期：2026-08-12
- 目标分支：`research/graph-index`
- 基线提交：`48a0b9f feat(graph): add multi-type direct adjacency indexes`
- 前置能力：
  - CSR `IndexedExpandExec`；
  - Row-Backed Direct Adjacency Index；
  - Multi-Type Direct Adjacency bundle；
  - `DirectAdjacencyExpandExec`；
  - `LanceGetVByIdExec`；
  - `Join / Csr / DirectAdjacency` 显式 Expand 模式；
  - immutable generation、descriptor-last publish 和 source snapshot 校验。
- 目标能力：实现一种 PostgreSQL GIN 风格的、索引自身覆盖邻接 payload 的图索引，使查询
  `source_id` 后直接从 entry leaf 或 posting pages 得到 `dst_ids`，不再访问独立 adjacency
  Dataset，也不再执行 `Dataset::take_rows`。
- 建议名称：英文统一使用 **Covering Adjacency Index**；本文讨论的第一版物理实现称为
  **GIN-Style Covering Adjacency Index**。

本文档是以下两份计划的下一阶段研究计划：

- [direct-adjacency-index-plan.md](direct-adjacency-index-plan.md)
- [multi-type-direct-adjacency-index-plan.md](multi-type-direct-adjacency-index-plan.md)

当前 Row-Backed Direct Adjacency 已验证：

```text
GraphIndexKey
  → adjacency.src_id BTree
  → adjacency row IDs
  → Dataset::take_rows
  → List<dst_id>
  → DirectAdjacencyExpandExec
  → LanceGetVByIdExec
```

本阶段要验证的路径是：

```text
GraphIndexKey
  → source_id entry tree
  → inline posting list / posting tree pages
  → dst_id chunks
  → CoveringAdjacencyExpandExec
  → LanceGetVByIdExec
```

它的核心变化不是替换 planner 中的一次 API 调用，而是将邻接访问从：

```text
secondary index lookup
→ base adjacency table row fetch
```

变成：

```text
index-only adjacency lookup
```

## 2. 研究问题

本阶段需要回答：

> 对 immutable、按 source 精确访问的图邻接数据，使用 GIN 风格的
> `source_id → inline posting list / posting tree` 索引，能否在 Lance 的列式节点表、对象存储、
> Arrow 执行和 immutable generation 模型中，比当前
> `BTree → row_id → adjacency Dataset::take_rows` 两阶段访问取得稳定、可泛化的收益？

需要分别验证以下假设：

### H1：Index-only lookup

Covering 路径应严格满足：

```text
adjacency Dataset::take_rows count = 0
```

查询 source 后直接返回邻接 payload。

### H2：减少固定访问成本

对于小 frontier、小 degree 查询，Covering 路径应减少：

- adjacency row ID materialization；
- adjacency Dataset metadata/fragment/file 定位；
- 独立 `take_rows` 调用；
- adjacency RecordBatch 中间对象构造；
- scalar-index cache 与 Dataset cache 之间的切换；
- 对象存储 range request 数量。

### H3：保持大邻接流式访问

对于 hub 或 supernode，索引不能要求：

```text
一个 source 的完整邻接列表
→ 一个 leaf tuple
→ 一个 Arrow List value
→ 一次性加载到内存
```

大邻接必须按 posting pages 分块并流式输出。

### H4：Degree-aware layout

真实图通常是 power-law 分布。索引应允许：

```text
small degree
→ inline posting list

large degree
→ posting tree / overflow posting pages
```

并验证 inline threshold、entry page size 和 posting page size 对空间、p50/p95/p99、cache 和
对象存储请求的影响。

### H5：与列式节点表协同

Covering index 只覆盖图拓扑：

```text
source semantic ID → target semantic IDs
```

target 节点属性仍保存在 Lance columnar Dataset 中，并继续通过：

```text
target semantic IDs
→ target ID BTree
→ LanceGetVByIdExec
```

完成物化。需要判断邻接侧收益在完整 GetV 路径中是否仍然可见。

## 3. 与 PostgreSQL GIN 的对应关系

PostgreSQL GIN 的核心结构是：

```text
key
→ small posting list inline in entry-tree leaf tuple
→ posting tree when the posting list is too large
```

本文采用相同的两级决策：

```text
source_id
→ InlinePosting(dst_ids)
→ PostingTreeRef when adjacency is too large
```

映射关系：

| PostgreSQL GIN | Covering Adjacency Index |
|---|---|
| entry key | `source_id` |
| posting item | `dst_id` |
| posting list | 一个 source 的邻接列表 |
| inline posting list | leaf tuple 中的邻接 payload |
| posting tree | 大邻接的 posting pages/tree |
| posting-list length | degree |
| entry tree | source directory / B+Tree |
| indexed relation row ID | target semantic node ID |

需要保留的 GIN 原则：

1. entry tree 与 posting payload 属于同一个索引 generation；
2. 小 posting list 和 key 共置；
3. posting list 过大时，entry leaf 只保存稳定 posting-tree reference；
4. posting pages 有独立校验和和统计；
5. 索引可以在不读取 base table 的情况下返回完整 posting payload；
6. 批量 key lookup 应合并 entry/posting page 访问。

不能直接照搬的 GIN 语义：

1. GIN posting 通常是 row-ID 集合；图邻接必须保留 parallel edge multiplicity；
2. 图邻接 payload 是 target semantic ID，不是 target Dataset row ID；
3. 图查询必须支持 duplicate source input replay；
4. 一个 source 的邻接需要按关系类型、label 和方向隔离；
5. supernode 邻接需要 Arrow/DataFusion 流式输出；
6. 第一版是 immutable generation，不实现 GIN pending list 和在线 page split；
7. 第一版不要求对 `dst_id` 做点查或范围查，posting tree 首要目标是顺序输出完整邻接。

参考资料：

- PostgreSQL GIN Internals：<https://www.postgresql.org/docs/current/gin.html#GIN-IMPLEMENTATION>
- Terrace：<https://doi.org/10.1145/3448016.3457313>
- Sortledton：<https://doi.org/10.14778/3514061.3514065>
- LiveGraph：<https://doi.org/10.14778/3384345.3384351>
- A+ Indexes：<https://arxiv.org/abs/2004.00130>

## 4. 现有路径与目标路径

### 4.1 Row-Backed Direct Adjacency

当前 component 包含：

```text
DirectAdjacencyIndexHandle
  ├── Lance adjacency Dataset
  │     src_id | List<dst_id>
  ├── src_id BTree ScalarIndex
  └── DirectAdjacencyMetadata
```

查询：

```text
source IDs
→ ScalarIndex::search(IsIn)
→ RowIdTreeMap
→ Dataset::take_rows
→ src_id + List<dst_id>
→ expand
```

这一结构已经实现按需读取，但仍然是：

```text
index-assisted table lookup
```

### 4.2 GIN-Style Covering Adjacency

目标 component：

```text
CoveringAdjacencyIndexHandle
  ├── cached entry-tree root/directory
  ├── entry leaf pages
  ├── posting pages/tree
  └── CoveringAdjacencyMetadata
```

查询：

```text
source IDs
→ entry-tree page lookup
→ InlinePosting / PostingTreeRef
→ posting chunks
→ expand
```

目标是完全移除邻接侧的：

```text
RowIdTreeMap
adjacency Dataset
Dataset::take_rows
adjacency Dataset version
adjacency scalar_index_name
```

target 节点侧的 `LanceGetVByIdExec` 不变。

## 5. 第一版设计结论摘要

第一版采用以下明确决策：

1. 新增独立的 `CoveringAdjacencyIndex`，不修改 Lance `ScalarIndex` 公共契约；
2. 不 fork `lance-index`，先在 lance-graph 内验证图专用 payload index；
3. 一个 component 仍对应一个完整 `GraphIndexKey`；
4. component 内的 entry key 只有 `source_id`，关系类型不重复编码在每个 posting item 中；
5. entry tree 使用 source-sorted、immutable、bulk-built 的 B+Tree/sparse-directory 结构；
6. 小邻接作为 `InlinePosting` 直接存放在 entry leaf；
7. 大邻接在 entry leaf 中保存 `PostingTreeRef`，payload 存入独立 posting pages；
8. posting tree 第一版按 `edge ordinal/chunk ordinal` 组织，不按 `dst_id` 去重或集合化；
9. posting pages 保留 parallel edges、自环和构建输入的稳定邻接顺序；
10. lookup 输出 chunked adjacency batches，不要求一个 source 对应一个巨大 Arrow List；
11. root/fence directory eager load，entry/posting pages 按需读取；
12. 批量 source lookup 先稳定去重，再按 entry page 分组；
13. 相邻 page range 允许合并，远端 page read 使用 bounded concurrency；
14. 新增独立 `CoveringAdjacencyExpandExec`，不立即重构当前 Direct 算子；
15. 新增显式 `ExpandExecutionMode::CoveringAdjacency { index_name }`；
16. 指定索引不存在、损坏、stale 或不兼容时 planning error，不回退；
17. 第一版 immutable generation，descriptor 最后发布；
18. 第一版复用当前 multi-type 思路，但使用独立 Covering bundle/registry，避免污染现有
    Row-Backed Direct benchmark；
19. 第一版不保存 edge properties，不实现 target-property covering；
20. 第一版不实现在线增删边、pending list、delta posting 或 background rollup；
21. 第一版不实现复杂成本优化器，benchmark 显式指定后端；
22. 第一版构建器要求输入按 `(source_id, stable_edge_ordinal)` 排序，外部排序作为后续独立工作。

## 6. 为什么不直接扩展 Lance ScalarIndex

当前 Lance `ScalarIndex` 的查询契约是：

```rust
async fn search(
    &self,
    query: &dyn AnyQuery,
    metrics: &dyn MetricsCollector,
) -> Result<SearchResult>;
```

`SearchResult` 只返回：

```text
Exact(RowIdTreeMap)
AtMost(RowIdTreeMap)
AtLeast(RowIdTreeMap)
```

即：

```text
predicate → matching row IDs
```

Covering adjacency 需要：

```text
source IDs → variable-length adjacency payload stream
```

若直接修改 `SearchResult`：

- 会影响所有 scalar index implementations；
- 会混淆“过滤索引”和“payload index”职责；
- `RowIdTreeMap` 的 Exact/AtMost/AtLeast 语义不适合邻接 chunk；
- 无法自然表达 supernode 的多 chunk 流；
- 会在尚未证明收益前扩大 Lance 公共 API 变更范围。

因此第一版定义图专用接口。若实验取得稳定收益，再评估抽象成 Lance 通用：

```text
CoveringIndex / PayloadIndex / IndexOnlyLookup
```

的价值。

## 7. 逻辑对象与 API

### 7.1 Query

第一版只需要精确 source lookup：

```rust
pub struct AdjacencyLookupQuery {
    pub source_ids: ArrayRef,
}
```

不复用 `SargableQuery`，因为第一版不支持 source range 和 null 查询。

### 7.2 Lookup options

```rust
pub struct AdjacencyLookupOptions {
    pub max_output_chunk_edges: usize,
}
```

默认建议：

```text
max_output_chunk_edges     = 8,192
```

第一版为保持 supernode backpressure，posting page 按 chunk 串行拉取。bounded parallel page
prefetch 和 adjacent-range coalescing 保留为 P8 后续优化，不作为已经生效的 v1 查询选项。

### 7.3 Lookup result schema

不使用一个 source 一个无限大的 `List`，而使用：

```text
source_id: ID
chunk_ordinal: UInt32
dst_ids: List<ID>
is_last: Boolean
```

示例：

```text
source_id | chunk_ordinal | dst_ids          | is_last
----------+---------------+------------------+--------
42        | 0             | [7, 19, ...]     | false
42        | 1             | [31, 88, ...]    | true
43        | 0             | [8, 11]          | true
```

对于 inline posting：

```text
chunk_ordinal = 0
is_last = true
```

### 7.4 Index trait

建议接口：

```rust
#[async_trait]
pub trait CoveringAdjacencyIndex: Send + Sync + std::fmt::Debug {
    fn metadata(&self) -> &CoveringAdjacencyMetadata;

    async fn lookup(
        &self,
        source_ids: ArrayRef,
        options: AdjacencyLookupOptions,
        metrics: Arc<dyn AdjacencyIndexMetrics>,
    ) -> Result<SendableRecordBatchStream>;
}
```

索引层负责：

- entry page 定位；
- entry/posting page 读取；
- inline/overflow 解码；
- source 到 adjacency chunks 的返回；
- page/read/cache metrics。

索引层不负责：

- 上游输入行复制；
- duplicate source input replay；
- target 变量命名；
- DataFusion output batch 拼接；
- target GetV；
- target property filter。

这些仍由物理执行算子负责。

## 8. Metadata 与句柄

### 8.1 Component metadata

```rust
pub struct CoveringAdjacencyMetadata {
    pub key: GraphIndexKey,

    pub source_id_data_type: DataType,
    pub target_id_data_type: DataType,

    pub num_sources: u64,
    pub num_edges: u64,
    pub max_degree: u64,

    pub generation: u64,
    pub format_version: u32,

    pub index_uri: String,
    pub entry_tree_uri: String,
    pub posting_tree_uri: String,

    pub entry_page_target_bytes: u64,
    pub inline_posting_threshold_bytes: u64,
    pub posting_page_target_bytes: u64,
    pub compression: CoveringAdjacencyCompression,

    pub num_entry_pages: u64,
    pub num_inline_sources: u64,
    pub num_posting_tree_sources: u64,
    pub num_posting_pages: u64,

    pub source_uri: Option<String>,
    pub source_version: Option<u64>,
}
```

第一版仍要求：

```text
source_id_data_type == target_id_data_type
```

并支持：

```text
UInt32 / UInt64 / Int32 / Int64
```

### 8.2 Handle

```rust
pub struct CoveringAdjacencyIndexHandle {
    pub index: Arc<dyn CoveringAdjacencyIndex>,
    pub metadata: CoveringAdjacencyMetadata,
}
```

### 8.3 Logical reference

```rust
pub struct CoveringAdjacencyReference {
    pub index_name: String,
    pub bundle_generation: u64,
    pub key: GraphIndexKey,
    pub component_generation: u64,
    pub format_version: u32,
}
```

physical planner 必须重新校验 reference 与 registry handle identity。

## 9. Entry Tree

### 9.1 逻辑结构

每个 component 的 entry tree 是：

```text
source_id
→ InlinePosting
→ PostingTreeRef
```

逻辑 entry：

```rust
pub struct CoveringAdjacencyEntry {
    pub source_id: ScalarValue,
    pub degree: u64,
    pub posting: PostingLocation,
}

pub enum PostingLocation {
    Inline {
        offset: u32,
        length: u32,
    },
    Tree {
        root_page_id: u64,
        first_posting_page_id: u64,
        num_posting_pages: u32,
    },
}
```

持久化格式不直接使用 Rust enum 序列化，而使用稳定 Arrow columns。

### 9.2 Entry leaf page layout

建议 Arrow-compatible leaf page：

```text
source_id: ID
degree: UInt64
posting_kind: UInt8       # 0 = inline, 1 = posting tree
inline_offset: UInt32
inline_length: UInt32
posting_root_page_id: UInt64
first_posting_page_id: UInt64
num_posting_pages: UInt32
```

page 末尾或独立 child array 保存：

```text
inline_neighbors: ID[]
```

为了避免 Arrow 每行一个小 `List` 的 offset/validity 开销，可以物理上使用：

```text
entry fixed columns
+
one flat inline_neighbors buffer
```

但公开解码结果仍转换为 `List<ID>` chunks。

### 9.3 Entry root/fence directory

第一版采用 immutable bulk-built entry tree。root directory 保存：

```text
min_source_id
max_source_id
page_id
file_offset
byte_length
num_entries
checksum
```

root/fence directory 在 load 时 eager 加载到内存。

查找：

```text
source_id
→ binary search fence directory
→ entry leaf page
→ binary search source_id within leaf
```

第一版允许这是一棵两层静态 B+Tree：

```text
in-memory root directory
→ immutable leaf pages
```

若 entry leaf 数量使 root directory 过大，再扩展多级 internal pages；不应第一版提前实现通用
任意深度 B+Tree。

### 9.4 Byte-size-aware packing

entry page 不能只按 source 数量切分，因为 inline adjacency 是可变长 payload。

构建时按编码后字节估算：

```text
current_entry_page_bytes
+ fixed_entry_bytes
+ inline_payload_bytes
<= entry_page_target_bytes
```

超过目标大小时关闭当前 page，开始新 page。

第一版建议默认：

```text
entry_page_target_bytes          = 64 KiB
inline_posting_threshold_bytes   = 8 KiB
```

这些只是 benchmark 初始参数，不是格式常量。

## 10. Inline Posting List

### 10.1 Eligibility

一个 source 可以 inline，当且仅当：

```text
encoded_neighbor_bytes <= inline_posting_threshold_bytes
```

且加入当前 entry page 后不超过 hard page limit。

### 10.2 Semantics

inline posting 必须保留：

- parallel edges；
- self-loop；
- stable neighbor order；
- degree；
- 空邻接与 missing source 的区分策略。

第一版继续采用稀疏 source layout：degree 为 0 的 source 默认不写 entry。lookup missing source
等价于零邻居。

### 10.3 Encoding

第一版使用原始定宽编码：

```text
UInt32 / UInt64 / Int32 / Int64
```

不在第一版引入 delta、varint 或 bit packing，避免把“消除第二次 table read”的收益与压缩收益
混在同一个实验中。

后续 compression 实验可以增加：

```text
DeltaBitPack
FrameOfReference
VarInt
```

## 11. Posting Tree

### 11.1 目标

Posting tree 用于不能 inline 的大邻接：

```text
entry leaf
→ PostingTreeRef
→ one or more posting pages
→ dst_id chunks
```

它解决：

- entry leaf 过大；
- leaf fan-out 下降；
- supernode 一次性内存物化；
- 大邻接污染普通 entry page cache；
- 对象存储大范围串行读取不可控。

### 11.2 Posting item semantics

与 GIN 不同，posting items 不集合化。每条边对应一个目标 ID occurrence：

```text
[7, 7, 9]
```

必须保留两个 `7`。

### 11.3 Posting page key

第一版 posting pages 按：

```text
(source_id, chunk_ordinal)
```

或在 component 内部使用：

```text
(posting_tree_id, chunk_ordinal)
```

组织，而不是按 `dst_id` 排序。

原因：第一版查询需要完整扫描 source 邻接，不需要在邻接内查某个 target；按 edge ordinal
组织可以直接保留顺序和重复度。

### 11.4 Posting leaf page layout

```text
posting_tree_id: UInt64
chunk_ordinal: UInt32
edge_start_ordinal: UInt64
neighbor_count: UInt32
neighbors: ID[]
is_last: Boolean
checksum: UInt64
```

公开读取时转换成：

```text
source_id
chunk_ordinal
List<dst_id>
is_last
```

### 11.5 Posting internal directory

对于第一版，entry leaf 已保存：

```text
first_posting_page_id
num_posting_pages
```

若 posting pages 在文件中连续，则 lookup 可以一次取得完整 extent directory，并根据 options：

- 顺序读取；
- bounded parallel range read；
- 相邻 range coalescing；
- 分块流式输出。

第一版不需要为单个 source 构建复杂的通用 B+Tree internal nodes。`PostingTreeRef` 的逻辑语义
保留，物理上先实现 immutable extent/chunk directory。只有当：

- 一个 source 拥有极多 posting pages；
- 需要邻接内 seek/range；
- 需要在线增量 page split；

时再实现多级 posting tree。

这保持了 GIN 的 inline/posting-tree决策，但避免在第一版为不需要的查询能力实现过度复杂的
平衡树。

### 11.6 Posting page size

默认建议：

```text
posting_page_target_bytes = 256 KiB
```

benchmark 至少比较：

```text
64 KiB / 256 KiB / 1 MiB
```

对象存储下 page 太小会增加请求；page 太大会增加无效读取、解码延迟和 cache 污染。

## 12. 持久化格式

### 12.1 目录结构

单 component：

```text
covering-component-generation-7/
├── descriptor.json
├── entry-directory.json
├── entry-pages.idx
├── posting-directory.json
└── posting-pages.idx
```

若 page writer 最终使用 Lance IndexStore，可以将 `.idx` 替换为其 index file 命名；descriptor
必须屏蔽底层文件名差异。

### 12.2 Descriptor

```json
{
  "format": "lance-graph-covering-adjacency",
  "format_version": 1,
  "index_kind": "gin_style_covering_adjacency",
  "generation": 7,
  "key": {
    "relationship_type": "friend_of",
    "source_label": "person",
    "target_label": "person",
    "direction": "outgoing"
  },
  "source_id_data_type": "int64",
  "target_id_data_type": "int64",
  "num_sources": 1000000,
  "num_edges": 10000000,
  "max_degree": 1000000,
  "entry_page_target_bytes": 65536,
  "inline_posting_threshold_bytes": 8192,
  "posting_page_target_bytes": 262144,
  "compression": "none",
  "num_entry_pages": 1421,
  "num_inline_sources": 995000,
  "num_posting_tree_sources": 5000,
  "num_posting_pages": 1823,
  "entry_directory_uri": "entry-directory.json",
  "entry_pages_uri": "entry-pages.idx",
  "posting_directory_uri": "posting-directory.json",
  "posting_pages_uri": "posting-pages.idx",
  "source_uri": "...",
  "source_version": 19
}
```

### 12.3 Format version

```rust
pub const COVERING_ADJACENCY_INDEX_FORMAT_VERSION: u32 = 1;
```

reader 对未知 version 必须返回 `Incompatible`，不能猜测读取。

### 12.4 Descriptor-last publish

发布顺序：

```text
1. 写 posting pages
2. 写 posting directory
3. 写 entry pages
4. 写 entry directory
5. reopen 并校验所有 page ranges/checksums/counts
6. 写 descriptor.json
```

descriptor 是索引 generation 完整可见的标志。

### 12.5 Checksums

每个 entry/posting page 至少记录：

```text
page_id
byte_length
checksum
```

load 不扫描全部 payload，但 lookup 读取 page 时必须校验 checksum。构建完成时执行一次完整
reopen validation。

## 13. 构建路径

### 13.1 输入契约

第一版 builder 接受 source-sorted edge stream：

```text
src_id
dst_id
```

要求：

```text
src_id 单调非递减
同一 src_id 内保持输入顺序
endpoint 非 null
ID 类型匹配 metadata
```

如果输入不是有序流，builder 返回明确错误。

不在第一版 builder 内实现全量外部排序，原因是：

- 研究重点是 index-only lookup；
- 外部排序是独立工程问题；
- 当前 benchmark 可以按 source 顺序生成边；
- 可以用 DataFusion sort 或预处理 pipeline 提供有序输入。

### 13.2 Streaming group

构建器只保留当前 source 的邻接：

```text
read edge stream
→ accumulate one source adjacency
→ decide inline/posting tree
→ emit entry/posting pages
→ release source buffer
```

内存上限近似为：

```text
max(
  one source adjacency chunk,
  one entry page,
  one posting page
)
```

对于 supernode，不能缓存完整邻接；当当前 source 超过 inline threshold 后，立即转换为 posting
writer，并按 posting page size 流式 flush。

### 13.3 Inline-to-posting transition

一个 source 初始进入 inline candidate buffer：

```text
while encoded_bytes <= inline_threshold:
    buffer neighbors
```

一旦超过 threshold：

```text
allocate posting_tree_id
flush buffered neighbors into first posting page
continue streaming remaining neighbors into posting pages
emit PostingTreeRef entry when source ends
```

不需要先完整知道 degree。

### 13.4 Page packing

entry builder 在 source 完成后估算 entry encoded size：

```text
if current_page + entry > target_page_bytes:
    flush current entry page
append entry to new page
```

设置 hard maximum，防止单页异常膨胀。由于 posting payload 超过 threshold 后外置，entry tuple
本身始终有界。

### 13.5 Counts

构建时统计：

- `num_sources`；
- `num_edges`；
- `max_degree`；
- degree histogram；
- inline/tree source 数；
- inline/posting payload bytes；
- entry/posting page 数；
- page fill ratio；
- build spill/peak memory。

degree histogram 建议使用对数桶：

```text
0
1
2-3
4-7
8-15
...
2^k..2^(k+1)-1
```

## 14. Load 与 Cache

### 14.1 Eager load

component load 时只 eager 加载：

- descriptor；
- entry directory/root；
- posting directory；
- page metadata/checksum map；
- source snapshot identity。

不加载 entry/posting payload pages。

### 14.2 Cache 分层

建议区分：

```text
EntryDirectoryCache
EntryPageCache
PostingPageCache
```

Entry directory 可以常驻；entry/posting page 使用独立预算。

原因是 supernode posting scan 不应淘汰所有普通 entry pages。

### 14.3 第一版 cache policy

第一版使用简单 bounded LRU：

- entry directory pinned；
- entry page LRU；
- posting page LRU；
- posting page cache 可以关闭，用于 cold/scan benchmark。

不在第一版实现 TinyLFU 或 degree-aware admission，但 metrics 必须记录 degree 与 cache behavior，
为后续研究提供依据。

### 14.4 后续 degree-aware admission

后续可研究：

```text
small/medium posting page
→ admit to cache

supernode sequential posting pages
→ scan-resistant / no-admit
```

## 15. 批量 Lookup 算法

输入 batch：

```text
[42, 100, 42, 9000, 100, null]
```

步骤：

```text
1. 跳过 null
2. stable deduplicate
   → [42, 100, 9000]

3. entry directory lookup
   42,100 → entry page 3
   9000   → entry page 17

4. group by entry page
   page 3  → [42,100]
   page 17 → [9000]

5. 每个 entry page 只读/解码一次

6. inline entries 直接产生 chunks

7. posting-tree entries 收集 posting page ranges

8. coalesce adjacent posting ranges

9. bounded parallel page reads

10. 按 source_id/chunk_ordinal 输出 adjacency chunks
```

索引 lookup 可以对 source 去重；执行算子必须保留原 input-row positions，用于回放。

### 15.1 Missing source

entry tree 中不存在 source：

```text
lookup result 中不输出该 source
```

Expand 语义为零邻居，不是错误。

### 15.2 Page coalescing

相邻 ranges：

```text
[offset=0, len=64KiB]
[offset=64KiB, len=64KiB]
```

可以合并为一次：

```text
[offset=0, len=128KiB]
```

需要设置最大 coalesced range，避免把大量未命中 page 一并读取。

### 15.3 Backpressure

lookup 返回 stream；posting page reader 不能无限预取。bounded concurrency 与 DataFusion 下游
消费共同提供 backpressure。

## 16. Multi-Type Bundle 与 Registry

### 16.1 第一版使用独立 bundle

为了保持研究对比清晰，第一版新增：

```text
MultiTypeCoveringAdjacencyIndexHandle
MultiTypeCoveringAdjacencyIndexBuilder
MultiTypeCoveringAdjacencyIndexStore
```

而不是立即把当前 `MultiTypeDirectAdjacencyIndexHandle` 改成多种 component kind 的 enum。

理由：

- Row-Backed Direct 是稳定 baseline；
- Covering 格式和生命周期仍处于实验阶段；
- 独立 registry 可以避免同名 bundle 的 component kind 歧义；
- benchmark 可以强制选择后端；
- 若 Covering 证明有效，再统一 bundle abstraction。

### 16.2 Named bundle lookup

```text
(index_name, GraphIndexKey)
→ exact covering component
```

不同 bundle 可以包含相同 `GraphIndexKey`。

### 16.3 Generation

沿用：

```text
bundle_generation
component_generation
```

registry 原子替换完整 bundle handle。

### 16.4 Registration rules

与当前 Direct bundle 一致：

- new name：注册；
- higher generation：原子替换；
- equal generation + equal identity：幂等；
- equal generation + different identity：conflict；
- older generation：conflict。

## 17. 显式选择 API

新增：

```rust
pub enum ExpandExecutionMode {
    Join,
    Csr,
    DirectAdjacency { index_name: String },
    CoveringAdjacency { index_name: String },
}
```

便利函数：

```rust
ExpandExecutionMode::covering_adjacency("social_covering")?
```

行为：

| Mode | 指定索引可用 | 指定索引不可用/损坏/stale |
|---|---|---|
| `Join` | Join | Join |
| `Csr` | CSR | Planning error |
| `DirectAdjacency(name)` | Row-Backed Direct | Planning error |
| `CoveringAdjacency(name)` | Covering | Planning error |

不自动在 Direct 与 Covering 之间选择，不 fallback。

## 18. Planner

### 18.1 Eligibility

第一版 Covering 继承当前 Direct eligibility：

- exactly one relationship type；
- outgoing 或 incoming；
- 完整 source/target label；
- 无 relationship variable；
- 无 relationship property filter/return；
- target variable 未复用；
- source/target ID 为支持的整数类型；
- source/target ID 类型相同；
- exact `GraphIndexKey` component 存在。

undirected 和 `[:A|B]` 仍不支持。

### 18.2 Logical reference

planner：

```text
query pattern
→ GraphIndexKey
→ (index_name, GraphIndexKey) registry lookup
→ CoveringAdjacencyReference
→ AdjacencyExpandNode
```

可以复用当前 `AdjacencyExpandNode`，扩展 `ExpandIndexReference`：

```rust
pub enum ExpandIndexReference {
    Csr(IndexReference),
    DirectAdjacency(DirectAdjacencyReference),
    CoveringAdjacency(CoveringAdjacencyReference),
}
```

### 18.3 Physical revalidation

physical planner 必须再次验证：

- bundle name；
- bundle generation；
-完整 `GraphIndexKey`；
- component generation；
- format version；
- source/target ID types；
- source snapshot；
- root/directory identity。

失败时不能使用另一个 handle。

## 19. CoveringAdjacencyExpandExec

### 19.1 输入输出

输入：

```text
上游 RecordBatch
+ source semantic ID column
```

输出：

```text
复制的上游 columns
+ target semantic ID endpoint column
```

与当前 `DirectAdjacencyExpandExec` schema 兼容，因此 target 路径继续使用
`LanceGetVByIdExec`。

### 19.2 执行步骤

```text
1. 读取 input batch source IDs
2. 记录每个 source 对应的 input row positions
3. stable deduplicate source IDs
4. CoveringAdjacencyIndex::lookup(unique sources)
5. 接收 source/chunk/dst_ids stream
6. 对该 source 的每个 input row position 回放 chunk 中所有 edge occurrences
7. 按 max_output_batch_rows 输出 RecordBatch
8. 将 target semantic IDs 交给 GetV
```

### 19.3 Supernode streaming

对于：

```text
source 42 degree = 10,000,000
```

算子不能建立：

```text
HashMap<42, ArrayRef of all 10M neighbors>
```

而应逐 chunk：

```text
read posting page chunk
→ replay against all input positions of source 42
→ emit output batches
→ release chunk
```

### 19.4 Duplicate input source

输入：

```text
42 row A
42 row B
```

邻接：

```text
42 → [7, 7, 9]
```

输出必须为六行：

```text
A→7, A→7, A→9,
B→7, B→7, B→9
```

索引只读一次 source 42；算子负责回放。

### 19.5 Metrics

算子级：

- `input_batches`；
- `input_rows`；
- `source_lookup_keys`；
- `unique_source_lookup_keys`；
- `duplicate_source_lookup_keys`；
- `missing_sources`；
- `adjacency_chunks`；
- `neighbors_decoded`；
- `output_batches`；
- `output_rows`；
- `lookup_time`；
- `replay_time`。

索引级：

- `entry_directory_lookups`；
- `entry_pages_requested/read/cache_hit`；
- `inline_posting_hits`；
- `posting_tree_hits`；
- `posting_pages_requested/read/cache_hit`；
- `range_requests`；
- `coalesced_range_requests`；
- `entry_bytes_read`；
- `posting_bytes_read`；
- `checksums_verified`；
- `max_inflight_reads`。

## 20. Physical EXPLAIN

示例：

```text
CoveringAdjacencyExpandExec:
  index_name=social_covering,
  bundle_generation=3,
  relationship_type=friend_of,
  source_label=person,
  target_label=person,
  direction=Outgoing,
  component_generation=7,
  format=gin_style_covering_adjacency,
  entry_page_bytes=65536,
  inline_threshold_bytes=8192,
  posting_page_bytes=262144
```

完整计划：

```text
ProjectionExec(a.name, b.name)
└── FilterExec(b.age > 30)
    └── LanceGetVByIdExec(Person.person_id AS b)
        └── CoveringAdjacencyExpandExec
            │ index_name = social_covering
            │ component = FRIEND_OF(Person → Person, Outgoing)
            │ lookup = index_only
            └── LanceScanExec(Person AS a, name = "Alice")
```

必须不存在：

```text
relationship TableScan
source-relationship HashJoinExec
adjacency Dataset::take_rows
```

## 21. Snapshot 与更新语义

### 21.1 Immutable generation

第一版：

```text
relationship snapshot N
→ covering component generation G
→ immutable files
```

更新：

```text
relationship snapshot N+1
→ build generation G+1
→ validate
→ publish descriptor
→ atomically replace bundle
```

### 21.2 Source validation

沿用：

```rust
IndexSourceValidation::AllowUnknown
IndexSourceValidation::RequireExact(GraphSourceIdentity)
```

stale 时显式 Covering mode 返回错误。

### 21.3 不保存 target row ID

posting items 保存 target semantic ID，不保存 target Lance row ID。

原因：

- target row ID 与 Dataset version 绑定；
- compaction/rewrite 后可能变化；
- 会让 topology index 强耦合 target 物理布局；
- target semantic ID 更适合跨 generation；
- 现有 `LanceGetVByIdExec` 已负责 semantic ID → row ID。

### 21.4 后续 delta posting

后续可借鉴 GIN/Dgraph：

```text
immutable base posting
+ delta postings
→ rollup
```

但第一版明确不实现，因为 parallel-edge 删除、snapshot、posting-tree merge 和 cache invalidation
需要单独设计。

## 22. 错误与一致性

至少覆盖：

- descriptor missing/corrupt；
- format/version 不兼容；
- empty index；
- duplicate source entry；
- source IDs 非单调输入；
- endpoint null/type mismatch；
- entry directory range overlap/gap；
- entry page checksum mismatch；
- entry source 不在 page fence 范围；
- inline offset/length 越界；
- degree 与 inline length 不一致；
- posting tree ref 越界；
- posting page checksum mismatch；
- posting chunk ordinal 不连续；
- posting `is_last` 缺失或重复；
- posting neighbor count 与 degree 不一致；
- aggregate source/edge/page counts 不一致；
- source snapshot stale；
- registry generation conflict；
- planning/physical planning 之间 handle 替换；
- page read I/O failure；
- unsupported ID type；
- batch size/concurrency 为零。

索引损坏必须返回明确错误，不能把损坏 source 当作 degree 0。

## 23. 测试计划

### 23.1 Page codec 单元测试

覆盖：

- 四种整数 ID；
- empty page；
- one source；
- multiple sources；
- inline offsets；
- fence min/max；
- page size boundary；
- checksum round trip；
- corrupt offsets/checksum；
- stable source sort；
- parallel edges/self-loop。

### 23.2 Inline/posting transition

构造 threshold 附近邻接：

```text
threshold - 1 byte → inline
threshold          → inline
threshold + 1 byte → posting tree
```

并验证结果一致。

### 23.3 Posting tree 测试

- one posting page；
- multiple posting pages；
- supernode；
- chunk ordinal；
- `is_last`；
- page checksum；
- truncated tree；
- page count/degree mismatch；
- bounded memory；
- stream backpressure。

### 23.4 Builder/store/load

- sorted input build；
- unsorted input rejection；
- descriptor-last；
- drop runtime objects 后 reload；
- local relative URI；
- absolute URI；
- source identity exact/stale；
- counts/histogram；
- incomplete generation 不可 load。

### 23.5 Registry

- named bundle isolation；
- exact `(index_name, GraphIndexKey)`；
- equal generation idempotence；
- equal generation conflict；
- old generation rejection；
- new generation atomic replacement；
- old Arc remains usable。

### 23.6 Planner

- correct component；
- missing bundle/component；
- Direct 与 Covering 不混淆；
- no fallback；
- outgoing/incoming；
- cross-label；
- unsupported relationship property/variable；
- physical generation revalidation；
- EXPLAIN identity。

### 23.7 Physical correctness

与 Join、CSR、Row-Backed Direct 做 multiset 对比，覆盖：

- missing source；
- degree 1；
- duplicate input source；
- parallel edges；
- self-loop；
- null source input；
- multiple input batches；
- inline source；
- posting-tree source；
- one source crossing many posting pages；
- output batch boundary；
- incoming；
- Person → Company；
- FRIEND_OF/FOLLOWS/BLOCKS exact type。

### 23.8 Index-only assertion

测试必须通过 metrics 或 mock page store 证明：

```text
adjacency_dataset_take_rows = 0
```

而不是仅通过 EXPLAIN 字符串间接推断。

## 24. Benchmark 计划

### 24.1 对比后端

```text
Join
CSR
Row-Backed Direct
GIN-Style Covering
```

### 24.2 分层 benchmark

#### 纯邻接 lookup

```text
source IDs → dst_id chunks
```

排除 target GetV，直接测量索引结构。

#### Expand-only

包含：

- source input batch；
- stable deduplicate；
- input-row replay；
- List/chunk decode；
- output batching。

不包含 target GetV。

#### End-to-end

```text
source scan/filter
→ adjacency expand
→ target indexed GetV
→ target predicate
→ projection
```

### 24.3 数据分布

#### Uniform

```text
1M sources
10M edges
degree = 10
```

重点测 inline posting。

#### Star/Hub

```text
1M sources
fixed total edges where applicable
query one hub
degree = 10 / 100 / 1K / 10K / 100K / 1M
```

重点测 inline-to-posting transition 和 supernode streaming。

#### Power-law/Zipf

```text
1M sources
10M/100M edges
Zipf degree distribution
```

重点测真实 inline/tree 比例、cache 和 p99。

#### Sparse source domain

```text
1M possible sources
100K sources with edges
```

重点测 missing source 和 directory size。

#### Multi-source frontier

```text
frontier = 1 / 16 / 256 / 4,096
```

重点测 page grouping、range coalescing 和并发读取。

#### Multi-type

沿用：

```text
FRIEND_OF 7M
FOLLOWS   2M
BLOCKS    1M
```

验证 exact component 裁剪。

### 24.4 参数实验

第一轮固定：

```text
entry page          = 64 KiB
inline threshold    = 8 KiB
posting page        = 256 KiB
compression         = none
```

确认架构有效后，再分别改变单个维度：

```text
entry page:
4 / 16 / 64 / 256 KiB

inline threshold:
256 B / 1 / 4 / 8 / 16 KiB

posting page:
64 / 256 KiB / 1 MiB

compression:
none / delta-bitpack
```

不第一轮执行所有参数笛卡尔积。

### 24.5 Cache 场景

```text
warm directory + warm pages
warm directory + cold pages
cold component open
repeated hot source
Zipf repeated sources
supernode scan with posting cache disabled/enabled
```

### 24.6 Storage 场景

第一版最低要求：

- local filesystem；
- instrumented object-store reader，记录 range request/bytes。

若环境允许，再增加：

- S3-compatible/MinIO；
- warm local cache；
- cold remote cache。

### 24.7 Metrics

构建：

- build wall time；
- peak RSS；
- input edges/s；
- total persisted bytes；
- bytes/edge；
- entry/posting bytes；
- inline/tree source ratio；
- entry/posting page fill ratio；
- directory memory；
- max buffered source bytes。

查询：

- p50/p95/p99；
- neighbors/s；
- entry/posting page reads；
- cache hit ratio；
- range request count；
- bytes read；
- over-read bytes；
- page decode time；
- replay time；
- output rows；
- target GetV time；
- end-to-end time。

### 24.8 Benchmark 正确性

每个正式 case 在测量前必须：

- 比较 Join/CSR/Direct/Covering 的 sorted multiset；
- 断言 expected degree；
- 断言正确 relationship type；
- 断言 `CoveringAdjacencyExpandExec`；
- 断言没有 relationship scan/HashJoin；
- 断言 adjacency `take_rows = 0`；
- 断言 target `LanceGetVByIdExec` 仍存在（端到端 indexed case）。

## 25. 成功判据

### 25.1 正确性

- 与 Join/CSR/Row-Backed Direct multiset 一致；
- parallel edge、self-loop、duplicate source 和 chunked supernode 正确；
- multi-type/direction/label 不串读；
- snapshot/generation 正确。

### 25.2 Index-only

- 邻接 lookup 不打开 adjacency Dataset；
- 邻接 lookup 不返回 adjacency row IDs；
- 邻接 lookup 不调用 `Dataset::take_rows`；
- inline source 只需 entry page 即得到完整邻接。

### 25.3 性能

不设置脆弱的固定百分比硬断言，但研究结论至少应满足之一：

1. cold local 小 frontier 明显减少 latency；
2. remote/object-store range request 数明显减少；
3. warm lookup 不显著劣于 Row-Backed Direct；
4. power-law 下普通 source p99 不被 hub posting scan 明显污染；
5. supernode 能流式输出且内存与 degree 解耦；
6. entry directory 和 page cache 成本可控。

若只在极窄 warm-cache case 获得纳秒级收益，而 persisted size、构建复杂度和 p99 明显恶化，
应判定第一版研究假设不成立，而不是继续扩展功能。

### 25.4 可解释性

必须能回答性能差异来自：

- 少了一次 `take_rows`；
- 少了多少 range requests；
- 少读了多少 bytes；
- inline hit ratio；
- posting page 数；
- page cache hit；
- target GetV 是否掩盖邻接收益。

## 26. 非目标

第一版不包含：

- 修改 Lance `ScalarIndex` / `SearchResult`；
- 通用 payload-index API upstream；
- 在线 B+Tree page split/merge；
- GIN pending list；
- delta posting、删除 posting、rollup；
- background compaction；
- relationship properties；
- target properties 内嵌；
- `List<Struct<dst_id, edge_id, properties>>`；
- edge row ID；
- target Lance row ID；
- `[:A|B]` multi-type union；
- undirected query；
- variable-length traversal；
- BFS/shortest path；
- 邻接内 `dst_id` membership/range query；
- 自动 CSR/Direct/Covering/Join 成本选择；
- 自动 fallback；
- Python API；
- 分布式 build；
- builder 内置通用 external sort；
- 所有 compression codecs；
- 复杂 cache admission policy。

## 27. 建议模块布局

```text
crates/lance-graph/src/index/
├── metadata.rs
├── registry.rs
├── selection.rs
├── direct_adjacency/
└── covering_adjacency/
    ├── mod.rs
    ├── metadata.rs
    ├── descriptor.rs
    ├── builder.rs
    ├── entry_tree.rs
    ├── posting_tree.rs
    ├── page_codec.rs
    ├── store.rs
    ├── cache.rs
    ├── metrics.rs
    └── bundle.rs
```

物理执行：

```text
crates/lance-graph/src/datafusion_planner/indexed_expand/
├── logical.rs
├── planner.rs
├── physical.rs
└── covering_physical.rs
```

若 `covering_physical.rs` 很小，可暂时放入 `physical.rs`；但 page lookup 不应放入物理算子
文件，应留在 index module。

Benchmark：

```text
crates/lance-graph-benches/benches/indexed_expand/
├── star.rs
├── direct_adjacency.rs
├── multi_type_direct_adjacency.rs
└── covering_adjacency.rs
```

## 28. 分阶段实施

### P0：冻结研究契约（已完成）

工作：

- 冻结 `CoveringAdjacencyIndex` lookup contract；
- 冻结 chunked result schema；
- 冻结 GIN inline/posting-tree 术语；
- 冻结第一版 page/descriptor format；
- 确认不修改 Lance ScalarIndex；
- 定义 benchmark hypotheses 和 baseline。

完成条件：无需实现 planner，即可从文档判断何为 index-only adjacency lookup。

### P1：Page codec 与静态 entry directory（已完成）

工作：

- entry page 自描述二进制 codec；
- source fence directory；
- byte-size-aware page packing；
- checksums；
- local object-store range read；
- exact source lookup。

完成条件：source 可以从 entry page 读取 inline posting，且不依赖 adjacency Dataset。

### P2：Posting tree/pages（已完成）

工作：

- inline threshold；
- inline-to-posting transition；
- posting page writer/reader；
- chunk ordinal/degree validation；
- supernode streaming；
- bounded memory。

完成条件：一个 1M-degree source 可以跨多个 posting pages 流式返回。

### P3：Builder、Store 与 Handle（已完成）

工作：

- sorted edge stream builder；
- descriptor-last publish；
- source identity；
- reopen validation；
- page/count statistics；
- immutable handle。

完成条件：构建后 drop runtime objects，仍能从 descriptor 重新加载并查询。

### P4：Named Multi-Type Covering Bundle（已完成）

工作：

- bundle metadata/descriptor；
- exact `(index_name, GraphIndexKey)`；
- all-or-nothing load；
- generation conflict；
- atomic replacement。

完成条件：FRIEND_OF/FOLLOWS/BLOCKS 同一 bundle 精确选择且不串读。

### P5：Planner 与 Physical Exec（已完成）

工作：

- `ExpandExecutionMode::CoveringAdjacency`；
- logical reference；
- physical revalidation；
- `CoveringAdjacencyExpandExec`；
- chunk replay；
- metrics/EXPLAIN；
- target GetV 接入。

完成条件：端到端计划没有 relationship scan、source join 和 adjacency take_rows。

### P6：Correctness 与故障测试（已完成）

工作：

- inline/tree/source edge cases；
- corrupt/truncated pages；
- snapshot/generation；
- parallel/self-loop/duplicate input；
- incoming/cross-label/multi-type；
- 与三条 baseline multiset 对比。

完成条件：所有第一版完成标准的正确性项有自动测试。

### P7：分层 Benchmark（部分完成）

工作：

- pure lookup；
- expand-only；
- end-to-end；
- uniform/star/power-law/sparse；
- frontier scale；
- warm/cold/cache；
- local/instrumented object store；
- page parameter experiments。

完成条件：能够解释 Covering 相比 Row-Backed Direct 的 latency、requests、bytes 和内存差异。

### P8：研究结论与下一步决策（已形成首轮结论）

根据 benchmark，只选择一个方向继续：

1. 邻居压缩；
2. object-store page prefetch/coalescing；
3. degree-aware cache admission；
4. delta posting/rollup；
5. predicate/edge-property covering；
6. Lance 通用 PayloadIndex 提案；
7. 若收益不足，停止扩展并保留实验结果。

首轮实现结果（2026-08-12）：

- uniform degree=10、默认 inline threshold 下，pure lookup 在 frontier 1/16/256/4096 时约为
  `1.3 µs / 14.1 µs / 222 µs / 4.09 ms`；
- degree=10,000 hub 走 posting pages，pure lookup 约 `56.8 µs`，expand-only 约
  `885 µs`；
- sparse frontier=4096 返回 41 条边，pure lookup 约 `632 µs`，一次 entry range read，后续
  4095 次 entry cache hit；
- power-law frontier=256 返回 48,296 条边，pure lookup 约 `619 µs`，共 26 次 range read
  （7 entry + 19 posting），读取约 450 KiB entry 和 363 KiB posting；
- 1M source、10M edge、hub degree=10,000 的端到端 GetV：Join 约 `262 ms`，Covering
  约 `58.2 ms`，CSR 约 `63.8 ms`，Row-Backed Direct 约 `57.6 ms`；
- Covering 已消除 adjacency Dataset 和 `take_rows`，但端到端与 Row-Backed Direct 的差异被
  target `LanceGetVByIdExec` 主导成本明显压缩。

因此下一步单一研究方向选择：

> 先实现/验证 posting page prefetch 与 range coalescing，并增加 GetV 分段 metrics；暂不进入
> 在线更新、delta posting 或 edge-property covering。

## 29. 风险与缓解

### 29.1 只是把 adjacency Dataset 重新发明成 page file

风险：Covering index 可能只是另一种手写 Dataset，复杂度增加但收益很小。

缓解：

- pure lookup benchmark 隔离 `take_rows` 成本；
- metrics 比较 range requests/bytes；
- page 直接返回 adjacency payload；
- 明确停止条件。

### 29.2 Entry page fan-out 降低

风险：inline payload 使 entry page 过大、source fan-out 过低。

缓解：

- byte-size-aware packing；
- inline threshold；
- hard page size；
- page fill/fan-out metrics。

### 29.3 Hub 污染 cache

风险：supernode posting pages 淘汰普通 entry pages。

缓解：

- entry/posting cache 分离；
- posting cache 可关闭；
- 后续 degree-aware admission。

### 29.4 对象存储请求放大

风险：posting page 太小导致大量 range requests。

缓解：

- 256 KiB 初始 posting page；
- range coalescing；
- bounded parallel reads；
- request metrics。

### 29.5 Posting tree 名称与物理 extent 不一致

风险：第一版实际上是 immutable extent directory，不是完整可更新 B+Tree。

缓解：

- logical contract 使用 `PostingTreeRef`；
- 文档明确第一版物理实现；
- 不宣称支持在线 tree update；
- 后续有实际需求再实现多级 tree。

### 29.6 Bag semantics 被 posting 编码集合化

风险：借鉴 GIN/Lucene 时误去重 dst IDs。

缓解：

- posting item 定义为 edge occurrence；
- parallel edge 自动测试；
- counts 用 edge count，不用 unique neighbor count。

### 29.7 Source input replay 内存

风险：duplicate source 或大量 input positions 导致 replay map 过大。

缓解：

- 每个 DataFusion input batch 独立处理；
- positions 使用 compact UInt32；
- output 分批；
- 不缓存完整 supernode adjacency。

### 29.8 Builder 输入排序限制

风险：研究原型只能接收 sorted edges。

缓解：

- 明确第一版契约；
- benchmark 生成有序数据；
- 后续接 DataFusion external sort；
- 不用内存 HashMap 假装可扩展构建。

### 29.9 Target GetV 掩盖收益

风险：end-to-end 中 target fetch 占主导，Covering 看起来没有收益。

缓解：

- pure lookup；
- expand-only；
- GetV metrics 分解；
- low/high degree 分层。

### 29.10 与现有 Direct API 过早统一

风险：实验格式污染稳定 baseline。

缓解：

- 独立 mode、bundle、handle、exec；
- 成功后再统一 abstraction。

## 30. 第一版完成标准

以下条件全部满足后，GIN-Style Covering Adjacency 第一版才算完成：

- [x] 新增独立 `CoveringAdjacencyIndex`，不修改 Lance `ScalarIndex`；
- [x] 一个 component 对应一个完整 `GraphIndexKey`；
- [x] source-sorted edge stream 可跨 RecordBatch 流式构建索引；
- [x] entry directory/root 可以定位 exact source leaf page；
- [x] 小邻接直接 inline 在 entry leaf；
- [x] 大邻接使用 `PostingTreeRef` 和 posting pages；
- [x] entry page 按编码字节而不是固定 source 数切分；
- [x] posting page 支持 supernode chunked streaming；
- [x] parallel edges 和 stable order 保留；
- [x] missing source 等价于 degree 0；
- [x] duplicate input source 正确回放；
- [x] page 构建内存不与总边数线性增长；
- [x] descriptor-last publish；
- [x] format version、checksums、aggregate counts 完整校验；
- [x] drop runtime objects 后可以重新 load；
- [x] source snapshot stale 被拒绝；
- [x] named multi-type Covering bundle all-or-nothing load；
- [x] registry 支持 `(index_name, GraphIndexKey)` exact lookup；
- [x] bundle generation 原子替换；
- [x] 新增显式 `ExpandExecutionMode::CoveringAdjacency`；
- [x] 缺失/损坏 Covering index planning/runtime error，不 fallback；
- [x] 新增 `CoveringAdjacencyExpandExec`；
- [x] physical planner revalidate generation/identity；
- [x] physical EXPLAIN 显示 GIN-style layout identity；
- [x] adjacency lookup 不产生 adjacency row IDs；
- [x] adjacency lookup 不调用 `Dataset::take_rows`；
- [x] target 节点继续使用 `LanceGetVByIdExec`；
- [x] 与 Join/CSR/Row-Backed Direct multiset 一致；
- [x] inline/tree/incoming/cross-label/multi-type 自动测试通过；
- [x] benchmark 覆盖 pure lookup、expand-only、end-to-end；
- [x] benchmark 覆盖 power-law 和 sparse 数据分布；
- [x] benchmark 覆盖 uniform、star/hub 和 frontier 1/16/256/4096；
- [x] runtime metrics 可报告 page reads、bytes、cache hit 和 checksum；
- [x] benchmark 通过 Covering metrics snapshot 自动输出 range requests、bytes、cache、page、
  inline/posting hit、checksum 和 adjacency `take_rows=0`；
- [x] 形成首轮研究结论：继续 page prefetch/coalescing 与 GetV 分解，不进入在线更新。

## 31. 后续演进方向

第一版完成后，可能的方向包括：

1. `dst_id` delta/bit-pack compression；
2. degree-aware entry/posting cache admission；
3. posting page prefetch 和 object-store range coalescing；
4. source directory 多级 B+Tree；
5. 真正可更新 posting tree；
6. immutable base + delta posting + rollup；
7. `List<Struct<dst_id, edge_id, properties>>`；
8. A+ Indexes 风格 predicate-covering adjacency；
9. target property row-ID intersection；
10. 多类型关系 union；
11. 多跳 frontier-native covering lookup；
12. Lance 通用 `PayloadIndex`/`CoveringIndex` API 提案。

第一版应保持研究变量清晰：

> 只验证 GIN 风格 inline/posting-tree 邻接 payload 是否能通过 index-only lookup 消除当前
> Row-Backed Direct 的第二次 adjacency table read，并在 Lance/Arrow/对象存储环境下带来可解释的
> 性能收益。
