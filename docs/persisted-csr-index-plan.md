# Lance Graph 持久化 CSR 索引实施计划

## 1. 文档状态

- 状态：Implemented for v1 local/default-object-store workflow
- 基线提交：`0d903d8 feat(graph): add CSR-backed indexed expand`
- 目标阶段：内存 CSR IndexedExpand 之后的下一阶段
- 主要模块：`lance-graph::csr_index`、`lance-graph::index`、查询入口和 benchmark

本文档定义如何把当前进程内 CSR 索引扩展为可持久化、可重新加载、可验证来源版本、可在进程重启后继续用于 `IndexedExpand` 的索引。

当前实现采用：

- `offsets.lance`、`neighbors.lance` 和最后发布的 `manifest.json`；
- manifest format version `1`；
- neighbors `position` 列；
- Lance 自身校验加完整结构验证，v1 不额外计算 component 内容哈希；
- 调用方提供不可变 generation URI；
- 异步 eager load，加载后继续使用内存 `CsrIndex`；
- source URI/version 由调用方选择是否精确校验。

自定义 object-store storage options、远程故障注入、自动 generation 分配和旧 generation GC 仍属于后续 hardening，不改变 v1 磁盘格式。

## 2. 背景和当前基线

当前版本已经完成以下闭环：

```text
relationship RecordBatch
  → CsrIndexBuilder::try_build()
  → Arc<CsrIndex>
  → InMemoryGraphIndexRegistry
  → IndexedExpand logical node
  → GraphQueryPlanner
  → IndexedExpandExec
  → DataFusion query result
```

当前实现具备：

- CSR offsets 和 neighbors 内存结构；
- CSR 输入 ID、null、负数、越界和溢出校验；
- `GraphIndexKey`、`GraphIndexMetadata` 和 generation；
- `InMemoryGraphIndexRegistry`；
- `Disabled`、`Prefer`、`Require` 使用策略；
- 单跳 outgoing `IndexedExpand`；
- Join 与 CSR indexed 查询结果和 benchmark 对比。

当前实现不具备：

- 将 CSR 数据写入 Lance dataset；
- 持久化索引 manifest；
- 从 Lance dataset 重建 `CsrIndex`；
- 进程重启后的索引发现和恢复；
- source dataset version 的完整 stale 检查；
- 不完整写入、格式损坏和并发发布保护；
- 持久化索引的冷加载 benchmark。

`CsrIndex::to_record_batch()` 和 `neighbors_to_record_batch()` 只提供 Arrow 导出能力，不构成持久化闭环。

## 3. 目标

完成后，调用方应能执行以下流程：

```text
阶段 A：构建和发布

relationship Lance dataset @ version N
  → build outgoing CSR
  → write immutable persisted generation
  → write manifest as commit marker
  → return PersistedCsrIndexDescriptor

阶段 B：进程重启后加载和查询

PersistedCsrIndexDescriptor
  → validate manifest and source identity
  → load offsets and neighbors
  → validated CsrIndex reconstruction
  → register in memory registry
  → existing IndexedExpand query API
```

必须满足以下结果：

1. 加载路径不读取 relationship table，也不调用 `CsrIndexBuilder`；
2. 进程退出并清空所有内存对象后，仍能从持久化 URI 恢复索引；
3. 恢复后的索引与构建前索引具有相同的顶点数、边数、neighbor 顺序和重复边语义；
4. `IndexUsagePolicy::Require` 能明确区分 missing、corrupt、stale 和 incompatible index；
5. `Prefer` 在索引不可用时安全回退 Join；
6. query steady-state 仍使用当前内存 `IndexedExpandExec`，持久化不改变算子语义；
7. index 写入、冷加载和查询分别计时，避免把加载成本混入 steady-state query benchmark。

## 4. 非目标

第一版持久化不包含：

- 每次邻接查询直接随机访问磁盘上的 CSR；
- mmap 或零拷贝 neighbors；
- 增量更新或增量合并；
- edge payload 和 relationship property；
- incoming CSR、undirected expand 和 variable-length expand；
- 稀疏或字符串节点 ID 的 `VertexIdMap`；
- 分布式构建；
- 自动垃圾回收旧 generation；
- 多进程共享内存 cache；
- 加密、访问控制和跨账户凭据管理。

第一版定义的是“持久化于存储、加载后驻留内存”的 CSR：

```text
storage at rest: Lance datasets
query execution: Arc<CsrIndex> in memory
```

## 5. 设计原则

### 5.1 保持查询执行路径不变

持久化只负责产生一个经过验证的 `Arc<CsrIndex>`。加载完成后继续复用：

- `GraphIndexRegistry`；
- `IndexReference`；
- `GraphQueryPlanner`；
- `IndexedExpandExec`。

不在第一版同时引入新的 disk-backed physical operator。

### 5.2 索引 generation 不可变

一个已经发布的 generation 不允许原地覆盖。重建索引必须产生新 generation：

```text
generation 7: immutable
generation 8: immutable
```

registry 可以切换到新 generation，但磁盘内容不能被就地修改。

### 5.3 Manifest 最后写入

offsets 和 neighbors 成功写入并完成校验后，最后写 manifest。读取方只把存在完整 manifest 的 generation 视为已发布。

```text
write offsets
  → write neighbors
  → validate both datasets
  → write manifest last
```

如果进程在 manifest 写入前失败，该目录属于未发布 generation，不允许被 loader 使用。

### 5.4 所有反序列化必须走 validated constructor

不允许 loader 直接填充 `CsrIndex` 私有字段。新增统一的 validated constructor，内存构建和持久化加载共享相同的不变量检查。

### 5.5 来源版本和索引存储位置分离

当前 `source_uri` 表示 relationship 数据源，不表示索引自身位置。持久化描述符必须单独记录 `index_uri`，不能复用 `source_uri`。

## 6. 持久化目录布局

推荐第一版采用两个 Lance component dataset 加一个 manifest：

```text
<index_uri>/
  manifest.json
  offsets.lance/
  neighbors.lance/
```

其中 `<index_uri>` 必须指向唯一且不可变的 generation，例如：

```text
<index_root>/
  friend_of-person-person-outgoing/
    generation-0000000000000007/
      manifest.json
      offsets.lance/
      neighbors.lance/
```

第一版 API 接受完整 generation URI，不负责维护 `latest` 指针。这样可以避免在本地文件系统和 object store 上实现不同的原子 rename 语义。

### 6.1 `offsets.lance` schema

推荐 schema：

```text
vertex_id: UInt64, non-null
offset:    UInt64, non-null
degree:    UInt64, non-null
```

行数必须等于 `num_vertices`。第 `v` 行描述：

```text
offsets[v] = offset
offsets[v + 1] = offset + degree
```

最后一行必须满足：

```text
last.offset + last.degree == num_edges
```

loader 重建完整 `num_vertices + 1` 长度的 offsets，并把最后一个 sentinel 设置为 `num_edges`。

### 6.2 `neighbors.lance` schema

推荐 schema：

```text
position: UInt64, non-null
dst_id:   UInt64, non-null
```

`position` 必须是连续的：

```text
0, 1, 2, ..., num_edges - 1
```

显式保存 position 的原因是避免把 fragment scan 顺序隐式当成格式契约。第一版 loader 可以按 `position` 排序或验证输入已经有序。

如果后续确认 Lance 对不可变单次写入 dataset 提供足够强的稳定行序保证，可以在 format v2 中评估移除 position，但不能无版本地改变 v1 schema。

### 6.3 为什么不使用一个 List 列

备选格式是：

```text
vertex_id: UInt64
neighbors: LargeList<UInt64>
```

它更接近 adjacency list，但第一版不选择它，原因是：

- 当前 `CsrIndex` 已经有 offsets 和 flat neighbors 导出语义；
- 两 component 格式更容易逐项验证 CSR invariants；
- flat neighbors 更适合后续 mmap、range read 或分块加载；
- 超高 degree 节点不会形成单个超大 nested value；
- offsets 和 neighbors 可独立选择 Lance 编码和 row-group 大小。

## 7. Manifest 格式

Manifest 使用有显式版本的 JSON。第一版建议结构：

```json
{
  "format": "lance-graph-csr",
  "format_version": 1,
  "state": "complete",
  "key": {
    "relationship_type": "friend_of",
    "source_label": "person",
    "target_label": "person",
    "direction": "outgoing"
  },
  "source_id_field": "person_id",
  "target_id_field": "person_id",
  "id_data_type": "int64",
  "num_vertices": 100000,
  "num_edges": 1000000,
  "source": {
    "uri": "/datasets/friend_of.lance",
    "version": 12
  },
  "generation": 7,
  "components": {
    "offsets": {
      "path": "offsets.lance",
      "dataset_version": 1,
      "rows": 100000
    },
    "neighbors": {
      "path": "neighbors.lance",
      "dataset_version": 1,
      "rows": 1000000
    }
  }
}
```

### 7.1 稳定枚举编码

Manifest 不直接序列化 Arrow `DataType` 的 Debug 文本。定义稳定的 persisted enum：

```rust
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedVertexIdType {
    UInt32,
    UInt64,
    Int32,
    Int64,
}
```

加载时显式转换到 Arrow `DataType`。未知值必须返回 incompatible format 错误。

方向同样使用稳定字符串：

```text
outgoing
incoming
```

### 7.2 Manifest 不记录绝对 component URI

component 使用相对 path，使整个 immutable generation 目录可以整体复制。loader 必须拒绝：

- `..` 路径；
- 绝对 component path；
- 越过 generation root 的路径。

### 7.3 校验和

第一版先依赖 Lance dataset 自身的存储校验和和严格结构验证。Manifest 可预留可选字段：

```json
"checksum": {
  "algorithm": "blake3",
  "value": "..."
}
```

是否在 v1 强制 component 级内容哈希，应在 P0 spike 中根据写入开销确定。即使不强制 checksum，行数、offsets invariants、neighbor 范围和 dataset version 仍必须验证。

## 8. 核心 Rust 类型调整

### 8.1 `CsrIndex` validated constructor

新增：

```rust
impl CsrIndex {
    pub fn try_from_parts(
        offsets: Vec<u64>,
        neighbors: Vec<u64>,
        num_vertices: u64,
    ) -> Result<Self>;
}
```

必须检查：

- `offsets.len() == num_vertices + 1`；
- `offsets[0] == 0`；
- offsets 单调不减；
- `offsets.last() == neighbors.len()`；
- 所有 neighbor `< num_vertices`；
- `num_vertices` 和长度转换不溢出。

`CsrIndexBuilder::try_build()` 最终也应调用或复用相同验证逻辑，避免构建路径和加载路径产生两套不变量。

### 8.2 持久化描述符

新增独立类型，不把 index URI 混入 source metadata：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedCsrIndexDescriptor {
    pub index_uri: String,
    pub format_version: u32,
    pub metadata: PersistedGraphIndexMetadata,
}
```

运行时仍使用：

```rust
pub struct CsrIndexHandle {
    pub index: Arc<CsrIndex>,
    pub metadata: GraphIndexMetadata,
}
```

descriptor 表示 storage reference，handle 表示已加载运行时对象。

### 8.3 存储 API

推荐新增模块：

```text
crates/lance-graph/src/index/persistence/
  mod.rs
  manifest.rs
  writer.rs
  loader.rs
```

第一版 API：

```rust
pub struct CsrIndexStore;

impl CsrIndexStore {
    pub async fn write(
        index_uri: &str,
        handle: &CsrIndexHandle,
        options: CsrIndexWriteOptions,
    ) -> Result<PersistedCsrIndexDescriptor>;

    pub async fn load(
        descriptor: &PersistedCsrIndexDescriptor,
        validation: IndexSourceValidation,
    ) -> Result<CsrIndexHandle>;

    pub async fn read_descriptor(
        index_uri: &str,
    ) -> Result<PersistedCsrIndexDescriptor>;
}
```

如果本地文件系统和 object store 的打开方式无法通过一个静态类型干净复用，再引入 trait：

```rust
#[async_trait]
pub trait GraphIndexStore: Send + Sync {
    async fn write_csr(...) -> Result<PersistedCsrIndexDescriptor>;
    async fn load_csr(...) -> Result<CsrIndexHandle>;
}
```

不要在需求确认前为单一 Lance backend 建立过度抽象。

## 9. Writer 流程

### 9.1 输入

Writer 接收：

- 唯一 generation URI；
- 已验证的 `CsrIndexHandle`；
- Lance `WriteParams` 或受控的 write options；
- 可选 storage options；
- 是否执行写后校验。

### 9.2 写入步骤

```text
1. 验证 URI 尚不存在或没有 complete manifest
2. 从 CsrIndex 生成 offsets batch stream
3. WriteMode::Create 写 offsets.lance
4. 从 CsrIndex 生成 neighbors batch stream
5. WriteMode::Create 写 neighbors.lance
6. 重新打开两个 dataset，检查 schema、version 和行数
7. 生成 manifest
8. 最后写 manifest.json
9. 返回 descriptor
```

禁止使用 `WriteMode::Overwrite` 覆盖已发布 generation。

### 9.3 分块导出

当前 `neighbors_to_record_batch()` 会 clone 完整 neighbors。百万级或更大图会形成额外峰值内存。

持久化实现应新增分块迭代接口，例如：

```rust
pub fn neighbor_batches(
    &self,
    batch_size: usize,
) -> impl Iterator<Item = Result<RecordBatch>> + '_;
```

offsets 同样分块输出。验收时需要记录峰值内存，确保 writer 不再复制整份 CSR。

### 9.4 失败清理

Writer 失败时可以留下未发布 component，但不能留下 `state=complete` manifest。

第一版不要求同步删除所有残留对象；提供显式 cleanup 工具或记录待清理 generation。读取方必须忽略没有完整 manifest 的目录。

## 10. Loader 流程

### 10.1 加载步骤

```text
1. 读取 manifest
2. 检查 format 和 format_version
3. 检查 state == complete
4. 安全解析 component relative path
5. 检查 GraphIndexKey 和 ID type
6. 按策略校验 source URI/version
7. 打开 offsets.lance 和 neighbors.lance
8. 检查 Lance schema、dataset version 和行数
9. 读取并重建 offsets Vec<u64>
10. 读取并重建 neighbors Vec<u64>
11. 调用 CsrIndex::try_from_parts()
12. 检查 metadata dimensions
13. 构造 CsrIndexHandle
```

### 10.2 内存上限

Loader 最终需要分配：

```text
(num_vertices + 1) × 8 bytes
+ num_edges × 8 bytes
```

加载前根据 manifest 做 checked arithmetic，并支持可选限制：

```rust
pub struct CsrIndexLoadOptions {
    pub max_vertices: Option<u64>,
    pub max_edges: Option<u64>,
    pub max_memory_bytes: Option<u64>,
}
```

超过限制必须在读取大组件前失败。

### 10.3 不信任 manifest

Manifest 中的 counts 只用于预检查，不是可信事实。最终必须以 component 实际数据和 `try_from_parts()` 验证结果为准。

## 11. Registry 集成

### 11.1 第一版采用显式 eager load

当前 `GraphIndexRegistry::get_csr()` 是同步 API，并且 physical planning 也是同步解析 handle。不能在 `get_csr()` 内安全加入异步存储 IO。

因此第一版采用：

```rust
let handle = CsrIndexStore::load(&descriptor, validation).await?;
let registry = Arc::new(InMemoryGraphIndexRegistry::new());
registry.register_loaded_csr(handle)?;

query.execute_with_catalog_context_and_indexes(
    catalog,
    context,
    registry,
    IndexUsagePolicy::Require,
).await?;
```

加载发生在查询规划前，现有 registry trait 保持同步。

### 11.2 严格注册已加载 generation

当前 `register_csr()` 在同 key 的 generation 不递增时会自动修改 generation。持久化加载不能静默修改 manifest generation，否则磁盘 descriptor、logical `IndexReference` 和运行时 handle 会不一致。

新增严格 API：

```rust
pub fn register_loaded_csr(&self, handle: CsrIndexHandle) -> Result<()>;
```

规则：

- 相同 key、相同 generation、相同 source identity：允许幂等注册；
- 相同 key、更高 generation：替换；
- 相同 key、更低 generation：返回错误或忽略，不能改写 generation；
- 相同 key、相同 generation、不同 metadata：返回冲突错误。

后续可以增加批量加载：

```rust
pub async fn load_persisted_indexes(
    descriptors: impl IntoIterator<Item = PersistedCsrIndexDescriptor>,
    validation: IndexSourceValidation,
) -> Result<Arc<InMemoryGraphIndexRegistry>>;
```

### 11.3 Lazy load 作为后续工作

如果未来需要第一次查询时 lazy load，应增加异步 query preparation 阶段，而不是在 DataFusion physical planner 中阻塞 async runtime。

## 12. Source version 和 stale 语义

### 12.1 Source identity

定义：

```rust
pub struct GraphSourceIdentity {
    pub uri: String,
    pub version: Option<u64>,
}
```

持久化索引至少记录 relationship source identity。第一版要求构建 Lance relationship dataset 时记录精确 version。

### 12.2 加载校验策略

```rust
pub enum IndexSourceValidation {
    RequireExact(GraphSourceIdentity),
    AllowUnknown,
}
```

语义：

- `RequireExact`：URI 或 version 不同立即返回 `StaleIndex`；
- `AllowUnknown`：只检查索引内部完整性，用于纯内存数据、测试或调用方无法提供版本的场景；
- 不提供 `IgnoreMismatch`，避免生产路径显式接受已知 stale index。

### 12.3 与使用策略组合

推荐调用流程：

```text
Prefer:
  load success and fresh → register → IndexedExpand
  missing/stale/incompatible → do not register → Join fallback

Require:
  load success and fresh → register → IndexedExpand
  missing/stale/incompatible → return explicit error before planning
```

需要扩展 `IndexFallbackReason` 和错误类型，至少包含：

- `PersistedIndexMissing`；
- `PersistedIndexIncomplete`；
- `PersistedIndexCorrupt`；
- `UnsupportedIndexFormatVersion`；
- `StaleSourceVersion`；
- `IndexGenerationConflict`；
- `IndexMemoryLimitExceeded`。

## 13. 发布和重建生命周期

第一版显式 API：

```text
build generation G
  → write immutable G
  → load and validate G
  → application swaps registry entry to G
```

旧 generation 在仍有 query 持有 `Arc<CsrIndexHandle>` 时继续可用。registry 替换只影响后续 plan。

不要在本阶段自动删除旧 generation。删除策略必须等到：

- 没有 active reader；
- 新 generation 已发布且可加载；
- retention policy 明确；
- object store delete 的失败恢复明确。

## 14. 错误和可观察性

错误消息必须携带：

- index URI；
- GraphIndexKey；
- generation；
- component 名称；
- manifest expected/actual value；
- source expected/actual version；
- 原始 Lance/Arrow error source。

至少记录以下指标：

```text
csr_index_write_duration
csr_index_write_bytes
csr_index_load_duration
csr_index_load_bytes
csr_index_num_vertices
csr_index_num_edges
csr_index_generation
csr_index_load_failures_by_reason
```

查询 operator 指标继续由 `IndexedExpandExec` 负责，不把持久化加载指标混入 execution metrics。

## 15. 测试计划

### 15.1 `CsrIndex::try_from_parts()` 单元测试

覆盖：

- empty graph；
- isolated vertices；
- parallel edges；
- self-loop；
- 高 degree；
- offsets 长度错误；
- offsets 首项不是 0；
- offsets 非单调；
- terminal offset 不等于 neighbors 长度；
- neighbor 越界；
- usize/u64 转换溢出。

### 15.2 Manifest 单元测试

覆盖：

- v1 round trip；
- key lowercase normalization；
- 四种整数 ID type；
- unknown format；
- unknown format version；
- unknown ID type；
- 非 complete state；
- component path traversal；
- 缺少必填字段；
- counts overflow。

### 15.3 本地 Lance round-trip 测试

使用 `tempfile`：

```text
build CSR
  → write generation URI
  → drop builder/index/registry
  → load only from URI
  → compare metadata, offsets-derived degrees and every neighbor slice
```

测试必须显式证明 load 阶段没有调用 `CsrIndexBuilder`。

### 15.4 损坏数据测试

覆盖：

- 只有 offsets、没有 neighbors；
- 两个 component 存在但没有 manifest；
- manifest 行数与 dataset 不同；
- offsets schema 错误；
- neighbors schema 错误；
- position 不连续；
- offsets 非单调；
- truncated neighbors；
- neighbor ID 越界；
- manifest key 与调用方 expected key 不同；
- source version stale。

### 15.5 跨 registry 生命周期 E2E

测试分成两个明确 scope：

```text
scope A:
  build and persist
  drop all Arc<CsrIndex> and registry

scope B:
  read descriptor
  load persisted CSR
  create new registry and SessionContext
  execute IndexUsagePolicy::Require query
```

断言：

- `EXPLAIN` 或 plan display 包含 IndexedExpand；
- Join 与 persisted indexed 结果 multiset 相同；
- registry generation 与 manifest 相同；
- stale source 在 `Require` 下失败；
- stale source 在 `Prefer` 下回退 Join。

### 15.6 Object store 集成测试

在本地 round-trip 稳定后，再增加受环境控制的 S3-compatible/object store 测试。CI 没有凭据时跳过，不能让基础本地持久化测试依赖网络。

## 16. Benchmark 计划

在 `lance-graph-benches/benches/indexed_expand/` 增加：

```text
persisted_index.rs
```

分三个独立 group。

### 16.1 写入成本

```text
persisted_csr_write
  sources_1k_degree_10
  sources_10k_degree_10
  sources_100k_degree_10
```

测量：

- CSR 已构建后的持久化时间；
- 输出 offsets/neighbors/manifest 总字节；
- 峰值内存；
- 不混入 relationship scan 和 CSR build。

CSR build 继续由现有 `graph_index_build` 测量。

### 16.2 冷加载成本

```text
persisted_csr_load_cold
```

每次 measurement 必须使用新 registry。Linux 本地 cold case 可以参考 disk benchmark 使用 `posix_fadvise`，但需要明确它只表示本地 page-cache cold，不代表所有 object-store cold start。

同时提供 warm load/cache case，但不能把两者混在一个统计组。

### 16.3 查询成本

比较：

```text
join
in_memory_indexed
persisted_loaded_indexed
```

`persisted_loaded_indexed` 在计时开始前完成 load，验证加载后的 steady-state 查询与现有 in-memory indexed 路径没有显著结构性回退。

正式结果至少报告：

- 1k/10k/100k source；
- 固定 degree 和固定 selective hub；
- relationship 总边数；
- index 文件大小；
- cold load time；
- warm query time。

## 17. 实施里程碑

### P0：格式 spike 和决策冻结

工作：

- 用当前 Lance 版本验证 offsets/neighbors schema 写入和扫描；
- 验证 local path 与 object-store URI 的统一打开方式；
- 验证 manifest 写入所需的 storage API；
- 测量 position 列的体积成本；
- 决定 v1 是否强制 component checksum。

完成条件：

- 一个小图可以手工写出两个 Lance dataset 并读回；
- format v1 schema 和 manifest 字段冻结；
- 未解决的 Lance API 问题有明确结论，不把未知假设带入正式实现。

### P1：Validated reconstruction 和 manifest

工作：

- `CsrIndex::try_from_parts()`；
- 让 builder 复用统一验证；
- persisted enums；
- manifest serde 和 validation；
- descriptor 类型；
- 专用错误原因。

完成条件：

- 所有 malformed parts 和 manifest 单元测试通过；
- 现有内存 CSR 和 IndexedExpand 测试不变且通过。

### P2：分块 writer

工作：

- offsets/neighbor batch iterators；
- Lance component writer；
- manifest-last publish；
- write-back validation；
- incomplete generation 行为。

完成条件：

- 百万边索引可以在受控峰值内存下写入；
- 写入失败不会产生可被 loader 接受的 complete index；
- 已发布 generation 不能被 overwrite。

### P3：Validated loader

工作：

- descriptor/manifest reader；
- component schema 和行数验证；
- 分块读取和 Vec 重建；
- memory limit；
- `try_from_parts()`；
- source identity 验证。

完成条件：

- tempdir round-trip 通过；
- corruption matrix 通过；
- 加载阶段不访问 relationship source。

### P4：Registry 和查询集成

工作：

- `register_loaded_csr()` 严格 generation 语义；
- 批量 eager load helper；
- `Prefer`/`Require` 的 missing/stale/incompatible 行为；
- persisted-index E2E query test。

完成条件：

- 新进程语义下加载后可执行 IndexedExpand；
- `Require` 不会 fallback；
- `Prefer` 在不可用时正确回退；
- Join 与 indexed result multiset 一致。

### P5：Lifecycle 和 benchmark

工作：

- immutable generation publish API；
- registry generation swap；
- metrics；
- write/load/query benchmark；
- README 和 API 文档。

完成条件：

- 新 generation 可安全替换旧 generation；
- benchmark 分离 build、write、load 和 query；
- 文档包含 local path 的完整示例。

### P6：Object store hardening

工作：

- S3-compatible integration；
- storage options 传递；
- timeout/retry 分类；
- interrupted multipart/write 场景；
- orphan generation 管理方案。

完成条件：

- object store 上 manifest-last 可见性语义经过测试；
- 重试不会覆盖 complete generation；
- 无网络和无凭据错误可诊断。

## 18. 推荐 PR 拆分

建议按以下顺序提交：

1. `feat(graph-index): add validated CSR reconstruction and manifest types`
2. `feat(graph-index): persist CSR components to Lance datasets`
3. `feat(graph-index): load and validate persisted CSR indexes`
4. `feat(graph-index): register persisted generations for indexed expand`
5. `test(graph-index): cover persisted CSR corruption and stale versions`
6. `bench(graph-index): measure CSR write, load, and steady-state query`
7. `feat(graph-index): harden persisted CSR for object stores`

每个 PR 必须保持现有 Join 和内存 indexed 路径可用，避免一个超大提交同时修改存储格式、planner 和物理执行。

## 19. 验收标准

### 19.1 功能

- 支持 outgoing CSR v1 写入和加载；
- 进程内对象全部销毁后，可仅凭 persisted descriptor 恢复索引；
- empty graph、isolated node、parallel edge 和 self-loop round-trip 不丢失语义；
- 四种受支持的源 ID 类型 metadata round-trip 正确；
- 加载后的 `CsrIndex::num_vertices()`、`num_edges()` 和每个 neighbor slice 与原索引一致；
- 持久化索引可执行当前单跳 selective `IndexedExpand`；
- Join 与 persisted indexed 查询结果 multiset 一致。

### 19.2 正确性和失败语义

- 不完整 generation 不可见；
- unsupported format version 明确失败；
- schema/count/offset/neighbor 损坏明确失败；
- source version mismatch 明确标记 stale；
- generation 冲突不会被静默改写；
- `Require` 对 unavailable persisted index 返回明确错误；
- `Prefer` 安全回退 Join。

### 19.3 性能和资源

- CSR build、persist write、cold load 和 steady-state query 分别统计；
- steady-state query 不包含存储 IO；
- writer 使用分块 batch，不复制完整 neighbors 数组；
- loader 在读取前执行 checked memory estimate；
- 100,000 sources、1,000,000 edges 的现有 star workload 可完成 write、drop、reload 和 query；
- persisted-loaded steady-state query 使用与 in-memory indexed 相同的 physical operator。

### 19.4 兼容性

- Manifest 必须包含 `format_version`；
- v1 loader 拒绝未知 major format；
- 当前内存 registry 和 query API 继续工作；
- 没有 persisted index 的用户不需要修改现有调用路径；
- local filesystem 测试不依赖网络服务。

## 20. 开放问题

P0 必须回答：

1. Manifest 使用 Lance object-store abstraction 写 JSON，还是使用一个单行 `manifest.lance` dataset？
2. v1 是否强制 component 内容 checksum，还是先依赖 Lance 校验和加结构验证？
3. neighbors 的 `position` 列对空间和读取速度的实际影响是多少？
4. source URI 的 canonicalization 规则是什么，尤其是本地相对路径和 object-store URI？
5. 当前 catalog 是否能提供 relationship dataset version，还是第一版要求调用方显式传入？
6. storage options 如何安全传递且不写入 manifest？
7. generation 由调用方分配、catalog 分配，还是 store 使用冲突安全的唯一 ID？
8. 加载多个大型 CSR 时，registry 的总内存预算由谁管理？

在这些问题冻结前，不开始对外承诺持久化格式兼容性。

## 21. 最终完成定义

持久化 CSR 只有在下面这条测试链完整通过后才算完成：

```text
write relationship Lance dataset at version N
  → build CSR
  → persist immutable CSR generation G
  → destroy builder, CsrIndex, registry, and SessionContext
  → create a new registry and SessionContext
  → load G only from its persisted URI
  → validate source version N
  → execute the query with IndexUsagePolicy::Require
  → observe IndexedExpand in the plan
  → compare the full result multiset with Join
  → verify no relationship scan or CSR rebuild occurred during load/query
```

只实现 Arrow 导出、只把 URI 写入 metadata、或者在测试中继续持有原 `Arc<CsrIndex>`，都不满足该完成定义。
