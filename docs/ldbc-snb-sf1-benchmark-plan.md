# LDBC SNB SF1 图索引 Benchmark 实施计划

## 1. 目标

在 `lance-graph-index` 内新增一套基于 LDBC SNB Interactive SF1 数据分布的单系统 benchmark，
在相同 Lance 节点表、关系表、查询和 seed 上比较以下四种关系展开路径：

1. `Join`：现有关系表扫描与 Hash Join；
2. `CSR`：内存 CSR `IndexedExpandExec`；
3. `Direct Adjacency`：Lance 邻接 Dataset + `src_id` BTree；
4. `Covering Adjacency`：索引页直接覆盖邻接 posting。

本 benchmark 用于补充现有可控合成实验。合成实验继续解释 depth、degree、frontier 和关系表规模
等单一变量；LDBC 实验验证这些结论在长尾 degree 和真实相关性的生成数据上是否仍成立。

## 2. 第一阶段范围

第一阶段只处理：

```text
Person
Person_KNOWS_Person
```

暂不处理完整 LDBC schema、update stream、关系属性过滤、远端对象存储和跨系统对比。

选择这个范围的原因：

- 四种展开路径都已经支持同 label 的固定深度关系展开；
- SF1 的 Person/KNOWS 足以提供长尾 degree、重复路径和多跳 frontier；
- 可以在不修改生产查询引擎的前提下完成第一版；
- 避免多 label dense-ID 空间、同名关系、多目标类型和关系属性同时进入实验。

## 3. 数据语义与约束

### 3.1 Dense ID

当前 CSR 使用 `offsets[vertex_id]`，因此 benchmark 不直接使用 LDBC 原始 Person ID。预处理阶段
建立稳定映射：

```text
original_person_id -> person_id: Int64 in [0, person_count)
```

Join、CSR、Direct 和 Covering 全部使用同一 dense ID，原始 ID 保存在节点属性中用于追溯。

### 3.2 KNOWS 方向

LDBC `KNOWS` 是无向关系。第一阶段把每条逻辑边物化为两条有向关系：

```text
A KNOWS B -> A -> B, B -> A
```

四种路径统一执行 outgoing 查询。manifest 同时记录 logical edge count 和 physical directed edge
count，空间结果同时报告每逻辑边和每物理邻接项的成本。

### 3.3 查询语义

固定多跳查询使用 exact-depth path 语义并保留重复路径；`DISTINCT` workload 单独命名。它与旧
Python benchmark 中维护 `visited` 的 BFS `1..k` 可达节点语义不同，结果和报告中不能混用。

## 4. 目录结构

新增：

```text
crates/lance-graph-benches/benches/ldbc_snb/
  README.md
  workload.rs
  common/
    mod.rs
    config.rs
    dataset.rs
    fixture.rs
    indexes.rs
    queries.rs
    results.rs
    seeds.rs
    validation.rs
  scripts/
    prepare_sf1.py
    summarize.py
```

准备后的数据位于仓库外部：

```text
<LDBC_SNB_ROOT>/
  manifest.json
  seeds.json
  datasets/
    Person.lance/
    KNOWS.lance/
  indexes/
    csr/
    direct/
    covering/
```

## 5. 数据准备

`prepare_sf1.py` 负责：

1. 读取一个或多个 LDBC Person CSV；
2. 按原始 ID 排序并生成 dense ID；
3. 读取一个或多个 Person-knows-Person CSV；
4. 映射 source/target dense ID；
5. 双向物化 KNOWS，并按 `(src_id, dst_id)` 排序；
6. 写出 `Person.lance` 和 `KNOWS.lance`；
7. 统计 degree、二跳路径数和三跳路径数；
8. 按 degree 分位数生成固定 seed；
9. 写出 `manifest.json` 和 `seeds.json`。

脚本通过显式 `--person` 和 `--knows` 参数接收 glob，可适配不同 Datagen 输出分片和文件名。
第一阶段只要求 Person CSV 含 `id/firstName/lastName`，KNOWS CSV 含两个 Person ID；列名匹配
忽略大小写和下划线。

## 6. Rust Fixture 与索引

`LdbcSnbFixture` 持有：

```text
Tokio runtime
InMemoryCatalog
join SessionContext
indexed SessionContext
InMemoryGraphIndexRegistry
GraphConfig
manifest / seeds
```

节点表确保存在 `person_id` BTree。索引 setup 不进入查询计时：

- CSR：从 `KNOWS.lance` 构建、持久化并重新加载；
- Direct：构建单 component，并包装为名为 `ldbc_snb_direct` 的 bundle；
- Covering：构建单 component，并包装为名为 `ldbc_snb_covering` 的 bundle。

如果持久化 descriptor 已存在，则读取并验证 source URI/version 后加载。只加载
`LDBC_SNB_MODES` 本次选择的索引；Join-only 运行不加载任何图索引。设置
`LDBC_SNB_REBUILD_INDEXES=1` 时只显式重建本次选择的索引目录。

Join 和 indexed path 使用不同 `SessionContext`，避免自定义 planner 污染 Join baseline。

## 7. Workload

第一版实现：

| ID | 查询 | 适用 seed |
|---|---|---|
| `one_hop` | 一跳 KNOWS，返回目标 ID 与姓名 | low/medium/high/hub |
| `two_hop` | exact two-hop，保留路径重复度 | low/medium/high/hub，受输出阈值约束 |
| `two_hop_distinct` | two-hop 唯一 endpoint | low/medium/high/hub，受输出阈值约束 |
| `three_hop` | exact three-hop，保留路径重复度 | `three_hop_path_count` 不超过阈值 |

batch frontier 留到下一阶段；不能通过循环 N 次单点查询冒充 batch query。

默认每个非空 degree bucket 选择 5 个 seed，默认最大预估路径数为 1,000,000。环境变量允许缩小
为 smoke case 或扩大正式实验。

## 8. 正确性与计划校验

计时前，对每个 query case 执行 Join baseline 和本次选择的索引模式：

```text
sorted multiset(Join)
  == sorted multiset(CSR)
  == sorted multiset(Direct)
  == sorted multiset(Covering)
```

同时校验物理计划：

- Join 包含 `HashJoinExec`；
- CSR 包含 `IndexedExpandExec`；
- Direct 包含 `DirectAdjacencyExpandExec`；
- Covering 包含 `CoveringAdjacencyExpandExec`；
- 三种索引路径不包含 `HashJoinExec`，不扫描 `KNOWS`；
- 返回目标属性时三种索引路径包含 `LanceGetVByIdExec`。

完整 plan 保存到结果目录，方便优化器变化时分析失败原因。

## 9. 计时与结果

LDBC workload 使用自定义 `harness = false` runner，而不是为每个 seed 创建 Criterion group。
每个 seed、mode、iteration 单独写一行 `raw_results.csv`。

计时包含：

- logical/physical planning；
- 起点过滤；
- 关系展开；
- target GetV；
- DISTINCT；
- RecordBatch 完整收集。

计时不包含：

- CSV 导入；
- scalar/graph index 构建；
- descriptor 读取与索引加载；
- query 构造；
- explain；
- 正确性比较。

默认执行 2 次 warmup 和 10 次正式测量。同一进程选择多个 mode 时，正式测量按 iteration 轮换
mode 顺序，减少固定顺序造成的 warm-cache 偏差。内存受限机器应使用一个 mode、一个 workload、
一个独立进程，并在实验后合并各进程的 `raw_results.csv`。

原始结果字段至少包含：

```text
dataset, scale_factor, query_id, hop, distinct, mode,
seed_id, original_seed_id, degree_bucket, degree,
iteration, latency_ms, result_rows, success, error
```

`summarize.py` 可接受一个或多个独立结果目录，输出按 mode/query/degree bucket 分组的 count、
mean、p50、p95、p99、平均输出行数和相对 Join 的 median speedup。

## 10. 配置

第一版使用环境变量，避免增加 CLI/config 框架：

```text
LDBC_SNB_ROOT                 必填，准备后的数据根目录
LDBC_SNB_WARMUP_RUNS          默认 2
LDBC_SNB_MEASURE_RUNS         默认 10
LDBC_SNB_SEEDS_PER_BUCKET     默认 5
LDBC_SNB_MAX_PATHS            默认 1000000
LDBC_SNB_WORKLOADS            默认 one_hop,two_hop,two_hop_distinct,three_hop
LDBC_SNB_MODES                默认 join,csr,direct,covering
LDBC_SNB_REBUILD_INDEXES      默认 0
LDBC_SNB_RESULTS_DIR          默认 <root>/results
```

## 11. 实施步骤与验收

### 阶段 A：计划与数据准备

- 新增本文档和 benchmark README；
- 实现 CSV 转换、dense ID、双向 KNOWS、seed/manifest。

验收：tiny LDBC 风格 CSV 能生成两个 Lance Dataset，所有 edge ID 位于合法 dense ID 范围。

### 阶段 B：Fixture 与四路径

- 实现 Dataset 打开、节点 BTree、catalog/context；
- 实现三类索引构建/加载；
- 实现四种执行模式。

验收：tiny 数据上一跳查询的四种结果 multiset 完全一致。

### 阶段 C：Workload、验证与报告

- 实现四个 workload；
- 实现计划检查、warmup、重复测量和 CSV；
- 实现 summary。

验收：一次命令生成 `raw_results.csv`、`summary.md`、`plans/` 和 `run_manifest.json`。

### 阶段 D：工程检查

运行：

```bash
cargo fmt --all -- --check
cargo check -p lance-graph-benches --bench ldbc_snb_workload
python -m py_compile \
  crates/lance-graph-benches/benches/ldbc_snb/scripts/prepare_sf1.py \
  crates/lance-graph-benches/benches/ldbc_snb/scripts/summarize.py
```

再使用 tiny 数据执行端到端 smoke benchmark。

## 12. 后续阶段

第一版稳定后再增加：

- batch frontier 1/10/100/1000；
- SF10；
- Post、Comment、Forum 和多类型 bundle；
- incoming index；
- relationship properties；
- index build/load/size 独立报告；
- local cold 与远端对象存储；
- cost-based 自动选择。

## 13. 当前实施与 SF1 Smoke 状态（2026-08-17）

阶段 A 至 D 已完成。使用本机 LDBC SNB SF1 CSV 准备出 9,892 个 Person、180,623 条逻辑
KNOWS 和 361,246 条双向物理邻接记录。真实数据 smoke 严格采用“一个 mode × 一个 workload ×
一个独立进程”，共执行 16 个 benchmark 进程，产生 56 条正式测量记录和 98 份物理计划，所有
执行、完整结果 multiset 比较和计划检查均通过。

Smoke 参数为每个 degree bucket 取 1 个 seed、0 次 warmup、1 次 measurement；二跳限制为
100,000 条预估路径，三跳限制为 200,000 条预估路径。因此三跳只覆盖 low/medium case，未在
内存受限机器上执行约 89.5 万路径的 high case 和约 747.9 万路径的 hub case。这些单次数据只
用于端到端正确性和资源安全验证，不作为正式性能结论。
