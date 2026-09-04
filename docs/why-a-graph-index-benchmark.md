# 为什么 Lance 上需要专用图索引：量化方案

## 1. 要回答的问题

本研究不先假设“图索引一定更快”，而是验证一个更具体的命题：

> 当查询只从一个很小的起始集合出发时，普通关系执行仍会为每一跳扫描、构建或探测远大于
> 当前 frontier 的关系数据；专用邻接索引把工作量从“关系表规模”改为“实际访问的邻居规模”。

需要分别回答两个问题：

1. 深度增加时，重复的关系表 Join 开销怎样增长？
2. 节点出度增加时，索引能消除哪些固定开销，哪些结果展开成本无法消除？

二者不能只用一张 latency 图混在一起。深度实验要区分“重复 Join”与“结果基数增长”；
高出度实验要区分“找到邻居”与“物化大量邻居”。

## 2. 两条执行路径

以固定起点的 `d` 跳查询为例：

```cypher
MATCH (v0:Person {person_id: 0})
      -[:FRIEND_OF]->(v1:Person)
      ...
      -[:FRIEND_OF]->(vd:Person)
RETURN vd.person_id
```

普通 Join 路径的每一跳都包含关系表访问和关系 Join，随后还要连接目标节点表：

```text
small frontier
  -> relationship table scan
  -> source-to-relationship Hash Join
  -> target-node lookup/Join
  -> next frontier
```

CSR + GetV 路径为：

```text
small frontier
  -> adjacency lookup
  -> visit only matching neighbors
  -> scalar-indexed target GetV
  -> next frontier
```

索引的价值不在于消灭遍历必须产生的输出，而在于避免为找出这些输出读取和组织无关边。

## 3. 成本模型

记：

- `E`：关系表总边数；
- `N`：节点表总行数；
- `d`：查询深度；
- `F_i`：第 `i` 跳输入 frontier 的行数，`F_0` 为起始集合；
- `A_i`：第 `i` 跳实际命中的邻接项数，也是下一跳展开前的结果行数；
- `B`：平均分支因子；无去重树形扩展时，`F_i ~= B^i`；
- `L(F_i)`：索引定位 `F_i` 个 source 的成本；内存 CSR 可近似为 `O(F_i)`，磁盘邻接索引还
  包含 scalar lookup 和按需 I/O。

在当前没有关系 source lookup 的基线计划中，如果每一跳完整扫描关系表，则模型工作量为：

```text
relationship rows if full scan = d * E
join work ~= sum(i=1..d) [build_or_scan(E) + probe(F_(i-1))]
```

如果每跳还通过普通 Hash Join 物化目标节点，另有与 `d * N` 同阶的 build/scan 风险。实际
优化器可能交换 build/probe 侧、复用缓存、使用动态过滤或做谓词下推，所以必须同时保存物理
计划与运行时指标，不能把上述表达式当成所有数据库的精确时间模型。

邻接索引的查询工作量近似为：

```text
adjacency work ~= sum(i=1..d) [L(F_(i-1)) + A_i]
target GetV work ~= lookups for the produced endpoint IDs
```

因此，小 frontier 下最关键的对比是：

```text
Join:  与 d * E 强相关
Index: 与实际访问的 sum(A_i) 强相关
```

但是，当 `B` 很大或深度很深时，`sum(A_i)` 本身会指数增长。索引不能消除合法结果的枚举、
传输和物化成本；它只移除无关数据访问。这也是高出度实验预期会出现的两个区间：

1. 低到中等出度：Join 固定扫描成本占主导，索引加速明显；
2. 极高出度：两条路径都逐渐被输出规模主导，加速比收窄，但索引仍应减少无关边读取。

## 4. 实验矩阵

### 4.1 深度：隔离重复 Join

新增 Criterion benchmark：`indexed_expand_depth`。

默认参数：

| 参数 | 默认值 | 目的 |
|---|---:|---|
| source rows | 100,000 | 固定普通表规模 |
| fanout | 1 | 每个深度只返回 1 行，排除结果膨胀 |
| depth | 1, 2, 3, 4, 5 | 测量每增加一跳的边际成本 |

在 `fanout=1` 时：

```text
output rows = 1
Join relationship rows if every hop fully scans = depth * E
CSR neighbor visits = depth
```

这条曲线最直接回答“深度查询为什么需要专用索引”。如果 latency 随深度近似线性增长，
同时 Join 每跳重复访问关系表，而 CSR 只做一次邻接访问，就能把原因归到执行工作量而不是
输出行数。

### 4.2 深度 + frontier 增长

同一 benchmark 默认还运行 `fanout=4`：

```text
output rows at depth d = 4^d
neighbor visits through depth d = sum(i=1..d) 4^i
```

它用于定位从“关系表扫描主导”转向“frontier/结果主导”的拐点。不要把这条曲线单独作为
索引必要性的证据，因为它同时改变深度和输出规模。

### 4.3 高出度：固定关系表，改变命中邻居数

现有 `indexed_expand_star` 已经满足关键控制条件：

| 参数 | 值 |
|---|---:|
| source rows | 1,000,000 |
| total edges | 10,000,000，固定 |
| queried hub degree | 10 / 100 / 1,000 / 10,000 |
| compared paths | Join / CSR + GetV / Direct Adjacency + GetV / Covering Adjacency + GetV |

因为 `E` 固定而 degree 改变，这条曲线可以区分：

- Join 路径中与全表规模相关的固定成本；
- 各路径随命中邻居数增加的增量成本；
- Direct/Covering Adjacency 的按需 I/O 或覆盖读取是否优于全量 CSR 常驻内存；
- degree 很大后，输出物化是否成为共同瓶颈。

### 4.4 两个补充控制变量

仅完成上述两条曲线后，还建议补两组数据，以免结论只适用于一个固定图规模：

1. 固定 `depth=3, fanout=1`，改变 `E`：验证 Join latency 对关系表规模的敏感度，以及索引
   latency 是否基本保持不变；
2. 固定 `E` 和 degree，改变起始 source 数 `F_0`：验证索引优势随 frontier 增长在何处消失，
   为未来 cost-based planner 提供阈值。

这两个控制变量已经由 `graph_index_motivation` 实现：

```text
graph_execution_relationship_scale
graph_execution_start_frontier
```

两组端到端实验都显式报告三条路径：

```text
join_no_edge_index
join_edge_btree
csr_get_v
```

必须分开建立 Dataset，避免为了微基准创建的 edge BTree 被 Full Join 基线自动使用，导致把
“普通标量索引已经消除的扫描成本”错误归因给无索引 Join。

同时要注意：edge BTree 的“存在”不等于 Hash Join 会自动变成参数化 index lookup。当前
DataFusion 计划即使看到 `src_id` BTree，仍可能保留 `HashJoinExec + relationship scan`。
因此：

- `join_edge_btree` 测量“普通索引存在时当前通用优化器实际选择的端到端计划”；
- `edge_scalar_btree` 微基准测量“执行器显式以每跳 frontier 驱动 BTree lookup”时的能力上界。

两者之间的差距本身就是专用图物理算子/规划规则的动机：不仅需要一种数据结构，还需要让
执行计划把当前 frontier 传给邻接访问路径。

### 4.5 普通数据库标量索引基线

只比较 Full Join 与 CSR 还不足以支撑普适结论，因为普通数据库可以给 edge table 的
`src_id` 建 BTree，并通过 index lookup 查出当前 source 的 edge rows。因此增加独立的关系
访问微基准：

```text
adjacency_access_scan_vs_scalar_vs_csr
```

它在同一份 Lance edge Dataset 上比较：

1. `full_edge_scan`：每跳读取完整 `src_id + dst_id`，再匹配当前 frontier；
2. `edge_scalar_btree`：对 edge-row `src_id` BTree 做批量 `IsIn`，枚举 row IDs 后 `take_rows`；
3. `csr`：直接访问 `offsets + neighbors`。

这组实验只测关系访问，不包含 Cypher planning、目标节点 GetV 或属性物化。它回答两个不同层次
的问题：

- edge scalar index 能否消除全表扫描；
- 消除全表扫描以后，普通 edge-row BTree 的索引搜索、row-ID 枚举和离散 `take_rows`，相比
  连续邻接布局还剩多少开销。

CSR 是内存常驻的性能上界之一，并非 Direct Adjacency 的替代结论。正式分析需要把它和
Direct/Covering Adjacency 的持久化体积、冷启动、远端 I/O 与内存成本一起考虑。

### 4.6 固定总边数的高出度关系访问

`adjacency_access_high_degree` 固定 source count 和 total edge count，只改变 source `0` 的
degree，并比较 full scan、edge BTree 和 CSR。这避免“提高 degree 的同时也增大关系表”造成
混淆，并能找到普通 BTree 从选择性访问转向大量离散 edge-row 读取的拐点。

## 5. 必须采集的指标

### 5.1 主指标

每个 case 至少报告：

- latency：median、p95 或 Criterion confidence interval；
- output rows；
- speedup：`Join median / indexed median`；
- 每输出行耗时：只作为辅助指标，不能替代绝对 latency；
- physical plan：确认 Join 数、`IndexedExpandExec` 数和关系表 scan 是否符合预期。

### 5.2 工作量指标

为了使结论可以解释，建议从 DataFusion metrics 或自定义算子 metric 导出：

- relationship scan rows / bytes；
- 每个 Hash Join 的 build rows、probe rows、output rows；
- peak memory，以及 spill bytes / spill count；
- 每跳 frontier rows；
- adjacency sources looked up；
- adjacency entries returned；
- target IDs requested、unique target IDs、GetV batches；
- Direct/Covering Adjacency 的 dataset rows/bytes read、scalar-index lookup 数和 cache hit。

当前深度 benchmark 会打印一个 Join 全扫描模型值和一个可静态确定的 CSR 访问计数，作为实现
运行时 metrics 前的对照：

```text
join_relationship_rows_if_full_scan = E * depth
indexed_neighbor_visits = sum(A_i)
```

运行时 metrics 更有说服力，因为它能反映过滤下推、分区、缓存和物理 Join 侧选择。

### 5.3 冷热状态

查询结果至少分为：

- warm process + warm OS page cache：代表服务化重复查询；
- fresh process + local cold page cache：代表本地冷启动；
- remote object store cold/warm：若目标场景包含 S3/GCS/Azure，必须单列，不能用本地
  `posix_fadvise` 结果代替。

索引构建、持久化和加载成本单独报告，不计入 steady-state query latency；同时通过摊销模型
回答索引何时回本：

```text
break_even_queries = (build_time + maintenance_cost) /
                     (join_query_time - indexed_query_time)
```

## 6. 运行方法

快速正确性与计划检查：

```bash
LANCE_GRAPH_DEPTH_SOURCES=1000 \
LANCE_GRAPH_DEPTH_FANOUTS=1,2 \
LANCE_GRAPH_DEPTHS=1,2,3 \
cargo bench -p lance-graph-benches --bench indexed_expand_depth -- \
  --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10
```

正式深度实验：

```bash
cargo bench -p lance-graph-benches --bench indexed_expand_depth
```

固定起点高出度实验：

```bash
cargo bench -p lance-graph-benches --bench indexed_expand_star
```

完整动机矩阵：

```bash
cargo bench -p lance-graph-benches --bench graph_index_motivation
```

缩小矩阵做正确性检查：

```bash
LANCE_GRAPH_SCALE_SOURCES=1000,2000 \
LANCE_GRAPH_FRONTIER_SOURCES=2000 \
LANCE_GRAPH_FRONTIER_SIZES=1,10 \
LANCE_GRAPH_ACCESS_SOURCES=2000 \
LANCE_GRAPH_ACCESS_FRONTIERS=1,10 \
cargo bench -p lance-graph-benches --bench graph_index_motivation -- \
  --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10
```

为了得到论文或设计评审可用的数据，应固定机器、CPU governor、并发度和编译提交；每组实验
至少独立运行三轮，并保存 Criterion 原始结果、物理计划、Lance/DataFusion 版本与图参数。

## 7. 如何判定“需要专用索引”

不要预先规定必须达到某个加速倍数。以下证据组合成立时，设计专用索引就有充分依据：

1. `fanout=1` 且输出恒为一行时，Join latency 的每跳增量明显大于索引路径，并与重复关系表
   scan/Hash Join 指标一致；
2. 固定 query degree、增大 `E` 时，Join 成本随 `E` 增长，而索引成本主要由实际邻居数决定；
3. 高出度曲线显示索引在输出主导前消除了固定扫描成本，并明确呈现输出主导后的极限；
4. peak memory、spill 或远端读取字节数证明 Join 路径的资源放大不仅是平均 latency 问题；
5. 把 build/maintenance 纳入摊销后，目标查询频率下仍能在可接受次数内回本。

反过来，如果关系表很小、绝大多数查询都接近全图扫描、图更新极频繁且查询很少，普通 Join
可能已经足够。专用索引应由 workload 的选择性、深度、出度分布、存储介质和查询/更新比决定，
而不是由“数据叫做图”决定。

## 8. 当前基准能支持与不能支持的结论

当前实现可以支持：

- 同一个 Lance/DataFusion 执行栈内 Join 与索引路径的端到端对比；
- 深度增加时的重复 Join 成本曲线；
- 固定总边数下的高出度曲线；
- 正确性和物理计划形状验证。

当前实现还不能单独证明：

- 相对 PostgreSQL、DuckDB、Spark 或专用图数据库的跨系统优势；
- 任意对象存储环境的冷启动行为；
- 有环图上的去重、simple-path 语义或最短路性能；
- 高更新率 workload 下索引维护的总拥有成本。

这些是后续实验，不应混入第一阶段“Lance 普通表执行为什么需要邻接访问路径”的核心论证。
