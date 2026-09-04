# 图索引动机实验：阶段性结果

## 1. 实验范围

本结果用于回答 Lance/DataFusion 当前执行栈中的两个问题：

1. 固定小起点时，深度增加为何放大通用 Join 成本；
2. 即使普通 edge table 已有 `src_id` BTree，专用邻接访问路径还解决了什么问题。

所有数字来自同一台开发机的 Criterion quick run。参数为 warm-up `0.05–0.1s`、measurement
`0.1–0.2s`、sample size `10`。这些结果适合验证趋势和设计动机，不应作为发布版性能承诺。

## 2. 深度：输出恒为一行

图包含 100,000 个节点和 100,000 条边，fanout 固定为 1。无论深度如何，查询只返回一行，
因此排除了结果基数增长。

| depth | Join median | CSR + GetV median | speedup | Join 全扫描模型 edge rows | CSR neighbor visits |
|---:|---:|---:|---:|---:|---:|
| 1 | 133.90 ms | 28.54 ms | 4.69x | 100,000 | 1 |
| 2 | 234.79 ms | 31.39 ms | 7.48x | 200,000 | 2 |
| 3 | 332.52 ms | 32.33 ms | 10.29x | 300,000 | 3 |
| 4 | 432.67 ms | 36.97 ms | 11.70x | 400,000 | 4 |

Join 每增加一跳约增加 100 ms，而 CSR + GetV 每跳只增加数毫秒。输出始终是一行，所以差异
来自重复关系执行、计划/算子和目标访问，不是结果爆炸。

## 3. 关系表规模：固定 depth=3、fanout=1、输出一行

| edge rows | Join，无 edge index | Join，edge BTree 存在 | CSR + GetV |
|---:|---:|---:|---:|
| 1,000 | 300.98 ms | 359.88 ms | 36.41 ms |
| 10,000 | 330.55 ms | 358.11 ms | 35.65 ms |
| 100,000 | 369.48 ms | 384.68 ms | 35.89 ms |

这条 quick-run 曲线中的固定规划/执行开销很大，因此 `E` 的斜率没有关系访问微基准那么明显；
但两个结论很清楚：

1. CSR + GetV 对关系表规模基本不敏感；
2. edge BTree 的存在没有使当前端到端 Hash Join 自动变成 frontier-driven index lookup。

物理计划仍包含 6 个 `HashJoinExec`（三跳中 source/target Join），并继续出现 relationship scan。
因此需要区分：

```text
index exists
!=
query plan uses the index as an adjacency access path
```

## 4. 普通 edge BTree 能解决多少扫描

关系访问微基准固定：100,000 sources、400,000 edges、fanout=4、depth=3。它不包含 Cypher
planning 和 target GetV。

| start frontier | final rows | full edge scan | edge `src_id` BTree | CSR | full-scan rows | selected edge rows |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 64 | 86.87 ms | 5.53 ms | 0.52 µs | 1,200,000 | 84 |
| 10 | 640 | 92.81 ms | 6.22 ms | 2.59 µs | 1,200,000 | 840 |
| 100 | 6,400 | 93.34 ms | 9.72 ms | 16.01 µs | 1,200,000 | 8,400 |
| 1,000 | 64,000 | 106.95 ms | 136.06 ms | 190.41 µs | 1,200,000 | 84,000 |

普通 BTree 对小 frontier 非常有效：frontier=1 时把约 86.9 ms 降到约 5.5 ms。但当 frontier
增大到 1,000 时，要枚举并离散读取 84,000 条 edge rows，BTree 路径反而慢于顺序全扫描。

这给出了 cost-based planner 所需的真实拐点：

```text
small frontier  -> ordinary edge BTree is useful
large frontier  -> edge-row random lookup loses to sequential scan
CSR             -> contiguous adjacency enumeration remains cheap
```

CSR 是内存常驻上界，微秒数不能直接与持久化 Direct/Covering Adjacency 等价。但它证明了专用
布局可以把核心访问工作约束在实际邻居规模，而不是 row-ID 枚举和离散 `take_rows`。

## 5. 高出度：固定 400,000 总边数

只改变 source `0` 的 degree，其他边重新分布，关系表总规模保持不变。

| hub degree | full edge scan | edge `src_id` BTree | CSR |
|---:|---:|---:|---:|
| 1 | 31.99 ms | 2.19 ms | 0.053 µs |
| 10 | 31.47 ms | 2.56 ms | 0.061 µs |
| 100 | 30.93 ms | 1.80 ms | 0.069 µs |
| 1,000 | 31.00 ms | 1.82 ms | 0.267 µs |
| 10,000 | 30.51 ms | 2.77 ms | 2.39 µs |

曲线分离出了三类成本：

- full scan 几乎完全由 `E` 决定，与 query degree 无关；
- edge BTree 有约 2 ms 的 lookup/row-fetch 固定成本，并随大量取行缓慢增加；
- CSR 基本按邻居枚举量增长，没有毫秒级固定 I/O/索引搜索成本。

在当前 400K edge 数据量下，即使 degree=10K，普通 BTree 仍优于全扫描。这并不否定专用索引；
它说明最公平的动机不是“普通索引完全没用”，而是：

1. 通用 Hash Join 不会必然采用 frontier-driven edge lookup；
2. 显式采用普通 BTree 后，仍承担 index search、row-ID 枚举和离散 edge-row fetch；
3. 专用邻接布局能把同一 source 的邻居连续化，并为批量 frontier 提供专门的执行算子。

## 6. 起始 frontier：端到端反例

固定 100,000 sources、400,000 edges、fanout=4、depth=2：

| start frontier | final rows | Join，无 edge index | Join，edge BTree 存在 | CSR + GetV |
|---:|---:|---:|---:|---:|
| 1 | 16 | 239.01 ms | 249.51 ms | 32.96 ms |
| 10 | 160 | 380.44 ms | 372.04 ms | 885.57 ms |
| 100 | 1,600 | 390.98 ms | 372.19 ms | 857.87 ms |
| 1,000 | 16,000 | 389.42 ms | 363.69 ms | 806.75 ms |

这是本轮最重要的反例：CSR 的邻接访问微基准仍为微秒级，但端到端 `CSR + GetV` 在
frontier >= 10 后退化到约 0.8–0.9 秒。

因此当前瓶颈不是 CSR adjacency lookup，而是在多跳端到端组合中的 source/endpoint 物化、
重复 ID、批次传播、算子组合或调度。补充运行现有 `indexed_get_v_degree_cost/get_v_by_id_only`
后，单独 GetV 的 median 为：

| requested IDs | GetV-only median |
|---:|---:|
| 1 | 2.53 ms |
| 10 | 2.64 ms |
| 100 | 2.66 ms |
| 1,000 | 3.46 ms |
| 10,000 | 27.85 ms |

这说明 10–1,000 ID 的单次 GetV 没有 0.8 秒级阶跃，不能把端到端反例简单归因于 Lance scalar
lookup。需要进一步采集每一跳 `IndexedExpandExec` 和 `LanceGetVByIdExec` 的实际 input rows、
unique IDs、lookup batches、execution time 和重复度。

进一步运行单跳物理组合也得到同样结论：

| degree | IndexedExpand-only | one-hop IndexedExpand + GetV |
|---:|---:|---:|
| 1 | 14.31 µs | 2.52 ms |
| 10 | 4.31 µs | 2.73 ms |
| 100 | 6.64 µs | 2.63 ms |
| 1,000 | 11.23 µs | 3.32 ms |
| 10,000 | 61.76 µs | 25.58 ms |

单跳 `IndexedExpand + GetV` 到 1,000 degree 仍只有约 3.3 ms，所以 0.8 秒级问题进一步收窄到
多跳 logical/physical plan 组合，而不是 CSR Expand 或单跳 GetV 本身。

专用图索引是必要但不充分条件：

```text
fast adjacency lookup
  + efficient batched GetV
  + frontier-aware physical planning
  + duplicate handling / batching
= fast end-to-end traversal
```

后续设计不应只优化索引文件格式，还必须为大 frontier 定义 cost threshold，并允许 planner 在
CSR + GetV、edge BTree lookup、Direct/Covering Adjacency 和顺序 Join/scan 之间切换。

## 7. 当前可下的结论

阶段性证据支持：

1. 小起点、深查询时，专用邻接路径能消除每跳大量通用 Join 开销；
2. 普通 edge BTree 能显著改善小 frontier 的 edge access，但“存在索引”不会让 Hash Join 自动
   变成邻接访问；
3. 普通 BTree 在 frontier 增大后会被 row-ID 枚举和离散取行拖累，并存在退化为不如顺序扫描
   的拐点；
4. 高出度下，专用连续邻接布局比 edge-row BTree 更接近纯输出枚举成本；
5. 当前 CSR + GetV 对大 frontier 有严重端到端瓶颈，需要作为索引设计的一部分继续优化。

阶段性证据不支持：

- “所有图查询都应该强制使用 CSR”；
- “普通 scalar index 没有价值”；
- “内存 CSR 的微秒结果等同于对象存储上的持久化索引”；
- “只实现图索引，不改 planner/GetV 就能覆盖所有 frontier”。

更准确的产品结论是：需要专用图访问路径和 cost-based 选择，而不是单一索引后端。
