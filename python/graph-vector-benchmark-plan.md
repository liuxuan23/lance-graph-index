# 图向量融合 Benchmark 执行计划

## 1. 目标

在 `python/` 下实现一套可重复运行的图向量融合 benchmark，验证 `lance-graph-index` 在同一份
图数据和向量数据上执行以下两类查询路径的正确性与性能：

1. **Graph-first**：先执行图遍历得到候选 Message，再按向量距离返回 Top-K；
2. **Vector-first**：先通过 Lance 向量索引得到候选 Message，再执行图条件过滤。

benchmark 参考 TigerVector 的实验方法，使用 LDBC SNB 图数据和 SIFT 向量，但不宣称精确复现
TigerVector 的论文结果。TigerVector 未公开完整查询、向量映射和运行参数，本项目会固定并公开自己的
数据映射、查询定义和运行配置。

## 2. 第一版范围

第一版只完成单机、只读 benchmark：

- 图数据使用 LDBC SNB；开发和 CI 使用小规模数据，正式实验使用 SF10；
- 为 `Post` 和 `Comment` 分配 SIFT 128 维向量；
- 以修改后的 LDBC IC3、IC5、IC6、IC9 和 IC11 为查询入口；
- 使用 1、2、3 跳 `KNOWS` 控制图候选集合大小；
- 默认使用 L2 距离和 Top-10；
- 比较 Graph-first、Vector-first 和精确结果基线；
- 只测试本地 Lance Dataset，不包含对象存储、并发压测和跨数据库对比。

## 3. 目录结构

实现代码放在：

```text
python/benchmarks/graph_vector/
  README.md
  prepare_data.py
  workload.py
  run_benchmark.py
  summarize.py
```

准备后的数据和运行结果放在用户指定的仓库外目录，避免把大型 LDBC/SIFT 数据提交到 Git：

```text
<BENCHMARK_ROOT>/
  manifest.json
  datasets/
  queries.json
  results/
```

## 4. 数据准备

`prepare_data.py` 负责：

1. 读取 LDBC SNB 的 Person、Post、Comment、KNOWS 和 creator 相关数据；
2. 将 Post 和 Comment 统一为可检索的 Message 数据，同时保留原始类型和 ID；
3. 按稳定排序把采样的 SIFT 向量确定性地映射到 Message；
4. 将节点和关系写成 Lance Dataset；
5. 为 Message 的向量列创建 Lance 向量索引；
6. 写出 `manifest.json`，记录数据规模、向量来源、映射规则、随机种子和索引参数。

相同输入和随机种子必须生成相同的数据与查询样本。

## 5. Workload

`workload.py` 保存版本化的查询定义。每个查询都由图条件确定一个 Message 候选集合，再按给定查询
向量返回 Top-K。第一版覆盖：

| 查询组 | 图侧入口 | 变化参数 |
|---|---|---|
| IC3 | Person 与位置/时间条件相关的 Message | `KNOWS` 跳数、起点 Person |
| IC5 | Person 的社群及其 Message | `KNOWS` 跳数、起点 Person |
| IC6 | Person 邻域中的标签相关 Message | `KNOWS` 跳数、Tag |
| IC9 | Person 邻域中的近期 Message | `KNOWS` 跳数、时间上限 |
| IC11 | Person 邻域中的组织相关 Message | `KNOWS` 跳数、Country/Year |

由于 TigerVector 没有公开修改后的查询正文，这些查询需要在 `README.md` 中明确记录相对标准 LDBC
查询的改动。`queries.json` 保存固定的起点、过滤参数和查询向量，正式计时期间不再随机生成参数。

## 6. 执行路径

`run_benchmark.py` 对每个查询执行三种模式：

- `exact`：执行完整图查询，对全部图候选计算精确向量距离，作为结果正确性的基线；
- `graph_first`：执行图查询得到候选集，再使用 `VectorSearch` 完成向量排序；
- `vector_first`：使用 Lance 向量索引取得扩大后的向量候选集，再执行相同的图条件并返回 Top-K。

Vector-first 的向量候选数必须作为显式参数记录，不能只记录最终 Top-K。若现有 Python API 不能把
ANN 候选安全地传入带过滤条件的图查询，应先补充最小的候选 ID 输入能力，再实现该执行路径。

## 7. 指标与结果

每次运行至少记录：

```text
query_id, mode, hop, top_k, vector_candidates, graph_candidates,
graph_ms, vector_ms, total_ms, recall_at_k, result_ids
```

- 延迟统计报告 p50 和 p95；
- `recall_at_k` 以 `exact` 模式的 Top-K 为基准；
- 数据准备、索引构建和参数生成不计入查询延迟；
- 默认执行 2 次 warm-up 和 10 次正式测量；
- 原始记录写入 CSV，`summarize.py` 生成汇总 Markdown。

## 8. 实施顺序

### 阶段 A：数据与查询

- 实现 LDBC/SIFT 数据转换和确定性向量映射；
- 写出 manifest 和固定查询参数；
- 在小数据上验证节点、关系和向量数量。

验收：同一输入重复准备得到相同 manifest、Message ID 和向量映射。

### 阶段 B：执行与正确性

- 实现 `exact`、`graph_first` 和 `vector_first`；
- 对每个查询检查 Graph-first 与 exact 结果一致；
- 计算 Vector-first 相对 exact 的 Recall@K。

验收：小数据上的 exact 与 Graph-first Top-K 完全一致，Vector-first 能输出合法且可计算召回率的结果。

### 阶段 C：测量与报告

- 增加 warm-up、重复运行、分阶段计时和 CSV 输出；
- 实现 p50、p95、候选数量、Recall@K 汇总；
- 在 SF10 上运行正式实验并保存运行 manifest。

验收：一条命令能够生成原始 CSV、汇总 Markdown 和本次运行的完整配置。

## 9. 完成标准

第一版完成时应满足：

- 数据和查询参数可确定性重建；
- 三种执行模式使用相同的查询语义和 Top-K 定义；
- 正确性检查在计时前完成，错误结果不会进入性能汇总；
- 每个结果都能追溯到数据版本、代码版本、随机种子和索引参数；
- README 给出从数据准备到生成报告的最短运行命令。
