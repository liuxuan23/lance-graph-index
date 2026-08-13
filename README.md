# Lance Graph Index

面向 Lakehouse 的图查询与多模态融合查询引擎，也是用于构建多模态知识图谱底座的实验性项目。

Lance Graph Index 将属性图映射到 Lance、Arrow、Parquet 或 Delta Lake 表上，以 Rust、DataFusion 和 Apache Arrow 为执行基础，同时提供 Cypher、SQL、向量检索、知识抽取与 GraphRAG 工具链。项目的目标不是把图数据复制进独立图数据库，而是让图拓扑、业务属性、向量表征和原始多模态资源继续保存在 Lakehouse 中，并在同一数据底座上完成关系遍历、结构化过滤和语义检索。

> 项目状态：Alpha / Research Prototype。查询 API、图索引格式和多模态数据模型仍在快速演进，当前更适合研究验证、原型开发和参与共建。

## 为什么做这个项目

多模态知识图谱通常同时包含几类数据：

- 实体、关系和业务属性；
- 文本、图片、音频、视频等原始资源或资源 URI；
- 由文本模型或多模态模型生成的向量表征；
- Parquet、Delta Lake、Lance 等 Lakehouse 表；
- 图遍历、关系约束、向量相似度和分析型 SQL 等不同查询方式。

如果这些能力分别落在图数据库、向量数据库、数仓和对象存储中，数据同步、版本一致性与查询编排会迅速变复杂。Lance Graph Index 尝试提供一条 Lakehouse-native 路径：

```text
原始多模态数据 / 业务表
          │
          ├── 实体与关系抽取
          ├── 多模态编码与向量化
          ▼
Lance / Arrow / Parquet / Delta Lake
          │
          ├── 属性图映射与 Catalog
          ├── Cypher / SQL
          ├── 图邻接索引与节点索引
          └── 向量检索与融合排序
          ▼
知识检索 / GraphRAG / 分析查询 / API 服务
```

## 核心能力

| 方向 | 当前能力 | 状态 |
|---|---|---|
| Lakehouse 图查询 | 将节点表和关系表映射为属性图，使用 Cypher 执行单跳、多跳和变长路径查询 | 已实现 |
| 统一执行 | Cypher 逻辑计划下推到 DataFusion，结果以 Arrow `RecordBatch` / `Table` 返回 | 已实现 |
| SQL 分析 | 直接对已注册数据集执行 SQL，无需图映射 | 已实现 |
| 图向量融合 | 图查询生成候选集后进行 L2、Cosine 或 Dot 向量重排 | 已实现 |
| Lance 向量查询 | 对简单查询可使用 Lance ANN 路径，并在不适用时使用精确重排路径 | 已实现 |
| 图索引 | CSR、持久化 CSR、Direct Adjacency、Multi-Type Adjacency、GIN 风格 Covering Adjacency | Rust 研究实现 |
| Indexed GetV | 通过节点 ID 标量索引物化目标节点，减少目标节点全表 Join | Rust 研究实现 |
| Lakehouse Catalog | 目录命名空间、Unity Catalog，以及 Delta Lake / Parquet 表发现与查询 | 已实现 |
| 知识图谱构建 | 文本实体关系抽取、嵌入生成、Lance 持久化、Cypher 查询、自然语言问答 | 已实现 |
| 服务化 | Python API、命令行工具、FastAPI 组件和 Streamlit 示例 | 已实现 |

### 1. Lakehouse-native 图查询

节点和关系保持为普通列式表，通过 `GraphConfig` 声明图语义：

```text
Person.lance       person_id | name | age | embedding
Company.lance      company_id | name | industry
WORKS_FOR.lance    person_id | company_id | position
```

映射后即可使用 Cypher：

```cypher
MATCH (p:Person)-[:WORKS_FOR]->(c:Company)
WHERE p.age > $min_age
RETURN p.name, c.name
ORDER BY p.name
```

当前支持的主要 Cypher 范围包括：

- 节点标签、带类型和方向的关系模式；
- 固定多跳与变长路径；
- 属性比较以及 `AND`、`OR`、`NOT`、`EXISTS`；
- 参数绑定；
- `DISTINCT`、基础聚合、`ORDER BY`、`SKIP` 和 `LIMIT`；
- 查询计划解释，以及向 SQL / Spark SQL 方言转换。

`OPTIONAL MATCH` 和子查询目前可以被解析，但尚未进入完整执行支持范围。

### 2. 图查询与向量检索融合

项目当前提供两类融合路径：

1. **Graph-first**：先通过关系、类型和属性约束获得候选集，再执行向量重排；
2. **Vector-first**：简单查询在输入为 Lance Dataset 时可优先使用 Lance ANN 检索，再进入图查询流程。

Graph-first 适合“先满足确定关系，再按语义相关度排序”的场景，例如：

- 在某个项目关联的全部文本、图片和音频中查找最相关内容；
- 先沿知识图谱定位某类实体，再执行语义召回；
- 用权限、时间、来源或业务关系约束向量搜索范围；
- GraphRAG 中的种子实体召回、邻居扩展与答案生成。

### 3. 面向图遍历的专用索引

普通关系执行通常需要在每一跳扫描关系表并进行 Hash Join。对于小起点集合和深层遍历，这部分开销可能远大于真正访问的邻居数量。本项目正在同一 Lance / DataFusion 执行栈内研究专用图访问路径：

```text
Join 基线
  frontier -> relationship scan -> Hash Join -> target-node Join

索引路径
  frontier -> adjacency lookup -> Indexed GetV -> next frontier
```

当前 Rust 侧包含：

- **In-memory CSR**：以内存邻接表执行 `IndexedExpand`；
- **Persisted CSR**：将 `offsets` 和 `neighbors` 保存为不可变 Lance generation；
- **Direct Adjacency Index**：`src_id -> List<dst_id>`，并为 `src_id` 建立 BTree 标量索引；
- **Multi-Type Direct Adjacency**：按关系类型、标签组合和方向组织独立组件；
- **GIN-Style Covering Adjacency**：从 entry/posting pages 直接返回邻接 payload，避免二次读取邻接表；
- **Indexed GetV**：根据语义节点 ID 批量查找并物化目标节点属性。

这些索引目前主要通过 Rust API 显式选择执行后端，用于验证图规模、查询深度、节点出度、冷暖缓存和对象存储访问模式下的收益。自动成本选择、在线增量维护和 Python 图索引管理 API 仍在演进中。

## 多模态知识图谱数据模型

Lance Graph Index 不限定具体模态编码器。推荐将不同模态统一为“资源节点 + 业务实体 + 关系 + 向量”的表模型：

```text
Asset
  asset_id | modality | uri | text | metadata | embedding

Entity
  entity_id | entity_type | name | context | embedding

RELATIONSHIP
  source_entity_id | target_entity_id | relationship_type | description
```

图片、音频或视频可以保留在对象存储中，图节点保存 URI、元数据和由外部模型生成的联合向量；文本则可以使用项目内置的 LLM / heuristic 抽取和 OpenAI-compatible embedding 流程。只要不同模态被编码到兼容的向量空间，就可以与图约束组合查询。

当前仓库内置的自动抽取流水线以**文本**为主。图片、音频和视频的解析、OCR、ASR、caption 或多模态 embedding 需要由外部模型完成，再以 Arrow / Lance 表接入。这一边界会随着多模态 ingestion 组件的建设继续扩展。

## 快速开始

### 环境要求

- Rust 1.82 或更高版本；
- Python 3.11（包声明支持 Python 3.9+，项目开发推荐 3.11）；
- [`uv`](https://docs.astral.sh/uv/)；
- `protoc`，用于构建部分 Lance 相关依赖。

Debian / Ubuntu 可以通过以下命令安装 `protoc`：

```bash
sudo apt-get install protobuf-compiler
```

### 从源码安装 Python 包

```bash
git clone https://github.com/liuxuan23/lance-graph-index.git
cd lance-graph-index/python

uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install 'maturin[patchelf]'
uv pip install -e '.[tests]'
maturin develop
```

### 多模态图查询 + 向量重排

下面的示例把文本、图片和音频资源放进同一张 `Asset` 表。示例中的向量视为已经由兼容的多模态编码器生成：

```python
import pyarrow as pa

from lance_graph import CypherQuery, DistanceMetric, GraphConfig, VectorSearch

assets = pa.table(
    {
        "asset_id": [1, 2, 3],
        "name": ["Lakehouse design", "Architecture diagram", "Team meeting"],
        "modality": ["text", "image", "audio"],
        "uri": [
            "s3://demo/docs/design.md",
            "s3://demo/images/architecture.png",
            "s3://demo/audio/meeting.wav",
        ],
        "embedding": pa.array(
            [
                [1.0, 0.0, 0.0],
                [0.8, 0.2, 0.0],
                [0.0, 1.0, 0.0],
            ],
            type=pa.list_(pa.float32()),
        ),
    }
)

topics = pa.table(
    {
        "topic_id": [10],
        "name": ["lakehouse"],
    }
)

contains = pa.table(
    {
        "topic_id": [10, 10, 10],
        "asset_id": [1, 2, 3],
    }
)

config = (
    GraphConfig.builder()
    .with_node_label("Topic", "topic_id")
    .with_node_label("Asset", "asset_id")
    .with_relationship("CONTAINS", "topic_id", "asset_id")
    .build()
)

query = CypherQuery(
    """
    MATCH (t:Topic)-[:CONTAINS]->(a:Asset)
    WHERE t.name = 'lakehouse'
    RETURN a.asset_id, a.name, a.modality, a.uri, a.embedding
    """
).with_config(config)

result = query.execute_with_vector_rerank(
    {
        "Topic": topics,
        "Asset": assets,
        "CONTAINS": contains,
    },
    VectorSearch("a.embedding")
    .query_vector([1.0, 0.0, 0.0])
    .metric(DistanceMetric.Cosine)
    .top_k(2),
)

print(result.select(["a.name", "a.modality", "_distance"]).to_pylist())
```

这个查询先通过 `Topic -[:CONTAINS]-> Asset` 关系约束候选资源，再在候选集中按语义距离排序。

### 直接执行 Cypher

```python
import pyarrow as pa

from lance_graph import CypherQuery, GraphConfig

people = pa.table(
    {
        "person_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [28, 34, 29],
    }
)

config = GraphConfig.builder().with_node_label("Person", "person_id").build()

result = (
    CypherQuery(
        "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name, p.age"
    )
    .with_config(config)
    .with_parameter("min_age", 30)
    .execute({"Person": people})
)

print(result.to_pylist())
# [{'p.name': 'Bob', 'p.age': 34}]
```

对于需要重复查询同一批数据集的场景，可以使用 `CypherEngine` 缓存 Catalog 和执行上下文；分析型查询也可以通过 `SqlQuery` 或 `SqlEngine` 直接执行 SQL。

## 构建和查询知识图谱

`knowledge_graph` Python 包提供文本知识抽取、实体和关系落盘、embedding、自然语言问答、CLI 与 FastAPI 组件。

### 无外部模型的本地流程

```bash
cd python
source .venv/bin/activate

knowledge_graph --root ./demo_graph --init
knowledge_graph \
  --root ./demo_graph \
  --extractor heuristic \
  --embedding-model none \
  --extract-and-add "Alice joined the graph team."

knowledge_graph \
  --root ./demo_graph \
  "MATCH (e:Entity) RETURN e.name, e.entity_type LIMIT 10"
```

### LLM 抽取、embedding 与问答

默认抽取器和语义问答使用 OpenAI-compatible API：

```bash
export OPENAI_API_KEY=your-api-key

knowledge_graph --root ./demo_graph --extract-and-add notes.txt
knowledge_graph --root ./demo_graph --ask "Who is working on the graph project?"
```

可以使用 `--llm-model`、`--embedding-model` 和 `--llm-config` 配置模型、兼容服务地址、请求头等客户端参数。`--extract-preview` 可以在写入前查看抽取结果。

## Unity Catalog 与 Lakehouse 表

`lance-graph-catalog` 可以通过 Unity Catalog 发现 Delta Lake 或 Parquet 表，并将它们注册到 DataFusion：

```python
from lance_graph import UnityCatalog

catalog = UnityCatalog("http://localhost:8080/api/2.1/unity-catalog")

print(catalog.list_catalogs())
print(catalog.list_schemas("unity"))
print(catalog.list_tables("unity", "default"))

engine = catalog.create_sql_engine("unity", "default")
result = engine.execute("SELECT * FROM marksheet WHERE mark > 80")
print(result.to_pandas())
```

访问 S3、Azure 或 GCS 时，可以通过 `storage_options` 传递对应的存储配置。请避免把凭证提交到仓库。

## 项目架构

```text
lance-graph-index/
├── crates/
│   ├── lance-graph/          # Cypher、逻辑计划、DataFusion 执行和图索引
│   ├── lance-graph-catalog/  # Namespace、Catalog、Unity Catalog
│   ├── lance-graph-python/   # PyO3 Python 绑定
│   └── lance-graph-benches/  # 查询和图索引基准测试
├── python/
│   ├── python/lance_graph/   # Python 查询 API
│   ├── python/knowledge_graph/ # 抽取、存储、语义检索、问答和服务
│   ├── python/tests/         # Python 功能测试
│   └── streamlit_app/        # 可视化示例
├── examples/                 # Cypher 与知识图谱示例
└── docs/                     # 图索引设计、持久化方案和研究记录
```

主要组件关系：

```text
Cypher Parser / SQL
        │
        ▼
Graph Logical Plan
        │
        ▼
DataFusion Physical Plan
        │
        ├── Scan / Filter / Join / Aggregate
        ├── IndexedExpand
        ├── Direct/Covering Adjacency Expand
        ├── Indexed GetV
        └── Vector Search / Rerank
        │
        ▼
Arrow + Lance + Lakehouse Catalog
```

## 开发与验证

### Rust

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```

运行代表性 benchmark：

```bash
cargo bench -p lance-graph-benches --bench graph_execution
cargo bench -p lance-graph-benches --bench indexed_expand_star
cargo bench -p lance-graph-benches --bench covering_adjacency
```

快速本地运行 Criterion 时可以追加：

```bash
-- --warm-up-time 1 --measurement-time 2 --sample-size 10
```

### Python

```bash
cd python
source .venv/bin/activate

maturin develop
pytest python/tests/ -v
make lint
```

## 设计文档

- [Indexed Expand 设计](docs/indexed-expand-design.md)
- [Indexed GetV 设计](docs/indexed-getv-design.md)
- [持久化 CSR 索引方案](docs/persisted-csr-index-plan.md)
- [Direct Adjacency Index 方案](docs/direct-adjacency-index-plan.md)
- [Multi-Type Direct Adjacency Index 方案](docs/multi-type-direct-adjacency-index-plan.md)
- [GIN 风格 Covering Adjacency Index 方案](docs/covering-adjacency-index-plan.md)

## 当前边界与后续方向

当前重点是建立可验证的 Lakehouse 图执行和图向量融合基础，以下能力仍属于后续工作：

- 图片、音频、视频的内置解析器和端到端 ingestion pipeline；
- 基于 workload 统计信息的图索引自动选择；
- 图索引在线增量更新、compaction 和 generation 管理；
- 更完整的 Cypher 语义，包括 `OPTIONAL MATCH` 和子查询执行；
- 面向远端对象存储的 I/O、缓存和分布式执行优化；
- 多模态 schema、provenance、版本与评测体系标准化。

欢迎围绕图查询执行、邻接索引、Lakehouse Catalog、多模态抽取、GraphRAG 和 benchmark 提交 Issue 或 Pull Request。

## License

[Apache License 2.0](LICENSE)

## 相关链接

- [Lance Graph Index](https://github.com/liuxuan23/lance-graph-index)
- [Lance](https://lance.org/)
- [Apache Arrow](https://arrow.apache.org/)
- [Apache DataFusion](https://datafusion.apache.org/)
- [Unity Catalog](https://github.com/unitycatalog/unitycatalog)
