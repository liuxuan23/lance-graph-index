# Lance Graph Benchmarks

Performance benchmarks for the `lance-graph` crate.

## Running Benchmarks

From the repository root:

```bash
# Run all benchmarks
cargo bench -p lance-graph-benches

# Run specific benchmark
cargo bench -p lance-graph-benches --bench graph_execution

# Run the disk-backed graph execution benchmark
cargo bench -p lance-graph-benches --bench graph_execution_disk

# Run the CSR build-only benchmark
cargo bench -p lance-graph-benches --bench graph_index_build

# Run the star-topology Join vs CSR indexed benchmark
cargo bench -p lance-graph-benches --bench indexed_expand_star

# Run exact-depth Join vs CSR traversal benchmarks
cargo bench -p lance-graph-benches --bench indexed_expand_depth

# Run the full motivation matrix (scale, frontier, and ordinary edge BTree)
cargo bench -p lance-graph-benches --bench graph_index_motivation

# Run persisted CSR write, warm-load, and local cold-load benchmarks
cargo bench -p lance-graph-benches --bench persisted_csr_index

# Run multi-type Direct Adjacency bundle load and component-selection benchmarks
cargo bench -p lance-graph-benches --bench multi_type_direct_adjacency

# Run the prepared LDBC SNB SF1 Person/KNOWS workload
LDBC_SNB_ROOT=/data/ldbc-snb-prepared/sf1 \
  cargo bench -p lance-graph-benches --bench ldbc_snb_workload
```

## Benchmarks

Benchmark sources are grouped by workload:

```text
benches/execution/
  graph_execution.rs
  graph_execution_disk.rs
benches/indexed_expand/
  graph_index_build.rs
  persisted_index.rs
  star.rs
  depth.rs
  motivation.rs
  direct_adjacency.rs
  multi_type_direct_adjacency.rs
benches/ldbc_snb/
  workload.rs
  common/
  scripts/
```

- **graph_execution**: End-to-end query execution benchmarks
  - Basic node filtering and projection
  - Single-hop relationship expansion
  - Two-hop relationship expansion
  - Tests with datasets of varying sizes (100, 10K, 1M rows)
- **graph_execution_disk**: The same query workloads resolved through a
  directory namespace and executed against Lance datasets on disk. The
  benchmark reports separate warm-cache and Linux local page-cache cold-start
  groups.
- **graph_index_build**: CSR construction cost for Int64 edge IDs at several
  graph sizes and average degrees. Index construction is not included in query
  timings.
- **indexed_expand_star**: Join, CSR + GetV, and Direct Adjacency + GetV execution on a fixed
  star workload with 1,000,000 sources and 10,000,000 total edges. Hub `42` uses degree `10`,
  `100`, `1,000`, and `10,000`; total edge count remains fixed. Each indexed case explicitly
  selects its `ExpandExecutionMode`, and setup builds/persists/loads both index forms outside
  query timing.
- **indexed_expand_depth**: Exact-depth traversal from one fixed source over a
  fixed-size Lance relationship table. Fanout `1` keeps result cardinality at
  one and isolates repeated self-Join/table-scan cost as depth rises. Fanout
  `4` also measures frontier growth. The benchmark prints the modeled
  relationship-row count if every Join hop fully scans the relationship table,
  plus the neighbor-visit count for CSR. Use
  `LANCE_GRAPH_DEPTH_SOURCES`, `LANCE_GRAPH_DEPTH_FANOUTS`, and
  `LANCE_GRAPH_DEPTHS` to override the default `100000`, `1,4`, and
  `1,2,3,4,5` workloads.
- **graph_index_motivation**: Completes the motivation matrix with three
  controlled groups. `graph_execution_relationship_scale` keeps depth `3`,
  fanout `1`, and output at one row while scaling the relationship table from
  1K to 100K edges; it reports Join without an edge index, Join with an
  ordinary edge `src_id` BTree, and CSR + GetV. `graph_execution_start_frontier`
  fixes graph size and varies the starting frontier with the same three paths.
  `adjacency_access_scan_vs_scalar_vs_csr`
  isolates relationship access and compares a repeated full Lance edge scan,
  an ordinary `src_id` BTree over edge rows followed by `take_rows`, and CSR.
  `adjacency_access_high_degree` fixes total edge count and changes only one
  hub's out-degree, using the same three access paths.
  Environment variables prefixed with `LANCE_GRAPH_SCALE_`,
  `LANCE_GRAPH_FRONTIER_`, and `LANCE_GRAPH_ACCESS_` override each group.
  `LANCE_GRAPH_DEGREE_SOURCES`, `LANCE_GRAPH_DEGREE_EDGES`, and
  `LANCE_GRAPH_DEGREES` control the high-degree group.
- **persisted_csr_index**: Separately measures immutable CSR generation writes,
  warm loads, and Linux local page-cache cold loads from `offsets.lance`,
  `neighbors.lance`, and `manifest.json`. CSR construction is outside all
  measurements, and every load uses a fresh registry. The cold group uses
  `posix_fadvise(..., POSIX_FADV_DONTNEED)` and does not represent remote object
  store cold starts. The star query benchmark also compares an in-memory index
  with a separately persisted and reloaded index; persistence and load happen
  outside query timing.
- **direct_adjacency_index**: Measures Direct Adjacency build/persist, reports generation byte
  size, and measures warm and local page-cache-cold load for the same 1M-source/10M-edge workload
  and four selected hub degrees.
- **multi_type_direct_adjacency**: Builds a named bundle whose FRIEND_OF, FOLLOWS, and BLOCKS
  components contain 7M, 2M, and 1M edges respectively (1M source IDs and 10M total edges).
  It measures exact `(index_name, GraphIndexKey)` component lookup and warm eager-load cost for
  bundles with 1, 3, 10, and 50 components. Extra components after the first three contain one
  edge; the component-count load cases use one edge for every component so they isolate
  descriptor/Dataset/scalar-index open overhead without rebuilding the 10M-edge query workload.
- **ldbc_snb_workload**: Opens prepared LDBC SNB SF1 `Person` and bidirectionally materialized
  `KNOWS` Lance datasets, loads or builds CSR, Direct Adjacency, and Covering Adjacency indexes,
  verifies every indexed result and physical plan against Join, then records per-seed warm-query
  samples for one-, two-, distinct-two-, and bounded three-hop workloads. See
  `docs/ldbc-snb-sf1-benchmark-plan.md` and `benches/ldbc_snb/README.md` for data preparation and
  environment variables.

## Note

This crate is not published to crates.io and is excluded from releases.
It exists solely for performance testing during development.
