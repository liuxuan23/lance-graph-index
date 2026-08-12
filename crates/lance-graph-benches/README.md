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

# Run persisted CSR write, warm-load, and local cold-load benchmarks
cargo bench -p lance-graph-benches --bench persisted_csr_index

# Run multi-type Direct Adjacency bundle load and component-selection benchmarks
cargo bench -p lance-graph-benches --bench multi_type_direct_adjacency
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
  direct_adjacency.rs
  multi_type_direct_adjacency.rs
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

## Note

This crate is not published to crates.io and is excluded from releases.
It exists solely for performance testing during development.
