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
- **indexed_expand_star**: Join and CSR-indexed execution on a star topology.
  Every source has the same fan-out, producing up to 100,000 sources and
  1,000,000 relationship rows, while the query selects only hub vertex `42`.
  This keeps the result at 10 neighbors while increasing the unrelated
  relationship data seen by the Join path. CSR construction happens during
  setup, outside query timings, and the indexed path uses
  `IndexUsagePolicy::Require` so it cannot silently fall back to the
  relationship-table Join path.
- **persisted_csr_index**: Separately measures immutable CSR generation writes,
  warm loads, and Linux local page-cache cold loads from `offsets.lance`,
  `neighbors.lance`, and `manifest.json`. CSR construction is outside all
  measurements, and every load uses a fresh registry. The cold group uses
  `posix_fadvise(..., POSIX_FADV_DONTNEED)` and does not represent remote object
  store cold starts. The star query benchmark also compares an in-memory index
  with a separately persisted and reloaded index; persistence and load happen
  outside query timing.

## Note

This crate is not published to crates.io and is excluded from releases.
It exists solely for performance testing during development.
