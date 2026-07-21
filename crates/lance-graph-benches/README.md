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
```

## Benchmarks

- **graph_execution**: End-to-end query execution benchmarks
  - Basic node filtering and projection
  - Single-hop relationship expansion
  - Two-hop relationship expansion
  - Tests with datasets of varying sizes (100, 10K, 1M rows)
- **graph_execution_disk**: The same query workloads resolved through a
  directory namespace and executed against Lance datasets on disk. The
  benchmark reports separate warm-cache and Linux local page-cache cold-start
  groups.

## Note

This crate is not published to crates.io and is excluded from releases.
It exists solely for performance testing during development.
