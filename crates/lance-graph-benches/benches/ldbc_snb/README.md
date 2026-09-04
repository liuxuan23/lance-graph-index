# LDBC SNB SF1 Benchmark

This directory contains the single-system LDBC SNB workload benchmark described in
[`docs/ldbc-snb-sf1-benchmark-plan.md`](../../../../docs/ldbc-snb-sf1-benchmark-plan.md).

The first implementation uses the SF1 `Person` and `Person_knows_Person` data and compares:

- relationship-table Join;
- persisted/reloaded CSR;
- persisted/reloaded Direct Adjacency;
- persisted/reloaded Covering Adjacency.

## Prepare data

```bash
python crates/lance-graph-benches/benches/ldbc_snb/scripts/prepare_sf1.py \
  --person '/path/to/social_network/dynamic/Person/*.csv' \
  --knows '/path/to/social_network/dynamic/Person_knows_Person/*.csv' \
  --output /data/ldbc-snb-prepared/sf1
```

The globs are expanded by the script. Repeat `--person` or `--knows` when the files are in
multiple locations. Use `--overwrite` only when replacing existing prepared Lance datasets.
After replacing `KNOWS.lance`, set `LDBC_SNB_REBUILD_INDEXES=1` on each indexed benchmark run so
the selected persisted graph index is rebuilt from the new Dataset version.

## Run

```bash
LDBC_SNB_ROOT=/data/ldbc-snb-prepared/sf1 \
cargo bench -p lance-graph-benches --bench ldbc_snb_workload
```

Useful smoke settings:

```bash
LDBC_SNB_ROOT=/data/ldbc-snb-prepared/sf1 \
LDBC_SNB_WARMUP_RUNS=1 \
LDBC_SNB_MEASURE_RUNS=1 \
LDBC_SNB_SEEDS_PER_BUCKET=1 \
LDBC_SNB_WORKLOADS=one_hop \
cargo bench -p lance-graph-benches --bench ldbc_snb_workload
```

On a memory-constrained host, run one mode and one workload per process. Only the selected index
is loaded or built; `join` loads no graph index. For example:

```bash
LDBC_SNB_ROOT=/data/ldbc-snb-prepared/sf1 \
LDBC_SNB_RESULTS_DIR=/data/ldbc-snb-prepared/sf1/results/csr-one-hop \
LDBC_SNB_MODES=csr \
LDBC_SNB_WORKLOADS=one_hop \
LDBC_SNB_SEEDS_PER_BUCKET=1 \
LDBC_SNB_WARMUP_RUNS=0 \
LDBC_SNB_MEASURE_RUNS=1 \
cargo bench -p lance-graph-benches --bench ldbc_snb_workload
```

Repeat the command in separate processes for `join`, `direct`, and `covering`, and use a distinct
`LDBC_SNB_RESULTS_DIR` for every mode/workload pair. Set `LDBC_SNB_MAX_PATHS` conservatively for
two- and three-hop runs.

Results are written under `<LDBC_SNB_ROOT>/results/<run-id>/` unless
`LDBC_SNB_RESULTS_DIR` is set. Regenerate one run's Markdown summary with:

```bash
python crates/lance-graph-benches/benches/ldbc_snb/scripts/summarize.py \
  /data/ldbc-snb-prepared/sf1/results/<run-id>
```

Combine independently executed mode/workload runs to compute cross-mode speedups with:

```bash
python crates/lance-graph-benches/benches/ldbc_snb/scripts/summarize.py \
  /data/ldbc-snb-prepared/sf1/results/join-one-hop/<run-id> \
  /data/ldbc-snb-prepared/sf1/results/csr-one-hop/<run-id> \
  /data/ldbc-snb-prepared/sf1/results/direct-one-hop/<run-id> \
  /data/ldbc-snb-prepared/sf1/results/covering-one-hop/<run-id> \
  --output /data/ldbc-snb-prepared/sf1/results/one-hop-summary.md
```
