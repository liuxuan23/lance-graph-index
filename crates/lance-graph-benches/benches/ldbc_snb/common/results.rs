use super::config::{BenchmarkConfig, BenchmarkMode};
use super::dataset::DatasetManifest;
use super::queries::QueryCase;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize)]
pub struct RawResult {
    pub dataset: String,
    pub scale_factor: String,
    pub query_id: String,
    pub hop: usize,
    pub distinct: bool,
    pub mode: String,
    pub seed_id: i64,
    pub original_seed_id: i64,
    pub degree_bucket: String,
    pub degree: u64,
    pub iteration: usize,
    pub latency_ms: f64,
    pub result_rows: usize,
    pub success: bool,
    pub error: String,
}

pub struct ResultWriter {
    pub output_dir: PathBuf,
    csv: csv::Writer<fs::File>,
    rows: Vec<RawResult>,
}

impl ResultWriter {
    pub fn create(config: &BenchmarkConfig, manifest: &DatasetManifest) -> Result<Self, String> {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock is before UNIX epoch: {error}"))?
            .as_secs();
        let output_dir = config
            .results_dir
            .join(format!("{epoch}-{}", std::process::id()));
        fs::create_dir_all(&output_dir)
            .map_err(|error| format!("failed to create {}: {error}", output_dir.display()))?;
        let csv_path = output_dir.join("raw_results.csv");
        let csv = csv::Writer::from_path(&csv_path)
            .map_err(|error| format!("failed to create {}: {error}", csv_path.display()))?;
        let run_manifest = json!({
            "dataset": manifest,
            "benchmark": {
                "warmup_runs": config.warmup_runs,
                "measure_runs": config.measure_runs,
                "seeds_per_bucket": config.seeds_per_bucket,
                "max_paths": config.max_paths,
                "modes": config.modes.iter().map(|mode| mode.name()).collect::<Vec<_>>(),
                "workloads": config.workloads.iter().map(|kind| kind.name()).collect::<Vec<_>>(),
                "rebuild_indexes": config.rebuild_indexes,
            },
            "package_version": env!("CARGO_PKG_VERSION"),
        });
        let manifest_path = output_dir.join("run_manifest.json");
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&run_manifest)
                .map_err(|error| format!("failed to serialize run manifest: {error}"))?,
        )
        .map_err(|error| format!("failed to write {}: {error}", manifest_path.display()))?;
        Ok(Self {
            output_dir,
            csv,
            rows: Vec::new(),
        })
    }

    pub fn plan_dir(&self) -> PathBuf {
        self.output_dir.join("plans")
    }

    pub fn record(
        &mut self,
        manifest: &DatasetManifest,
        case: &QueryCase,
        mode: BenchmarkMode,
        iteration: usize,
        latency_ms: f64,
        result: Result<usize, String>,
    ) -> Result<(), String> {
        let (result_rows, success, error) = match result {
            Ok(rows) => (rows, true, String::new()),
            Err(error) => (0, false, error),
        };
        let row = RawResult {
            dataset: manifest.dataset.clone(),
            scale_factor: manifest.scale_factor.clone(),
            query_id: case.workload.name().to_string(),
            hop: case.workload.hop(),
            distinct: case.workload.distinct(),
            mode: mode.name().to_string(),
            seed_id: case.seed.record.person_id,
            original_seed_id: case.seed.record.original_person_id,
            degree_bucket: case.seed.bucket.clone(),
            degree: case.seed.record.degree,
            iteration,
            latency_ms,
            result_rows,
            success,
            error,
        };
        self.csv
            .serialize(&row)
            .map_err(|error| format!("failed to write benchmark CSV: {error}"))?;
        self.csv
            .flush()
            .map_err(|error| format!("failed to flush benchmark CSV: {error}"))?;
        self.rows.push(row);
        Ok(())
    }

    pub fn finish(mut self) -> Result<PathBuf, String> {
        self.csv
            .flush()
            .map_err(|error| format!("failed to flush benchmark CSV: {error}"))?;
        let summary_path = self.output_dir.join("summary.md");
        fs::write(&summary_path, render_summary(&self.rows))
            .map_err(|error| format!("failed to write {}: {error}", summary_path.display()))?;
        Ok(self.output_dir)
    }
}

#[derive(Default)]
struct SummaryGroup {
    latencies: Vec<f64>,
    result_rows: Vec<usize>,
}

fn render_summary(rows: &[RawResult]) -> String {
    let mut groups = BTreeMap::<(String, String, String), SummaryGroup>::new();
    for row in rows.iter().filter(|row| row.success) {
        let group = groups
            .entry((
                row.query_id.clone(),
                row.degree_bucket.clone(),
                row.mode.clone(),
            ))
            .or_default();
        group.latencies.push(row.latency_ms);
        group.result_rows.push(row.result_rows);
    }
    let mut join_medians = BTreeMap::<(String, String), f64>::new();
    for ((query, bucket, mode), group) in &groups {
        if mode == "join" {
            join_medians.insert(
                (query.clone(), bucket.clone()),
                percentile(&group.latencies, 0.5),
            );
        }
    }
    let mut output = String::from(
        "# LDBC SNB Graph Index Benchmark Summary\n\n\
         | query | degree bucket | mode | runs | mean ms | p50 ms | p95 ms | p99 ms | mean rows | join / mode |\n\
         |---|---|---|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for ((query, bucket, mode), group) in groups {
        let mean_latency = group.latencies.iter().sum::<f64>() / group.latencies.len() as f64;
        let p50 = percentile(&group.latencies, 0.5);
        let p95 = percentile(&group.latencies, 0.95);
        let p99 = percentile(&group.latencies, 0.99);
        let mean_rows =
            group.result_rows.iter().sum::<usize>() as f64 / group.result_rows.len() as f64;
        let speedup = join_medians
            .get(&(query.clone(), bucket.clone()))
            .map(|join| join / p50)
            .unwrap_or(0.0);
        output.push_str(&format!(
            "| {query} | {bucket} | {mode} | {} | {mean_latency:.3} | {p50:.3} | \
             {p95:.3} | {p99:.3} | {mean_rows:.1} | {speedup:.2}x |\n",
            group.latencies.len()
        ));
    }
    output
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index]
}

pub fn ensure_output_parent(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))
}
