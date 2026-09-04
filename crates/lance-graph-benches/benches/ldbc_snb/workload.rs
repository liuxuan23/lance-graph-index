// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

mod common;

use common::config::{BenchmarkConfig, BenchmarkMode};
use common::fixture::LdbcSnbFixture;
use common::queries::build_query_cases;
use common::results::{ensure_output_parent, ResultWriter};
use common::seeds::load_seeds;
use common::validation::validate_cases;
use std::time::Instant;

fn main() {
    if let Err(error) = run() {
        eprintln!("LDBC SNB benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = BenchmarkConfig::from_env()?;
    ensure_output_parent(&config.results_dir)?;
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("failed to create Tokio runtime: {error}"))?;
    let fixture = runtime.block_on(LdbcSnbFixture::open(&config))?;
    let seeds = load_seeds(&config.root, config.seeds_per_bucket)?;
    let cases = build_query_cases(&config, &fixture.graph_config, &seeds)?;
    let mut writer = ResultWriter::create(&config, &fixture.datasets.manifest)?;
    runtime.block_on(validate_cases(
        &fixture,
        &cases,
        &config.modes,
        &writer.plan_dir(),
    ))?;

    for _ in 0..config.warmup_runs {
        for case in &cases {
            for mode in &config.modes {
                runtime.block_on(fixture.execute(&case.query, *mode))?;
            }
        }
    }

    for iteration in 0..config.measure_runs {
        let ordered_modes = rotated_modes(&config.modes, iteration);
        for case in &cases {
            for mode in &ordered_modes {
                let start = Instant::now();
                let result = runtime
                    .block_on(fixture.execute(&case.query, *mode))
                    .map(|batch| batch.num_rows());
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                writer.record(
                    &fixture.datasets.manifest,
                    case,
                    *mode,
                    iteration,
                    latency_ms,
                    result,
                )?;
            }
        }
    }
    let output_dir = writer.finish()?;
    println!(
        "Completed {} cases across {} modes; results: {}",
        cases.len(),
        config.modes.len(),
        output_dir.display()
    );
    Ok(())
}

fn rotated_modes(modes: &[BenchmarkMode], iteration: usize) -> Vec<BenchmarkMode> {
    let mut ordered = modes.to_vec();
    if !ordered.is_empty() {
        let offset = iteration % ordered.len();
        ordered.rotate_left(offset);
    }
    ordered
}
