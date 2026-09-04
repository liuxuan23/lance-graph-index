use super::config::BenchmarkMode;
use super::fixture::LdbcSnbFixture;
use super::queries::QueryCase;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use std::fs;
use std::path::Path;

pub async fn validate_cases(
    fixture: &LdbcSnbFixture,
    cases: &[QueryCase],
    modes: &[BenchmarkMode],
    plan_dir: &Path,
) -> Result<(), String> {
    fs::create_dir_all(plan_dir)
        .map_err(|error| format!("failed to create {}: {error}", plan_dir.display()))?;
    for case in cases {
        let baseline = fixture.execute(&case.query, BenchmarkMode::Join).await?;
        let expected = canonical_rows(&baseline)?;
        let mut checked_modes = vec![BenchmarkMode::Join];
        checked_modes.extend(
            modes
                .iter()
                .copied()
                .filter(|mode| *mode != BenchmarkMode::Join),
        );
        checked_modes.sort_unstable();
        checked_modes.dedup();
        for mode in checked_modes {
            let plan = fixture.explain(&case.query, mode).await?;
            validate_plan(&plan, mode)?;
            let plan_path = plan_dir.join(format!("{}_{}.txt", case.id, mode.name()));
            fs::write(&plan_path, &plan)
                .map_err(|error| format!("failed to write {}: {error}", plan_path.display()))?;
            if mode != BenchmarkMode::Join {
                let actual = fixture.execute(&case.query, mode).await?;
                let actual = canonical_rows(&actual)?;
                if actual != expected {
                    return Err(format!(
                        "{} result mismatch for {}: join_rows={}, indexed_rows={}",
                        mode.name(),
                        case.id,
                        expected.len(),
                        actual.len()
                    ));
                }
            }
        }
    }
    Ok(())
}

fn canonical_rows(batch: &RecordBatch) -> Result<Vec<(i64, String)>, String> {
    if batch.num_columns() != 2 {
        return Err(format!(
            "expected two result columns, got {}",
            batch.num_columns()
        ));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "result column 0 is not Int64".to_string())?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "result column 1 is not Utf8".to_string())?;
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            let name = if names.is_null(row) {
                String::new()
            } else {
                names.value(row).to_string()
            };
            (ids.value(row), name)
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    Ok(rows)
}

fn validate_plan(plan: &str, mode: BenchmarkMode) -> Result<(), String> {
    let lower = plan.to_ascii_lowercase();
    match mode {
        BenchmarkMode::Join => {
            require(plan, "HashJoinExec", mode)?;
        }
        BenchmarkMode::Csr => {
            require(plan, "IndexedExpandExec", mode)?;
            validate_indexed_plan(plan, &lower, mode)?;
        }
        BenchmarkMode::Direct => {
            require(plan, "DirectAdjacencyExpandExec", mode)?;
            validate_indexed_plan(plan, &lower, mode)?;
        }
        BenchmarkMode::Covering => {
            require(plan, "CoveringAdjacencyExpandExec", mode)?;
            validate_indexed_plan(plan, &lower, mode)?;
        }
    }
    Ok(())
}

fn validate_indexed_plan(plan: &str, lower: &str, mode: BenchmarkMode) -> Result<(), String> {
    require(plan, "LanceGetVByIdExec", mode)?;
    if plan.contains("HashJoinExec") {
        return Err(format!(
            "{} plan retained HashJoinExec:\n{plan}",
            mode.name()
        ));
    }
    if lower.contains("tablescan: knows") {
        return Err(format!("{} plan scanned KNOWS:\n{plan}", mode.name()));
    }
    Ok(())
}

fn require(plan: &str, needle: &str, mode: BenchmarkMode) -> Result<(), String> {
    if plan.contains(needle) {
        Ok(())
    } else {
        Err(format!(
            "{} plan does not contain {needle}:\n{plan}",
            mode.name()
        ))
    }
}
