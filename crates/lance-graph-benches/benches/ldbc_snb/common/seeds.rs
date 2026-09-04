use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct SeedRecord {
    pub person_id: i64,
    pub original_person_id: i64,
    pub degree: u64,
    pub two_hop_path_count: u64,
    pub three_hop_path_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct SeedFile {
    pub groups: BTreeMap<String, Vec<SeedRecord>>,
}

#[derive(Debug, Clone)]
pub struct BenchmarkSeed {
    pub bucket: String,
    pub record: SeedRecord,
}

pub fn load_seeds(root: &Path, per_bucket: usize) -> Result<Vec<BenchmarkSeed>, String> {
    let path = root.join("seeds.json");
    let bytes =
        fs::read(&path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let file: SeedFile = serde_json::from_slice(&bytes)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
    let mut seeds = Vec::new();
    for bucket in ["low", "medium", "high", "hub"] {
        if let Some(records) = file.groups.get(bucket) {
            seeds.extend(
                records
                    .iter()
                    .take(per_bucket)
                    .cloned()
                    .map(|record| BenchmarkSeed {
                        bucket: bucket.to_string(),
                        record,
                    }),
            );
        }
    }
    if seeds.is_empty() {
        return Err(format!(
            "{} contains no low/medium/high/hub benchmark seeds",
            path.display()
        ));
    }
    Ok(seeds)
}
