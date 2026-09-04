use super::config::{BenchmarkConfig, WorkloadKind};
use super::seeds::BenchmarkSeed;
use lance_graph::{CypherQuery, GraphConfig};

pub struct QueryCase {
    pub id: String,
    pub workload: WorkloadKind,
    pub seed: BenchmarkSeed,
    pub query: CypherQuery,
}

pub fn build_query_cases(
    benchmark: &BenchmarkConfig,
    graph_config: &GraphConfig,
    seeds: &[BenchmarkSeed],
) -> Result<Vec<QueryCase>, String> {
    let mut cases = Vec::new();
    for seed in seeds {
        for workload in &benchmark.workloads {
            let estimated_paths = match workload {
                WorkloadKind::OneHop => seed.record.degree,
                WorkloadKind::TwoHop | WorkloadKind::TwoHopDistinct => {
                    seed.record.two_hop_path_count
                }
                WorkloadKind::ThreeHop => seed.record.three_hop_path_count,
            };
            if estimated_paths > benchmark.max_paths {
                continue;
            }
            let cypher = cypher_for(*workload, seed.record.person_id);
            let query = CypherQuery::new(&cypher)
                .map_err(|error| format!("failed to parse {cypher}: {error}"))?
                .with_config(graph_config.clone());
            cases.push(QueryCase {
                id: format!(
                    "{}_{}_seed_{}",
                    workload.name(),
                    seed.bucket,
                    seed.record.person_id
                ),
                workload: *workload,
                seed: seed.clone(),
                query,
            });
        }
    }
    if cases.is_empty() {
        return Err("no LDBC query cases survived the seed/path filters".into());
    }
    Ok(cases)
}

fn cypher_for(workload: WorkloadKind, seed: i64) -> String {
    match workload {
        WorkloadKind::OneHop => format!(
            "MATCH (p:Person {{person_id: {seed}}})-[:KNOWS]->(f:Person) \
             RETURN f.person_id, f.first_name"
        ),
        WorkloadKind::TwoHop => format!(
            "MATCH (p:Person {{person_id: {seed}}})-[:KNOWS]->(f1:Person)\
             -[:KNOWS]->(f2:Person) RETURN f2.person_id, f2.first_name"
        ),
        WorkloadKind::TwoHopDistinct => format!(
            "MATCH (p:Person {{person_id: {seed}}})-[:KNOWS]->(f1:Person)\
             -[:KNOWS]->(f2:Person) RETURN DISTINCT f2.person_id, f2.first_name"
        ),
        WorkloadKind::ThreeHop => format!(
            "MATCH (p:Person {{person_id: {seed}}})-[:KNOWS]->(f1:Person)\
             -[:KNOWS]->(f2:Person)-[:KNOWS]->(f3:Person) \
             RETURN f3.person_id, f3.first_name"
        ),
    }
}
