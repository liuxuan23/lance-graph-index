use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BenchmarkMode {
    Join,
    Csr,
    Direct,
    Covering,
}

impl BenchmarkMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Csr => "csr",
            Self::Direct => "direct",
            Self::Covering => "covering",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "join" => Ok(Self::Join),
            "csr" => Ok(Self::Csr),
            "direct" | "direct_adjacency" => Ok(Self::Direct),
            "covering" | "covering_adjacency" => Ok(Self::Covering),
            other => Err(format!("unsupported LDBC benchmark mode: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkloadKind {
    OneHop,
    TwoHop,
    TwoHopDistinct,
    ThreeHop,
}

impl WorkloadKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::OneHop => "one_hop",
            Self::TwoHop => "two_hop",
            Self::TwoHopDistinct => "two_hop_distinct",
            Self::ThreeHop => "three_hop",
        }
    }

    pub fn hop(self) -> usize {
        match self {
            Self::OneHop => 1,
            Self::TwoHop | Self::TwoHopDistinct => 2,
            Self::ThreeHop => 3,
        }
    }

    pub fn distinct(self) -> bool {
        matches!(self, Self::TwoHopDistinct)
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "one_hop" => Ok(Self::OneHop),
            "two_hop" => Ok(Self::TwoHop),
            "two_hop_distinct" => Ok(Self::TwoHopDistinct),
            "three_hop" => Ok(Self::ThreeHop),
            other => Err(format!("unsupported LDBC workload: {other}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub root: PathBuf,
    pub results_dir: PathBuf,
    pub warmup_runs: usize,
    pub measure_runs: usize,
    pub seeds_per_bucket: usize,
    pub max_paths: u64,
    pub modes: Vec<BenchmarkMode>,
    pub workloads: Vec<WorkloadKind>,
    pub rebuild_indexes: bool,
}

impl BenchmarkConfig {
    pub fn from_env() -> Result<Self, String> {
        let root = env::var("LDBC_SNB_ROOT")
            .map(PathBuf::from)
            .map_err(|_| "LDBC_SNB_ROOT must point to a prepared SF1 directory".to_string())?;
        let results_dir = env::var("LDBC_SNB_RESULTS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| root.join("results"));
        let modes = parse_list(
            "LDBC_SNB_MODES",
            "join,csr,direct,covering",
            BenchmarkMode::parse,
        )?;
        let workloads = parse_list(
            "LDBC_SNB_WORKLOADS",
            "one_hop,two_hop,two_hop_distinct,three_hop",
            WorkloadKind::parse,
        )?;
        if modes.is_empty() {
            return Err("LDBC_SNB_MODES must contain at least one mode".into());
        }
        if workloads.is_empty() {
            return Err("LDBC_SNB_WORKLOADS must contain at least one workload".into());
        }
        Ok(Self {
            root,
            results_dir,
            warmup_runs: parse_usize("LDBC_SNB_WARMUP_RUNS", 2)?,
            measure_runs: parse_usize("LDBC_SNB_MEASURE_RUNS", 10)?,
            seeds_per_bucket: parse_usize("LDBC_SNB_SEEDS_PER_BUCKET", 5)?,
            max_paths: parse_u64("LDBC_SNB_MAX_PATHS", 1_000_000)?,
            modes,
            workloads,
            rebuild_indexes: parse_bool("LDBC_SNB_REBUILD_INDEXES", false)?,
        })
    }
}

fn parse_list<T>(
    name: &str,
    default: &str,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(parse)
        .collect()
}

fn parse_usize(name: &str, default: usize) -> Result<usize, String> {
    env::var(name)
        .map(|value| value.parse::<usize>().map_err(|error| error.to_string()))
        .unwrap_or(Ok(default))
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn parse_u64(name: &str, default: u64) -> Result<u64, String> {
    env::var(name)
        .map(|value| value.parse::<u64>().map_err(|error| error.to_string()))
        .unwrap_or(Ok(default))
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn parse_bool(name: &str, default: bool) -> Result<bool, String> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        other => Err(format!("invalid {name}: {other}")),
    }
}
