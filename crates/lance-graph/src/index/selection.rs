use super::{CoveringAdjacencyReference, DirectAdjacencyReference, IndexReference};

use crate::error::{GraphError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ExpandExecutionMode {
    #[default]
    Join,
    Csr,
    DirectAdjacency {
        index_name: String,
    },
    CoveringAdjacency {
        index_name: String,
    },
}

impl ExpandExecutionMode {
    pub fn direct_adjacency(index_name: impl Into<String>) -> Result<Self> {
        let index_name = index_name.into().trim().to_lowercase();
        if index_name.is_empty() {
            return Err(GraphError::PlanError {
                message: "direct adjacency index name must not be empty".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        Ok(Self::DirectAdjacency { index_name })
    }

    pub fn covering_adjacency(index_name: impl Into<String>) -> Result<Self> {
        let index_name = index_name.into().trim().to_lowercase();
        if index_name.is_empty() {
            return Err(GraphError::PlanError {
                message: "covering adjacency index name must not be empty".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        Ok(Self::CoveringAdjacency { index_name })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ExpandIndexReference {
    Csr(IndexReference),
    DirectAdjacency(DirectAdjacencyReference),
    CoveringAdjacency(CoveringAdjacencyReference),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandPlanDecision {
    Join,
    Indexed(ExpandIndexReference),
}
