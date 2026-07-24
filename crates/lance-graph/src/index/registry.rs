use super::metadata::{CsrIndexHandle, GraphIndexKey};
use crate::error::{GraphError, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub trait GraphIndexRegistry: Send + Sync {
    fn get_csr(&self, key: &GraphIndexKey) -> Result<Option<Arc<CsrIndexHandle>>>;
}

#[derive(Debug, Default)]
pub struct InMemoryGraphIndexRegistry {
    indexes: RwLock<HashMap<GraphIndexKey, Arc<CsrIndexHandle>>>,
}

impl InMemoryGraphIndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_csr(&self, mut handle: CsrIndexHandle) -> Result<()> {
        if handle.metadata.num_vertices != handle.index.num_vertices()
            || handle.metadata.num_edges != handle.index.num_edges()
        {
            return Err(GraphError::PlanError {
                message: "CSR metadata does not match index dimensions".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        let mut indexes = self.indexes.write().map_err(|_| GraphError::PlanError {
            message: "index registry lock poisoned".into(),
            location: snafu::Location::new(file!(), line!(), column!()),
        })?;
        if let Some(previous) = indexes.get(&handle.metadata.key) {
            if handle.metadata.generation <= previous.metadata.generation {
                handle.metadata.generation = previous.metadata.generation.saturating_add(1);
            }
        }
        indexes.insert(handle.metadata.key.clone(), Arc::new(handle));
        Ok(())
    }
    pub fn remove(&self, key: &GraphIndexKey) -> Result<Option<Arc<CsrIndexHandle>>> {
        Ok(self
            .indexes
            .write()
            .map_err(|_| GraphError::PlanError {
                message: "index registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .remove(key))
    }
}
impl GraphIndexRegistry for InMemoryGraphIndexRegistry {
    fn get_csr(&self, key: &GraphIndexKey) -> Result<Option<Arc<CsrIndexHandle>>> {
        Ok(self
            .indexes
            .read()
            .map_err(|_| GraphError::PlanError {
                message: "index registry lock poisoned".into(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .get(key)
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{GraphIndexKey, GraphIndexMetadata, IndexDirection};
    use crate::CsrIndexBuilder;
    use arrow_schema::DataType;

    #[test]
    fn register_and_lookup_is_case_insensitive() {
        let index = CsrIndexBuilder::new()
            .with_num_vertices(2)
            .add_edge(0, 1)
            .try_build()
            .unwrap();
        let key = GraphIndexKey::new("KNOWS", "Person", "PERSON", IndexDirection::Outgoing);
        let handle = CsrIndexHandle {
            index: Arc::new(index),
            metadata: GraphIndexMetadata {
                key: key.clone(),
                source_id_field: "id".into(),
                target_id_field: "id".into(),
                id_data_type: DataType::Int64,
                num_vertices: 2,
                num_edges: 1,
                source_uri: None,
                source_version: None,
                generation: 7,
            },
        };
        let registry = InMemoryGraphIndexRegistry::new();
        registry.register_csr(handle).unwrap();
        let lookup = registry
            .get_csr(&GraphIndexKey::new(
                "knows",
                "person",
                "person",
                IndexDirection::Outgoing,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(lookup.metadata.generation, 7);
    }
}
