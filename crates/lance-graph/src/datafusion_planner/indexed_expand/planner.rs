use super::{AdjacencyExpandNode, IndexedExpandExec};
use crate::datafusion_planner::get_v::GetVExtensionPlanner;
use crate::index::GraphIndexRegistry;
use crate::node_lookup::NodeLookupRegistry;
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use std::fmt;
use std::sync::Arc;

pub struct IndexedExpandExtensionPlanner {
    pub indexes: Arc<dyn GraphIndexRegistry>,
}
impl fmt::Debug for IndexedExpandExtensionPlanner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexedExpandExtensionPlanner").finish()
    }
}
#[async_trait]
impl ExtensionPlanner for IndexedExpandExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<AdjacencyExpandNode>() else {
            return Ok(None);
        };
        let child = physical_inputs.first().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan("IndexedExpand missing physical child".into())
        })?;
        let field = node
            .schema()
            .field_with_unqualified_name(node.output_target_column())
            .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))?
            .clone();
        match node.index_ref() {
            crate::index::ExpandIndexReference::Csr(reference) => {
                let Some(handle) = self
                    .indexes
                    .get_csr(&reference.key)
                    .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))?
                else {
                    return Err(datafusion::common::DataFusionError::Plan(format!(
                        "CSR index not found for {:?}",
                        reference.key
                    )));
                };
                if handle.metadata.generation != reference.generation {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "CSR index generation changed during planning".into(),
                    ));
                }
                Ok(Some(Arc::new(IndexedExpandExec::try_new(
                    child.clone(),
                    handle.index.clone(),
                    node.source_column(),
                    field.into(),
                    node.max_output_batch_rows(),
                )?)))
            }
            crate::index::ExpandIndexReference::DirectAdjacency(reference) => {
                let Some(handle) = self
                    .indexes
                    .get_direct_adjacency(&reference.index_name, &reference.key)
                    .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))?
                else {
                    return Err(datafusion::common::DataFusionError::Plan(format!(
                        "Direct Adjacency index not found for {:?}",
                        reference.key
                    )));
                };
                let bundle = self
                    .indexes
                    .get_direct_adjacency_bundle(&reference.index_name)
                    .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))?
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Plan(format!(
                            "Direct Adjacency bundle {:?} not found",
                            reference.index_name
                        ))
                    })?;
                if bundle.metadata.bundle_generation != reference.bundle_generation
                    || handle.metadata.generation != reference.component_generation
                    || handle.metadata.dataset_version != reference.dataset_version
                {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "Direct Adjacency index generation changed during planning".into(),
                    ));
                }
                Ok(Some(Arc::new(
                    super::physical::DirectAdjacencyExpandExec::try_new(
                        child.clone(),
                        handle,
                        node.source_column(),
                        field.into(),
                        node.max_output_batch_rows(),
                    )?
                    .with_bundle_identity(
                        reference.index_name.clone(),
                        reference.bundle_generation,
                    ),
                )))
            }
            crate::index::ExpandIndexReference::CoveringAdjacency(reference) => {
                let handle = self
                    .indexes
                    .get_covering_adjacency(&reference.index_name, &reference.key)
                    .map_err(|error| datafusion::common::DataFusionError::Plan(error.to_string()))?
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Plan(format!(
                            "Covering Adjacency index not found for {:?}",
                            reference.key
                        ))
                    })?;
                let bundle = self
                    .indexes
                    .get_covering_adjacency_bundle(&reference.index_name)
                    .map_err(|error| datafusion::common::DataFusionError::Plan(error.to_string()))?
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Plan(format!(
                            "Covering Adjacency bundle {:?} not found",
                            reference.index_name
                        ))
                    })?;
                if bundle.metadata.bundle_generation != reference.bundle_generation
                    || handle.metadata.generation != reference.component_generation
                    || handle.metadata.format_version != reference.format_version
                {
                    return Err(datafusion::common::DataFusionError::Plan(
                        "Covering Adjacency index generation changed during planning".into(),
                    ));
                }
                Ok(Some(Arc::new(
                    super::physical::CoveringAdjacencyExpandExec::try_new(
                        child.clone(),
                        handle,
                        node.source_column(),
                        field.into(),
                        node.max_output_batch_rows(),
                    )?
                    .with_bundle_identity(
                        reference.index_name.clone(),
                        reference.bundle_generation,
                    ),
                )))
            }
        }
    }
}

pub struct GraphQueryPlanner {
    pub indexes: Arc<dyn GraphIndexRegistry>,
    pub node_lookups: Arc<dyn NodeLookupRegistry>,
}
impl fmt::Debug for GraphQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("GraphQueryPlanner").finish()
    }
}
#[async_trait]
impl datafusion::execution::context::QueryPlanner for GraphQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(IndexedExpandExtensionPlanner {
                indexes: self.indexes.clone(),
            }),
            Arc::new(GetVExtensionPlanner {
                node_lookups: self.node_lookups.clone(),
            }),
        ])
        .create_physical_plan(logical_plan, session_state)
        .await
    }
}
