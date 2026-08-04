use super::{IndexedExpandExec, IndexedExpandNode};
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
        let Some(node) = node.as_any().downcast_ref::<IndexedExpandNode>() else {
            return Ok(None);
        };
        let Some(handle) = self
            .indexes
            .get_csr(&node.index_ref().key)
            .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))?
        else {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "CSR index not found for {:?} generation {}",
                node.index_ref().key,
                node.index_ref().generation
            )));
        };
        if handle.metadata.generation != node.index_ref().generation {
            return Err(datafusion::common::DataFusionError::Plan(
                "CSR index generation changed during planning".into(),
            ));
        }
        let child = physical_inputs.first().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan("IndexedExpand missing physical child".into())
        })?;
        let field = node
            .schema()
            .field_with_unqualified_name(node.output_target_column())
            .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))?
            .clone();
        Ok(Some(Arc::new(IndexedExpandExec::try_new(
            child.clone(),
            handle.index.clone(),
            node.source_column(),
            field.into(),
            node.max_output_batch_rows(),
        )?)))
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
