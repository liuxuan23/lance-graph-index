use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::{GetVNode, LanceGetVByIdExec};
use crate::node_lookup::NodeLookupRegistry;

pub struct GetVExtensionPlanner {
    pub node_lookups: Arc<dyn NodeLookupRegistry>,
}

impl fmt::Debug for GetVExtensionPlanner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("GetVExtensionPlanner").finish()
    }
}

#[async_trait]
impl ExtensionPlanner for GetVExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<GetVNode>() else {
            return Ok(None);
        };
        let Some(handle) = self
            .node_lookups
            .get(&node.lookup_ref().key)
            .map_err(|e| DataFusionError::Plan(e.to_string()))?
        else {
            return Err(DataFusionError::Plan(format!(
                "GetV lookup not found for {:?}",
                node.lookup_ref().key
            )));
        };
        if handle.metadata.scalar_index_name != node.lookup_ref().scalar_index_name
            || handle.metadata.dataset_version != node.lookup_ref().dataset_version
        {
            return Err(DataFusionError::Plan(
                "GetV node lookup changed during planning".into(),
            ));
        }
        let target_id = node
            .target_schema()
            .field_with_name(node.target_id_field())
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
        if target_id.data_type() != &handle.metadata.id_data_type {
            return Err(DataFusionError::Plan(format!(
                "GetV target ID type {:?} does not match lookup index type {:?}",
                target_id.data_type(),
                handle.metadata.id_data_type
            )));
        }
        let child = physical_inputs
            .first()
            .ok_or_else(|| DataFusionError::Plan("GetV missing physical child".into()))?;
        Ok(Some(Arc::new(
            LanceGetVByIdExec::try_new_with_scalar_index(
                child.clone(),
                handle.dataset.clone(),
                handle.scalar_index.clone(),
                node.input_id_column(),
                node.target_id_field(),
                node.target_variable(),
                node.target_schema().clone(),
                node.target_predicates().to_vec(),
                handle.metadata.scalar_index_name.clone(),
                handle.metadata.dataset_version,
                node.max_lookup_keys(),
            )?,
        )))
    }
}
