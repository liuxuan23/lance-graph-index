// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! DataFusion-based physical planner for graph queries
//!
//! Translates graph logical plans into DataFusion logical plans using a two-phase approach:
//!
//! ## Phase 1: Analysis
//! - Assigns unique IDs to relationship instances to avoid column conflicts
//! - Collects variable-to-label mappings and required datasets
//!
//! ## Phase 2: Plan Building
//! - Nodes -> Table scans, Relationships -> Linking tables, Traversals -> Joins
//! - Variable-length paths (`*1..3`) use unrolling: generate fixed-length plans + UNION
//! - All columns qualified as `{variable}__{column}` to avoid ambiguity

pub mod analysis;
mod builder;
mod config_helpers;
mod expression;
pub mod get_v;
pub mod indexed_expand;
mod join_ops;
mod scan_ops;
mod udf;
pub mod vector_ops;

#[cfg(test)]
mod test_fixtures;

// Re-export public types
pub use analysis::{PlanningContext, QueryAnalysis, RelationshipInstance};

use crate::config::GraphConfig;
use crate::error::Result;
use crate::index::{
    ExpandExecutionMode, ExpandIndexReference, ExpandPlanDecision, GraphIndexKey,
    GraphIndexRegistry, IndexReference,
};
use crate::logical_plan::LogicalOperator;
use crate::node_lookup::{NodeLookupKey, NodeLookupReference, NodeLookupRegistry};
use datafusion::logical_expr::LogicalPlan;
use lance_graph_catalog::GraphSourceCatalog;
use std::sync::Arc;

/// Planner abstraction for graph-to-physical planning
pub trait GraphPhysicalPlanner {
    fn plan(&self, logical_plan: &LogicalOperator) -> Result<LogicalPlan>;
}

/// DataFusion-based physical planner
pub struct DataFusionPlanner {
    pub(crate) config: GraphConfig,
    pub(crate) catalog: Option<Arc<dyn GraphSourceCatalog>>,
    pub(crate) indexes: Option<Arc<dyn GraphIndexRegistry>>,
    pub(crate) node_lookups: Option<Arc<dyn NodeLookupRegistry>>,
    pub(crate) expand_mode: ExpandExecutionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAccessFallbackReason {
    NodeLookupRegistryMissing,
    TargetProviderNotLance,
    TargetScalarIndexNotFound,
    TargetIdTypeMismatch,
    TargetVariableReused,
    UnsupportedTargetPredicate,
    StaleTargetDataset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAccessDecision {
    GetV(NodeLookupReference),
    Join(TargetAccessFallbackReason),
}

impl DataFusionPlanner {
    pub fn new(config: GraphConfig) -> Self {
        Self {
            config,
            catalog: None,
            indexes: None,
            node_lookups: None,
            expand_mode: ExpandExecutionMode::Join,
        }
    }

    pub fn with_catalog(config: GraphConfig, catalog: Arc<dyn GraphSourceCatalog>) -> Self {
        Self {
            config,
            catalog: Some(catalog),
            indexes: None,
            node_lookups: None,
            expand_mode: ExpandExecutionMode::Join,
        }
    }

    pub fn with_indexes(
        mut self,
        indexes: Arc<dyn GraphIndexRegistry>,
        expand_mode: ExpandExecutionMode,
    ) -> Self {
        self.indexes = Some(indexes);
        self.expand_mode = expand_mode;
        self
    }

    pub fn with_node_lookups(mut self, node_lookups: Arc<dyn NodeLookupRegistry>) -> Self {
        self.node_lookups = Some(node_lookups);
        self
    }

    pub fn expand_mode(&self) -> &ExpandExecutionMode {
        &self.expand_mode
    }

    pub(crate) fn select_expand_index(
        &self,
        relationship_types: &[String],
        source_label: &str,
        target_label: &str,
        direction: &crate::ast::RelationshipDirection,
        relationship_variable: Option<&str>,
        relationship_properties_empty: bool,
        source_id_type: &arrow_schema::DataType,
        target_id_type: &arrow_schema::DataType,
        target_variable_reused: bool,
    ) -> Result<ExpandPlanDecision> {
        if self.expand_mode == ExpandExecutionMode::Join {
            return Ok(ExpandPlanDecision::Join);
        }
        let invalid = |reason: &str| -> Result<ExpandPlanDecision> {
            Err(crate::error::GraphError::PlanError {
                message: format!(
                    "requested {:?} expand index is not eligible: {reason}",
                    self.expand_mode
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            })
        };
        if relationship_types.len() != 1 {
            return invalid("multiple relationship types");
        }
        let index_direction = match direction {
            crate::ast::RelationshipDirection::Outgoing => crate::index::IndexDirection::Outgoing,
            crate::ast::RelationshipDirection::Incoming => crate::index::IndexDirection::Incoming,
            crate::ast::RelationshipDirection::Undirected => {
                return invalid("undirected traversal");
            }
        };
        if relationship_variable.is_some() {
            return invalid("relationship variable is used");
        }
        if !relationship_properties_empty {
            return invalid("relationship properties are required");
        }
        if target_variable_reused {
            return invalid("target variable is reused");
        }
        if !matches!(
            source_id_type,
            arrow_schema::DataType::UInt32
                | arrow_schema::DataType::UInt64
                | arrow_schema::DataType::Int32
                | arrow_schema::DataType::Int64
        ) {
            return invalid("unsupported ID type");
        }
        if source_id_type != target_id_type {
            return invalid("source and target ID types differ");
        }
        let key = GraphIndexKey::new(
            &relationship_types[0],
            source_label,
            target_label,
            index_direction,
        );
        let Some(registry) = &self.indexes else {
            return Err(crate::error::GraphError::PlanError {
                message: format!(
                    "requested {:?} expand index requires a graph index registry",
                    self.expand_mode
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        };
        match &self.expand_mode {
            ExpandExecutionMode::Csr => {
                let handle =
                    registry
                        .get_csr(&key)?
                        .ok_or_else(|| crate::error::GraphError::PlanError {
                            message: format!("requested CSR index not found for {key:?}"),
                            location: snafu::Location::new(file!(), line!(), column!()),
                        })?;
                if handle.metadata.id_data_type != *source_id_type {
                    return invalid("CSR metadata ID type mismatch");
                }
                Ok(ExpandPlanDecision::Indexed(ExpandIndexReference::Csr(
                    IndexReference {
                        key,
                        generation: handle.metadata.generation,
                    },
                )))
            }
            ExpandExecutionMode::DirectAdjacency { index_name } => {
                let bundle = registry
                    .get_direct_adjacency_bundle(index_name)?
                    .ok_or_else(|| crate::error::GraphError::PlanError {
                        message: format!(
                            "requested Direct Adjacency bundle {index_name:?} is not registered"
                        ),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;
                let handle = registry
                    .get_direct_adjacency(index_name, &key)?
                    .ok_or_else(|| {
                    crate::error::GraphError::PlanError {
                        message: format!(
                            "requested Direct Adjacency component not found in bundle {index_name:?} for {key:?}"
                        ),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    }
                })?;
                if handle.metadata.id_data_type != *source_id_type {
                    return invalid("Direct Adjacency metadata ID type mismatch");
                }
                Ok(ExpandPlanDecision::Indexed(
                    ExpandIndexReference::DirectAdjacency(
                        handle.reference(index_name, bundle.metadata.bundle_generation),
                    ),
                ))
            }
            ExpandExecutionMode::Join => unreachable!(),
        }
    }

    pub(crate) fn select_target_access(
        &self,
        target_label: &str,
        target_id_field: &str,
        target_id_type: &arrow_schema::DataType,
        target_variable_reused: bool,
    ) -> Result<TargetAccessDecision> {
        if target_variable_reused {
            return Ok(TargetAccessDecision::Join(
                TargetAccessFallbackReason::TargetVariableReused,
            ));
        }
        let Some(registry) = &self.node_lookups else {
            return Ok(TargetAccessDecision::Join(
                TargetAccessFallbackReason::NodeLookupRegistryMissing,
            ));
        };
        let key = NodeLookupKey::new(target_label, target_id_field);
        let Some(handle) = registry.get(&key)? else {
            return Ok(TargetAccessDecision::Join(
                TargetAccessFallbackReason::TargetScalarIndexNotFound,
            ));
        };
        if &handle.metadata.id_data_type != target_id_type {
            return Ok(TargetAccessDecision::Join(
                TargetAccessFallbackReason::TargetIdTypeMismatch,
            ));
        }
        if handle.dataset.version().version != handle.metadata.dataset_version {
            return Ok(TargetAccessDecision::Join(
                TargetAccessFallbackReason::StaleTargetDataset,
            ));
        }
        Ok(TargetAccessDecision::GetV(handle.reference()))
    }

    /// Helper to convert DataFusion builder errors into GraphError::PlanError with context
    pub(crate) fn plan_error<E: std::fmt::Display>(
        &self,
        context: &str,
        error: E,
    ) -> crate::error::GraphError {
        crate::error::GraphError::PlanError {
            message: format!("{}: {}", context, error),
            location: snafu::Location::new(file!(), line!(), column!()),
        }
    }
}

impl GraphPhysicalPlanner for DataFusionPlanner {
    fn plan(&self, logical_plan: &LogicalOperator) -> Result<LogicalPlan> {
        // Phase 1: Analyze query structure
        let analysis = analysis::analyze(logical_plan)?;

        // Phase 2: Build execution plan with context
        let mut ctx = PlanningContext::new(&analysis);
        self.build_operator(&mut ctx, logical_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{
        BooleanExpression, ComparisonOperator, PropertyRef, PropertyValue, RelationshipDirection,
        ValueExpression,
    };
    use crate::logical_plan::LogicalOperator;
    use test_fixtures::{make_catalog, person_config, person_knows_config, person_scan};

    #[test]
    fn test_filter_preserves_error_context() {
        // Test that filter errors include helpful context
        let planner = DataFusionPlanner::with_catalog(person_config(), make_catalog());

        let scan = person_scan("p");

        // Create a filter with a property reference
        let filter = LogicalOperator::Filter {
            input: Box::new(scan),
            predicate: BooleanExpression::Comparison {
                left: ValueExpression::Property(PropertyRef {
                    variable: "p".to_string(),
                    property: "age".to_string(),
                }),
                operator: ComparisonOperator::GreaterThan,
                right: ValueExpression::Literal(PropertyValue::Integer(30)),
            },
        };

        let result = planner.plan(&filter);

        // Should succeed - this tests that valid filters work
        assert!(result.is_ok(), "Valid filter should succeed");
    }

    #[test]
    fn test_exists_on_relationship_property_is_qualified() {
        // Test that EXISTS on relationship properties uses qualified column names
        let planner = DataFusionPlanner::with_catalog(person_knows_config(), make_catalog());

        let scan_a = person_scan("a");
        let expand = LogicalOperator::Expand {
            input: Box::new(scan_a),
            source_variable: "a".to_string(),
            target_variable: "b".to_string(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".to_string()],
            direction: RelationshipDirection::Outgoing,
            relationship_variable: Some("r".to_string()),
            properties: Default::default(),
            target_properties: Default::default(),
        };
        let pred = BooleanExpression::Exists(PropertyRef {
            variable: "r".into(),
            property: "src_person_id".into(),
        });
        let filter = LogicalOperator::Filter {
            input: Box::new(expand),
            predicate: pred,
        };
        let df_plan = planner.plan(&filter).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(s.contains("Filter"), "missing Filter: {}", s);
        assert!(
            s.contains("r__src_person_id") || s.contains("IsNotNull"),
            "missing qualified rel column or IsNotNull in filter: {}",
            s
        );
    }

    #[test]
    fn test_in_list_on_relationship_property_is_qualified() {
        // Test that IN lists on relationship properties use qualified column names
        let planner = DataFusionPlanner::with_catalog(person_knows_config(), make_catalog());

        let scan_a = person_scan("a");
        let expand = LogicalOperator::Expand {
            input: Box::new(scan_a),
            source_variable: "a".to_string(),
            target_variable: "b".to_string(),
            target_label: "Person".to_string(),
            relationship_types: vec!["KNOWS".to_string()],
            direction: RelationshipDirection::Outgoing,
            relationship_variable: Some("r".to_string()),
            properties: Default::default(),
            target_properties: Default::default(),
        };
        let filter = LogicalOperator::Filter {
            input: Box::new(expand),
            predicate: BooleanExpression::In {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "r".into(),
                    property: "src_person_id".into(),
                }),
                list: vec![
                    ValueExpression::Literal(PropertyValue::Integer(1)),
                    ValueExpression::Literal(PropertyValue::Integer(2)),
                ],
            },
        };
        let df_plan = planner.plan(&filter).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(s.contains("Filter"), "missing Filter: {}", s);
        assert!(
            s.contains("r__src_person_id"),
            "missing qualified rel column in IN list filter: {}",
            s
        );
    }

    #[test]
    fn test_exists_and_in_on_node_props_materialized() {
        // Test that EXISTS and IN expressions on node properties work correctly
        let planner = DataFusionPlanner::with_catalog(person_config(), make_catalog());

        let scan_a = person_scan("a");
        let pred = BooleanExpression::And(
            Box::new(BooleanExpression::Exists(PropertyRef {
                variable: "a".into(),
                property: "name".into(),
            })),
            Box::new(BooleanExpression::In {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "a".into(),
                    property: "age".into(),
                }),
                list: vec![
                    ValueExpression::Literal(PropertyValue::Integer(20)),
                    ValueExpression::Literal(PropertyValue::Integer(30)),
                ],
            }),
        );
        let filter = LogicalOperator::Filter {
            input: Box::new(scan_a),
            predicate: pred,
        };
        let df_plan = planner.plan(&filter).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(s.contains("Filter"), "missing Filter: {}", s);
        assert!(
            s.contains("a__name") || s.contains("IsNotNull"),
            "missing EXISTS on a__name: {}",
            s
        );
        assert!(
            s.contains("a__age") || s.contains("age"),
            "missing IN on a.age: {}",
            s
        );
    }
}
