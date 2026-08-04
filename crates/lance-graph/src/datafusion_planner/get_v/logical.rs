use std::fmt;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::case_insensitive::qualify_column;
use crate::node_lookup::NodeLookupReference;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GetVNode {
    input: LogicalPlan,
    target_variable: String,
    input_id_column: String,
    target_id_field: String,
    lookup_ref: NodeLookupReference,
    target_schema: SchemaRef,
    target_predicates: Vec<Expr>,
    max_lookup_keys: usize,
    schema: DFSchemaRef,
}

impl GetVNode {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: LogicalPlan,
        target_variable: impl Into<String>,
        input_id_column: impl Into<String>,
        target_id_field: impl Into<String>,
        lookup_ref: NodeLookupReference,
        target_schema: SchemaRef,
        target_predicates: Vec<Expr>,
        max_lookup_keys: usize,
    ) -> Result<Self> {
        let target_variable = target_variable.into().to_lowercase();
        let input_id_column = input_id_column.into();
        let target_id_field = target_id_field.into();
        if max_lookup_keys == 0 {
            return Err(datafusion::common::DataFusionError::Plan(
                "GetV lookup batch size must be greater than zero".into(),
            ));
        }
        let input_id = input
            .schema()
            .field_with_unqualified_name(&input_id_column)
            .map_err(|_| {
                datafusion::common::DataFusionError::Plan(format!(
                    "GetV input ID column '{}' is missing",
                    input_id_column
                ))
            })?;
        let target_id = target_schema
            .field_with_name(&target_id_field)
            .map_err(|_| {
                datafusion::common::DataFusionError::Plan(format!(
                    "GetV target ID field '{}' is missing",
                    target_id_field
                ))
            })?;
        if input_id.data_type() != target_id.data_type() {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "GetV ID type mismatch: input {:?}, target {:?}",
                input_id.data_type(),
                target_id.data_type()
            )));
        }

        let mut fields = input.schema().as_arrow().fields().to_vec();
        for field in target_schema.fields() {
            let name = qualify_column(&target_variable, field.name());
            if fields.iter().any(|existing| existing.name() == &name) {
                return Err(datafusion::common::DataFusionError::Plan(format!(
                    "GetV output column '{}' collides with input",
                    name
                )));
            }
            fields.push(Arc::new(field.as_ref().clone().with_name(name)));
        }
        let schema = Arc::new(DFSchema::try_from(Arc::new(Schema::new(fields)))?);

        Ok(Self {
            input,
            target_variable,
            input_id_column,
            target_id_field,
            lookup_ref,
            target_schema,
            target_predicates,
            max_lookup_keys,
            schema,
        })
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
    pub fn target_variable(&self) -> &str {
        &self.target_variable
    }
    pub fn input_id_column(&self) -> &str {
        &self.input_id_column
    }
    pub fn target_id_field(&self) -> &str {
        &self.target_id_field
    }
    pub fn lookup_ref(&self) -> &NodeLookupReference {
        &self.lookup_ref
    }
    pub fn target_schema(&self) -> &SchemaRef {
        &self.target_schema
    }
    pub fn target_predicates(&self) -> &[Expr] {
        &self.target_predicates
    }
    pub fn max_lookup_keys(&self) -> usize {
        self.max_lookup_keys
    }
}

impl PartialOrd for GetVNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            (
                self.target_variable.as_str(),
                self.input_id_column.as_str(),
                self.target_id_field.as_str(),
                &self.lookup_ref,
                self.max_lookup_keys,
            )
                .cmp(&(
                    other.target_variable.as_str(),
                    other.input_id_column.as_str(),
                    other.target_id_field.as_str(),
                    &other.lookup_ref,
                    other.max_lookup_keys,
                )),
        )
    }
}

impl UserDefinedLogicalNodeCore for GetVNode {
    fn name(&self) -> &str {
        "GetV"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // Target predicates are evaluated by the internal Lance lookup, not by
        // this node's logical input. Exposing them here would make DataFusion
        // resolve target columns against the IndexedExpand child schema.
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GetV: target={} AS {}, input_id={}, target_id={}, scalar_index={}, dataset_version={}, max_lookup_keys={}",
            self.lookup_ref.key.target_label,
            self.target_variable,
            self.input_id_column,
            self.target_id_field,
            self.lookup_ref.scalar_index_name,
            self.lookup_ref.dataset_version,
            self.max_lookup_keys
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return Err(datafusion::common::DataFusionError::Plan(
                "GetV expects one input".into(),
            ));
        }
        if !exprs.is_empty() {
            return Err(datafusion::common::DataFusionError::Plan(
                "GetV does not expose target lookup predicates as child expressions".into(),
            ));
        }
        Self::try_new(
            inputs.remove(0),
            self.target_variable.clone(),
            self.input_id_column.clone(),
            self.target_id_field.clone(),
            self.lookup_ref.clone(),
            self.target_schema.clone(),
            self.target_predicates.clone(),
            self.max_lookup_keys,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use datafusion::logical_expr::{LogicalPlanBuilder, UserDefinedLogicalNodeCore};
    use lance_graph_catalog::SimpleTableSource;

    fn input_plan() -> LogicalPlan {
        let input_source = Arc::new(SimpleTableSource::new(Arc::new(Schema::new(vec![
            Field::new("rel__dst_id", DataType::Int64, false),
        ]))));
        LogicalPlanBuilder::scan("expanded", input_source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn lookup_ref() -> NodeLookupReference {
        NodeLookupReference {
            key: crate::node_lookup::NodeLookupKey::new("Person", "person_id"),
            scalar_index_name: "person_id_btree".into(),
            dataset_version: 1,
        }
    }

    fn target_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("person_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn appends_qualified_target_schema() {
        let node = GetVNode::try_new(
            input_plan(),
            "b",
            "rel__dst_id",
            "person_id",
            lookup_ref(),
            target_schema(),
            vec![],
            8192,
        )
        .unwrap();
        assert!(node
            .schema()
            .field_with_unqualified_name("b__person_id")
            .is_ok());
        assert!(node.schema().field_with_unqualified_name("b__name").is_ok());
        let plan = LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(node.clone()),
        });
        let explain = format!("{}", plan.display_indent());
        assert!(explain.contains("person_id_btree"));
        assert!(explain.contains("dataset_version=1"));
        assert!(node
            .with_exprs_and_inputs(vec![], vec![])
            .unwrap_err()
            .to_string()
            .contains("one input"));
    }

    #[test]
    fn validates_columns_types_collisions_and_batch_size() {
        assert!(GetVNode::try_new(
            input_plan(),
            "b",
            "missing",
            "person_id",
            lookup_ref(),
            target_schema(),
            vec![],
            8192,
        )
        .unwrap_err()
        .to_string()
        .contains("input ID column"));

        assert!(GetVNode::try_new(
            input_plan(),
            "b",
            "rel__dst_id",
            "missing",
            lookup_ref(),
            target_schema(),
            vec![],
            8192,
        )
        .unwrap_err()
        .to_string()
        .contains("target ID field"));

        let wrong_type = Arc::new(Schema::new(vec![Field::new(
            "person_id",
            DataType::UInt64,
            false,
        )]));
        assert!(GetVNode::try_new(
            input_plan(),
            "b",
            "rel__dst_id",
            "person_id",
            lookup_ref(),
            wrong_type,
            vec![],
            8192,
        )
        .unwrap_err()
        .to_string()
        .contains("type mismatch"));

        let collision = Arc::new(Schema::new(vec![Field::new(
            "dst_id",
            DataType::Int64,
            false,
        )]));
        assert!(GetVNode::try_new(
            input_plan(),
            "rel",
            "rel__dst_id",
            "dst_id",
            NodeLookupReference {
                key: crate::node_lookup::NodeLookupKey::new("Person", "dst_id"),
                ..lookup_ref()
            },
            collision,
            vec![],
            8192,
        )
        .unwrap_err()
        .to_string()
        .contains("collides"));

        assert!(GetVNode::try_new(
            input_plan(),
            "b",
            "rel__dst_id",
            "person_id",
            lookup_ref(),
            target_schema(),
            vec![],
            0,
        )
        .unwrap_err()
        .to_string()
        .contains("greater than zero"));
    }
}
