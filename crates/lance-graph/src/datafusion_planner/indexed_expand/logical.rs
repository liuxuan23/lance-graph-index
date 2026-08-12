use crate::index::ExpandIndexReference;
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::{DFSchema, DFSchemaRef, Result as DFResult};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AdjacencyExpandNode {
    input: LogicalPlan,
    source_column: String,
    output_target_column: String,
    index_ref: ExpandIndexReference,
    output_id_type: DataType,
    schema: DFSchemaRef,
    max_output_batch_rows: usize,
}

impl AdjacencyExpandNode {
    pub fn try_new(
        input: LogicalPlan,
        source_column: impl Into<String>,
        output_target_column: impl Into<String>,
        index_ref: ExpandIndexReference,
        output_id_type: DataType,
        max_output_batch_rows: usize,
    ) -> DFResult<Self> {
        let source_column = source_column.into();
        let output_target_column = output_target_column.into();
        if input
            .schema()
            .field_with_unqualified_name(&source_column)
            .is_err()
        {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "AdjacencyExpand source column '{}' is missing",
                source_column
            )));
        }
        if input
            .schema()
            .field_with_unqualified_name(&output_target_column)
            .is_ok()
        {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "AdjacencyExpand output column '{}' collides with input",
                output_target_column
            )));
        }
        if max_output_batch_rows == 0 {
            return Err(datafusion::common::DataFusionError::Plan(
                "AdjacencyExpand batch size must be greater than zero".into(),
            ));
        }
        let mut fields = input.schema().as_arrow().fields().to_vec();
        fields.push(Arc::new(Field::new(
            &output_target_column,
            output_id_type.clone(),
            false,
        )));
        let schema = Arc::new(DFSchema::try_from(Arc::new(Schema::new(fields)))?);
        Ok(Self {
            input,
            source_column,
            output_target_column,
            index_ref,
            output_id_type,
            schema,
            max_output_batch_rows,
        })
    }
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
    pub fn source_column(&self) -> &str {
        &self.source_column
    }
    pub fn output_target_column(&self) -> &str {
        &self.output_target_column
    }
    pub fn index_ref(&self) -> &ExpandIndexReference {
        &self.index_ref
    }
    pub fn output_id_type(&self) -> &DataType {
        &self.output_id_type
    }
    pub fn max_output_batch_rows(&self) -> usize {
        self.max_output_batch_rows
    }
}

impl PartialOrd for AdjacencyExpandNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            (
                self.source_column.as_str(),
                self.output_target_column.as_str(),
                &self.index_ref,
                &self.output_id_type,
                self.max_output_batch_rows,
            )
                .cmp(&(
                    other.source_column.as_str(),
                    other.output_target_column.as_str(),
                    &other.index_ref,
                    &other.output_id_type,
                    other.max_output_batch_rows,
                )),
        )
    }
}

impl UserDefinedLogicalNodeCore for AdjacencyExpandNode {
    fn name(&self) -> &str {
        "AdjacencyExpand"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AdjacencyExpand: relationship_type={}, direction={:?}, source={}, target={}, index={:?}", self.index_ref_key().relationship_type, self.index_ref_key().direction, self.source_column, self.output_target_column, self.index_ref)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if inputs.len() != 1 {
            return Err(datafusion::common::DataFusionError::Plan(
                "AdjacencyExpand expects one input".into(),
            ));
        }
        Self::try_new(
            inputs.remove(0),
            self.source_column.clone(),
            self.output_target_column.clone(),
            self.index_ref.clone(),
            self.output_id_type.clone(),
            self.max_output_batch_rows,
        )
    }
}

impl AdjacencyExpandNode {
    fn index_ref_key(&self) -> &crate::index::GraphIndexKey {
        match &self.index_ref {
            ExpandIndexReference::Csr(reference) => &reference.key,
            ExpandIndexReference::DirectAdjacency(reference) => &reference.key,
        }
    }
}
