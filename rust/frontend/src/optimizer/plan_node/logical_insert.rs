use std::fmt;

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{ColPrunable, IntoPlanRef, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::catalog::{ColumnId, TableId};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LogicalInsert {
    table_id: TableId,
    columns: Vec<ColumnId>,
    input: PlanRef,
    schema: Schema,
}

impl LogicalInsert {
    /// Create a LogicalInsert node. Used internally by optimizer.
    pub fn new(input: PlanRef, table_id: TableId, columns: Vec<ColumnId>) -> Self {
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        Self {
            table_id,
            columns,
            input,
            schema,
        }
    }

    /// Create a LogicalInsert node. Used by planner.
    pub fn create(input: PlanRef, table_id: TableId, columns: Vec<ColumnId>) -> Result<Self> {
        Ok(Self::new(input, table_id, columns))
    }
}

impl PlanTreeNodeUnary for LogicalInsert {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.table_id, self.columns.clone())
    }
}
impl_plan_tree_node_for_unary! {LogicalInsert}

impl WithSchema for LogicalInsert {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
impl WithOrder for LogicalInsert {}
impl WithDistribution for LogicalInsert {}

impl fmt::Display for LogicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl ColPrunable for LogicalInsert {}
impl ToBatch for LogicalInsert {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}
impl ToStream for LogicalInsert {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}
