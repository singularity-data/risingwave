use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, LogicalBase, PlanRef, ToBatch, ToStream};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::property::WithSchema;
use crate::session::QueryContextRef;

/// `LogicalValues` builds rows according to a list of expressions
#[derive(Debug, Clone)]
pub struct LogicalValues {
    pub base: LogicalBase,
    rows: Vec<Vec<ExprImpl>>,
}

impl LogicalValues {
    /// Create a LogicalValues node. Used internally by optimizer.
    pub fn new(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: QueryContextRef) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        let base = LogicalBase {
            schema,
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        Self { rows, base }
    }

    /// Create a LogicalValues node. Used by planner.
    pub fn create(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: QueryContextRef) -> Result<Self> {
        // No additional checks after binder.
        Ok(Self::new(rows, schema, ctx))
    }

    /// Get a reference to the logical values' rows.
    pub fn rows(&self) -> &[Vec<ExprImpl>] {
        self.rows.as_ref()
    }
}

impl_plan_tree_node_for_leaf! {LogicalValues}

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogicalValues")
            .field("rows", &self.rows)
            .field("schema", &self.schema())
            .finish()
    }
}

impl ColPrunable for LogicalValues {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let (rows, fields) = required_cols
            .ones()
            .map(|id| (self.rows[id].clone(), self.schema().fields[id].clone()))
            .unzip();
        Self::new(rows, Schema { fields }, self.base.ctx.clone()).into()
    }
}

impl ToBatch for LogicalValues {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}

impl ToStream for LogicalValues {
    fn to_stream(&self) -> PlanRef {
        unimplemented!("Stream values executor is unimplemented!")
    }
}
