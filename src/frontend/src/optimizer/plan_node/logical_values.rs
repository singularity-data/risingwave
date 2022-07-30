// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::{fmt, vec};

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};

use super::{
    BatchValues, ColPrunable, LogicalFilter, PlanBase, PlanRef, PredicatePushdown, ToBatch,
    ToStream,
};
use crate::expr::{Expr, ExprImpl};
use crate::session::OptimizerContextRef;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalValues` builds rows according to a list of expressions
#[derive(Debug, Clone)]
pub struct LogicalValues {
    pub base: PlanBase,
    rows: Arc<[Vec<ExprImpl>]>,
}

impl LogicalValues {
    /// Create a [`LogicalValues`] node. Used internally by optimizer.
    pub fn new(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: OptimizerContextRef) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        let base = PlanBase::new_logical(ctx, schema, vec![]);
        Self {
            rows: rows.into(),
            base,
        }
    }

    /// Create a [`LogicalValues`] node. Used by planner.
    pub fn create(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: OptimizerContextRef) -> PlanRef {
        // No additional checks after binder.
        Self::new(rows, schema, ctx).into()
    }

    /// Get a reference to the logical values' rows.
    pub fn rows(&self) -> &[Vec<ExprImpl>] {
        self.rows.as_ref()
    }
}

impl_plan_tree_node_for_leaf! { LogicalValues }

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogicalValues")
            .field("rows", &self.rows)
            .field("schema", &self.schema())
            .finish()
    }
}

impl ColPrunable for LogicalValues {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let rows = self
            .rows
            .iter()
            .map(|row| required_cols.iter().map(|i| row[*i].clone()).collect())
            .collect();
        let fields = required_cols
            .iter()
            .map(|i| self.schema().fields[*i].clone())
            .collect();
        Self::new(rows, Schema { fields }, self.base.ctx.clone()).into()
    }
}

impl PredicatePushdown for LogicalValues {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalValues {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchValues::new(self.clone()).into())
    }
}

impl ToStream for LogicalValues {
    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        Err(RwError::from(ErrorCode::NotImplemented(
            "Stream values executor is unimplemented!".to_string(),
            None.into(),
        )))
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, crate::utils::ColIndexMapping)> {
        Err(RwError::from(ErrorCode::NotImplemented(
            "Stream values executor is unimplemented!".to_string(),
            None.into(),
        )))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataType, Datum};

    use super::*;
    use crate::expr::Literal;
    use crate::session::OptimizerContext;

    fn literal(val: i32) -> ExprImpl {
        Literal::new(Datum::Some(val.into()), DataType::Int32).into()
    }

    /// Pruning
    /// ```text
    /// Values([[0, 1, 2], [3, 4, 5])
    /// ```
    /// with required columns [0, 2] will result in
    /// ```text
    /// Values([[0, 2], [3, 5])
    /// ```
    #[tokio::test]
    async fn test_prune_filter() {
        let ctx = OptimizerContext::mock().await;
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ]);
        // Values([[0, 1, 2], [3, 4, 5])
        let values = LogicalValues::new(
            vec![
                vec![literal(0), literal(1), literal(2)],
                vec![literal(3), literal(4), literal(5)],
            ],
            schema,
            ctx,
        );

        let required_cols = vec![0, 2];
        let pruned = values.prune_col(&required_cols);

        let values = pruned.as_logical_values().unwrap();
        let rows: &[Vec<ExprImpl>] = values.rows();

        // expected output: Values([[0, 2], [3, 5])
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0][0], literal(0));
        assert_eq!(rows[0][1], literal(2));
        assert_eq!(rows[1].len(), 2);
        assert_eq!(rows[1][0], literal(3));
        assert_eq!(rows[1][1], literal(5));
    }
}
