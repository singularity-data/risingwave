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

use std::{fmt, vec};

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{
    gen_filter_and_pushdown, BatchUpdate, ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::TableId;
use crate::expr::ExprImpl;
use crate::utils::{ColIndexMapping, Condition};

/// [`LogicalUpdate`] iterates on input relation, set some columns, and inject update records into
/// specified table.
///
/// It corresponds to the `UPDATE` statements in SQL.
#[derive(Debug, Clone)]
pub struct LogicalUpdate {
    pub base: PlanBase,
    table_source_name: String, // explain-only
    source_id: TableId,        // TODO: use SourceId
    input: PlanRef,
    exprs: Vec<ExprImpl>,
}

impl LogicalUpdate {
    /// Create a [`LogicalUpdate`] node. Used internally by optimizer.
    pub fn new(
        input: PlanRef,
        table_source_name: String,
        source_id: TableId,
        exprs: Vec<ExprImpl>,
    ) -> Self {
        let ctx = input.ctx();
        // TODO: support `RETURNING`.
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let base = PlanBase::new_logical(ctx, schema, vec![]);
        Self {
            base,
            table_source_name,
            source_id,
            input,
            exprs,
        }
    }

    /// Create a [`LogicalUpdate`] node. Used by planner.
    pub fn create(
        input: PlanRef,
        table_source_name: String,
        source_id: TableId,
        exprs: Vec<ExprImpl>,
    ) -> Result<Self> {
        Ok(Self::new(input, table_source_name, source_id, exprs))
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ table: {}, exprs: {:?} }}",
            name, self.table_source_name, self.exprs
        )
    }

    /// Get the logical update's source id.
    #[must_use]
    pub fn source_id(&self) -> TableId {
        self.source_id
    }

    pub fn exprs(&self) -> &[ExprImpl] {
        self.exprs.as_ref()
    }
}

impl PlanTreeNodeUnary for LogicalUpdate {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.table_source_name.clone(),
            self.source_id,
            self.exprs.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { LogicalUpdate }

impl fmt::Display for LogicalUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalUpdate")
    }
}

impl ColPrunable for LogicalUpdate {
    fn prune_col(&self, _required_cols: &[usize]) -> PlanRef {
        let required_cols: Vec<_> = (0..self.input.schema().len()).collect();
        self.clone_with_input(self.input.prune_col(&required_cols))
            .into()
    }
}

impl PredicatePushdown for LogicalUpdate {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond())
    }
}

impl ToBatch for LogicalUpdate {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchUpdate::new(new_logical).into())
    }
}

impl ToStream for LogicalUpdate {
    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("update should always be converted to batch plan");
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, crate::utils::ColIndexMapping)> {
        unreachable!("update should always be converted to batch plan");
    }
}
