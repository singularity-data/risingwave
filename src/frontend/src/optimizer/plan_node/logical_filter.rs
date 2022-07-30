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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;

use super::{
    ColPrunable, CollectInputRef, LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::expr::{assert_input_ref, ExprImpl};
use crate::optimizer::plan_node::{BatchFilter, StreamFilter};
use crate::risingwave_common::error::Result;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalFilter` iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub base: PlanBase,
    predicate: Condition,
    input: PlanRef,
}

impl LogicalFilter {
    pub fn new(input: PlanRef, predicate: Condition) -> Self {
        let ctx = input.ctx();
        for cond in &predicate.conjunctions {
            assert_input_ref!(cond, input.schema().fields().len());
        }
        let schema = input.schema().clone();
        let column_cnt = schema.len();
        let pk_indices = input.pk_indices().to_vec();
        let mut functional_dependency = input.functional_dependency().clone();
        for i in &predicate.conjunctions {
            if let Some((col, _)) = i.as_eq_const() {
                functional_dependency.add_constant_column(column_cnt, &[col.index()])
            } else if let Some((left, right)) = i.as_eq_cond() {
                functional_dependency.add_functional_dependency_by_column_indices(
                    &[left.index()],
                    &[right.index()],
                    column_cnt,
                );
                functional_dependency.add_functional_dependency_by_column_indices(
                    &[right.index()],
                    &[left.index()],
                    column_cnt,
                );
            }
        }
        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);
        LogicalFilter {
            base,
            predicate,
            input,
        }
    }

    /// Create a `LogicalFilter` unless the predicate is always true
    pub fn create(input: PlanRef, predicate: Condition) -> PlanRef {
        if predicate.always_true() {
            input
        } else {
            LogicalFilter::new(input, predicate).into()
        }
    }

    /// the function will check if the predicate is bool expression
    pub fn create_with_expr(input: PlanRef, predicate: ExprImpl) -> PlanRef {
        let predicate = Condition::with_expr(predicate);
        Self::new(input, predicate).into()
    }

    /// Get the predicate of the logical join.
    pub fn predicate(&self) -> &Condition {
        &self.predicate
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        let input = self.input();
        let input_schema = input.schema();
        write!(
            f,
            "{} {{ predicate: {} }}",
            name,
            ConditionDisplay {
                condition: self.predicate(),
                input_schema
            }
        )
    }
}

impl PlanTreeNodeUnary for LogicalFilter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.predicate.clone())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let predicate = self.predicate().clone().rewrite_expr(&mut input_col_change);
        (Self::new(input, predicate), input_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalFilter}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_name(f, "LogicalFilter")
    }
}

impl ColPrunable for LogicalFilter {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let required_cols_bitset = FixedBitSet::from_iter(required_cols.iter().copied());

        let mut visitor = CollectInputRef::with_capacity(self.input().schema().len());
        self.predicate.visit_expr(&mut visitor);
        let predicate_required_cols: FixedBitSet = visitor.into();

        let mut predicate = self.predicate.clone();
        let input_required_cols = {
            let mut tmp = predicate_required_cols;
            tmp.union_with(&required_cols_bitset);
            tmp.ones().collect_vec()
        };
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        predicate = predicate.rewrite_expr(&mut mapping);

        let filter = LogicalFilter::new(self.input.prune_col(&input_required_cols), predicate);
        if input_required_cols == required_cols {
            filter.into()
        } else {
            // Given that `LogicalFilter` always has same schema of its input, if predicate's
            // `InputRef`s are not subset of the requirement of downstream operator, a
            // `LogicalProject` node should be added.
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = filter.schema().len();
            LogicalProject::with_mapping(
                filter.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl PredicatePushdown for LogicalFilter {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        self.input
            .predicate_pushdown(predicate.and(self.predicate.clone()))
    }
}

impl ToBatch for LogicalFilter {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchFilter::new(new_logical).into())
    }
}

impl ToStream for LogicalFilter {
    fn to_stream(&self) -> Result<PlanRef> {
        let new_input = self.input().to_stream()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(StreamFilter::new(new_logical).into())
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (filter, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((filter.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::LogicalValues;
    use crate::session::OptimizerContext;

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [2] will result in
    /// ```text
    /// Project(input_ref(1))
    ///   Filter(cond: input_ref(0)<5)
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_filter() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(None, DataType::Int32))),
                ],
            )
            .unwrap(),
        ));
        let filter: PlanRef =
            LogicalFilter::new(values.into(), Condition::with_expr(predicate)).into();

        // Perform the prune
        let required_cols = vec![2];
        let plan = filter.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 1);
        assert_eq_input_ref!(&project.exprs()[0], 1);

        let filter = project.input();
        let filter = filter.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[1]);
        assert_eq!(filter.schema().fields()[1], fields[2]);
        assert_eq!(filter.id().0, 3);

        let expr: ExprImpl = filter.predicate.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        let values = filter.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[1]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<null)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1, 0] will result in
    /// ```text
    /// Project(input_ref(1), input_ref(0))
    ///   Filter(cond: input_ref(1)<null)
    ///     TableScan(v1, v2)
    /// ```
    async fn test_prune_filter_with_order_required() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(None, DataType::Int32))),
                ],
            )
            .unwrap(),
        ));
        let filter: PlanRef =
            LogicalFilter::new(values.into(), Condition::with_expr(predicate)).into();

        // Perform the prune
        let required_cols = vec![1, 0];
        let plan = filter.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 1);
        assert_eq_input_ref!(&project.exprs()[1], 0);

        let filter = project.input();
        let filter = filter.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[0]);
        assert_eq!(filter.schema().fields()[1], fields[1]);

        let expr: ExprImpl = filter.predicate.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 1);
        let values = filter.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[0]);
        assert_eq!(values.schema().fields()[1], fields[1]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1, 2] will result in
    /// ```text
    ///   Filter(cond: input_ref(0)<5)
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_filter_no_project() {
        let ctx = OptimizerContext::mock().await;
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );

        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                ],
            )
            .unwrap(),
        ));
        let filter = LogicalFilter::new(values.into(), Condition::with_expr(predicate));

        // Perform the prune
        let required_cols = vec![1, 2];
        let plan = filter.prune_col(&required_cols);

        // Check the result
        let filter = plan.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[1]);
        assert_eq!(filter.schema().fields()[1], fields[2]);
        let expr: ExprImpl = filter.predicate.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);

        let values = filter.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[1]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }
}
