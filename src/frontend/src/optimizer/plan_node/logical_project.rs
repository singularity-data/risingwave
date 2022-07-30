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

use std::collections::HashMap;
use std::fmt;
use std::string::String;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;

use super::{
    gen_filter_and_pushdown, BatchProject, ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, StreamProject, ToBatch, ToStream,
};
use crate::expr::{
    assert_input_ref, Expr, ExprDisplay, ExprImpl, ExprRewriter, ExprVisitor, InputRef,
};
use crate::optimizer::plan_node::CollectInputRef;
use crate::optimizer::property::{Distribution, FunctionalDependencySet, Order, RequiredDist};
use crate::utils::{ColIndexMapping, Condition, Substitute};

/// Construct a `LogicalProject` and dedup expressions.
/// expressions
#[derive(Default)]
pub struct LogicalProjectBuilder {
    exprs: Vec<ExprImpl>,
    exprs_index: HashMap<ExprImpl, usize>,
}

impl LogicalProjectBuilder {
    /// add an expression to the `LogicalProject` and return the column index of the project's
    /// output
    pub fn add_expr(&mut self, expr: &ExprImpl) -> usize {
        if let Some(idx) = self.exprs_index.get(expr) {
            *idx
        } else {
            let index = self.exprs.len();
            self.exprs.push(expr.clone());
            self.exprs_index.insert(expr.clone(), index);
            index
        }
    }

    pub fn expr_index(&self, expr: &ExprImpl) -> Option<usize> {
        if expr.has_subquery() {
            return None;
        }
        self.exprs_index.get(expr).copied()
    }

    pub fn exprs_num(&self) -> usize {
        self.exprs.len()
    }

    /// build the `LogicalProject` from `LogicalProjectBuilder`
    pub fn build(self, input: PlanRef) -> LogicalProject {
        LogicalProject::new(input, self.exprs)
    }
}
/// `LogicalProject` computes a set of expressions from its input relation.
#[derive(Debug, Clone)]
pub struct LogicalProject {
    pub base: PlanBase,
    exprs: Vec<ExprImpl>,
    input: PlanRef,
}
impl LogicalProject {
    pub fn new(input: PlanRef, exprs: Vec<ExprImpl>) -> Self {
        assert!(
            exprs.iter().all(|e| !e.has_table_function()),
            "Project should not have table function."
        );

        let ctx = input.ctx();
        let schema = Self::derive_schema(&exprs, input.schema());
        let pk_indices = Self::derive_pk(input.schema(), input.pk_indices(), &exprs);
        for expr in &exprs {
            assert_input_ref!(expr, input.schema().fields().len());
            assert!(!expr.has_subquery());
            assert!(!expr.has_agg_call());
        }
        let functional_dependency =
            Self::derive_fd(input.schema().len(), input.functional_dependency(), &exprs);
        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);
        LogicalProject { base, exprs, input }
    }

    /// get the Mapping of columnIndex from output column index to input column index
    fn o2i_col_mapping_inner(input_len: usize, exprs: &[ExprImpl]) -> ColIndexMapping {
        let mut map = vec![None; exprs.len()];
        for (i, expr) in exprs.iter().enumerate() {
            map[i] = match expr {
                ExprImpl::InputRef(input) => Some(input.index()),
                _ => None,
            }
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    fn i2o_col_mapping_inner(input_len: usize, exprs: &[ExprImpl]) -> ColIndexMapping {
        Self::o2i_col_mapping_inner(input_len, exprs).inverse()
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        Self::o2i_col_mapping_inner(self.input.schema().len(), self.exprs())
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        Self::i2o_col_mapping_inner(self.input.schema().len(), self.exprs())
    }

    pub fn create(input: PlanRef, exprs: Vec<ExprImpl>) -> PlanRef {
        Self::new(input, exprs).into()
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    ///
    /// `mapping` should maps from `(0..input_fields.len())` to a consecutive range starting from 0.
    ///
    /// This is useful in column pruning when we want to add a project to ensure the output schema
    /// is correct.
    pub fn with_mapping(input: PlanRef, mapping: ColIndexMapping) -> Self {
        if mapping.target_size() == 0 {
            // The mapping is empty, so the parent actually doesn't need the output of the input.
            // This can happen when the parent node only selects constant expressions.
            return LogicalProject::new(input, vec![]);
        };
        let mut input_refs = vec![None; mapping.target_size()];
        for (src, tar) in mapping.mapping_pairs() {
            assert_eq!(input_refs[tar], None);
            input_refs[tar] = Some(src);
        }
        let input_schema = input.schema();
        let exprs: Vec<ExprImpl> = input_refs
            .into_iter()
            .map(|i| i.unwrap())
            .map(|i| InputRef::new(i, input_schema.fields()[i].data_type()).into())
            .collect();

        LogicalProject::new(input, exprs)
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    pub fn with_out_fields(input: PlanRef, out_fields: &FixedBitSet) -> Self {
        LogicalProject::with_out_col_idx(input, out_fields.ones())
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    pub fn with_out_col_idx(input: PlanRef, out_fields: impl Iterator<Item = usize>) -> Self {
        let input_schema = input.schema();
        let exprs = out_fields
            .map(|index| InputRef::new(index, input_schema[index].data_type()).into())
            .collect();
        LogicalProject::new(input, exprs)
    }

    fn derive_schema(exprs: &[ExprImpl], input_schema: &Schema) -> Schema {
        let o2i = Self::o2i_col_mapping_inner(input_schema.len(), exprs);
        let fields = exprs
            .iter()
            .enumerate()
            .map(|(id, expr)| {
                // Get field info from o2i.
                let (name, sub_fields, type_name) = match o2i.try_map(id) {
                    Some(input_idx) => {
                        let field = input_schema.fields()[input_idx].clone();
                        (field.name, field.sub_fields, field.type_name)
                    }
                    None => (
                        format!("{:?}", ExprDisplay { expr, input_schema }),
                        vec![],
                        String::new(),
                    ),
                };
                Field::with_struct(expr.return_type(), name, sub_fields, type_name)
            })
            .collect();
        Schema { fields }
    }

    fn derive_pk(input_schema: &Schema, input_pk: &[usize], exprs: &[ExprImpl]) -> Vec<usize> {
        let i2o = Self::i2o_col_mapping_inner(input_schema.len(), exprs);
        input_pk
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default()
    }

    fn derive_fd(
        input_len: usize,
        input_fd_set: &FunctionalDependencySet,
        exprs: &[ExprImpl],
    ) -> FunctionalDependencySet {
        let i2o = Self::i2o_col_mapping_inner(input_len, exprs);
        let mut fd_set = FunctionalDependencySet::new();
        for fd in input_fd_set.as_dependencies() {
            if let Some(fd) = i2o.rewrite_functional_dependency(fd) {
                fd_set.add_functional_dependency(fd);
            }
        }
        fd_set
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.exprs
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        builder.field(
            "exprs",
            &self
                .exprs()
                .iter()
                .map(|expr| ExprDisplay {
                    expr,
                    input_schema: self.input.schema(),
                })
                .collect_vec(),
        );
        builder.finish()
    }

    pub fn is_identity(&self) -> bool {
        self.schema().len() == self.input.schema().len()
            && self
                .exprs
                .iter()
                .zip_eq(self.input.schema().fields())
                .enumerate()
                .all(|(i, (expr, field))| {
                    matches!(expr, ExprImpl::InputRef(input_ref) if **input_ref == InputRef::new(i, field.data_type()))
                })
    }

    pub fn try_as_projection(&self) -> Option<Vec<usize>> {
        self.exprs
            .iter()
            .enumerate()
            .map(|(_i, expr)| match expr {
                ExprImpl::InputRef(input_ref) => Some(input_ref.index),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.exprs, self.input)
    }
}

impl PlanTreeNodeUnary for LogicalProject {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.exprs.clone())
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let exprs = self
            .exprs
            .clone()
            .into_iter()
            .map(|expr| input_col_change.rewrite_expr(expr))
            .collect();
        let proj = Self::new(input, exprs);
        // change the input columns index will not change the output column index
        let out_col_change = ColIndexMapping::identity(self.schema().len());
        (proj, out_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalProject}

impl fmt::Display for LogicalProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_name(f, "LogicalProject")
    }
}

impl ColPrunable for LogicalProject {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let input_col_num = self.input().schema().len();
        let mut input_required_appeared = FixedBitSet::with_capacity(input_col_num);

        // Record each InputRef's index.
        let mut input_ref_collector = CollectInputRef::with_capacity(input_col_num);
        required_cols.iter().for_each(|i| {
            if let ExprImpl::InputRef(ref input_ref) = self.exprs[*i] {
                let input_idx = input_ref.index;
                input_required_appeared.put(input_idx);
            } else {
                input_ref_collector.visit_expr(&self.exprs[*i]);
            }
        });
        let input_required_cols = {
            let mut tmp = FixedBitSet::from(input_ref_collector);
            tmp.union_with(&input_required_appeared);
            tmp
        };

        let input_required_cols = input_required_cols.ones().collect_vec();
        let new_input = self.input.prune_col(&input_required_cols);
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        // Rewrite each InputRef with new index.
        let exprs = required_cols
            .iter()
            .map(|&id| mapping.rewrite_expr(self.exprs[id].clone()))
            .collect();

        // Reconstruct the LogicalProject.
        LogicalProject::new(new_input, exprs).into()
    }
}

impl PredicatePushdown for LogicalProject {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        // convert the predicate to one that references the child of the project
        let mut subst = Substitute {
            mapping: self.exprs.clone(),
        };
        let predicate = predicate.rewrite_expr(&mut subst);

        gen_filter_and_pushdown(self, Condition::true_cond(), predicate)
    }
}

impl ToBatch for LogicalProject {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input.clone());
        if let Some(input_proj) = new_input.as_batch_project() {
            let outer_project = new_logical;
            let inner_project = input_proj.as_logical();
            let mut subst = Substitute {
                mapping: inner_project.exprs().clone(),
            };
            let exprs = outer_project
                .exprs()
                .iter()
                .cloned()
                .map(|expr| subst.rewrite_expr(expr))
                .collect();
            Ok(BatchProject::new(LogicalProject::new(inner_project.input(), exprs)).into())
        } else {
            Ok(BatchProject::new(new_logical).into())
        }
    }
}

impl ToStream for LogicalProject {
    fn to_stream_with_dist_required(&self, required_dist: &RequiredDist) -> Result<PlanRef> {
        let input_required = if required_dist.satisfies(&RequiredDist::AnyShard) {
            RequiredDist::Any
        } else {
            let input_required = self
                .o2i_col_mapping()
                .rewrite_required_distribution(required_dist);
            match input_required {
                RequiredDist::PhysicalDist(dist) => match dist {
                    Distribution::Single => RequiredDist::Any,
                    _ => RequiredDist::PhysicalDist(dist),
                },
                _ => input_required,
            }
        };
        let new_input = self.input().to_stream_with_dist_required(&input_required)?;
        let new_logical = self.clone_with_input(new_input.clone());
        let stream_plan = if let Some(input_proj) = new_input.as_stream_project() {
            let outer_project = new_logical;
            let inner_project = input_proj.as_logical();
            let mut subst = Substitute {
                mapping: inner_project.exprs().clone(),
            };
            let exprs = outer_project
                .exprs()
                .iter()
                .cloned()
                .map(|expr| subst.rewrite_expr(expr))
                .collect();
            StreamProject::new(LogicalProject::new(inner_project.input(), exprs))
        } else {
            StreamProject::new(new_logical)
        };
        required_dist.enforce_if_not_satisfies(stream_plan.into(), &Order::any())
    }

    fn to_stream(&self) -> Result<PlanRef> {
        self.to_stream_with_dist_required(&RequiredDist::Any)
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (proj, out_col_change) = self.rewrite_with_input(input.clone(), input_col_change);

        // Add missing columns of input_pk into the select list.
        let input_pk = input.pk_indices();
        let i2o = Self::i2o_col_mapping_inner(input.schema().len(), proj.exprs());
        let col_need_to_add = input_pk.iter().cloned().filter(|i| i2o.try_map(*i) == None);
        let input_schema = input.schema();
        let exprs =
            proj.exprs()
                .iter()
                .cloned()
                .chain(col_need_to_add.map(|idx| {
                    InputRef::new(idx, input_schema.fields[idx].data_type.clone()).into()
                }))
                .collect();
        let proj = Self::new(input, exprs);
        // The added columns is at the end, so it will not change existing column indices.
        // But the target size of `out_col_change` should be the same as the length of the new
        // schema.
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::with_target_size(map, proj.base.schema.len());
        Ok((proj.into(), out_col_change))
    }
}
#[cfg(test)]
mod tests {

    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::LogicalValues;
    use crate::session::OptimizerContext;

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Project(1, input_ref(2), input_ref(0)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns `[1, 2]` will result in
    /// ```text
    /// Project(input_ref(1), input_ref(0)<5)
    ///   TableScan(v1, v3)
    /// ```
    async fn test_prune_project() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
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
        let project = LogicalProject::new(
            values.into(),
            vec![
                ExprImpl::Literal(Box::new(Literal::new(None, ty.clone()))),
                InputRef::new(2, ty.clone()).into(),
                ExprImpl::FunctionCall(Box::new(
                    FunctionCall::new(
                        Type::LessThan,
                        vec![
                            ExprImpl::InputRef(Box::new(InputRef::new(0, ty.clone()))),
                            ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                        ],
                    )
                    .unwrap(),
                )),
            ],
        );

        // Perform the prune
        let required_cols = vec![1, 2];
        let plan = project.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 1);

        let expr = project.exprs()[1].clone();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);

        let values = project.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[0]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }
}
