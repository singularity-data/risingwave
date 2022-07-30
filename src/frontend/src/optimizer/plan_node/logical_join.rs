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
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::QueryMode;
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::plan_common::JoinType;

use super::{
    BatchProject, ColPrunable, CollectInputRef, LogicalProject, PlanBase, PlanNodeType, PlanRef,
    PlanTreeNodeBinary, PlanTreeNodeUnary, PredicatePushdown, StreamHashJoin, StreamProject,
    ToBatch, ToStream,
};
use crate::expr::{align_types, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    BatchFilter, BatchHashJoin, BatchLookupJoin, BatchNestedLoopJoin, EqJoinPredicate,
    LogicalFilter, StreamDynamicFilter, StreamFilter,
};
use crate::optimizer::property::{Distribution, RequiredDist};
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalJoin` combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition. In addition, the output columns are a subset of the columns of the left and
/// right columns, dependent on the output indices provided. A repeat output index is illegal.
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub base: PlanBase,
    left: PlanRef,
    right: PlanRef,
    on: Condition,
    join_type: JoinType,
    output_indices: Vec<usize>,
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("LogicalJoin");
        builder.field("type", &format_args!("{:?}", self.join_type()));

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "on",
            &format_args!(
                "{}",
                ConditionDisplay {
                    condition: self.on(),
                    input_schema: &concat_schema
                }
            ),
        );

        if verbose {
            if self
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &format_args!(
                        "{:?}",
                        &IndicesDisplay {
                            vec: self.output_indices(),
                            input_schema: &concat_schema,
                        }
                    ),
                );
            }
        }

        builder.finish()
    }
}

fn has_duplicate_index(indices: &[usize]) -> bool {
    for i in 1..indices.len() {
        if indices[i..].contains(&indices[i - 1]) {
            return true;
        }
    }
    false
}

impl LogicalJoin {
    pub(crate) fn new(left: PlanRef, right: PlanRef, join_type: JoinType, on: Condition) -> Self {
        let out_column_num =
            Self::out_column_num(left.schema().len(), right.schema().len(), join_type);
        Self::new_with_output_indices(left, right, join_type, on, (0..out_column_num).collect())
    }

    pub(crate) fn new_with_output_indices(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
        output_indices: Vec<usize>,
    ) -> Self {
        assert!(!has_duplicate_index(&output_indices));
        let ctx = left.ctx();
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type, &output_indices);
        let pk_indices = Self::derive_pk(
            left.schema().len(),
            right.schema().len(),
            left.pk_indices(),
            right.pk_indices(),
            join_type,
            &output_indices,
        );
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalJoin {
            base,
            left,
            right,
            on,
            join_type,
            output_indices,
        }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on_clause: ExprImpl,
    ) -> PlanRef {
        Self::new(left, right, join_type, Condition::with_expr(on_clause)).into()
    }

    pub fn out_column_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                left_len + right_len
            }
            JoinType::LeftSemi | JoinType::LeftAnti => left_len,
            JoinType::RightSemi | JoinType::RightAnti => right_len,
            JoinType::Unspecified => unreachable!(),
        }
    }

    pub fn internal_column_num(&self) -> usize {
        Self::out_column_num(
            self.left.schema().len(),
            self.right.schema().len(),
            self.join_type,
        )
    }

    fn i2l_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::identity_or_none(left_len + right_len, left_len)
            }

            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::identity(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::empty(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    fn i2r_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::empty(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::identity(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    fn l2i_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::i2l_col_mapping_inner(left_len, right_len, join_type).inverse()
    }

    fn r2i_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::i2r_col_mapping_inner(left_len, right_len, join_type).inverse()
    }

    /// get the Mapping of columnIndex from internal column index to left column index
    pub fn i2l_col_mapping(&self) -> ColIndexMapping {
        Self::i2l_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from internal column index to right column index
    pub fn i2r_col_mapping(&self) -> ColIndexMapping {
        Self::i2r_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from left column index to internal column index
    pub fn l2i_col_mapping(&self) -> ColIndexMapping {
        Self::l2i_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from right column index to internal column index
    pub fn r2i_col_mapping(&self) -> ColIndexMapping {
        Self::r2i_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(&self.output_indices, self.internal_column_num())
    }

    /// get the Mapping of columnIndex from output column index to internal column index
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.i2o_col_mapping().inverse()
    }

    pub(super) fn derive_schema(
        left_schema: &Schema,
        right_schema: &Schema,
        join_type: JoinType,
        output_indices: &[usize],
    ) -> Schema {
        let left_len = left_schema.len();
        let right_len = right_schema.len();
        let i2l = Self::i2l_col_mapping_inner(left_len, right_len, join_type);
        let i2r = Self::i2r_col_mapping_inner(left_len, right_len, join_type);
        let fields = output_indices
            .iter()
            .map(|&i| match (i2l.try_map(i), i2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(
                    "left len {}, right len {}, i {}, lmap {:?}, rmap {:?}",
                    left_len, right_len, i, i2l, i2r
                ),
            })
            .collect();
        Schema { fields }
    }

    pub(super) fn derive_pk(
        left_len: usize,
        right_len: usize,
        left_pk: &[usize],
        right_pk: &[usize],
        join_type: JoinType,
        output_indices: &[usize],
    ) -> Vec<usize> {
        let l2i = Self::l2i_col_mapping_inner(left_len, right_len, join_type);
        let r2i = Self::r2i_col_mapping_inner(left_len, right_len, join_type);
        let out_col_num = Self::out_column_num(left_len, right_len, join_type);
        let i2o = ColIndexMapping::with_remaining_columns(output_indices, out_col_num);
        left_pk
            .iter()
            .map(|index| l2i.try_map(*index))
            .chain(right_pk.iter().map(|index| r2i.try_map(*index)))
            .flatten()
            .map(|index| i2o.try_map(index))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default()
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }

    /// Get the join type of the logical join.
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Get the output indices of the logical join.
    pub fn output_indices(&self) -> &[usize] {
        &self.output_indices
    }

    /// Clone with new output indices
    pub fn clone_with_output_indices(&self, output_indices: Vec<usize>) -> Self {
        Self::new_with_output_indices(
            self.left.clone(),
            self.right.clone(),
            self.join_type,
            self.on.clone(),
            output_indices,
        )
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::new_with_output_indices(
            self.left.clone(),
            self.right.clone(),
            self.join_type,
            cond,
            self.output_indices.clone(),
        )
    }

    pub fn is_left_join(&self) -> bool {
        matches!(self.join_type(), JoinType::LeftSemi | JoinType::LeftAnti)
    }

    pub fn is_right_join(&self) -> bool {
        matches!(self.join_type(), JoinType::RightSemi | JoinType::RightAnti)
    }

    /// Try to split and pushdown `predicate` into a join's left/right child or the on clause.
    /// Returns the pushed predicates. The pushed part will be removed from the original predicate.
    ///
    /// `InputRef`s in the right `Condition` are shifted by `-left_col_num`.
    fn push_down(
        predicate: &mut Condition,
        left_col_num: usize,
        right_col_num: usize,
        push_left: bool,
        push_right: bool,
        push_on: bool,
    ) -> (Condition, Condition, Condition) {
        let conjunctions = std::mem::take(&mut predicate.conjunctions);
        let (mut left, right, mut others) =
            Condition { conjunctions }.split(left_col_num, right_col_num);

        let mut cannot_push = vec![];

        if !push_left {
            cannot_push.extend(left);
            left = Condition::true_cond();
        };

        let right = if push_right {
            let mut mapping = ColIndexMapping::with_shift_offset(
                left_col_num + right_col_num,
                -(left_col_num as isize),
            );
            right.rewrite_expr(&mut mapping)
        } else {
            cannot_push.extend(right);
            Condition::true_cond()
        };

        let on = if push_on {
            others.conjunctions.extend(std::mem::take(&mut cannot_push));
            others
        } else {
            cannot_push.extend(others);
            Condition::true_cond()
        };

        predicate.conjunctions = cannot_push;

        (left, right, on)
    }

    fn can_push_left_from_filter(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
        )
    }

    fn can_push_right_from_filter(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn can_push_on_from_filter(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
    }

    fn can_push_left_from_on(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::LeftSemi
        )
    }

    fn can_push_right_from_on(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightSemi
        )
    }

    /// find the comparison exprs and return their inputs, types and indices.
    fn find_comparison_exprs(
        left_col_num: usize,
        right_col_num: usize,
        exprs: &[ExprImpl],
    ) -> (Vec<ExprImpl>, Vec<ExprImpl>, Vec<(usize, Type)>) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        let mut left_exprs = vec![];
        let mut right_exprs = vec![];
        // indices and return types of function calls whose's inputs will be calculated in
        // `project`s
        let mut indices_and_ty_of_func_calls = vec![];
        let is_comparison_type = |ty| {
            matches!(
                ty,
                Type::LessThan
                    | Type::LessThanOrEqual
                    | Type::Equal
                    | Type::GreaterThan
                    | Type::GreaterThanOrEqual
            )
        };
        for (index, expr) in exprs.iter().enumerate() {
            if let ExprImpl::FunctionCall(func) = expr && is_comparison_type(func.get_expr_type()) {
                let (ty, left, right) = func.clone().decompose_as_binary();
                // we just cast the return types of inputs of equal conditions for `HashJoin`.
                // non-equal conditions don't need unnecssary explicit cast.
                let can_cast = ty == Type::Equal;
                let left_input_bits =
                    left.collect_input_refs(left_col_num + right_col_num);
                let right_input_bits =
                    right.collect_input_refs(left_col_num + right_col_num);
                let (mut left, mut right) = if left_input_bits.is_subset(&left_bit_map)
                    && right_input_bits.is_subset(&right_bit_map)
                {
                    (left, right)
                } else if left_input_bits.is_subset(&right_bit_map)
                    && right_input_bits.is_subset(&left_bit_map)
                {
                    (right, left)
                } else {
                    continue;
                };
                // when both `left` and `right` are `input_ref`, and they have the same return type or `func` is non-equal comparison expression, there is no need to calculate them in project.
                if left.as_input_ref().is_some() && right.as_input_ref().is_some() && (left.return_type() == right.return_type() || !can_cast) {
                    continue;
                }
                if can_cast {
                    // align return types to avoid error when executing join.
                    let mut v = vec![left, right];
                    let _data_type = align_types(v.iter_mut()).unwrap();
                    (left, right) = v.into_iter().next_tuple().unwrap();
                }
                left_exprs.push(left);
                {
                    let mut shift_with_offset = ColIndexMapping::with_shift_offset(left_col_num + right_col_num, -(left_col_num as isize));
                    let right = shift_with_offset.rewrite_expr(right);
                    right_exprs.push(right);
                }
                indices_and_ty_of_func_calls.push((index, ty));
            }
        }
        (left_exprs, right_exprs, indices_and_ty_of_func_calls)
    }

    /// use `col_index_mapping` to remap `exprs` and `output_indices`.
    fn remap_exprs_and_output_indices(
        exprs: Vec<ExprImpl>,
        output_indices: Vec<usize>,
        col_index_mapping: &mut ColIndexMapping,
    ) -> (Vec<ExprImpl>, Vec<usize>) {
        let exprs: Vec<ExprImpl> = exprs
            .into_iter()
            .map(|expr| col_index_mapping.rewrite_expr(expr))
            .collect();
        let output_indices = output_indices
            .into_iter()
            .map(|i| col_index_mapping.map(i))
            .collect();
        (exprs, output_indices)
    }

    /// Try to simplify the outer join with the predicate on the top of the join
    ///
    /// now it is just a naive implementation for comparison expression, we can give a more general
    /// implementation with constant folding in future
    fn simplify_outer(predicate: &Condition, left_col_num: usize, join_type: JoinType) -> JoinType {
        let (mut gen_null_in_left, mut gen_null_in_right) = match join_type {
            JoinType::LeftOuter => (false, true),
            JoinType::RightOuter => (true, false),
            JoinType::FullOuter => (true, true),
            _ => return join_type,
        };

        for expr in &predicate.conjunctions {
            if let ExprImpl::FunctionCall(func) = expr {
                match func.get_expr_type() {
                    ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::LessThan
                    | ExprType::LessThanOrEqual
                    | ExprType::GreaterThan
                    | ExprType::GreaterThanOrEqual => {
                        for input in func.inputs() {
                            if let ExprImpl::InputRef(input) = input {
                                let idx = input.index;
                                if idx < left_col_num {
                                    gen_null_in_left = false;
                                } else {
                                    gen_null_in_right = false;
                                }
                            }
                        }
                    }
                    _ => {}
                };
            }
        }

        match (gen_null_in_left, gen_null_in_right) {
            (true, true) => JoinType::FullOuter,
            (true, false) => JoinType::RightOuter,
            (false, true) => JoinType::LeftOuter,
            (false, false) => JoinType::Inner,
        }
    }

    fn convert_to_lookup_join(
        &self,
        logical_join: LogicalJoin,
        predicate: EqJoinPredicate,
    ) -> Option<PlanRef> {
        if self.right.as_ref().node_type() != PlanNodeType::LogicalScan {
            log::warn!(
                "Lookup Join only supports basic tables on the join's right side. A \
            different join will be used instead."
            );
            return None;
        }

        let logical_scan = self.right.as_logical_scan().unwrap();
        let table_desc = logical_scan.table_desc().clone();
        let output_column_ids = logical_scan.output_column_ids();

        // Verify that the right join key columns are the same as the primary key
        // TODO: Refactor Lookup Join so that prefixes of the primary key are allowed
        let eq_col_warn_message = "In Lookup Join, the right columns of the equality join \
        predicates must be the same as the primary key. A different join will be used instead.";

        let order_col_indices = table_desc.order_column_indices();
        if order_col_indices.len() != predicate.right_eq_indexes().len() {
            log::warn!("{}", eq_col_warn_message);
            return None;
        }

        for (i, eq_idx) in predicate.right_eq_indexes().into_iter().enumerate() {
            if order_col_indices[i] != output_column_ids[eq_idx].get_id() as usize {
                log::warn!("{}", eq_col_warn_message);
                return None;
            }
        }

        Some(BatchLookupJoin::new(logical_join, predicate, table_desc, output_column_ids).into())
    }
}

impl PlanTreeNodeBinary for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new_with_output_indices(
            left,
            right,
            self.join_type,
            self.on.clone(),
            self.output_indices.clone(),
        )
    }

    #[must_use]
    fn rewrite_with_left_right(
        &self,
        left: PlanRef,
        left_col_change: ColIndexMapping,
        right: PlanRef,
        right_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let (new_on, new_output_indices) = {
            let (mut map, _) = left_col_change.clone().into_parts();
            let (mut right_map, _) = right_col_change.clone().into_parts();
            for i in right_map.iter_mut().flatten() {
                *i += left.schema().len();
            }
            map.append(&mut right_map);
            let mut mapping = ColIndexMapping::new(map);

            let new_output_indices = self
                .output_indices
                .iter()
                .map(|&i| mapping.map(i))
                .collect::<Vec<_>>();
            let new_on = self.on().clone().rewrite_expr(&mut mapping);
            (new_on, new_output_indices)
        };

        let join = Self::new_with_output_indices(
            left,
            right,
            self.join_type,
            new_on,
            new_output_indices.clone(),
        );

        let new_i2o = ColIndexMapping::with_remaining_columns(
            &new_output_indices,
            join.internal_column_num(),
        );

        let old_o2i = self.o2i_col_mapping();

        let old_i2l = old_o2i
            .composite(&self.i2l_col_mapping())
            .composite(&left_col_change);
        let old_i2r = old_o2i
            .composite(&self.i2r_col_mapping())
            .composite(&right_col_change);
        let new_l2i = join.l2i_col_mapping().composite(&new_i2o);
        let new_r2i = join.r2i_col_mapping().composite(&new_i2o);

        let out_col_change = old_i2l
            .composite(&new_l2i)
            .union(&old_i2r.composite(&new_r2i));
        (join, out_col_change)
    }
}

impl_plan_tree_node_for_binary! { LogicalJoin }

impl ColPrunable for LogicalJoin {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        // make `required_cols` point to internal table instead of output schema.
        let required_cols = required_cols
            .iter()
            .map(|i| self.output_indices[*i])
            .collect_vec();
        let left_len = self.left.schema().fields.len();

        let total_len = self.left().schema().len() + self.right().schema().len();
        let mut resized_required_cols = FixedBitSet::with_capacity(total_len);

        required_cols.iter().for_each(|&i| {
            if self.is_right_join() {
                resized_required_cols.insert(left_len + i);
            } else {
                resized_required_cols.insert(i);
            }
        });

        // add those columns which are required in the join condition to
        // to those that are required in the output
        let mut visitor = CollectInputRef::new(resized_required_cols);
        self.on.visit_expr(&mut visitor);
        let left_right_required_cols = FixedBitSet::from(visitor).ones().collect_vec();

        let mut left_required_cols = Vec::new();
        let mut right_required_cols = Vec::new();
        left_right_required_cols.iter().for_each(|&i| {
            if i < left_len {
                left_required_cols.push(i);
            } else {
                right_required_cols.push(i - left_len);
            }
        });

        let mut on = self.on.clone();
        let mut mapping =
            ColIndexMapping::with_remaining_columns(&left_right_required_cols, total_len);
        on = on.rewrite_expr(&mut mapping);

        let new_output_indices = {
            let required_inputs_in_output = if self.is_left_join() {
                &left_required_cols
            } else if self.is_right_join() {
                &right_required_cols
            } else {
                &left_right_required_cols
            };

            let mapping =
                ColIndexMapping::with_remaining_columns(required_inputs_in_output, total_len);
            required_cols.iter().map(|&i| mapping.map(i)).collect_vec()
        };

        LogicalJoin::new_with_output_indices(
            self.left.prune_col(&left_required_cols),
            self.right.prune_col(&right_required_cols),
            self.join_type,
            on,
            new_output_indices,
        )
        .into()
    }
}

impl PredicatePushdown for LogicalJoin {
    /// Pushes predicates above and within a join node into the join node and/or its children nodes.
    ///
    /// # Which predicates can be pushed
    ///
    /// For inner join, we can do all kinds of pushdown.
    ///
    /// For left/right semi join, we can push filter to left/right and on-clause,
    /// and push on-clause to left/right.
    ///
    /// For left/right anti join, we can push filter to left/right, but on-clause can not be pushed
    ///
    /// ## Outer Join
    ///
    /// Preserved Row table
    /// : The table in an Outer Join that must return all rows.
    ///
    /// Null Supplying table
    /// : This is the table that has nulls filled in for its columns in unmatched rows.
    ///
    /// |                          | Preserved Row table | Null Supplying table |
    /// |--------------------------|---------------------|----------------------|
    /// | Join predicate (on)      | Not Pushed          | Pushed               |
    /// | Where predicate (filter) | Pushed              | Not Pushed           |
    fn predicate_pushdown(&self, mut predicate: Condition) -> PlanRef {
        let left_col_num = self.left.schema().len();
        let right_col_num = self.right.schema().len();
        let join_type = LogicalJoin::simplify_outer(&predicate, left_col_num, self.join_type);

        // rewrite output col referencing indices as internal cols
        let mut mapping = self.o2i_col_mapping();
        predicate = predicate.rewrite_expr(&mut mapping);

        let (left_from_filter, right_from_filter, on) = LogicalJoin::push_down(
            &mut predicate,
            left_col_num,
            right_col_num,
            LogicalJoin::can_push_left_from_filter(join_type),
            LogicalJoin::can_push_right_from_filter(join_type),
            LogicalJoin::can_push_on_from_filter(join_type),
        );

        let mut on = self.on.clone().and(on);
        let (left_from_on, right_from_on, old_on) = LogicalJoin::push_down(
            &mut on,
            left_col_num,
            right_col_num,
            LogicalJoin::can_push_left_from_on(join_type),
            LogicalJoin::can_push_right_from_on(join_type),
            false,
        );
        assert!(
            old_on.always_true(),
            "On-clause should not be pushed to on-clause."
        );

        let left_predicate = left_from_filter.and(left_from_on);
        let right_predicate = right_from_filter.and(right_from_on);

        let mut left = self.left.predicate_pushdown(left_predicate);
        let mut right = self.right.predicate_pushdown(right_predicate);
        let mut output_indices = self.output_indices().to_owned();
        // This code block pushes the calculation of inputs of join's comparison condition to join's
        // two sides.
        {
            let exprs = on.conjunctions;
            let (left_exprs, right_exprs, indices_and_ty_of_func_calls) =
                Self::find_comparison_exprs(left_col_num, right_col_num, &exprs);

            // used to shift indices of input_refs pointing the right side of `join` with
            // `left_exprs.len`.
            let mut col_index_mapping = {
                let map = (0..left_col_num)
                    .chain(
                        (left_col_num..left_col_num + right_col_num).map(|i| i + left_exprs.len()),
                    )
                    .map(Some)
                    .collect_vec();
                ColIndexMapping::new(map)
            };
            let (mut exprs, new_output_indices) =
                Self::remap_exprs_and_output_indices(exprs, output_indices, &mut col_index_mapping);
            output_indices = new_output_indices;

            // ```ignore
            // the internal table of join has has the following schema:
            // original left's columns | left_exprs | original right's columns | right_exprs
            //```
            // `left_index` and `right_index` will scan through `left_exprs` and `right_exprs`
            // respectively.
            let mut left_index = left_col_num;
            let mut right_index = left_col_num + left_exprs.len() + right_col_num;
            // replace chosen function calls.
            for (i, (index_of_func_call, ty)) in
                indices_and_ty_of_func_calls.into_iter().enumerate()
            {
                let left_input = InputRef::new(left_index, left_exprs[i].return_type());
                let right_input = InputRef::new(right_index, right_exprs[i].return_type());
                exprs[index_of_func_call] =
                    FunctionCall::new(ty, vec![left_input.into(), right_input.into()])
                        .unwrap()
                        .into();
                left_index += 1;
                right_index += 1;
            }
            on = Condition {
                conjunctions: exprs,
            };

            // add project to do the calculation.
            let new_input = |input: PlanRef, appended_exprs: Vec<ExprImpl>| {
                let mut exprs = input
                    .schema()
                    .data_types()
                    .into_iter()
                    .enumerate()
                    .map(|(i, data_type)| InputRef::new(i, data_type).into())
                    .collect_vec();
                exprs.extend(appended_exprs);
                LogicalProject::create(input, exprs)
            };
            if !left_exprs.is_empty() {
                // avoid unnecessary `project`s.
                left = new_input(left, left_exprs);
                right = new_input(right, right_exprs);
            }
        }

        let new_join =
            LogicalJoin::new_with_output_indices(left, right, join_type, on, output_indices);
        LogicalFilter::create(new_join.into(), predicate)
    }
}

impl ToBatch for LogicalJoin {
    fn to_batch(&self) -> Result<PlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );

        let left = self.left().to_batch()?;
        let right = self.right().to_batch()?;
        let logical_join = self.clone_with_left_right(left, right);

        let config = self.base.ctx.inner().session_ctx.config();

        if predicate.has_eq() {
            if config.get_batch_enable_lookup_join() {
                if config.get_query_mode() == QueryMode::Local {
                    if let Some(lookup_join) =
                        self.convert_to_lookup_join(logical_join.clone(), predicate.clone())
                    {
                        return Ok(lookup_join);
                    }
                } else {
                    log::warn!(
                        "Lookup Join can only be done in local mode. A different join will \
                    be used instead."
                    );
                }
            }

            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let new_output_indices = logical_join.output_indices.clone();
                let new_internal_column_num = logical_join.internal_column_num();
                let default_indices = (0..new_internal_column_num).collect::<Vec<_>>();
                let logical_join = logical_join.clone_with_output_indices(default_indices.clone());
                let eq_cond = EqJoinPredicate::new(
                    Condition::true_cond(),
                    predicate.eq_keys().to_vec(),
                    self.left.schema().len(),
                );
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = BatchHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                let plan = BatchFilter::new(logical_filter).into();
                if self.output_indices != default_indices {
                    let logical_project = LogicalProject::with_mapping(
                        plan,
                        ColIndexMapping::with_remaining_columns(
                            &new_output_indices,
                            new_internal_column_num,
                        ),
                    );
                    Ok(BatchProject::new(logical_project).into())
                } else {
                    Ok(plan)
                }
            } else {
                Ok(BatchHashJoin::new(logical_join, predicate).into())
            }
        } else {
            // Convert to Nested-loop Join for non-equal joins
            Ok(BatchNestedLoopJoin::new(logical_join).into())
        }
    }
}

impl ToStream for LogicalJoin {
    fn to_stream(&self) -> Result<PlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );

        if predicate.has_eq() {
            let right = self
                .right()
                .to_stream_with_dist_required(&RequiredDist::shard_by_key(
                    self.right().schema().len(),
                    &predicate.right_eq_indexes(),
                ))?;

            let r2l =
                predicate.r2l_eq_columns_mapping(self.left().schema().len(), right.schema().len());

            let left_dist = r2l.rewrite_required_distribution(&RequiredDist::PhysicalDist(
                right.distribution().clone(),
            ));

            let left = self.left().to_stream_with_dist_required(&left_dist)?;
            let logical_join = self.clone_with_left_right(left, right);

            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let default_indices = (0..self.internal_column_num()).collect::<Vec<_>>();

                // Temporarily remove output indices.
                let logical_join = logical_join.clone_with_output_indices(default_indices.clone());
                let eq_cond = EqJoinPredicate::new(
                    Condition::true_cond(),
                    predicate.eq_keys().to_vec(),
                    self.left.schema().len(),
                );
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = StreamHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                let plan = StreamFilter::new(logical_filter).into();
                if self.output_indices != default_indices {
                    let logical_project = LogicalProject::with_mapping(
                        plan,
                        ColIndexMapping::with_remaining_columns(
                            &self.output_indices,
                            self.internal_column_num(),
                        ),
                    );
                    Ok(StreamProject::new(logical_project).into())
                } else {
                    Ok(plan)
                }
            } else {
                Ok(StreamHashJoin::new(logical_join, predicate).into())
            }
        } else {
            let nested_loop_join_error = RwError::from(ErrorCode::NotImplemented(
                "stream nested-loop join".to_string(),
                None.into(),
            ));
            // If there is exactly one predicate, it is a comparison (<, <=, >, >=), and the
            // join is a `Inner` join, we can convert the scalar subquery into a
            // `StreamDynamicFilter`

            // Check if `Inner` subquery (no `IN` or `EXISTS` keywords)
            if self.join_type != JoinType::Inner {
                return Err(nested_loop_join_error);
            }

            // Check if right side is a scalar (for now, check if it is a simple agg)
            let maybe_simple_agg = if let Some(proj) = self.right().as_logical_project() {
                proj.input()
            } else {
                self.right()
            };

            if let Some(agg) = maybe_simple_agg.as_logical_agg() && agg.group_key().is_empty() {
                /* do nothing */
            } else {
                return Err(nested_loop_join_error);
            }

            // Check if the join condition is a correlated comparison
            let conj = &predicate.other_cond().conjunctions;

            let left_ref_index = if let Some(expr) = conj.first() && conj.len() == 1
            {
                if let Some((left_ref, _, right_ref)) = expr.as_comparison_cond()
                    && left_ref.index < self.left().schema().len()
                    && right_ref.index >= self.left().schema().len()
                {
                    left_ref.index
                } else {
                    return Err(nested_loop_join_error);
                }
            } else {
                return Err(nested_loop_join_error);
            };

            let left = self
                .left()
                .to_stream_with_dist_required(&RequiredDist::shard_by_key(
                    self.left().schema().len(),
                    &[left_ref_index],
                ))?;

            let right = self
                .right()
                .to_stream_with_dist_required(&RequiredDist::PhysicalDist(
                    Distribution::Broadcast,
                ))?;

            assert!(right.as_stream_exchange().is_some());

            assert_eq!(right.inputs().len(), 1);
            assert_eq!(
                *right.inputs().first().unwrap().distribution(),
                Distribution::Single
            );

            let plan = StreamDynamicFilter::new(
                left_ref_index,
                predicate.other_cond().clone(),
                left,
                right,
            )
            .into();

            // TODO: `DynamicFilterExecutor` should use `output_indices` in `ChunkBuilder`
            if self.output_indices != (0..self.internal_column_num()).collect::<Vec<_>>() {
                let logical_project = LogicalProject::with_mapping(
                    plan,
                    ColIndexMapping::with_remaining_columns(
                        &self.output_indices,
                        self.internal_column_num(),
                    ),
                );
                Ok(StreamProject::new(logical_project).into())
            } else {
                Ok(plan)
            }
        }
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (left, left_col_change) = self.left.logical_rewrite_for_stream()?;
        let left_len = left.schema().len();
        let (right, right_col_change) = self.right.logical_rewrite_for_stream()?;
        let (join, out_col_change) = self.rewrite_with_left_right(
            left.clone(),
            left_col_change,
            right.clone(),
            right_col_change,
        );

        let mapping = ColIndexMapping::with_remaining_columns(
            &join.output_indices,
            join.internal_column_num(),
        );

        let l2i = join.l2i_col_mapping().composite(&mapping);
        let r2i = join.r2i_col_mapping().composite(&mapping);

        // Add missing pk indices to the logical join
        let left_to_add = left
            .pk_indices()
            .iter()
            .cloned()
            .filter(|i| l2i.try_map(*i) == None);

        let right_to_add = right
            .pk_indices()
            .iter()
            .cloned()
            .filter(|i| r2i.try_map(*i) == None)
            .map(|i| i + left_len);

        let mut new_output_indices = join.output_indices.clone();
        if !self.is_right_join() {
            new_output_indices.extend(left_to_add);
        }
        if !self.is_left_join() {
            new_output_indices.extend(right_to_add);
        }

        let join_with_pk = join.clone_with_output_indices(new_output_indices);
        // the added columns is at the end, so it will not change the exists column index
        Ok((join_with_pk.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataType, Datum};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::{LogicalValues, PlanTreeNodeUnary};
    use crate::session::OptimizerContext;

    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [2,3] will result in
    /// ```text
    /// Project(input_ref(1), input_ref(2))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_prune_join() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let required_cols = vec![2, 3];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[2]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        let expr: ExprImpl = join.on.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    /// Semi join panicked previously at `prune_col`. Add test to prevent regression.
    #[tokio::test]
    async fn test_prune_semi_join() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(4, ty))),
                ],
            )
            .unwrap(),
        ));
        for join_type in [
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ] {
            let join = LogicalJoin::new(
                left.clone().into(),
                right.clone().into(),
                join_type,
                Condition::with_expr(on.clone()),
            );

            let offset = if join.is_right_join() { 3 } else { 0 };

            // Perform the prune
            let required_cols = vec![0];
            // key 0 is never used in the join (always key 1)
            let plan = join.prune_col(&required_cols);
            let as_plan = plan.as_logical_join().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 1);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);

            // Perform the prune
            let required_cols = vec![0, 1, 2];
            // should not panic here
            let plan = join.prune_col(&required_cols);
            let as_plan = plan.as_logical_join().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 3);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);
            assert_eq!(as_plan.schema().fields()[1], fields[offset + 1]);
            assert_eq!(as_plan.schema().fields()[2], fields[offset + 2]);
        }
    }

    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [1,3] will result in
    /// ```text
    /// Join(on: input_ref(0)=input_ref(1))
    ///   TableScan(v2)
    ///   TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_prune_join_no_project() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let required_cols = vec![1, 3];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[1]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        let expr: ExprImpl = join.on.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 1);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..2]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    /// Convert
    /// ```text
    /// Join(on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// to
    /// ```text
    /// Filter($2 == 42)
    ///   HashJoin(on: $1 = $3)
    ///     TableScan(v1, v2, v3)
    ///     TableScan(v4, v5, v6)
    /// ```
    #[tokio::test]
    async fn test_join_to_batch() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(DataType::Int32, format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );

        let eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, DataType::Int32))),
                ],
            )
            .unwrap(),
        ));
        let non_eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(
                        Datum::Some(42_i32.into()),
                        DataType::Int32,
                    ))),
                ],
            )
            .unwrap(),
        ));
        // Condition: ($1 = $3) AND ($2 == 42)
        let on_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(Type::And, vec![eq_cond.clone(), non_eq_cond.clone()]).unwrap(),
        ));

        let join_type = JoinType::Inner;
        let logical_join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on_cond),
        );

        // Perform `to_batch`
        let result = logical_join.to_batch().unwrap();

        // Expected plan: Filter($2 == 42) --> HashJoin($1 = $3)
        let batch_filter = result.as_batch_filter().unwrap();
        assert_eq!(
            ExprImpl::from(batch_filter.predicate().clone()),
            non_eq_cond
        );

        let input = batch_filter.input();
        let hash_join = input.as_batch_hash_join().unwrap();
        assert_eq!(
            ExprImpl::from(hash_join.eq_join_predicate().eq_cond()),
            eq_cond
        );
    }

    /// Convert
    /// ```text
    /// Join(join_type: left outer, on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// to
    /// ```text
    /// HashJoin(join_type: left outer, on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    #[tokio::test]
    #[ignore] // ignore due to refactor logical scan, but the test seem to duplicate with the explain test
              // framework, maybe we will remove it?
    async fn test_join_to_stream() {
        // let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        // let fields: Vec<Field> = (1..7)
        //     .map(|i| Field {
        //         data_type: DataType::Int32,
        //         name: format!("v{}", i),
        //     })
        //     .collect();
        // let left = LogicalScan::new(
        //     "left".to_string(),
        //     TableId::new(0),
        //     vec![1.into(), 2.into(), 3.into()],
        //     Schema {
        //         fields: fields[0..3].to_vec(),
        //     },
        //     ctx.clone(),
        // );
        // let right = LogicalScan::new(
        //     "right".to_string(),
        //     TableId::new(0),
        //     vec![4.into(), 5.into(), 6.into()],
        //     Schema {
        //         fields: fields[3..6].to_vec(),
        //     },
        //     ctx,
        // );
        // let eq_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(
        //         Type::Equal,
        //         vec![
        //             ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
        //             ExprImpl::InputRef(Box::new(InputRef::new(3, DataType::Int32))),
        //         ],
        //     )
        //     .unwrap(),
        // ));
        // let non_eq_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(
        //         Type::Equal,
        //         vec![
        //             ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
        //             ExprImpl::Literal(Box::new(Literal::new(
        //                 Datum::Some(42_i32.into()),
        //                 DataType::Int32,
        //             ))),
        //         ],
        //     )
        //     .unwrap(),
        // ));
        // // Condition: ($1 = $3) AND ($2 == 42)
        // let on_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(Type::And, vec![eq_cond, non_eq_cond]).unwrap(),
        // ));

        // let join_type = JoinType::LeftOuter;
        // let logical_join = LogicalJoin::new(
        //     left.into(),
        //     right.into(),
        //     join_type,
        //     Condition::with_expr(on_cond.clone()),
        // );

        // // Perform `to_stream`
        // let result = logical_join.to_stream();

        // // Expected plan: HashJoin(($1 = $3) AND ($2 == 42))
        // let hash_join = result.as_stream_hash_join().unwrap();
        // assert_eq!(hash_join.eq_join_predicate().all_cond().as_expr(), on_cond);
    }
    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [3, 2] will result in
    /// ```text
    /// Project(input_ref(2), input_ref(1))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_join_column_prune_with_order_required() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let required_cols = vec![3, 2];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[3]);
        assert_eq!(join.schema().fields()[1], fields[2]);

        let expr: ExprImpl = join.on.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }
}
