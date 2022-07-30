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
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};

use super::{
    gen_filter_and_pushdown, ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown,
    ToBatch, ToStream,
};
use crate::optimizer::plan_node::{BatchTopN, LogicalProject, StreamTopN};
use crate::optimizer::property::{FieldOrder, Order, OrderDisplay, RequiredDist};
use crate::planner::LIMIT_ALL_COUNT;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalTopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone)]
pub struct LogicalTopN {
    pub base: PlanBase,
    input: PlanRef,
    limit: usize,
    offset: usize,
    order: Order,
}

impl LogicalTopN {
    pub fn new(input: PlanRef, limit: usize, offset: usize, order: Order) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let pk_indices = input.pk_indices().to_vec();
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalTopN {
            base,
            input,
            limit,
            offset,
            order,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, limit: usize, offset: usize, order: Order) -> PlanRef {
        Self::new(input, limit, offset, order).into()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    /// `topn_order` returns the order of the Top-N operator. This naming is because `order()`
    /// already exists and it was designed to return the operator's physical property order.
    ///
    /// Note that `order()` and `topn_order()` may differ. For streaming query, `order()` which
    /// implies the output ordering of an operator, is never guaranteed; while `topn_order()` must
    /// be non-null because it's a critical information for Top-N operators to work
    pub fn topn_order(&self) -> &Order {
        &self.order
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        let input = self.input();
        let input_schema = input.schema();
        builder.field(
            "order",
            &format!(
                "{}",
                OrderDisplay {
                    order: self.topn_order(),
                    input_schema
                }
            ),
        );
        builder
            .field("limit", &format_args!("{}", self.limit()))
            .field("offset", &format_args!("{}", self.offset()))
            .finish()
    }
}

impl PlanTreeNodeUnary for LogicalTopN {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.limit, self.offset, self.order.clone())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (
            Self::new(
                input,
                self.limit,
                self.offset,
                input_col_change
                    .rewrite_required_order(&self.order)
                    .unwrap(),
            ),
            input_col_change,
        )
    }
}
impl_plan_tree_node_for_unary! {LogicalTopN}
impl fmt::Display for LogicalTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_name(f, "LogicalTopN")
    }
}

impl ColPrunable for LogicalTopN {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let input_required_bitset = FixedBitSet::from_iter(required_cols.iter().copied());
        let order_required_cols = {
            let mut order_required_cols = FixedBitSet::with_capacity(self.input().schema().len());
            self.order
                .field_order
                .iter()
                .for_each(|fo| order_required_cols.insert(fo.index));
            order_required_cols
        };

        let input_required_cols = {
            let mut tmp = order_required_cols;
            tmp.union_with(&input_required_bitset);
            tmp.ones().collect_vec()
        };
        let mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let new_order = Order {
            field_order: self
                .order
                .field_order
                .iter()
                .map(|fo| FieldOrder {
                    index: mapping.map(fo.index),
                    direct: fo.direct,
                })
                .collect(),
        };
        let new_input = self.input.prune_col(&input_required_cols);
        let top_n = Self::new(new_input, self.limit, self.offset, new_order).into();

        if input_required_cols == required_cols {
            top_n
        } else {
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = top_n.schema().len();
            LogicalProject::with_mapping(
                top_n,
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl PredicatePushdown for LogicalTopN {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond())
    }
}

impl ToBatch for LogicalTopN {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        let ret = BatchTopN::new(new_logical).into();

        if self.topn_order().satisfies(required_order) {
            Ok(ret)
        } else {
            Ok(required_order.enforce(ret))
        }
    }
}

impl ToStream for LogicalTopN {
    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        // Unlike `BatchTopN`, `StreamTopN` cannot guarantee the output order
        let (new_input, input_col_change) = self
            .input()
            .to_stream_with_dist_required(&RequiredDist::single())?;

        if self.offset() != 0 && self.limit == LIMIT_ALL_COUNT {
            return Err(RwError::from(InternalError(
                "Doesn't support OFFSET without LIMIT".to_string(),
            )));
        }

        let (top_n, out_col_change) = self.rewrite_with_input(new_input, input_col_change);
        Ok((StreamTopN::new(top_n).into(), out_col_change))
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (top_n, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((top_n.into(), out_col_change))
    }
}
