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

use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::HashJoinNode;

use super::{
    EqJoinPredicate, LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatchProst,
    ToDistributedBatch,
};
use crate::expr::Expr;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicateDisplay, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::utils::ColIndexMapping;

/// `BatchHashJoin` implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and then probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct BatchHashJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,
}

impl BatchHashJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = Self::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
            &logical
                .l2i_col_mapping()
                .composite(&logical.i2o_col_mapping()),
        );
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any());

        Self {
            base,
            logical,
            eq_join_predicate,
        }
    }

    fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        l2o_mapping: &ColIndexMapping,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => {
                l2o_mapping.rewrite_provided_distribution(left)
            }
            (_, _) => unreachable!(),
        }
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for BatchHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("BatchHashJoin");
        builder.field("type", &format_args!("{:?}", self.logical.join_type()));

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "predicate",
            &format_args!(
                "{}",
                EqJoinPredicateDisplay {
                    eq_join_predicate: self.eq_join_predicate(),
                    input_schema: &concat_schema
                }
            ),
        );

        if verbose {
            if self
                .logical
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &format_args!(
                        "{:?}",
                        &IndicesDisplay {
                            indices: self.logical.output_indices(),
                            input_schema: &concat_schema,
                        }
                    ),
                );
            }
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for BatchHashJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }

    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            self.logical.clone_with_left_right(left, right),
            self.eq_join_predicate.clone(),
        )
    }
}

impl_plan_tree_node_for_binary! { BatchHashJoin }

impl ToDistributedBatch for BatchHashJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        let right = self.right().to_distributed_with_required(
            &Order::any(),
            &RequiredDist::shard_by_key(
                self.right().schema().len(),
                &self.eq_join_predicate().right_eq_indexes(),
            ),
        )?;
        let r2l = self
            .eq_join_predicate()
            .r2l_eq_columns_mapping(self.left().schema().len(), right.schema().len());
        let left_dist = r2l.rewrite_required_distribution(&RequiredDist::PhysicalDist(
            right.distribution().clone(),
        ));
        let left = self
            .left()
            .to_distributed_with_required(&Order::any(), &left_dist)?;
        Ok(self.clone_with_left_right(left, right).into())
    }
}

impl ToBatchProst for BatchHashJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HashJoin(HashJoinNode {
            join_type: self.logical.join_type() as i32,
            left_key: self
                .eq_join_predicate
                .left_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            right_key: self
                .eq_join_predicate
                .right_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            output_indices: self
                .logical
                .output_indices()
                .iter()
                .map(|&x| x as u32)
                .collect(),
        })
    }
}

impl ToLocalBatch for BatchHashJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let right = RequiredDist::single()
            .enforce_if_not_satisfies(self.right().to_local()?, &Order::any())?;
        let left = RequiredDist::single()
            .enforce_if_not_satisfies(self.left().to_local()?, &Order::any())?;

        Ok(self.clone_with_left_right(left, right).into())
    }
}
