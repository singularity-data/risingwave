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

use risingwave_common::catalog::{ColumnId, Schema, TableDesc};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::LookupJoinNode;

use crate::expr::Expr;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, LogicalJoin, PlanBase, PlanTreeNodeBinary,
    PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::optimizer::PlanRef;

#[derive(Debug, Clone)]
pub struct BatchLookupJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Table description of the right side table
    right_table_desc: TableDesc,

    /// Output column ids of the right side table
    right_output_column_ids: Vec<ColumnId>,
}

impl BatchLookupJoin {
    pub fn new(
        logical: LogicalJoin,
        eq_join_predicate: EqJoinPredicate,
        right_table_desc: TableDesc,
        right_output_column_ids: Vec<ColumnId>,
    ) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = Self::derive_dist(logical.left().distribution());
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any());
        Self {
            base,
            logical,
            eq_join_predicate,
            right_table_desc,
            right_output_column_ids,
        }
    }

    fn derive_dist(left: &Distribution) -> Distribution {
        match left {
            Distribution::Single => Distribution::Single,
            _ => unreachable!(),
        }
    }

    fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for BatchLookupJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("BatchLookupJoin");
        builder.field("type", &format_args!("{:?}", self.logical.join_type()));

        let mut concat_schema = self.logical.left().schema().fields.clone();
        concat_schema.extend(self.logical.right().schema().fields.clone());
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

impl PlanTreeNodeUnary for BatchLookupJoin {
    fn input(&self) -> PlanRef {
        self.logical.left()
    }

    // Only change left side
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            self.logical
                .clone_with_left_right(input, self.logical.right()),
            self.eq_join_predicate.clone(),
            self.right_table_desc.clone(),
            self.right_output_column_ids.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { BatchLookupJoin }

impl ToDistributedBatch for BatchLookupJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        Err(ErrorCode::NotImplemented("Lookup Join in MPP mode".to_string(), None.into()).into())
    }
}

impl ToBatchProst for BatchLookupJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::LookupJoin(LookupJoinNode {
            join_type: self.logical.join_type() as i32,
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            build_side_key: self
                .eq_join_predicate
                .left_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            probe_side_table_desc: Some(self.right_table_desc.to_protobuf()),
            probe_side_vnode_mapping: self
                .right_table_desc
                .vnode_mapping
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
            probe_side_column_ids: self
                .right_output_column_ids
                .iter()
                .map(ColumnId::get_id)
                .collect(),
            output_indices: self
                .logical
                .output_indices()
                .iter()
                .map(|&x| x as u32)
                .collect(),
            worker_nodes: vec![], // To be filled in at local.rs
        })
    }
}

impl ToLocalBatch for BatchLookupJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;

        Ok(self.clone_with_input(input).into())
    }
}
