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

use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, Field, Schema, SchemaId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::HashJoinNode;

use super::utils::TableCatalogBuilder;
use super::{LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, StreamDeltaJoin, ToStreamProst};
use crate::catalog::table_catalog::TableCatalog;
use crate::expr::Expr;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay};
use crate::optimizer::property::Distribution;
use crate::utils::ColIndexMapping;

/// [`StreamHashJoin`] implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct StreamHashJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Whether to force use delta join for this join node. If this is true, then indexes will
    /// be create automatically when building the executors on meta service. For testing purpose
    /// only. Will remove after we have fully support shared state and index.
    is_delta: bool,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    is_append_only: bool,
}

impl StreamHashJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match logical.join_type() {
            JoinType::Inner => logical.left().append_only() && logical.right().append_only(),
            _ => false,
        };

        let dist = Self::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
            &logical
                .l2i_col_mapping()
                .composite(&logical.i2o_col_mapping()),
        );

        let force_delta = ctx.inner().session_ctx.config().get_delta_join();

        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.pk_indices.to_vec(),
            dist,
            append_only,
        );

        Self {
            base,
            logical,
            eq_join_predicate,
            is_delta: force_delta,
            is_append_only: append_only,
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.logical.join_type()
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub(super) fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        side2o_mapping: &ColIndexMapping,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => {
                side2o_mapping.rewrite_provided_distribution(left)
            }
            (_, _) => panic!(),
        }
    }

    /// Convert this hash join to a delta join plan
    pub fn to_delta_join(&self) -> StreamDeltaJoin {
        StreamDeltaJoin::new(self.logical.clone(), self.eq_join_predicate.clone())
    }
}

impl fmt::Display for StreamHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = if self.is_delta {
            f.debug_struct("StreamDeltaHashJoin")
        } else if self.is_append_only {
            f.debug_struct("StreamAppendOnlyHashJoin")
        } else {
            f.debug_struct("StreamHashJoin")
        };

        let verbose = self.base.ctx.is_explain_verbose();
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

        if self.append_only() {
            builder.field("append_only", &format_args!("{}", true));
        }
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

impl PlanTreeNodeBinary for StreamHashJoin {
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

impl_plan_tree_node_for_binary! { StreamHashJoin }

impl ToStreamProst for StreamHashJoin {
    fn to_stream_prost_body(&self) -> NodeBody {
        let left_key_indices = self.eq_join_predicate.left_eq_indexes();
        let right_key_indices = self.eq_join_predicate.right_eq_indexes();
        let left_key_indices_prost = left_key_indices.iter().map(|idx| *idx as i32).collect_vec();
        let right_key_indices_prost = right_key_indices
            .iter()
            .map(|idx| *idx as i32)
            .collect_vec();
        NodeBody::HashJoin(HashJoinNode {
            join_type: self.logical.join_type() as i32,
            left_key: left_key_indices_prost,
            right_key: right_key_indices_prost,
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            is_delta_join: self.is_delta,
            left_table: Some(
                infer_internal_table_catalog(self.left(), left_key_indices).to_prost(
                    SchemaId::placeholder() as u32,
                    DatabaseId::placeholder() as u32,
                ),
            ),
            right_table: Some(
                infer_internal_table_catalog(self.right(), right_key_indices).to_prost(
                    SchemaId::placeholder() as u32,
                    DatabaseId::placeholder() as u32,
                ),
            ),
            output_indices: self
                .logical
                .output_indices()
                .iter()
                .map(|&x| x as u32)
                .collect(),
            is_append_only: self.is_append_only,
        })
    }
}

fn infer_internal_table_catalog(input: PlanRef, join_key_indices: Vec<usize>) -> TableCatalog {
    let base = input.plan_base();
    let schema = &base.schema;

    let append_only = input.append_only();
    let dist_keys = base.dist.dist_column_indices().to_vec();

    // The pk of hash join internal table should be join_key + input_pk.
    let mut pk_indices = join_key_indices;
    // TODO(yuhao): dedup the dist key and pk.
    pk_indices.extend(&base.pk_indices);

    let mut columns_fields = schema.fields().to_vec();

    // The join degree at the end of internal table.
    let degree_column_field = Field::with_name(DataType::Int64, "_degree");
    columns_fields.push(degree_column_field);

    let mut internal_table_catalog_builder = TableCatalogBuilder::new();

    columns_fields.iter().for_each(|field| {
        internal_table_catalog_builder.add_column(field);
    });

    pk_indices.iter().for_each(|idx| {
        internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending)
    });

    internal_table_catalog_builder.build(dist_keys, append_only)
}
