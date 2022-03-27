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

use risingwave_common::catalog::Schema;

use super::PlanNodeId;
use crate::optimizer::property::{Distribution, Order};
use crate::session::OptimizerContextRef;

/// the common fields of all nodes, please make a field named `base` in
/// every planNode and correctly valued it when construct the planNode.
#[derive(Clone, Debug)]
pub struct PlanBase {
    pub id: PlanNodeId,
    pub ctx: OptimizerContextRef,
    pub schema: Schema,
    /// the pk indices of the PlanNode's output, a empty pk_indices vec means there is no pk
    pub pk_indices: Vec<usize>,
    /// The order property of the PlanNode's output, store an `Order::any()` here will not affect
    /// correctness, but insert unnecessary sort in plan
    pub order: Order,
    /// The distribution property of the PlanNode's output, store an `Distribution::any()` here
    /// will not affect correctness, but insert unnecessary exchange in plan
    pub dist: Distribution,
    /// The append-only property of the PlanNode's output is a stream-only property. Append-only
    /// means the stream contains only insert operation.
    pub append_only: bool,
}
impl PlanBase {
    pub fn new_logical(ctx: OptimizerContextRef, schema: Schema, pk_indices: Vec<usize>) -> Self {
        let id = ctx.next_plan_node_id();
        Self {
            id,
            ctx,
            schema,
            pk_indices,
            dist: Distribution::any().clone(),
            order: Order::any().clone(),
            // Logical plan node won't touch `append_only` field
            append_only: true,
        }
    }
    pub fn new_stream(
        ctx: OptimizerContextRef,
        schema: Schema,
        pk_indices: Vec<usize>,
        dist: Distribution,
        append_only: bool,
    ) -> Self {
        // assert!(!pk_indices.is_empty()); TODO: reopen it when ensure the pk for stream op
        let id = ctx.next_plan_node_id();
        Self {
            id,
            ctx,
            schema,
            dist,
            order: Order::any().clone(),
            pk_indices,
            append_only,
        }
    }
    pub fn new_batch(
        ctx: OptimizerContextRef,
        schema: Schema,
        dist: Distribution,
        order: Order,
    ) -> Self {
        let id = ctx.next_plan_node_id();
        Self {
            id,
            ctx,
            schema,
            dist,
            order,
            pk_indices: vec![],
            // Batch plan node won't touch `append_only` field
            append_only: true,
        }
    }
}
