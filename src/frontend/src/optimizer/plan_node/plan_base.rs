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

use paste::paste;
use risingwave_common::catalog::Schema;

use super::*;
use crate::for_all_plan_nodes;
use crate::optimizer::property::{Distribution, FunctionalDependencySet, Order};
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
    /// The order property of the PlanNode's output, store an `&Order::any()` here will not affect
    /// correctness, but insert unnecessary sort in plan
    pub order: Order,
    /// The distribution property of the PlanNode's output, store an `Distribution::any()` here
    /// will not affect correctness, but insert unnecessary exchange in plan
    pub dist: Distribution,
    /// The append-only property of the PlanNode's output is a stream-only property. Append-only
    /// means the stream contains only insert operation.
    pub append_only: bool,
    pub functional_dependency: FunctionalDependencySet,
}

impl PlanBase {
    pub fn new_logical(
        ctx: OptimizerContextRef,
        schema: Schema,
        pk_indices: Vec<usize>,
        functional_dependency: FunctionalDependencySet,
    ) -> Self {
        let id = ctx.next_plan_node_id();
        Self {
            id,
            ctx,
            schema,
            pk_indices,
            dist: Distribution::Single,
            order: Order::any(),
            // Logical plan node won't touch `append_only` field
            append_only: true,
            functional_dependency,
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
            order: Order::any(),
            pk_indices,
            append_only,
            functional_dependency: FunctionalDependencySet::new(),
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
            functional_dependency: FunctionalDependencySet::new(),
        }
    }
}
macro_rules! impl_base_delegate {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl [<$convention $name>] {
                pub fn id(&self) -> PlanNodeId {
                    self.plan_base().id
                }
                 pub fn ctx(&self) -> OptimizerContextRef {
                    self.plan_base().ctx.clone()
                }
                pub fn schema(&self) -> &Schema {
                    &self.plan_base().schema
                }
                pub fn pk_indices(&self) -> &[usize] {
                    &self.plan_base().pk_indices
                }
                pub fn order(&self) -> &Order {
                    &self.plan_base().order
                }
                pub fn distribution(&self) -> &Distribution {
                    &self.plan_base().dist
                }
                pub fn append_only(&self) -> bool {
                    self.plan_base().append_only
                }
                pub fn functional_dependency(&self) -> &FunctionalDependencySet {
                    &self.plan_base().functional_dependency
                }
            }
        })*
    }
}
for_all_plan_nodes! { impl_base_delegate }
