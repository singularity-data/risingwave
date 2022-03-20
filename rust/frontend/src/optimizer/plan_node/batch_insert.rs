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
//
use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::InsertNode;

use super::{LogicalInsert, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::PlanBase;
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchInsert` implements [`LogicalInsert`]
#[derive(Debug, Clone)]
pub struct BatchInsert {
    pub base: PlanBase,
    logical: LogicalInsert,
}

impl BatchInsert {
    pub fn new(logical: LogicalInsert) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            Distribution::any().clone(),
            Order::any().clone(),
        );
        BatchInsert { logical, base }
    }
}

impl fmt::Display for BatchInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchInsert")
    }
}

impl PlanTreeNodeUnary for BatchInsert {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchInsert }

impl WithSchema for BatchInsert {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchInsert {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchInsert {
    fn to_batch_prost_body(&self) -> NodeBody {
        #[allow(unreachable_code)]
        NodeBody::Insert(InsertNode {
            table_source_ref_id: todo!("fill source id here"),
            column_ids: todo!("this field is unused now"),
        })
    }
}
