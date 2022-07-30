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

use super::*;
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::ColIndexMapping;
use crate::{for_batch_plan_nodes, for_logical_plan_nodes, for_stream_plan_nodes};

/// `ToStream` converts a logical plan node to streaming physical node
/// with an optional required distribution.
///
/// when implement this trait you can choose the two ways
/// - Implement `to_stream` and use the default implementation of `to_stream_with_dist_required`
/// - Or, if the required distribution is given, there will be a better plan. For example a hash
///   join with hash-key(a,b) and the plan is required hash-distributed by (a,b,c). you can
///   implement `to_stream_with_dist_required`, and implement `to_stream` with
///   `to_stream_with_dist_required(RequiredDist::Any)`. you can see [`LogicalProject`] as an
///   example.
pub trait ToStream {
    /// `logical_rewrite_for_stream` will rewrite the logical node, and return (`new_plan_node`,
    /// `col_mapping`), the `col_mapping` is for original columns have been changed into some other
    /// position.
    ///
    /// Now it is used to:
    /// 1. ensure every plan node's output having pk column
    /// 2. add `row_count`() in every Agg
    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)>;

    /// `to_stream` is equivalent to `to_stream_with_dist_required(RequiredDist::Any)`
    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)>;

    /// convert the plan to streaming physical plan and satisfy the required distribution
    fn to_stream_with_dist_required(
        &self,
        required_dist: &RequiredDist,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (ret, mapping) = self.to_stream()?;
        Ok((
            mapping
                .rewrite_required_distribution(&required_dist)
                .enforce_if_not_satisfies(ret, &Order::any())?,
            mapping,
        ))
    }
}

/// `ToBatch` allows to convert a logical plan node to batch physical node
/// with an optional required order.
///
/// when implement this trait you can choose the two ways
/// - Implement `to_batch` and use the default implementation of `to_batch_with_order_required`
/// - Or, if the required order is given, there will be a better plan. For example a join with
///   join-key(a,b) and the plan is required sorted by (a,b,c), a sort merge join is better. you can
///   implement `to_batch_with_order_required`, and implement `to_batch` with
///   `to_batch_with_order_required(&Order::any())`. you can see [`LogicalJoin`] as an example.
pub trait ToBatch {
    /// `to_batch` is equivalent to `to_batch_with_order_required(&Order::any())`
    fn to_batch(&self) -> Result<PlanRef>;
    /// convert the plan to batch physical plan and satisfy the required Order
    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let ret = self.to_batch()?;
        required_order.enforce_if_not_satisfies(ret)
    }
}

/// Converts a batch physical plan to local plan for local execution.
///
/// This is quite similar to `ToBatch`, but different in several ways. For example it converts
/// scan to exchange + scan.
pub trait ToLocalBatch {
    fn to_local(&self) -> Result<PlanRef>;

    /// Convert the plan to batch local physical plan and satisfy the required Order
    fn to_local_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let ret = self.to_local()?;
        required_order.enforce_if_not_satisfies(ret)
    }
}

/// `ToDistributedBatch` allows to convert a batch physical plan to distributed batch plan, by
/// insert exchange node, with an optional required order and distributed.
///
/// when implement this trait you can choose the two ways
/// - Implement `to_distributed` and use the default implementation of
///   `to_distributed_with_required`
/// - Or, if the required order and distribution is given, there will be a better plan. For example
///   a hash join with hash-key(a,b) and the plan is required hash-distributed by (a,b,c). you can
///   implement `to_distributed_with_required`, and implement `to_distributed` with
///   `to_distributed_with_required(&Order::any())`.
pub trait ToDistributedBatch {
    /// `to_distributed` is equivalent to `to_distributed_with_required(RequiredDist::Any,
    /// &Order::any())`
    fn to_distributed(&self) -> Result<PlanRef>;
    /// insert the exchange in batch physical plan to satisfy the required Distribution and Order.
    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &RequiredDist,
    ) -> Result<PlanRef> {
        let ret = self.to_distributed()?;
        let ret = required_order.enforce_if_not_satisfies(ret)?;
        required_dist.enforce_if_not_satisfies(ret, required_order)
    }
}

/// Implement [`ToBatch`] for batch and streaming node.
macro_rules! impl_to_batch {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToBatch for [<$convention $name>] {
                fn to_batch(&self) -> Result<PlanRef> {
                    panic!("converting into batch is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! { impl_to_batch }
for_stream_plan_nodes! { impl_to_batch }

/// Implement [`ToStream`] for batch and streaming node.
macro_rules! impl_to_stream {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToStream for [<$convention $name>] {
                fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)>{
                    panic!("converting to stream is only allowed on logical plan")
                }
                fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)>{
                    panic!("logical rewrite is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! { impl_to_stream }
for_stream_plan_nodes! { impl_to_stream }

/// impl `ToDistributedBatch`  for logical and streaming node.
macro_rules! ban_to_distributed {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToDistributedBatch for [<$convention $name>] {
                fn to_distributed(&self) -> Result<PlanRef> {
                    panic!("converting to distributed is only allowed on batch plan")
                }
            })*
        }
    }
}
for_logical_plan_nodes! { ban_to_distributed }
for_stream_plan_nodes! { ban_to_distributed }

/// impl `ToLocalBatch`  for logical and streaming node.
macro_rules! ban_to_local {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToLocalBatch for [<$convention $name>] {
                fn to_local(&self) -> Result<PlanRef> {
                    panic!("converting to distributed is only allowed on batch plan")
                }
            })*
        }
    }
}
for_logical_plan_nodes! { ban_to_local }
for_stream_plan_nodes! { ban_to_local }
