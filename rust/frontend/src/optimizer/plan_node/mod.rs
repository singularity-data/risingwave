#![allow(rustdoc::private_intra_doc_links)]
//! Defines all kinds of node in the plan tree, each node represent a relational expression.
//!
//! We use a immutable style tree structure, every Node are immutable and cannot be modified after
//! it has been created. If you want to modify the node, such as rewriting the expression in a
//! ProjectNode or changing a node's input node, you need to create a new node. We use Rc as the
//! node's reference, and a node just storage its inputs' reference, so change a node just need
//! create one new node but not the entire sub-tree.
//!
//! So when you want to add a new node, make sure:
//! - each field in the node struct are private
//! - recommend to implement the construction of Node in a unified `new()` function, if have multi
//!   methods to construct, make they have a consistent behavior
//! - all field should be valued in construction, so the properties' derivation should be finished
//!   in the `new()` function.

use std::fmt::{Debug, Display};
use std::rc::Rc;

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{self, DynClone};
use paste::paste;
use risingwave_common::catalog::Schema;
use risingwave_pb::plan::PlanNode as BatchPlanProst;
use risingwave_pb::stream_plan::StreamNode as StreamPlanProst;

use super::property::{
    Distribution, Order, WithConvention, WithDistribution, WithOrder, WithSchema,
};

/// The common trait over all plan nodes. Used by optimizer framework which will treate all node as
/// `dyn PlanNode`
///
/// We split the trait into lots of sub-trait so that we can easily use macro to impl them.
pub trait PlanNode:
    PlanTreeNode
    + DynClone
    + Debug
    + Display
    + Downcast
    + WithConvention
    + WithOrder
    + WithDistribution
    + WithSchema
    + WithContext
    + WithId
    + ColPrunable
    + ToBatch
    + ToStream
    + ToDistributedBatch
    + ToProst
{
    fn node_type(&self) -> PlanNodeType;
}

impl_downcast!(PlanNode);
pub type PlanRef = Rc<dyn PlanNode>;

#[derive(Clone, Debug, Copy)]
pub struct PlanNodeId(pub i32);

/// the common fields of logical nodes, please make a field named `base` in
/// every planNode and correctly valued it when construct the planNode.
#[derive(Clone, Debug)]
pub struct LogicalBase {
    pub id: PlanNodeId,
    pub schema: Schema,
    pub ctx: QueryContextRef,
}

/// the common fields of batch nodes, please make a field named `base` in
/// every planNode and correctly valued it when construct the planNode.

#[derive(Clone, Debug)]
pub struct BatchBase {
    pub id: PlanNodeId,
    /// the order property of the PlanNode's output, store an `Order::any()` here will not affect
    /// correctness, but insert unnecessary sort in plan
    pub order: Order,
    /// the distribution property of the PlanNode's output, store an `Distribution::any()` here
    /// will not affect correctness, but insert unnecessary exchange in plan
    pub dist: Distribution,

    pub ctx: QueryContextRef,
}

/// the common fields of stream nodes, please make a field named `base` in
/// every planNode and correctly valued it when construct the planNode.
#[derive(Clone, Debug)]
pub struct StreamBase {
    pub id: PlanNodeId,
    /// the distribution property of the PlanNode's output, store an `Distribution::any()` here
    /// will not affect correctness, but insert unnecessary exchange in plan
    pub dist: Distribution,
    pub ctx: QueryContextRef, /* TODO: pk derive
                               * pub pk_indices: Vec<u32> */
}

impl dyn PlanNode {
    /// Write explain the whole plan tree.
    pub fn explain(&self, level: usize, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        writeln!(f, "{}{}", " ".repeat(level * 2), self)?;
        for input in self.inputs() {
            input.explain(level + 1, f)?;
        }
        Ok(())
    }

    pub fn to_batch_prost(&self) -> BatchPlanProst {
        let node_body = Some(self.to_batch_prost_body());
        let children = self
            .inputs()
            .into_iter()
            .map(|plan| plan.to_batch_prost())
            .collect();
        let identity = format!("{:?}", self);
        BatchPlanProst {
            children,
            identity,
            node_body,
        }
    }

    #[allow(unreachable_code)]
    pub fn to_stream_prost(&self) -> StreamPlanProst {
        let node = Some(self.to_stream_prost_body());
        let input = self
            .inputs()
            .into_iter()
            .map(|plan| plan.to_stream_prost())
            .collect();
        let identity = format!("{:?}", self);
        StreamPlanProst {
            input,
            identity,
            node,
            operator_id: todo!(),
            pk_indices: todo!(),
        }
    }
}

#[macro_use]
mod plan_tree_node;
pub use plan_tree_node::*;
mod col_pruning;
pub use col_pruning::*;
mod convert;
pub use convert::*;
mod eq_join_predicate;
pub use eq_join_predicate::*;
mod to_prost;
pub use to_prost::*;

mod batch_exchange;
mod batch_filter;
mod batch_hash_join;
mod batch_limit;
mod batch_project;
mod batch_seq_scan;
mod batch_sort;
mod batch_sort_merge_join;
mod logical_agg;
mod logical_filter;
mod logical_insert;
mod logical_join;
mod logical_limit;
mod logical_project;
mod logical_scan;
mod logical_topn;
mod logical_values;
mod stream_exchange;
mod stream_filter;
mod stream_hash_join;
mod stream_project;
mod stream_table_source;

pub use batch_exchange::BatchExchange;
pub use batch_filter::BatchFilter;
pub use batch_hash_join::BatchHashJoin;
pub use batch_limit::BatchLimit;
pub use batch_project::BatchProject;
pub use batch_seq_scan::BatchSeqScan;
pub use batch_sort::BatchSort;
pub use batch_sort_merge_join::BatchSortMergeJoin;
pub use logical_agg::LogicalAgg;
pub use logical_filter::LogicalFilter;
pub use logical_insert::LogicalInsert;
pub use logical_join::LogicalJoin;
pub use logical_limit::LogicalLimit;
pub use logical_project::LogicalProject;
pub use logical_scan::LogicalScan;
pub use logical_topn::LogicalTopN;
pub use logical_values::LogicalValues;
pub use stream_exchange::StreamExchange;
pub use stream_filter::StreamFilter;
pub use stream_hash_join::StreamHashJoin;
pub use stream_project::StreamProject;
pub use stream_table_source::StreamTableSource;

use crate::optimizer::property::{WithContext, WithId};
use crate::session::QueryContextRef;

/// [`for_all_plan_nodes`] includes all plan nodes. If you added a new plan node
/// inside the project, be sure to add here and in its conventions like [`for_logical_plan_nodes`]
///
/// Every tuple has two elements, where `{ convention, name }`
/// You can use it as follows
/// ```rust
/// macro_rules! use_plan {
///     ([], $({ $convention:ident, $name:ident }),*) => {};
/// }
/// risingwave_frontend::for_all_plan_nodes! { use_plan }
/// ```
/// See the following implementations for example.
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ Logical, Agg }
            ,{ Logical, Filter }
            ,{ Logical, Project }
            ,{ Logical, Scan }
            ,{ Logical, Insert }
            ,{ Logical, Join }
            ,{ Logical, Values }
            ,{ Logical, Limit }
            ,{ Logical, TopN }
            // ,{ Logical, Sort } we don't need a LogicalSort, just require the Order
            ,{ Batch, Project }
            ,{ Batch, Filter }
            ,{ Batch, SeqScan }
            ,{ Batch, HashJoin }
            ,{ Batch, SortMergeJoin }
            ,{ Batch, Sort }
            ,{ Batch, Exchange }
            ,{ Batch, Limit }
            ,{ Stream, Project }
            ,{ Stream, Filter }
            ,{ Stream, TableSource }
            ,{ Stream, HashJoin }
            ,{ Stream, Exchange }
        }
    };
}
/// `for_logical_plan_nodes` includes all plan nodes with logical convention.
#[macro_export]
macro_rules! for_logical_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ Logical, Agg }
            ,{ Logical, Filter }
            ,{ Logical, Project }
            ,{ Logical, Scan }
            ,{ Logical, Insert }
            ,{ Logical, Join }
            ,{ Logical, Values }
            ,{ Logical, Limit }
            ,{ Logical, TopN }
            // ,{ Logical, Sort} not sure if we will support Order by clause in subquery/view/MV
            // if we dont support thatk, we don't need LogicalSort, just require the Order at the top of query
        }
    };
}

/// `for_batch_plan_nodes` includes all plan nodes with batch convention.
#[macro_export]
macro_rules! for_batch_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ Batch, Project }
            ,{ Batch, Filter }
            ,{ Batch, SeqScan }
            ,{ Batch, HashJoin }
            ,{ Batch, Limit }
            ,{ Batch, SortMergeJoin }
            ,{ Batch, Sort }
            ,{ Batch, Exchange }
        }
    };
}

/// `for_stream_plan_nodes` includes all plan nodes with stream convention.
#[macro_export]
macro_rules! for_stream_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ Stream, Project }
            ,{ Stream, Filter }
            ,{ Stream, TableSource }
            ,{ Stream, HashJoin }
            ,{ Stream, Exchange }
        }
    };
}

/// impl PlanNodeType fn for each node.
macro_rules! enum_plan_node_type {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            /// each enum value represent a PlanNode struct type, help us to dispatch and downcast
            #[derive(PartialEq, Debug)]
            pub enum PlanNodeType{
                $( [<$convention $name>] ),*
            }

            $(impl PlanNode for [<$convention $name>] {
                fn node_type(&self) -> PlanNodeType{
                    PlanNodeType::[<$convention $name>]
                }
            })*
        }
    }
}
for_all_plan_nodes! { enum_plan_node_type }

/// impl fn plan_ref for each node.
macro_rules! impl_plan_ref {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl From<[<$convention $name>]> for PlanRef {
                fn from(plan: [<$convention $name>]) -> Self {
                    std::rc::Rc::new(plan)
                }
            })*
        }
    }
}

for_all_plan_nodes! { impl_plan_ref }

/// impl plan node downcast fn for each node.
macro_rules! impl_down_cast_fn {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            #[allow(unused)]
            impl dyn PlanNode {
                $( pub fn [< as_$convention:snake _ $name:snake>](&self) -> Option<&[<$convention $name>]> {
                    self.downcast_ref::<[<$convention $name>]>()
                } )*
            }
        }
    }
}
for_all_plan_nodes! { impl_down_cast_fn }
