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

#![allow(rustdoc::private_intra_doc_links)]
//! Defines all kinds of node in the plan tree, each node represent a relational expression.
//!
//! We use a immutable style tree structure, every Node are immutable and cannot be modified after
//! it has been created. If you want to modify the node, such as rewriting the expression in a
//! `ProjectNode` or changing a node's input node, you need to create a new node. We use Rc as the
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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::batch_plan::PlanNode as BatchPlanProst;
use risingwave_pb::stream_plan::StreamNode as StreamPlanProst;

use super::property::{Distribution, FunctionalDependencySet, Order};

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as
/// `dyn PlanNode`
///
/// We split the trait into lots of sub-trait so that we can easily use macro to impl them.
pub trait PlanNode:
    PlanTreeNode
    + DynClone
    + Debug
    + Display
    + Downcast
    + ColPrunable
    + ToBatch
    + ToStream
    + ToDistributedBatch
    + ToProst
    + ToLocalBatch
    + PredicatePushdown
{
    fn node_type(&self) -> PlanNodeType;
    fn plan_base(&self) -> &PlanBase;
    fn convention(&self) -> Convention;
}

impl_downcast!(PlanNode);
pub type PlanRef = Rc<dyn PlanNode>;

#[derive(Clone, Debug, Copy)]
pub struct PlanNodeId(pub i32);

#[derive(Debug, PartialEq)]
pub enum Convention {
    Logical,
    Batch,
    Stream,
}

impl dyn PlanNode {
    /// Write explain the whole plan tree.
    pub fn explain(&self, level: usize, f: &mut impl std::fmt::Write) -> std::fmt::Result {
        writeln!(f, "{}{}", " ".repeat(level * 2), self)?;
        for input in self.inputs() {
            input.explain(level + 1, f)?;
        }
        Ok(())
    }

    /// Explain the plan node and return a string.
    pub fn explain_to_string(&self) -> Result<String> {
        let mut output = String::new();
        self.explain(0, &mut output)
            .map_err(|e| ErrorCode::InternalError(format!("failed to explain: {}", e)))?;
        Ok(output)
    }

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

    /// Serialize the plan node and its children to a batch plan proto.
    pub fn to_batch_prost(&self) -> BatchPlanProst {
        self.to_batch_prost_identity(true)
    }

    /// Serialize the plan node and its children to a batch plan proto without the identity field
    /// (for testing).
    pub fn to_batch_prost_identity(&self, identity: bool) -> BatchPlanProst {
        let node_body = Some(self.to_batch_prost_body());
        let children = self
            .inputs()
            .into_iter()
            .map(|plan| plan.to_batch_prost_identity(identity))
            .collect();
        BatchPlanProst {
            children,
            identity: if identity {
                format!("{:?}", self)
            } else {
                "".into()
            },
            node_body,
        }
    }

    /// Serialize the plan node and its children to a stream plan proto.
    ///
    /// Note that [`StreamTableScan`] has its own implementation of `to_stream_prost`. We have a
    /// hook inside to do some ad-hoc thing for [`StreamTableScan`].
    pub fn to_stream_prost(&self) -> StreamPlanProst {
        self.to_stream_prost_auto_fields(true)
    }

    /// Serialize the plan node and its children to a stream plan proto without identity and without
    /// operator id (for testing).
    pub fn to_stream_prost_auto_fields(&self, auto_fields: bool) -> StreamPlanProst {
        if let Some(stream_table_scan) = self.as_stream_table_scan() {
            return stream_table_scan.adhoc_to_stream_prost(auto_fields);
        }
        if let Some(stream_index_scan) = self.as_stream_index_scan() {
            return stream_index_scan.adhoc_to_stream_prost(auto_fields);
        }

        let node = Some(self.to_stream_prost_body());
        let input = self
            .inputs()
            .into_iter()
            .map(|plan| plan.to_stream_prost_auto_fields(auto_fields))
            .collect();
        // TODO: support pk_indices and operator_id
        StreamPlanProst {
            input,
            identity: if auto_fields {
                format!("{}", self)
            } else {
                "".into()
            },
            node_body: node,
            operator_id: if auto_fields { self.id().0 as u64 } else { 0 },
            pk_indices: self.pk_indices().iter().map(|x| *x as u32).collect(),
            fields: self.schema().to_prost(),
            append_only: self.append_only(),
        }
    }
}

mod plan_base;
pub use plan_base::*;
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
mod predicate_pushdown;
pub use predicate_pushdown::*;

mod batch_delete;
mod batch_exchange;
mod batch_expand;
mod batch_filter;
mod batch_hash_agg;
mod batch_hash_join;
mod batch_hop_window;
mod batch_insert;
mod batch_limit;
mod batch_lookup_join;
mod batch_nested_loop_join;
mod batch_project;
mod batch_project_set;
mod batch_seq_scan;
mod batch_simple_agg;
mod batch_sort;
mod batch_table_function;
mod batch_topn;
mod batch_update;
mod batch_values;
mod logical_agg;
mod logical_apply;
mod logical_delete;
mod logical_expand;
mod logical_filter;
mod logical_hop_window;
mod logical_insert;
mod logical_join;
mod logical_limit;
mod logical_multi_join;
mod logical_project;
mod logical_project_set;
mod logical_scan;
mod logical_source;
mod logical_table_function;
mod logical_topn;
mod logical_update;
mod logical_values;
mod stream_delta_join;
mod stream_dynamic_filter;
mod stream_exchange;
mod stream_expand;
mod stream_filter;
mod stream_global_simple_agg;
mod stream_hash_agg;
mod stream_hash_join;
mod stream_hop_window;
mod stream_index_scan;
mod stream_local_simple_agg;
mod stream_materialize;
mod stream_project;
mod stream_project_set;
mod stream_sink;
mod stream_source;
mod stream_table_scan;
mod stream_topn;

pub mod utils;

pub use batch_delete::BatchDelete;
pub use batch_exchange::BatchExchange;
pub use batch_expand::BatchExpand;
pub use batch_filter::BatchFilter;
pub use batch_hash_agg::BatchHashAgg;
pub use batch_hash_join::BatchHashJoin;
pub use batch_hop_window::BatchHopWindow;
pub use batch_insert::BatchInsert;
pub use batch_limit::BatchLimit;
pub use batch_lookup_join::BatchLookupJoin;
pub use batch_nested_loop_join::BatchNestedLoopJoin;
pub use batch_project::BatchProject;
pub use batch_project_set::BatchProjectSet;
pub use batch_seq_scan::BatchSeqScan;
pub use batch_simple_agg::BatchSimpleAgg;
pub use batch_sort::BatchSort;
pub use batch_table_function::BatchTableFunction;
pub use batch_topn::BatchTopN;
pub use batch_update::BatchUpdate;
pub use batch_values::BatchValues;
pub use logical_agg::{LogicalAgg, PlanAggCall, PlanAggCallDisplay};
pub use logical_apply::LogicalApply;
pub use logical_delete::LogicalDelete;
pub use logical_expand::LogicalExpand;
pub use logical_filter::LogicalFilter;
pub use logical_hop_window::LogicalHopWindow;
pub use logical_insert::LogicalInsert;
pub use logical_join::LogicalJoin;
pub use logical_limit::LogicalLimit;
pub use logical_multi_join::{LogicalMultiJoin, LogicalMultiJoinBuilder};
pub use logical_project::{LogicalProject, LogicalProjectBuilder};
pub use logical_project_set::LogicalProjectSet;
pub use logical_scan::LogicalScan;
pub use logical_source::LogicalSource;
pub use logical_table_function::LogicalTableFunction;
pub use logical_topn::LogicalTopN;
pub use logical_update::LogicalUpdate;
pub use logical_values::LogicalValues;
pub use stream_delta_join::StreamDeltaJoin;
pub use stream_dynamic_filter::StreamDynamicFilter;
pub use stream_exchange::StreamExchange;
pub use stream_expand::StreamExpand;
pub use stream_filter::StreamFilter;
pub use stream_global_simple_agg::StreamGlobalSimpleAgg;
pub use stream_hash_agg::StreamHashAgg;
pub use stream_hash_join::StreamHashJoin;
pub use stream_hop_window::StreamHopWindow;
pub use stream_index_scan::StreamIndexScan;
pub use stream_local_simple_agg::StreamLocalSimpleAgg;
pub use stream_materialize::StreamMaterialize;
pub use stream_project::StreamProject;
pub use stream_project_set::StreamProjectSet;
pub use stream_sink::StreamSink;
pub use stream_source::StreamSource;
pub use stream_table_scan::StreamTableScan;
pub use stream_topn::StreamTopN;

use crate::session::OptimizerContextRef;

/// `for_all_plan_nodes` includes all plan nodes. If you added a new plan node
/// inside the project, be sure to add here and in its conventions like `for_logical_plan_nodes`
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
            , { Logical, Agg }
            , { Logical, Apply }
            , { Logical, Filter }
            , { Logical, Project }
            , { Logical, Scan }
            , { Logical, Source }
            , { Logical, Insert }
            , { Logical, Delete }
            , { Logical, Update }
            , { Logical, Join }
            , { Logical, Values }
            , { Logical, Limit }
            , { Logical, TopN }
            , { Logical, HopWindow }
            , { Logical, TableFunction }
            , { Logical, MultiJoin }
            , { Logical, Expand }
            , { Logical, ProjectSet }
            // , { Logical, Sort } we don't need a LogicalSort, just require the Order
            , { Batch, SimpleAgg }
            , { Batch, HashAgg }
            , { Batch, Project }
            , { Batch, Filter }
            , { Batch, Insert }
            , { Batch, Delete }
            , { Batch, Update }
            , { Batch, SeqScan }
            , { Batch, HashJoin }
            , { Batch, NestedLoopJoin }
            , { Batch, Values }
            , { Batch, Sort }
            , { Batch, Exchange }
            , { Batch, Limit }
            , { Batch, TopN }
            , { Batch, HopWindow }
            , { Batch, TableFunction }
            , { Batch, Expand }
            , { Batch, LookupJoin }
            , { Batch, ProjectSet }
            , { Stream, Project }
            , { Stream, Filter }
            , { Stream, TableScan }
            , { Stream, Sink }
            , { Stream, Source }
            , { Stream, HashJoin }
            , { Stream, Exchange }
            , { Stream, HashAgg }
            , { Stream, LocalSimpleAgg }
            , { Stream, GlobalSimpleAgg }
            , { Stream, Materialize }
            , { Stream, TopN }
            , { Stream, HopWindow }
            , { Stream, DeltaJoin }
            , { Stream, IndexScan }
            , { Stream, Expand }
            , { Stream, DynamicFilter }
            , { Stream, ProjectSet }
        }
    };
}

/// `for_logical_plan_nodes` includes all plan nodes with logical convention.
#[macro_export]
macro_rules! for_logical_plan_nodes {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            , { Logical, Agg }
            , { Logical, Apply }
            , { Logical, Filter }
            , { Logical, Project }
            , { Logical, Scan }
            , { Logical, Source }
            , { Logical, Insert }
            , { Logical, Delete }
            , { Logical, Update }
            , { Logical, Join }
            , { Logical, Values }
            , { Logical, Limit }
            , { Logical, TopN }
            , { Logical, HopWindow }
            , { Logical, TableFunction }
            , { Logical, MultiJoin }
            , { Logical, Expand }
            , { Logical, ProjectSet }
            // , { Logical, Sort} not sure if we will support Order by clause in subquery/view/MV
            // if we dont support that, we don't need LogicalSort, just require the Order at the top of query
        }
    };
}

/// `for_batch_plan_nodes` includes all plan nodes with batch convention.
#[macro_export]
macro_rules! for_batch_plan_nodes {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            , { Batch, SimpleAgg }
            , { Batch, HashAgg }
            , { Batch, Project }
            , { Batch, Filter }
            , { Batch, SeqScan }
            , { Batch, HashJoin }
            , { Batch, NestedLoopJoin }
            , { Batch, Values }
            , { Batch, Limit }
            , { Batch, Sort }
            , { Batch, TopN }
            , { Batch, Exchange }
            , { Batch, Insert }
            , { Batch, Delete }
            , { Batch, Update }
            , { Batch, HopWindow }
            , { Batch, TableFunction }
            , { Batch, Expand }
            , { Batch, LookupJoin }
            , { Batch, ProjectSet }
        }
    };
}

/// `for_stream_plan_nodes` includes all plan nodes with stream convention.
#[macro_export]
macro_rules! for_stream_plan_nodes {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            , { Stream, Project }
            , { Stream, Filter }
            , { Stream, HashJoin }
            , { Stream, Exchange }
            , { Stream, TableScan }
            , { Stream, Sink }
            , { Stream, Source }
            , { Stream, HashAgg }
            , { Stream, LocalSimpleAgg }
            , { Stream, GlobalSimpleAgg }
            , { Stream, Materialize }
            , { Stream, TopN }
            , { Stream, HopWindow }
            , { Stream, DeltaJoin }
            , { Stream, IndexScan }
            , { Stream, Expand }
            , { Stream, DynamicFilter }
            , { Stream, ProjectSet }
        }
    };
}

/// impl [`PlanNodeType`] fn for each node.
macro_rules! enum_plan_node_type {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            /// each enum value represent a PlanNode struct type, help us to dispatch and downcast
            #[derive(Copy, Clone, PartialEq, Debug)]
            pub enum PlanNodeType {
                $( [<$convention $name>] ),*
            }

            $(impl PlanNode for [<$convention $name>] {
                fn node_type(&self) -> PlanNodeType{
                    PlanNodeType::[<$convention $name>]
                }
                fn plan_base(&self) -> &PlanBase {
                    &self.base
                }
                fn convention(&self) -> Convention {
                    Convention::$convention
                }
            })*
        }
    }
}

for_all_plan_nodes! { enum_plan_node_type }

/// impl fn `plan_ref` for each node.
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
            impl dyn PlanNode {
                $( pub fn [< as_$convention:snake _ $name:snake>](&self) -> Option<&[<$convention $name>]> {
                    self.downcast_ref::<[<$convention $name>]>()
                } )*
            }
        }
    }
}

for_all_plan_nodes! { impl_down_cast_fn }
