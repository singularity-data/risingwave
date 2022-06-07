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

//! Define all [`Rule`]

use super::PlanRef;

/// A one-to-one transform for the [`PlanNode`](super::plan_node::PlanNode), every [`Rule`] should
/// downcast and check if the node matches the rule.
pub trait Rule: Send + Sync {
    /// return err(()) if not match
    fn apply(&self, plan: PlanRef) -> Option<PlanRef>;
}

pub(super) type BoxedRule = Box<dyn Rule>;

mod project_join;
pub use project_join::*;
mod project_elim;
pub use project_elim::*;
mod project_merge;
pub use project_merge::*;
mod unnest_agg_for_loj;
pub use unnest_agg_for_loj::*;
mod pull_up_correlated_predicate;
pub use pull_up_correlated_predicate::*;
mod index_delta_join_new;
pub use index_delta_join_new::*;
mod index_delta_join;
pub use index_delta_join::*;
mod multijoin_filter;
pub use multijoin_filter::*;
mod multijoin_join;
pub use multijoin_join::*;
mod reorder_multijoin;
pub use reorder_multijoin::*;
