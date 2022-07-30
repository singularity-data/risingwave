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

//! Define all property of plan tree node, which actually represent property of the node's result.
//!
//! We have physical property [`Order`] and [`Distribution`] which is on batch or stream operator,
//! also, we have logical property which all [`PlanNode`][PlanNode] has.
//!
//! We have not give any common abstract trait for the property yet. They are not so much and we
//! don't need get a common behavior now. we can treat them as different traits of the
//! [`PlanNode`][PlanNode] now and refactor them when our optimizer need more
//! (such as an optimizer based on the Volcano/Cascades model).
//!
//! [PlanNode]: super::plan_node::PlanNode
pub(crate) mod order;
pub use order::*;
mod distribution;
pub use distribution::*;
mod func_dep;
pub use func_dep::*;
