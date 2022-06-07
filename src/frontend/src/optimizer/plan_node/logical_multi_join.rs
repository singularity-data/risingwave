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
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan_common::JoinType;

use super::{
    ColPrunable, LogicalFilter, LogicalJoin, LogicalProject, PlanBase, PlanRef, PlanTreeNodeBinary,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::optimizer::plan_node::PlanTreeNode;
use crate::utils::{ColIndexMapping, Condition, ConnectedComponentLabeller};

/// `LogicalMultiJoin` combines two or more relations according to some condition.
///
/// Each output row has fields from one the inputs. The set of output rows is a subset
/// of the cartesian product of all the inputs; The `LogicalMultiInnerJoin` is only supported
/// for inner joins as it implicitly assumes commutativity. Non-inner joins should be
/// expressed as 2-way `LogicalJoin`s.
#[derive(Debug, Clone)]
pub struct LogicalMultiJoin {
    pub base: PlanBase,
    inputs: Vec<PlanRef>,
    on: Condition,
}

impl fmt::Display for LogicalMultiJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogicalMultiJoin {{ on: {} }}", &self.on)
    }
}

impl LogicalMultiJoin {
    pub(crate) fn new(inputs: Vec<PlanRef>, on: Condition) -> Self {
        let inputs_len = inputs.len();
        let ctx = inputs[0].ctx();

        // TODO: better way to deduce schema and pk instead of creating new plan nodes
        let pseudo_node = Self {
            base: inputs[0].plan_base().clone(),
            inputs: inputs.clone(),
            on: on.clone(),
        };
        let pseudo_node = pseudo_node.as_reordered_left_deep_join(&(0..inputs_len).collect_vec());

        let base = PlanBase::new_logical(
            ctx,
            pseudo_node.schema().clone(),
            pseudo_node.pk_indices().to_vec(),
        );
        Self { base, inputs, on }
    }

    pub(crate) fn from_join(join: &PlanRef) -> Option<Self> {
        let logical_join = join.as_logical_join()?;
        if logical_join.join_type() != JoinType::Inner {
            return None;
        }
        let left = logical_join.left();
        let right = logical_join.right();

        let left_col_num = left.schema().len();
        let right_col_num = right.schema().len();

        let mut inputs = vec![];
        let mut conjunctions = logical_join.on().conjunctions.clone();
        if let Some(multi_join) = left.as_logical_multi_join() {
            inputs.extend(multi_join.inputs());
            conjunctions.extend(multi_join.on().clone());
        } else {
            inputs.push(left.clone());
        }
        if let Some(multi_join) = right.as_logical_multi_join() {
            inputs.extend(multi_join.inputs());
            let right_on = multi_join.on().clone();
            let mut mapping =
                ColIndexMapping::with_shift_offset(right_col_num, left_col_num as isize);
            let new_on = right_on.rewrite_expr(&mut mapping);
            conjunctions.extend(new_on.conjunctions);
        } else {
            inputs.push(right.clone());
        }

        // TODO: derive schema and pk
        Some(Self {
            base: logical_join.base.clone(),
            inputs,
            on: Condition { conjunctions },
        })
    }

    pub(crate) fn from_filter(join: &PlanRef) -> Option<Self> {
        let logical_filter = join.as_logical_filter()?;
        let input = logical_filter.input();
        let multijoin = input.as_logical_multi_join()?;

        // TODO: derive schema and pk
        Some(Self {
            base: logical_filter.base.clone(),
            inputs: multijoin.inputs().to_vec(),
            on: multijoin
                .on()
                .clone()
                .and(logical_filter.predicate().clone()),
        })
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::new(self.inputs.clone(), cond)
    }
}

impl PlanTreeNode for LogicalMultiJoin {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.inputs.clone().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> crate::optimizer::PlanRef {
        Self::new(inputs.to_vec(), self.on.clone()).into()
    }
}

impl LogicalMultiJoin {
    pub fn as_reordered_left_deep_join(&self, join_ordering: &[usize]) -> PlanRef {
        assert_eq!(join_ordering.len(), self.inputs.len());
        assert!(!join_ordering.is_empty());

        let base_plan = self.inputs[join_ordering[0]].clone();

        // Express as a cross join, we will rely on filter pushdown to push all of the join
        // conditions to convert into inner joins.
        let mut output = join_ordering[1..]
            .iter()
            .fold(base_plan, |join_chain, &index| {
                LogicalJoin::new(
                    join_chain,
                    self.inputs[index].clone(),
                    JoinType::Inner,
                    Condition::true_cond(),
                )
                .into()
            });

        if join_ordering != (0..self.schema().len()).collect::<Vec<_>>() {
            output =
                LogicalProject::with_mapping(output, self.mapping_from_ordering(join_ordering))
                    .into();
        }

        // We will later push down all of the filters back to the individual joins via the
        // `FilterJoinRule`.
        output = LogicalFilter::create(output, self.on.clone());

        output
    }

    /// Our heuristic join reordering algorithm will try to perform a left-deep join.
    /// It will try to do the following:
    ///
    /// 1. First, split the join graph, with eq join conditions as graph edges, into their connected
    ///    components. Repeat the procedure in 2. with the largest connected components down to
    ///    the smallest.
    /// 2. For each connected component, add joins to the chain, prioritizing adding those
    ///    joins to the bottom of the chain if their join conditions have:
    ///       a. eq joins between primary keys on both sides
    ///       b. eq joins with primary keys on one side
    ///       c. more equijoin conditions
    ///    in that order. This forms our selectivity heuristic.
    /// 3. Thirdly, we will emit a left-deep cross-join of each of the left-deep joins of the
    ///    connected components. Depending on the type of plan, this may result in a planner failure
    ///    (e.g. for streaming). No cross-join will be emitted for a single connected component.
    /// 4. Finally, we will emit, above the left-deep join tree:
    ///        a. a filter with the non eq conditions
    ///        b. a projection which reorders the output column ordering to agree with the
    ///           original ordering of the joins.
    ///   The filter will then be pushed down by another filter pushdown pass.
    pub(crate) fn heuristic_ordering(&self) -> Result<Vec<usize>> {
        let mut labeller = ConnectedComponentLabeller::new(self.inputs.len());

        let (eq_join_conditions, _) = self.on.clone().split_by_input_col_nums(
            &self.input_col_nums(),
            // only_eq=
            true,
        );

        // Iterate over all join conditions, whose keys represent edges on the join graph
        for k in eq_join_conditions.keys() {
            labeller.add_edge(k.0, k.1);
        }

        let mut edge_sets: Vec<_> = labeller.into_edge_sets();

        // Sort in decreasing order of len
        edge_sets.sort_by_key(|a| std::cmp::Reverse(a.len()));

        let mut join_ordering = vec![];

        for component in edge_sets {
            let mut eq_cond_edges: Vec<(usize, usize)> = component.into_iter().collect();

            // TODO(jon-chuang): add sorting of eq_cond_edges based on selectivity here
            eq_cond_edges.sort();

            if eq_cond_edges.is_empty() {
                // There is nothing to join in this connected component
                break;
            };

            let edge = eq_cond_edges.remove(0);
            join_ordering.extend(&vec![edge.0, edge.1]);

            while !eq_cond_edges.is_empty() {
                let mut found = vec![];
                for (idx, edge) in eq_cond_edges.iter().enumerate() {
                    // If the eq join condition is on the existing join, we don't add any new
                    // inputs to the join
                    if join_ordering.contains(&edge.1) && join_ordering.contains(&edge.0) {
                        found.push(idx);
                    } else {
                        // Else, the eq join condition involves a new input, or is not connected to
                        // the existing left deep tree. Handle accordingly.
                        let new_input = if join_ordering.contains(&edge.0) {
                            edge.1
                        } else if join_ordering.contains(&edge.1) {
                            edge.0
                        } else {
                            continue;
                        };
                        join_ordering.push(new_input);
                        found.push(idx);
                    }
                }
                // This ensures eq_cond_edges.len() is strictly decreasing per iteration
                // Since the graph is connected, it is always possible to find at least one edge
                // remaining that can be connected to the current join result.
                if found.is_empty() {
                    return Err(RwError::from(ErrorCode::InternalError(
                        "Connecting edge not found in join connected subgraph".into(),
                    )));
                }
                let mut idx = 0;
                eq_cond_edges.retain(|_| {
                    let keep = !found.contains(&idx);
                    idx += 1;
                    keep
                });
            }
        }
        // Deal with singleton inputs (with no eq condition joins between them whatsoever)
        for i in 0..self.inputs.len() {
            if !join_ordering.contains(&i) {
                join_ordering.push(i);
            }
        }
        Ok(join_ordering)
    }

    pub(crate) fn input_col_nums(&self) -> Vec<usize> {
        self.inputs.iter().map(|i| i.schema().len()).collect()
    }

    pub(crate) fn mapping_from_ordering(&self, ordering: &[usize]) -> ColIndexMapping {
        let offsets = self.input_col_offsets();
        let max_len = offsets[self.inputs.len()];
        let mut map = Vec::with_capacity(self.schema().len());
        let input_num_cols = self.input_col_nums();
        for &input_index in ordering {
            map.extend(
                (offsets[input_index]..offsets[input_index] + input_num_cols[input_index])
                    .map(Some),
            )
        }
        ColIndexMapping::with_target_size(map, max_len)
    }

    fn input_col_offsets(&self) -> Vec<usize> {
        self.inputs().iter().fold(vec![0], |mut v, i| {
            v.push(v.last().unwrap() + i.schema().len());
            v
        })
    }
}

impl ToStream for LogicalMultiJoin {
    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }

    fn to_stream(&self) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ToBatch for LogicalMultiJoin {
    fn to_batch(&self) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ColPrunable for LogicalMultiJoin {
    fn prune_col(&self, _required_cols: &[usize]) -> PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl PredicatePushdown for LogicalMultiJoin {
    fn predicate_pushdown(&self, _predicate: Condition) -> PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}
