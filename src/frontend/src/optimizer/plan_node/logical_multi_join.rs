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
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan_common::JoinType;

use super::{
    ColPrunable, LogicalFilter, LogicalJoin, LogicalProject, PlanBase, PlanNodeType, PlanRef,
    PlanTreeNodeBinary, PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::expr::{ExprImpl, ExprRewriter};
use crate::optimizer::plan_node::PlanTreeNode;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay, ConnectedComponentLabeller};

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
    output_indices: Vec<usize>,
    inner2output: ColIndexMapping,
    // XXX(st1page): these fields will be used in prune_col and
    // pk_derive soon.
    /// the mapping output_col_idx -> (input_idx, input_col_idx), **"output_col_idx" is internal,
    /// not consider output_indices**
    #[allow(unused)]
    inner_o2i_mapping: Vec<(usize, usize)>,
    inner_i2o_mappings: Vec<ColIndexMapping>,
}

impl fmt::Display for LogicalMultiJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogicalMultiJoin {{ on: {} }}", {
            let fields = self
                .inputs
                .iter()
                .flat_map(|input| input.schema().fields.clone())
                .collect_vec();
            let input_schema = Schema { fields };
            format!(
                "{}",
                ConditionDisplay {
                    condition: self.on(),
                    input_schema: &input_schema,
                }
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct LogicalMultiJoinBuilder {
    output_indices: Vec<usize>,
    /// the predicates in the on condition, we do not use Condition here to emit unnecessary
    /// simplify.
    conjunctions: Vec<ExprImpl>,
    inputs: Vec<PlanRef>,
    tot_input_col_num: usize,
}

impl LogicalMultiJoinBuilder {
    /// add a predicate above the plan, so they will be rewriten from the `output_indices` to the
    /// input indices
    pub fn add_predicate_above(&mut self, exprs: impl Iterator<Item = ExprImpl>) {
        let mut mapping = ColIndexMapping::with_target_size(
            self.output_indices.iter().map(|i| Some(*i)).collect(),
            self.tot_input_col_num,
        );
        self.conjunctions
            .extend(exprs.map(|expr| mapping.rewrite_expr(expr)));
    }

    pub fn build(self) -> LogicalMultiJoin {
        LogicalMultiJoin::new(
            self.inputs,
            Condition {
                conjunctions: self.conjunctions,
            },
            self.output_indices,
        )
    }

    pub fn into_parts(self) -> (Vec<usize>, Vec<ExprImpl>, Vec<PlanRef>, usize) {
        (
            self.output_indices,
            self.conjunctions,
            self.inputs,
            self.tot_input_col_num,
        )
    }

    pub fn new(plan: PlanRef) -> LogicalMultiJoinBuilder {
        match plan.node_type() {
            PlanNodeType::LogicalJoin => Self::with_join(plan),
            PlanNodeType::LogicalFilter => Self::with_filter(plan),
            PlanNodeType::LogicalProject => Self::with_project(plan),
            _ => Self::with_input(plan),
        }
    }

    fn with_join(plan: PlanRef) -> LogicalMultiJoinBuilder {
        let join: &LogicalJoin = plan.as_logical_join().unwrap();
        if join.join_type() != JoinType::Inner {
            return Self::with_input(plan);
        }
        let left = join.left();
        let right = join.right();

        let mut builder = Self::new(left);

        let (r_output_indices, r_conjunctions, mut r_inputs, r_tot_input_col_num) =
            Self::new(right).into_parts();

        // the mapping from the right's column index to the current multi join's internal column
        // index
        let mut shift_mapping = ColIndexMapping::with_shift_offset(
            r_tot_input_col_num,
            builder.tot_input_col_num as isize,
        );
        builder.inputs.append(&mut r_inputs);
        builder.tot_input_col_num += r_tot_input_col_num;

        builder.conjunctions.extend(
            r_conjunctions
                .into_iter()
                .map(|expr| shift_mapping.rewrite_expr(expr)),
        );

        builder.output_indices.extend(
            r_output_indices
                .into_iter()
                .map(|idx| shift_mapping.map(idx)),
        );
        builder.add_predicate_above(join.on().conjunctions.iter().cloned());

        builder.output_indices = join
            .output_indices()
            .iter()
            .map(|idx| builder.output_indices[*idx])
            .collect();
        builder
    }

    fn with_filter(plan: PlanRef) -> LogicalMultiJoinBuilder {
        let filter: &LogicalFilter = plan.as_logical_filter().unwrap();
        let mut builder = Self::new(filter.input());
        builder.add_predicate_above(filter.predicate().conjunctions.iter().cloned());
        builder
    }

    fn with_project(plan: PlanRef) -> LogicalMultiJoinBuilder {
        let proj: &LogicalProject = plan.as_logical_project().unwrap();
        let output_indices = match proj.try_as_projection() {
            Some(output_indices) => output_indices,
            None => return Self::with_input(plan),
        };
        let mut builder = Self::new(proj.input());
        builder.output_indices = output_indices
            .into_iter()
            .map(|i| builder.output_indices[i])
            .collect();
        builder
    }

    fn with_input(input: PlanRef) -> LogicalMultiJoinBuilder {
        LogicalMultiJoinBuilder {
            output_indices: (0..input.schema().len()).collect_vec(),
            conjunctions: vec![],
            tot_input_col_num: input.schema().len(),
            inputs: vec![input],
        }
    }

    pub fn inputs(&self) -> &[PlanRef] {
        self.inputs.as_ref()
    }
}
impl LogicalMultiJoin {
    pub(crate) fn new(inputs: Vec<PlanRef>, on: Condition, output_indices: Vec<usize>) -> Self {
        let input_schemas = inputs
            .iter()
            .map(|input| input.schema().clone())
            .collect_vec();

        let (inner_o2i_mapping, tot_col_num) = {
            let mut inner_o2i_mapping = vec![];
            let mut tot_col_num = 0;
            for (input_idx, input_schema) in input_schemas.iter().enumerate() {
                tot_col_num += input_schema.len();
                for (col_idx, _field) in input_schema.fields().iter().enumerate() {
                    inner_o2i_mapping.push((input_idx, col_idx));
                }
            }
            (inner_o2i_mapping, tot_col_num)
        };
        let inner2output = ColIndexMapping::with_remaining_columns(&output_indices, tot_col_num);

        let schema = Schema {
            fields: output_indices
                .iter()
                .map(|idx| inner_o2i_mapping[*idx])
                .map(|(input_idx, col_idx)| input_schemas[input_idx].fields()[col_idx].clone())
                .collect(),
        };

        let inner_i2o_mappings = {
            let mut i2o_maps = vec![];
            for input_schma in &input_schemas {
                let map = vec![None; input_schma.len()];
                i2o_maps.push(map);
            }
            for (out_idx, (input_idx, in_idx)) in inner_o2i_mapping.iter().enumerate() {
                i2o_maps[*input_idx][*in_idx] = Some(out_idx);
            }

            i2o_maps
                .into_iter()
                .map(|map| ColIndexMapping::with_target_size(map, tot_col_num))
                .collect_vec()
        };

        let pk_indices = {
            let mut pk_indices = vec![];
            for (i, input_pk) in inputs.iter().map(|input| input.pk_indices()).enumerate() {
                for input_pk_idx in input_pk {
                    pk_indices.push(inner_i2o_mappings[i].map(*input_pk_idx));
                }
            }
            pk_indices
                .into_iter()
                .map(|col_idx| inner2output.try_map(col_idx))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        };
        let base = PlanBase::new_logical(inputs[0].ctx(), schema, pk_indices);

        Self {
            base,
            inputs,
            on,
            output_indices,
            inner2output,
            inner_o2i_mapping,
            inner_i2o_mappings,
        }
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::new(self.inputs.clone(), cond, self.output_indices.clone())
    }
}

impl PlanTreeNode for LogicalMultiJoin {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.inputs.clone().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(
            inputs.to_vec(),
            self.on().clone(),
            self.output_indices.clone(),
        )
        .into()
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

        let total_col_num = self.inner2output.source_size();
        let reorder_mapping = {
            let mut reorder_mapping = vec![None; total_col_num];
            join_ordering
                .iter()
                .cloned()
                .flat_map(|input_idx| {
                    (0..self.inputs[input_idx].schema().len())
                        .into_iter()
                        .map(move |col_idx| self.inner_i2o_mappings[input_idx].map(col_idx))
                })
                .enumerate()
                .for_each(|(tar, src)| reorder_mapping[src] = Some(tar));
            reorder_mapping
        };
        output =
            LogicalProject::with_out_col_idx(output, reorder_mapping.iter().map(|i| i.unwrap()))
                .into();

        // We will later push down all of the filters back to the individual joins via the
        // `FilterJoinRule`.
        output = LogicalFilter::create(output, self.on.clone());
        output =
            LogicalProject::with_out_col_idx(output, self.output_indices.iter().cloned()).into();

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
}

impl ToStream for LogicalMultiJoin {
    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }

    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
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
