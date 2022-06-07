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

use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::optimizer::delta_join_solver::{
    DeltaJoinSolver, JoinEdge, JoinTable, LookupPath, StreamStrategy,
};
use crate::utils::Condition;

/// Use index scan and delta joins for supported queries.
pub struct IndexDeltaJoinRule {}

impl Rule for IndexDeltaJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        // Find a multi-join node
        let multi_join = plan.as_logical_multi_join()?;

        tracing::trace!("processing multi-way join in index delta join rule");

        // Require all children to be table scans
        let inputs = multi_join.inputs();
        let children = inputs
            .iter()
            .map(|x| x.as_logical_scan())
            .collect::<Option<Vec<_>>>()?;

        tracing::trace!("all children are table scans");

        fn match_index(join_indices: &[usize], table_scan: &LogicalScan) -> Option<PlanRef> {
            if table_scan.indexes().is_empty() {
                return None;
            }
            let column_descs = table_scan.column_descs();
            // We assume column id of create index's MV is exactly the same as corresponding columns
            // in the original table. We generate a list of column ids, and match them against
            // prefixes of indexes.
            let columns_to_match = join_indices
                .iter()
                .map(|idx| column_descs[*idx].column_id)
                .collect_vec();

            for (name, index) in table_scan.indexes() {
                // 1. Check if distribution keys are the same.
                // We don't assume the hash function we are using satisfies commutativity
                // `Hash(A, B) == Hash(B, A)`, so we consider order of each item in distribution
                // keys here.
                if index
                    .distribution_keys
                    .iter()
                    .map(|x| index.columns[*x].column_id)
                    .collect_vec()
                    != columns_to_match
                {
                    continue;
                }

                // 2. Check if the join keys are prefix of order keys

                // A HashSet containing remaining columns to match
                let mut remaining_to_match =
                    columns_to_match.iter().copied().collect::<HashSet<_>>();

                // Begin match join columns with index prefix. e.g., if the join columns are `a, b,
                // c`, and the index has `a, b, c` or `a, c, b` or any combination as prefix, then
                // we can use this index.
                for ordered_column in &index.order_desc {
                    let column_id = ordered_column.column_desc.column_id;

                    match remaining_to_match.remove(&column_id) {
                        true => continue, // matched
                        false => break,   // not matched
                    }
                }

                if remaining_to_match.is_empty() {
                    return Some(table_scan.to_index_scan(name, index).into());
                }
            }

            None
        }

        // Number of columns in each table.
        let input_cols = multi_join.input_col_nums();
        tracing::trace!("cols: {:?}", input_cols);

        let begin_offsets = {
            let mut offsets = vec![];
            let mut sum = 0;

            for &n in &input_cols {
                offsets.push(sum);
                sum += n;
            }
            offsets
        };

        // Firstly, we get all eq join conditions from the multi-join plan node.
        let (eq_join_conditions, _) = multi_join.on().clone().split_by_input_col_nums(
            &input_cols,
            // only eq conditions
            true,
        );

        // Stores `(table_1, table_2) -> (vec![col1, col2], vec![col3, col4])`,
        // where col1, col2, col3, col4 are the real column id in each table.
        let mut tables = BTreeMap::new();

        for (&(left_table_id, right_table_id), cond) in &eq_join_conditions {
            tracing::trace!("processing eq condition: {}", cond);

            if cond.conjunctions.is_empty() {
                return None;
            }
            let mut left_keys = vec![];
            let mut right_keys = vec![];
            for conj in &cond.conjunctions {
                let (left_key, right_key) = Condition::as_eq_cond(conj)?;
                // The keys are position in the schema instead of in table, so we need to map it to
                // the table column id.
                let l = left_key.index();
                let r = right_key.index();

                let left_table = begin_offsets.partition_point(|&offset| offset <= l) - 1;
                let right_table = begin_offsets.partition_point(|&offset| offset <= r) - 1;
                left_keys.push(l - begin_offsets[left_table]);
                right_keys.push(r - begin_offsets[right_table]);
            }
            tables.insert((left_table_id, right_table_id), (left_keys, right_keys));
        }

        tracing::trace!("resolved eq relation between tables: {:?}", tables);

        // All matched indices
        let mut matched_index = BTreeMap::new();

        // Check if all join keys have indices.
        for (&(left_table, right_table), (left_join_keys, right_join_keys)) in &tables {
            for (table, join_keys) in [
                (left_table, left_join_keys.clone()),
                (right_table, right_join_keys.clone()),
            ] {
                if !matched_index.contains_key(&(table, join_keys.clone())) {
                    tracing::trace!("matching index: {} with {:?}", left_table, join_keys);
                    let index = match_index(&join_keys, children[table])?;
                    matched_index.insert((table, join_keys), index);
                }
            }
        }

        tracing::trace!(
            "all matched index: {:?}",
            matched_index.keys().collect_vec()
        );

        let heuristic_ordering = multi_join.heuristic_ordering().ok()?;
        tracing::trace!("heuristic ordering: {:?}", heuristic_ordering);

        let solver = DeltaJoinSolver::new(
            StreamStrategy::LeftThisEpoch,
            tables
                .iter()
                .map(
                    |(&(left_table, right_table), (left_join_keys, right_join_keys))| JoinEdge {
                        left: JoinTable(left_table),
                        right: JoinTable(right_table),
                        left_join_keys: left_join_keys.clone(),
                        right_join_keys: right_join_keys.clone(),
                    },
                )
                .collect(),
            heuristic_ordering
                .iter()
                .map(|&table| JoinTable(table))
                .collect(),
        );

        let solution = solver.solve().ok()?;
        tracing::trace!("delta join solution: {:#?}", solution);

        let mut inputs = Vec::with_capacity(children.len());
        for LookupPath(stream, tables) in solution {
            let mut join_ordering = Vec::with_capacity(children.len());
            join_ordering.push(stream.0);
            join_ordering.extend(tables.iter().map(|x| x.0));
            let plan = multi_join.as_reordered_left_deep_join(&join_ordering);
            inputs.push(plan);
        }

        let lookup_union = LogicalUnion::new(inputs, UnionMode::StreamLastToFirst);

        Some(lookup_union.into())
    }
}

impl IndexDeltaJoinRule {
    #[allow(dead_code)]
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}
