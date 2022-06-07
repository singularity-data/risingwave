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

//! The solver for delta join, whic determines lookup order of a join plan.
//! All collection types in this module should be `BTree` to ensure determinism between runs.
//!
//! # Representation of Multi-Way Join
//!
//! In this module, `a*` means lookup executor that looks-up the arrangement `a` of the current
//! epoch. `a` means looks-up arrangement `a` of the previous epoch.
//!
//! Delta joins only support inner equal join. The solver is based on the following formula (take
//! 3-way join as an example):
//!
//! ```plain
//! d((A join1 B) join2 C)
//! = ((A + dA) join1 (B + dB)) join2 (C + dC) - A join1 B join2 C
//! = (A join1 B + A join1 dB + dA join1 (B + dB)) join2 (C + dC) - A join1 B join2 C
//! = A join1 B join2 (C + dC) + A join1 dB join2 (C + dC) + dA join1 (B + dB) join2 (C + dC) - A join1 B join2 C
//! = A join1 B join2 dC + A join1 dB join2 (C + dC) + dA join1 (B + dB) join2 (C + dC)
//! = dA join1 (B + dB) join2 (C + dC) + dB join1 A join2 (C + dC) + dC join2 B join1 A
//!
//! join1 means A join B using condition #1,
//! join2 means B join C using condition #2.
//! ```
//!
//! Inner joins satisfy commutative law and associative laws, so we can switch them back and forth
//! between joins.
//!
//! ... which generates the following look-up graph:
//!
//! ```plain
//! a -> b* -> c* -> output3
//! b -> a  -> c* -> output2
//! c -> b  -> a  -> output1
//! ```
//!
//! And the final output is `output1 <concat> output2 <concat> output3`. The concatenation is
//! ordered: all items from output 1 must appear before output 2.
//!
//! TODO: support dynamic filter and filter condition in lookups.
//!
//! # What need the caller do?
//!
//! After solving the delta join plan, caller will need to do a lot of things.
//!
//! * Use the correct index for every stream input. By looking at the first lookup fragment in each
//!   row, we can decide whether to use `a.x` or `a.y` as index for stream input.
//! * Insert exchanges between lookups of different distribution. Generally, if the whole row is
//!   operating on the same join key, we only need to do 1v1 exchanges between lookups. However, it
//!   would be possible that a row of lookup first join `a.x == b.x`, then `a.y == c.y`. In this
//!   case, we will need to insert hash exchange between these two lookups.
//! * Ensure the order of union. Always union from the last row to the first row.
//! * Insert exchange before union. Still the case for `a.x == b.x`, then `a.y == c.y`, it is
//!   possible that every lookup path produces different distribution. We need to shuffle them
//!   before feeding data to union.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, Result};
use itertools::Itertools;

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct JoinTable(pub usize);

/// Represents whether `left` and `right` can be joined using a condition.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JoinEdge {
    pub left: JoinTable,
    pub right: JoinTable,
    pub left_join_keys: Vec<usize>,
    pub right_join_keys: Vec<usize>,
}

impl JoinEdge {
    /// Reverse the order of the edge.
    pub fn reverse(&self) -> Self {
        Self {
            left: self.right,
            right: self.left,
            left_join_keys: self.right_join_keys.clone(),
            right_join_keys: self.left_join_keys.clone(),
        }
    }
}

/// Decides how to place arrangements over lookup nodes. Given a 3-way join example:
///
/// ```plain
/// a -> 1* -> 2* ->
/// b -> 3  -> 4* ->
/// c -> 5  -> 6  ->
/// ```
///
/// If user provides the multi-way join order of `(a join b) join c`, and set the strategy to be
/// [`ArrangeStrategy::LeftFirst`], and if three tables are of the same join key (the graph is
/// fully-connected and no shuffle needed), then we will place the arrangements over the lookup
/// nodes in the following way:
///
/// ```plain
/// a -> b* -> c* ->
/// b -> a  -> c* ->
/// c -> a  -> b  ->
/// ```
///
/// The left side of joins will be preferred for the left lookups when selecting arrangements.
///
/// If strategy is set to [`ArrangeStrategy::RightFirst`],
///
/// ```plain
/// a -> c* -> b* ->
/// b -> c  -> a* ->
/// c -> b  -> a  ->
/// ```
///
/// The right side of joins will be preferred for the left lookups when selecting arrangements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArrangeStrategy {
    /// The left-most table will be preferred to be the first lookup table.
    LeftFirst,
    /// The right-most table will be preferred to be the first lookup table.
    RightFirst,
}

/// Decides how to place stream inputs. Given a 3-way join example:
///
/// ```plain
/// x -> 1* -> 2* ->
/// x -> 3  -> 4* ->
/// x -> 5  -> 6  ->
/// ```
///
/// ... where `n*` means lookup this epoch. If user provides the multi-way join order of `(a join b)
/// join c`, and set the strategy to be [`StreamStrategy::LeftThisEpoch`], then we will place the
/// stream side as:
///
/// ```plain
/// a -> 1* -> 2* ->
/// b -> 3  -> 4* ->
/// c -> 5  -> 6  ->
/// ```
///
/// ... where `a`, the left-most table in multi-way join, passes through most number of
/// lookup-this-epoch.
///
/// If strategy is set to [`StreamStrategy::RightThisEpoch`],
///
/// ```plain
/// c -> 1* -> 2* ->
/// b -> 3  -> 4* ->
/// a -> 5  -> 6  ->
/// ```
///
/// ... where `c`, the right-most table in multi-way join, passes through most number of
/// lookup-this-epoch.
///
/// More lookup-this-epochs in a lookup row mean more latency when a barrier is flushed. If lookup
/// executor is set to lookup this epoch, it will wait until a barrier from the arrangement side
/// before actually starting lookups and joins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamStrategy {
    /// The left-most table will be preferred to be the stream to lookup this epoch.
    LeftThisEpoch,
    /// The right-most table will be preferred to be the stream to lookup this epoch.
    RightThisEpoch,
}

/// Given a multi-way inner join plan, the solver will produces the most efficient way of getting
/// those join done.
///
/// ## Input
///
/// [`DeltaJoinSolver`] needs the information of the cost of joining two tables. You'll need to
/// provide edges of the join graph. Each [`JoinEdge`] `(a, b) -> cost` describes the cost of inner
/// join a and b.
///
/// ## Output
///
/// The `solve` function will return a vector of [`LookupPath`]. See [`LookupPath`] for more.
pub struct DeltaJoinSolver {
    /// Strategy of arrangement placement, see docs of [`ArrangeStrategy`] for more details.
    arrange_strategy: ArrangeStrategy,

    /// Strategy of stream placement, see docs of [`StreamStrategy`] for more details.
    stream_strategy: StreamStrategy,

    /// Possible combination of joins. If `(a, b)` is in `edges`, then `a` and `b` can be joined.
    /// The edge is bi-directional, so only one pair (either `a, b` or `b, a`) needs to be present
    /// in this vector.
    edges: Vec<JoinEdge>,

    /// The recommended join order from optimizer in a multi-way join plan node.
    join_order: Vec<JoinTable>,
}

/// A row in the lookup plan, which includes the stream side arrangement, and all arrangements to be
/// placed in the lookup node.
///
/// For example, if we have `LookupPath(a, vec![b, c])`, then we have a row:
///
/// ```plain
/// a -> b -> c
/// ```
///
/// In `Vec<LookupPath>`, the first row always contains the most lookup-this-epochs. For example, if
/// we have:
///
/// ```plain
/// vec![
///   LookupPath(a, vec![b, c]),
///   LookupPath(b, vec![c, a]),
///   LookupPath(c, vec![a, b])
/// ]
/// ```
///
/// Then it means...
///
/// ```plain
/// a -> b* -> c* ->
/// b -> c  -> a* ->
/// c -> a  -> b  ->
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupPath(pub JoinTable, pub Vec<JoinTable>);

struct SolverEnv {
    /// Stores all join edges in map. [`JoinEdge`]'s left side is always the key in map.
    join_edge: BTreeMap<JoinTable, Vec<JoinEdge>>,

    /// Placement order of arrangements
    arrange_placement_order: Vec<JoinTable>,

    /// Placement order of streams
    stream_placement_order: Vec<JoinTable>,
}

impl SolverEnv {
    fn build_from(solver: &DeltaJoinSolver) -> Self {
        let mut join_edge = BTreeMap::new();

        for table in &solver.join_order {
            join_edge.insert(*table, vec![]);
        }
        for edge in &solver.edges {
            join_edge.get_mut(&edge.left).unwrap().push(edge.clone());
            join_edge.get_mut(&edge.right).unwrap().push(edge.reverse());
        }

        Self {
            arrange_placement_order: match solver.arrange_strategy {
                ArrangeStrategy::LeftFirst => solver.join_order.clone(),
                ArrangeStrategy::RightFirst => {
                    solver.join_order.iter().copied().rev().collect_vec()
                }
            },
            stream_placement_order: match solver.stream_strategy {
                StreamStrategy::LeftThisEpoch => solver.join_order.clone(),
                StreamStrategy::RightThisEpoch => {
                    solver.join_order.iter().copied().rev().collect_vec()
                }
            },
            join_edge,
        }
    }
}

impl DeltaJoinSolver {
    /// Generate a lookup path using the user provided strategy. The lookup path is generated in the
    /// following way:
    ///
    /// * Firstly, we find all tables that can be joined with the current join table set. The tables
    ///   should have the same join key as the current join set.
    /// * If there are multiple tables that satisfy this condition, we will pick tables according to
    ///   [`ArrangeStrategy`].
    /// * If not, then we have to do a shuffle. We pick all tables that have join condition with the
    ///   current table set.
    /// * And then pick a table using [`ArrangeStrategy`].
    ///
    /// Basically, this algorithm will greedily pick all tables with the same join key first (so
    /// that there won't be shuffle). Then, switch a distribution, and do this process again.
    fn find_lookup_path(
        &self,
        solver_env: &SolverEnv,
        input_stream: JoinTable,
    ) -> Result<LookupPath> {
        // The table available to query
        let mut current_table_set = BTreeSet::new();
        current_table_set.insert(input_stream);

        // The distribution of the current lookup.
        let mut current_distribution = vec![];

        // The final lookup path.
        let mut path = vec![];

        fn satisfies_distribution(
            current_distribution: &[(JoinTable, Vec<usize>)],
            edge: &JoinEdge,
        ) -> bool {
            // TODO: if `current_distribution` is a `BTreeSet`, we can know if the distribution in
            // O(1). But as we generally have few tables, so we do a linear scan on tables.
            if current_distribution.is_empty() {
                return true;
            }

            for (table, join_keys) in current_distribution {
                if table == &edge.left && join_keys == &edge.left_join_keys {
                    return true;
                }
            }

            false
        }

        'next_table: loop {
            assert!(
                path.len() < self.join_order.len(),
                "internal error: infinite loop"
            );

            // step 1: find tables that can be joined with `current_table_set` and satisfy the
            // current distribution.
            let mut reachable_tables = BTreeMap::new();

            for current_table in &current_table_set {
                for edge in &solver_env.join_edge[current_table] {
                    if !current_table_set.contains(&edge.right)
                        && satisfies_distribution(&current_distribution, edge)
                    {
                        reachable_tables.insert(edge.right, edge.clone());
                    }
                }
            }

            // step 2: place arrangements according to the arrange strategy, update current
            // distribution and current table set.
            for table in &solver_env.arrange_placement_order {
                if let Some(edge) = reachable_tables.get(table) {
                    current_table_set.insert(edge.right);
                    path.push(edge.right);
                    current_distribution.push((edge.right, edge.right_join_keys.clone()));
                    continue 'next_table;
                }
            }

            // no table can be joined using the same distribution at this point.

            // step 3: find all tables that can be joined with `current_table_set`, regardless of
            // their distribution.
            let mut reachable_tables = BTreeMap::new();

            for current_table in &current_table_set {
                for edge in &solver_env.join_edge[current_table] {
                    if !current_table_set.contains(&edge.right) {
                        reachable_tables.insert(edge.right, edge.clone());
                    }
                }
            }

            // step 4: place arrangements according to the arrange strategy, update current
            // distribution and current table set.
            for table in &solver_env.arrange_placement_order {
                if let Some(edge) = reachable_tables.get(table) {
                    current_table_set.insert(edge.right);
                    path.push(edge.right);
                    current_distribution.clear();
                    current_distribution.push((edge.right, edge.right_join_keys.clone()));
                    continue 'next_table;
                }
            }

            // step 5: no tables can be joined any more, what happened?
            if self.join_order.len() - 1 == path.len() {
                break;
            } else {
                // no table can be joined, while path is still incomplete
                return Err(anyhow!(
                    "join plan cannot be generated, tables not connected."
                ));
            }
        }

        Ok(LookupPath(input_stream, path))
    }

    pub fn solve(&self) -> Result<Vec<LookupPath>> {
        let solver_env = SolverEnv::build_from(self);

        solver_env
            .stream_placement_order
            .iter()
            .map(|x| self.find_lookup_path(&solver_env, *x))
            .collect()
    }

    pub fn new(
        stream_strategy: StreamStrategy,
        edges: Vec<JoinEdge>,
        join_order: Vec<JoinTable>,
    ) -> Self {
        Self {
            stream_strategy,
            edges,
            join_order,
            arrange_strategy: ArrangeStrategy::LeftFirst,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_2way_join() {
        let solver = DeltaJoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
            ]
        );

        let solver = DeltaJoinSolver {
            arrange_strategy: ArrangeStrategy::RightFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
            ]
        );

        let solver = DeltaJoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::RightThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
            ]
        );

        let solver = DeltaJoinSolver {
            arrange_strategy: ArrangeStrategy::RightFirst,
            stream_strategy: StreamStrategy::RightThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
            ]
        );
    }

    #[test]
    fn test_3way_join_one_key() {
        // Table 1: [2, 3] (composite key x)
        // Table 2: [3, 2] (composite key x)
        // Table 3: [1, 1] (composite key x)
        // t1.x == t2.x == t3.x

        let solver = DeltaJoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![
                JoinEdge {
                    left: JoinTable(1),
                    right: JoinTable(2),
                    left_join_keys: vec![2, 3],
                    right_join_keys: vec![3, 2],
                },
                JoinEdge {
                    left: JoinTable(2),
                    right: JoinTable(3),
                    left_join_keys: vec![3, 2],
                    right_join_keys: vec![1, 1],
                },
                JoinEdge {
                    left: JoinTable(3),
                    right: JoinTable(1),
                    left_join_keys: vec![1, 1],
                    right_join_keys: vec![2, 3],
                },
            ],
            join_order: vec![JoinTable(1), JoinTable(2), JoinTable(3)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2), JoinTable(3)]),
                LookupPath(JoinTable(2), vec![JoinTable(1), JoinTable(3)]),
                LookupPath(JoinTable(3), vec![JoinTable(1), JoinTable(2)])
            ]
        );
    }

    #[test]
    fn test_3way_join_two_keys() {
        // Table 1: [0] (key x)
        // Table 2: [0] (key x), [1] (key y)
        // Table 3: [1] (key y)
        // t1.x == t2.x and t2.y == t3.y
        //
        // t1 cannot directly join with t3 in this case, as they don't share the same key

        let solver = DeltaJoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![
                JoinEdge {
                    left: JoinTable(1),
                    right: JoinTable(2),
                    left_join_keys: vec![0],
                    right_join_keys: vec![0],
                },
                JoinEdge {
                    left: JoinTable(2),
                    right: JoinTable(3),
                    left_join_keys: vec![1],
                    right_join_keys: vec![1],
                },
            ],
            join_order: vec![JoinTable(1), JoinTable(2), JoinTable(3)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2), JoinTable(3)]),
                LookupPath(JoinTable(2), vec![JoinTable(1), JoinTable(3)]),
                LookupPath(JoinTable(3), vec![JoinTable(2), JoinTable(1)])
            ]
        );
    }

    #[test]
    fn test_invalid_plan() {
        let solver = DeltaJoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![0],
                right_join_keys: vec![0],
            }],
            join_order: vec![JoinTable(1), JoinTable(2), JoinTable(3)],
        };

        let result = solver.solve();

        assert!(result.is_err());
    }
}
