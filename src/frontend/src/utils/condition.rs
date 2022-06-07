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

use std::collections::{HashMap, HashSet};
use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;

use crate::expr::{
    factorization_expr, fold_boolean_constant, push_down_not, to_conjunctions,
    try_get_bool_constant, ExprImpl, ExprRewriter, ExprType, ExprVisitor, InputRef,
};

#[derive(Debug, Clone)]
pub struct Condition {
    /// Condition expressions in conjunction form (combined with `AND`)
    pub conjunctions: Vec<ExprImpl>,
}

impl IntoIterator for Condition {
    type IntoIter = std::vec::IntoIter<ExprImpl>;
    type Item = ExprImpl;

    fn into_iter(self) -> Self::IntoIter {
        self.conjunctions.into_iter()
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut conjunctions = self.conjunctions.iter();
        if let Some(expr) = conjunctions.next() {
            write!(f, "{:?}", expr)?;
        }
        if self.always_true() {
            write!(f, "true")?;
        } else {
            for expr in conjunctions {
                write!(f, " AND {:?}", expr)?;
            }
        }
        Ok(())
    }
}

impl Condition {
    pub fn with_expr(expr: ExprImpl) -> Self {
        let conjunctions = to_conjunctions(expr);

        Self { conjunctions }.simplify()
    }

    pub fn true_cond() -> Self {
        Self {
            conjunctions: vec![],
        }
    }

    pub fn always_true(&self) -> bool {
        self.conjunctions.is_empty()
    }

    /// Convert condition to an expression. If always true, return `None`.
    pub fn as_expr_unless_true(&self) -> Option<ExprImpl> {
        if self.always_true() {
            None
        } else {
            Some(self.clone().into())
        }
    }

    #[must_use]
    pub fn and(self, other: Self) -> Self {
        let mut ret = self;
        ret.conjunctions.extend(other.conjunctions);
        ret.simplify()
    }

    /// Split the condition expressions into 3 groups: left, right and others
    #[must_use]
    pub fn split(self, left_col_num: usize, right_col_num: usize) -> (Self, Self, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        self.group_by::<_, 3>(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_subset(&left_bit_map) {
                0
            } else if input_bits.is_subset(&right_bit_map) {
                1
            } else {
                2
            }
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    /// Split the condition expressions into (N choose 2) + 1 groups: those containing two columns
    /// from different buckets (and optionally, needing an equal condition between them), and
    /// others.
    ///
    /// `input_num_cols` are the number of columns in each of the input buckets. For instance, with
    /// bucket0: col0, col1, col2 | bucket1: col3, col4 | bucket2: col5
    /// `input_num_cols` = [3, 2, 1]
    ///
    /// Returns hashmap with keys of the form (bucket0, bucket1) where bucket0 < bucket1 in terms of
    /// their bucket index.
    ///
    /// `only_eq`: whether to only split those conditions with an eq condition predicate between two
    /// buckets.
    #[must_use]
    pub fn split_by_input_col_nums(
        self,
        input_col_nums: &[usize],
        only_eq: bool,
    ) -> (HashMap<(usize, usize), Self>, Self) {
        let mut bitmaps = Vec::with_capacity(input_col_nums.len());
        let mut cols_seen = 0;
        for cols in input_col_nums {
            bitmaps.push(FixedBitSet::from_iter(cols_seen..cols_seen + cols));
            cols_seen += cols;
        }

        let mut pairwise_conditions = HashMap::with_capacity(self.conjunctions.len());
        let mut non_eq_join = vec![];

        for expr in self.conjunctions {
            let input_bits = expr.collect_input_refs(cols_seen);
            let mut subset_indices = Vec::with_capacity(input_col_nums.len());
            for (bucket_idx, bitmap) in bitmaps.iter().enumerate() {
                if !input_bits.is_disjoint(bitmap) {
                    subset_indices.push(bucket_idx);
                }
            }
            if subset_indices.len() != 2 || (only_eq && Self::as_eq_cond(&expr).is_none()) {
                non_eq_join.push(expr);
            } else {
                // The key has the canonical ordering (lower, higher)
                let key = if subset_indices[0] < subset_indices[1] {
                    (subset_indices[0], subset_indices[1])
                } else {
                    (subset_indices[1], subset_indices[0])
                };
                let e = pairwise_conditions
                    .entry(key)
                    .or_insert_with(Condition::true_cond);
                e.conjunctions.push(expr);
            }
        }
        (
            pairwise_conditions,
            Condition {
                conjunctions: non_eq_join,
            },
        )
    }

    /// Returns the `InputRefs` of an Equality predicate if it matches
    /// ordered by the canonical ordering (lower, higher), else returns None
    pub fn as_eq_cond(expr: &ExprImpl) -> Option<(InputRef, InputRef)> {
        if let ExprImpl::FunctionCall(function_call) = expr.clone()
            && function_call.get_expr_type() == ExprType::Equal
            && let (_, ExprImpl::InputRef(x), ExprImpl::InputRef(y)) = function_call.decompose_as_binary()
        {
            if x.index() < y.index() {
                Some((*x, *y))
            } else {
                Some((*y, *x))
            }
        } else {
            None
        }
    }

    #[must_use]
    /// For [`EqJoinPredicate`], separate equality conditions which connect left columns and right
    /// columns from other conditions.
    ///
    /// The equality conditions are transformed into `(left_col_id, right_col_id)` pairs.
    ///
    /// [`EqJoinPredicate`]: crate::optimizer::plan_node::EqJoinPredicate
    pub fn split_eq_keys(
        self,
        left_col_num: usize,
        right_col_num: usize,
    ) -> (Vec<(InputRef, InputRef)>, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        let (mut eq_keys, mut others) = (vec![], vec![]);
        self.conjunctions.into_iter().for_each(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_disjoint(&left_bit_map) || input_bits.is_disjoint(&right_bit_map) {
                others.push(expr)
            } else if let Some(columns) = Self::as_eq_cond(&expr) {
                eq_keys.push(columns);
            } else {
                others.push(expr)
            }
        });

        (
            eq_keys,
            Condition {
                conjunctions: others,
            },
        )
    }

    /// Split the condition expressions into 2 groups: those referencing `columns` and others which
    /// are disjoint with columns.
    #[must_use]
    pub fn split_disjoint(self, columns: &FixedBitSet) -> (Self, Self) {
        self.group_by::<_, 2>(|expr| {
            let input_bits = expr.collect_input_refs(columns.len());
            if input_bits.is_disjoint(columns) {
                1
            } else {
                0
            }
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    /// Split the condition expressions into `N` groups.
    /// An expression `expr` is in the `i`-th group if `f(expr)==i`.
    ///
    /// # Panics
    /// Panics if `f(expr)>=N`.
    #[must_use]
    pub fn group_by<F, const N: usize>(self, f: F) -> [Self; N]
    where
        F: Fn(&ExprImpl) -> usize,
    {
        const EMPTY: Vec<ExprImpl> = vec![];
        let mut groups = [EMPTY; N];
        for (key, group) in &self.conjunctions.into_iter().group_by(|expr| {
            // i-th group
            let i = f(expr);
            assert!(i < N);
            i
        }) {
            groups[key].extend(group);
        }

        groups.map(|group| Condition {
            conjunctions: group,
        })
    }

    #[must_use]
    pub fn rewrite_expr(self, rewriter: &mut impl ExprRewriter) -> Self {
        Self {
            conjunctions: self
                .conjunctions
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect(),
        }
    }

    pub fn visit_expr(&self, visitor: &mut impl ExprVisitor) {
        self.conjunctions
            .iter()
            .for_each(|expr| visitor.visit_expr(expr))
    }

    /// Simplify conditions
    /// It simplify conditions by applying constant folding and removing unnecessary conjunctions
    fn simplify(self) -> Self {
        // boolean constant folding
        let conjunctions: Vec<_> = self
            .conjunctions
            .into_iter()
            .map(push_down_not)
            .map(fold_boolean_constant)
            .flat_map(to_conjunctions)
            .collect();
        let mut res: Vec<ExprImpl> = Vec::new();
        let mut visited: HashSet<ExprImpl> = HashSet::new();
        for expr in conjunctions {
            // factorization_expr requires hash-able ExprImpl
            if !expr.has_subquery() {
                let results_of_factorization = factorization_expr(expr);
                res.extend(
                    results_of_factorization
                        .clone()
                        .into_iter()
                        .filter(|expr| !visited.contains(expr)),
                );
                visited.extend(results_of_factorization);
            } else {
                // for subquery, simply give up factorization
                res.push(expr);
            }
        }
        // remove all constant boolean `true`
        res.retain(|expr| {
            if let Some(v) = try_get_bool_constant(expr) && v {
                false
            } else {
                true
            }
        });
        // if there is a `false` in conjunctions, the whole condition will be `false`
        for expr in &mut res {
            if let Some(v) = try_get_bool_constant(expr) {
                if !v {
                    res.clear();
                    res.push(ExprImpl::literal_bool(false));
                    break;
                }
            }
        }
        Self { conjunctions: res }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{FunctionCall, InputRef};

    #[test]
    fn test_split() {
        let left_col_num = 3;
        let right_col_num = 2;

        let ty = DataType::Int32;

        let mut rng = rand::thread_rng();
        let left: ExprImpl = FunctionCall::new(
            ExprType::Equal,
            vec![
                InputRef::new(rng.gen_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(rng.gen_range(0..left_col_num), ty.clone()).into(),
            ],
        )
        .unwrap()
        .into();
        let right: ExprImpl = FunctionCall::new(
            ExprType::LessThan,
            vec![
                InputRef::new(
                    rng.gen_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
                InputRef::new(
                    rng.gen_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
            ],
        )
        .unwrap()
        .into();
        let other: ExprImpl = FunctionCall::new(
            ExprType::GreaterThan,
            vec![
                InputRef::new(rng.gen_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(
                    rng.gen_range(left_col_num..left_col_num + right_col_num),
                    ty,
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let cond = Condition::with_expr(other.clone())
            .and(Condition::with_expr(right.clone()))
            .and(Condition::with_expr(left.clone()));
        let res = cond.split(left_col_num, right_col_num);
        assert_eq!(res.0.conjunctions, vec![left]);
        assert_eq!(res.1.conjunctions, vec![right]);
        assert_eq!(res.2.conjunctions, vec![other]);
    }
}
