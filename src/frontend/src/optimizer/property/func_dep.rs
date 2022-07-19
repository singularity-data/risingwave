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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use fixedbitset::FixedBitSet;

#[derive(Debug, PartialEq, Clone, Default)]
pub struct FunctionalDependencySet {
    fd: HashMap<FixedBitSet, FixedBitSet>,
}

impl FunctionalDependencySet {
    pub fn new() -> Self {
        Self { fd: HashMap::new() }
    }

    pub fn with_key(column_cnt: usize, pk_indices: &[usize]) -> Self {
        let mut tmp = Self::new();
        tmp.add_key_column_by_indices(column_cnt, pk_indices);
        tmp
    }

    pub fn add_functional_dependency(&mut self, from: FixedBitSet, to: FixedBitSet) {
        assert_eq!(
            from.len(),
            to.len(),
            "from and to should have the same length"
        );
        match self.fd.entry(from) {
            Entry::Vacant(e) => {
                e.insert(to);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().union_with(&to);
            }
        }
    }

    fn add_key_column_by_index(&mut self, column_cnt: usize, column_id: usize) {
        let mut from = FixedBitSet::with_capacity(column_cnt);
        from.set(column_id, true);
        let mut to = from.clone();
        to.toggle_range(0..to.len());
        self.add_functional_dependency(from, to);
    }

    pub fn add_key_column_by_indices(&mut self, column_cnt: usize, pk_indices: &[usize]) {
        for &i in pk_indices {
            self.add_key_column_by_index(column_cnt, i);
        }
    }

    pub fn add_constant_column_by_index(&mut self, column_cnt: usize, column_id: usize) {
        let mut to = FixedBitSet::with_capacity(column_cnt);
        to.set(column_id, true);
        self.add_functional_dependency(FixedBitSet::with_capacity(column_cnt), to);
    }

    pub fn add_functional_dependency_by_column_indices(
        &mut self,
        from: &[usize],
        to: &[usize],
        column_cnt: usize,
    ) {
        let from = {
            let mut tmp = FixedBitSet::with_capacity(column_cnt);
            for &i in from {
                tmp.set(i, true);
            }
            tmp
        };
        let to = {
            let mut tmp = FixedBitSet::with_capacity(column_cnt);
            for &i in to {
                tmp.set(i, true);
            }
            tmp
        };
        self.add_functional_dependency(from, to)
    }

    fn get_closure(&self, columns: FixedBitSet) -> FixedBitSet {
        let mut closure = columns;
        let mut no_updates;
        loop {
            no_updates = true;
            for (from, to) in &self.fd {
                if from.is_subset(&closure) {
                    closure.union_with(to);
                    no_updates = false;
                }
            }
            if no_updates {
                break;
            }
        }
        closure
    }

    pub fn is_determined_by(&self, determinant: FixedBitSet, dependant: FixedBitSet) -> bool {
        self.get_closure(determinant).is_superset(&dependant)
    }

    // pub fn rewrite_with_change(&self, col_change: ColIndexMapping) -> Self {

    // }
}
