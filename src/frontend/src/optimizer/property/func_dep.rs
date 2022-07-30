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

use fixedbitset::FixedBitSet;
use itertools::Itertools;

/// [`FunctionalDependency`] represent a dependency of from --> to.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct FunctionalDependency {
    pub from: FixedBitSet,
    pub to: FixedBitSet,
}

impl fmt::Display for FunctionalDependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let from = self.from.ones().collect_vec();
        let to = self.to.ones().collect_vec();
        f.write_fmt(format_args!("{:?} --> {:?}", from, to))
    }
}

impl FunctionalDependency {
    /// Create a [`FunctionalDependency`] with bitset.
    /// This indicate a from --> to dependency.
    pub fn new(from: FixedBitSet, to: FixedBitSet) -> Self {
        assert_eq!(
            from.len(),
            to.len(),
            "from and to should have the same length"
        );
        FunctionalDependency { from, to }
    }

    /// Create a [`FunctionalDependency`] with column indices.
    pub fn with_indices(column_cnt: usize, from: &[usize], to: &[usize]) -> Self {
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
        FunctionalDependency { from, to }
    }

    /// Create a [`FunctionalDependency`] with a key. The combination of these columns can determine
    /// all other columns.
    fn with_key(column_cnt: usize, key_indices: &[usize]) -> Self {
        let mut from = FixedBitSet::with_capacity(column_cnt);
        for &idx in key_indices {
            from.set(idx, true);
        }
        let mut to = from.clone();
        to.toggle_range(0..to.len());
        FunctionalDependency { from, to }
    }

    /// Create a [`FunctionalDependency`] for constant columns.
    /// These columns can be determined by any column.
    pub fn with_constant(column_cnt: usize, constant_indices: &[usize]) -> Self {
        let mut to = FixedBitSet::with_capacity(column_cnt);
        for &i in constant_indices {
            to.set(i, true);
        }
        FunctionalDependency {
            from: FixedBitSet::with_capacity(column_cnt),
            to,
        }
    }

    pub fn into_parts(self) -> (FixedBitSet, FixedBitSet) {
        (self.from, self.to)
    }
}

/// [`FunctionalDependencySet`] contains the functional dependencies.
///
/// It is used in optimizer to track the dependencies between columns.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct FunctionalDependencySet {
    /// `strict` contains all strict functional dependencies.
    ///
    /// The strict functional dependency use the **NULL=** semantic. It means that all NULLs are
    /// considered as equal. So for following table, A --> B is not valid.
    /// **NOT** allowed.
    /// ```text
    ///   A   | B
    /// ------|---
    ///  NULL | 1
    ///  NULL | 2
    /// ```
    strict: Vec<FunctionalDependency>,
}

impl fmt::Display for FunctionalDependencySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for fd in self.strict.iter().with_position() {
            match fd {
                itertools::Position::First(fd) | itertools::Position::Middle(fd) => {
                    f.write_fmt(format_args!("{}, ", fd))?;
                }
                itertools::Position::Last(fd) | itertools::Position::Only(fd) => {
                    f.write_fmt(format_args!("{}", fd))?;
                }
            }
        }
        f.write_str("}")
    }
}

impl FunctionalDependencySet {
    /// Create a empty [`FunctionalDependencySet`]
    pub fn new() -> Self {
        Self { strict: Vec::new() }
    }

    /// Create a [`FunctionalDependencySet`] with the indices of a key.
    ///
    /// The **combination** of these columns can determine all other columns.
    ///
    /// Note that if `key_indices` empty, an empty fd set will be returned.
    ///
    /// # Examples
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::{FunctionalDependencySet, FunctionalDependency};
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::with_key(4, &[1]);
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let FunctionalDependency { from, to } = &fd_inner[0];
    /// // 1 --> 0, 2, 3
    /// assert_eq!(from.ones().collect_vec(), &[1]);
    /// assert_eq!(to.ones().collect_vec(), &[0, 2, 3]);
    /// ```
    pub fn with_key(column_cnt: usize, key_indices: &[usize]) -> Self {
        let mut tmp = Self::new();
        if !key_indices.is_empty() {
            tmp.add_key_column(column_cnt, key_indices);
        }
        tmp
    }

    /// Create a [`FunctionalDependencySet`] with a dependency [`Vec`]
    pub fn with_dependencies(strict_dependencies: Vec<FunctionalDependency>) -> Self {
        Self {
            strict: strict_dependencies,
        }
    }

    pub fn as_dependencies_mut(&mut self) -> &mut Vec<FunctionalDependency> {
        &mut self.strict
    }

    pub fn as_dependencies(&self) -> &Vec<FunctionalDependency> {
        &self.strict
    }

    pub fn into_dependencies(self) -> Vec<FunctionalDependency> {
        self.strict
    }

    /// Add a functional dependency to a [`FunctionalDependencySet`].
    pub fn add_functional_dependency(&mut self, fd: FunctionalDependency) {
        let FunctionalDependency { from, to } = fd;
        assert_eq!(
            from.len(),
            to.len(),
            "from and to should have the same length"
        );
        if !to.is_clear() {
            match self.strict.iter().position(|elem| elem.from == from) {
                Some(idx) => self.strict[idx].to.union_with(&to),
                None => self.strict.push(FunctionalDependency::new(from, to)),
            }
        }
    }

    /// Add key columns to a  [`FunctionalDependencySet`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::{FunctionalDependencySet, FunctionalDependency};
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_key_column(4, &[1]);
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let FunctionalDependency { from, to } = &fd_inner[0];
    /// assert_eq!(from.ones().collect_vec(), &[1]);
    /// assert_eq!(to.ones().collect_vec(), &[0, 2, 3]);
    /// ```
    pub fn add_key_column(&mut self, column_cnt: usize, key_indices: &[usize]) {
        self.add_functional_dependency(FunctionalDependency::with_key(column_cnt, key_indices));
    }

    /// Add constant columns to a  [`FunctionalDependencySet`].
    ///
    /// # Examples
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::{FunctionalDependencySet, FunctionalDependency};
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_constant_column(4, &[1]);
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let FunctionalDependency { from, to } = &fd_inner[0];
    /// assert!(from.ones().collect_vec().is_empty());
    /// assert_eq!(to.ones().collect_vec(), &[1]);
    /// ```
    pub fn add_constant_column(&mut self, column_cnt: usize, constant_columns: &[usize]) {
        self.add_functional_dependency(FunctionalDependency::with_constant(
            column_cnt,
            constant_columns,
        ));
    }

    /// Add a dependency to [`FunctionalDependencySet`] using column indices.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::{FunctionalDependencySet, FunctionalDependency};
    /// # use itertools::Itertools;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_functional_dependency_by_column_indices(&[1, 2], &[0], 4); // (1, 2) --> (0), 4 columns
    /// let fd_inner = fd.into_dependencies();
    ///
    /// assert_eq!(fd_inner.len(), 1);
    ///
    /// let FunctionalDependency { from, to } = &fd_inner[0];
    /// assert_eq!(from.ones().collect_vec(), &[1, 2]);
    /// assert_eq!(to.ones().collect_vec(), &[0]);
    /// ```
    pub fn add_functional_dependency_by_column_indices(
        &mut self,
        from: &[usize],
        to: &[usize],
        column_cnt: usize,
    ) {
        self.add_functional_dependency(FunctionalDependency::with_indices(column_cnt, from, to))
    }

    fn get_closure(&self, columns: FixedBitSet) -> FixedBitSet {
        let mut closure = columns;
        let mut no_updates;
        loop {
            no_updates = true;
            for FunctionalDependency { from, to } in &self.strict {
                if from.is_subset(&closure) && !to.is_subset(&closure) {
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

    /// Return `true` if the dependency determinant -> dependant exists.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use risingwave_frontend::optimizer::property::{FunctionalDependencySet, FunctionalDependency};
    /// # use fixedbitset::FixedBitSet;
    /// let mut fd = FunctionalDependencySet::new();
    /// fd.add_functional_dependency_by_column_indices(&[1, 2], &[0], 5); // (1, 2) --> (0)
    /// fd.add_functional_dependency_by_column_indices(&[0, 1], &[3], 5); // (0, 1) --> (3)
    /// fd.add_functional_dependency_by_column_indices(&[3], &[4], 5); // (3) --> (4)
    /// let from = FixedBitSet::from_iter([1usize, 2usize].into_iter());
    /// let to = FixedBitSet::from_iter([4usize].into_iter());
    /// assert!(fd.is_determined_by(from, to)); // (1, 2) --> (4) holds
    /// ```
    pub fn is_determined_by(&self, determinant: FixedBitSet, dependant: FixedBitSet) -> bool {
        self.get_closure(determinant).is_superset(&dependant)
    }

    /// Return true if the combination of `columns` can fully determine other columns.
    pub fn is_key(&self, columns: FixedBitSet) -> bool {
        let all_columns = {
            let mut tmp = columns.clone();
            tmp.set_range(.., true);
            tmp
        };
        self.is_determined_by(columns, all_columns)
    }

    /// Remove redundant columns from the given set.
    ///
    /// Redundant columns can be functionally determined by other columns so there is no need to
    /// keep them in a key.
    ///
    /// Note that the accurate minimization algorithm can take O(2^n) time,
    /// so we use a approximate algorithm which use O(cn) time. `c` is the number of columns, and
    /// `n` is the number of functional dependency rules.
    ///
    /// This algorithm removes columns one by one and check
    /// whether the remaining columns can form a key or not. If the remaining columns can form a
    /// key, then this column can be removed.
    pub fn minimize_key(&self, key_indices: &[usize], column_cnt: usize) -> Vec<usize> {
        let mut key_bitset = FixedBitSet::from_iter(key_indices.iter().copied());
        key_bitset.grow(column_cnt);
        let res = self.minimize_key_inner(key_bitset);
        res.ones().collect_vec()
    }

    fn minimize_key_inner(&self, key: FixedBitSet) -> FixedBitSet {
        let mut new_key = key.clone();
        for i in key.ones() {
            new_key.set(i, false);
            if !self.is_key(new_key.clone()) {
                new_key.set(i, true);
            }
        }
        new_key
    }
}
