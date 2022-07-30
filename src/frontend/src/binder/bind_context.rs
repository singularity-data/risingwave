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
use std::fmt::Display;

use risingwave_common::catalog::Field;
use risingwave_common::error::{ErrorCode, Result};

#[derive(Debug, Clone)]
pub struct ColumnBinding {
    pub table_name: String,
    pub index: usize,
    pub is_hidden: bool,
    pub field: Field,
}

impl ColumnBinding {
    pub fn new(table_name: String, index: usize, is_hidden: bool, field: Field) -> Self {
        ColumnBinding {
            table_name,
            index,
            is_hidden,
            field,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Clause {
    Where,
    Values,
}

impl Display for Clause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Clause::Where => write!(f, "WHERE"),
            Clause::Values => write!(f, "VALUES"),
        }
    }
}

/// A `BindContext` that is only visible if the `LATERAL` keyword
/// is provided.
pub struct LateralBindContext {
    pub is_visible: bool,
    pub context: BindContext,
}

#[derive(Default, Debug, Clone)]
pub struct BindContext {
    // Columns of all tables.
    pub columns: Vec<ColumnBinding>,
    // Mapping column name to indexs in `columns`.
    pub indexs_of: HashMap<String, Vec<usize>>,
    // Mapping table name to [begin, end) of its columns.
    pub range_of: HashMap<String, (usize, usize)>,
    // `clause` identifies in what clause we are binding.
    pub clause: Option<Clause>,
}

impl BindContext {
    pub fn get_column_binding_index(
        &self,
        table_name: &Option<String>,
        column_name: &String,
    ) -> Result<usize> {
        match table_name {
            Some(table_name) => self.get_index_with_table_name(column_name, table_name),
            None => self.get_index(column_name),
        }
    }

    pub fn get_index(&self, column_name: &String) -> Result<usize> {
        let columns = self
            .indexs_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)))?;
        if columns.len() > 1 {
            Err(ErrorCode::InternalError(format!("Ambiguous column name: {}", column_name)).into())
        } else {
            Ok(columns[0])
        }
    }

    fn get_index_with_table_name(
        &self,
        column_name: &String,
        table_name: &String,
    ) -> Result<usize> {
        let column_indexes = self
            .indexs_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)))?;
        match column_indexes
            .iter()
            .find(|column_index| self.columns[**column_index].table_name == *table_name)
        {
            Some(column_index) => Ok(*column_index),
            None => Err(ErrorCode::ItemNotFound(format!(
                "missing FROM-clause entry for table \"{}\"",
                table_name
            ))
            .into()),
        }
    }

    /// Merges two `BindContext`s which are adjacent. For instance, the `BindContext` of two
    /// adjacent cross-joined tables.
    pub fn merge_context(&mut self, other: Self) -> Result<()> {
        let begin = self.columns.len();
        self.columns.extend(other.columns.into_iter().map(|mut c| {
            c.index += begin;
            c
        }));
        for (k, v) in other.indexs_of {
            let entry = self.indexs_of.entry(k).or_insert_with(Vec::new);
            entry.extend(v.into_iter().map(|x| x + begin));
        }
        for (k, (x, y)) in other.range_of {
            match self.range_of.entry(k) {
                Entry::Occupied(e) => {
                    return Err(ErrorCode::InternalError(format!(
                        "Duplicated table name while binding context {}",
                        e.key()
                    ))
                    .into());
                }
                Entry::Vacant(entry) => {
                    entry.insert((begin + x, begin + y));
                }
            }
        }
        // we assume that the clause is contained in the outer-level context
        Ok(())
    }
}

impl BindContext {
    pub fn new() -> Self {
        BindContext {
            columns: Vec::new(),
            indexs_of: HashMap::new(),
            range_of: HashMap::new(),
            clause: None,
        }
    }
}
