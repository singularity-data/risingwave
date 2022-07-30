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

use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_sqlparser::ast::{Ident, ObjectName, TableAlias, TableFactor, TableWithJoins};

use crate::{
    BinaryOperator, Column, Expr, Join, JoinConstraint, JoinOperator, SqlGenerator, Table,
};

fn create_join_on_clause(left: String, right: String) -> Expr {
    let left = Box::new(Expr::Identifier(Ident::new(left)));
    let right = Box::new(Expr::Identifier(Ident::new(right)));
    Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> (TableWithJoins, Vec<Table>) {
        match self.rng.gen_range(0..=50) {
            0..=39 => self.gen_simple_table(),
            40..=44 => self.gen_time_window_func(),
            // TODO: Enable after resolving: <https://github.com/singularity-data/risingwave/issues/2771>.
            45..=49 => self.gen_equijoin_clause(),
            50..=50 => self.gen_table_subquery(),
            _ => unreachable!(),
        }
    }

    fn gen_simple_table(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (relation, _, table) = self.gen_simple_table_factor();

        (
            TableWithJoins {
                relation,
                joins: vec![],
            },
            table,
        )
    }

    fn gen_simple_table_factor(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        let alias = self.gen_table_name_with_prefix("t");
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::new(&table.name)]),
            alias: Some(TableAlias {
                name: alias.as_str().into(),
                columns: vec![],
            }),
            args: vec![],
        };
        table.name = alias; // Rename the table.
        let columns = table.get_qualified_columns();
        (table_factor, columns, vec![table])
    }

    fn gen_table_factor(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        let current_context = self.new_local_context();
        let factor = self.gen_table_factor_inner();
        self.restore_context(current_context);
        factor
    }

    /// Generates a table factor, and provides bound columns.
    /// Generated column names should be qualified by table name.
    fn gen_table_factor_inner(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        // TODO: TableFactor::Derived, TableFactor::TableFunction, TableFactor::NestedJoin
        self.gen_simple_table_factor()
    }

    fn gen_equijoin_clause(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (left_factor, left_columns, mut left_table) = self.gen_table_factor();
        let (right_factor, right_columns, mut right_table) = self.gen_table_factor();

        let mut available_join_on_columns = vec![];
        for left_column in &left_columns {
            for right_column in &right_columns {
                // NOTE: We can support some composite types if we wish to in the future.
                // see: https://www.postgresql.org/docs/14/functions-comparison.html.
                // For simplicity only support scalar types for now.
                let left_ty = left_column.data_type;
                let right_ty = right_column.data_type;
                if left_ty.is_scalar() && right_ty.is_scalar() && (left_ty == right_ty) {
                    available_join_on_columns.push((left_column, right_column))
                }
            }
        }
        if available_join_on_columns.is_empty() {
            return self.gen_simple_table();
        }
        let i = self.rng.gen_range(0..available_join_on_columns.len());
        let (left_column, right_column) = available_join_on_columns[i];
        let join_on_expr =
            create_join_on_clause(left_column.name.clone(), right_column.name.clone());

        let right_factor_with_join = Join {
            relation: right_factor,
            join_operator: JoinOperator::Inner(JoinConstraint::On(join_on_expr)),
        };
        left_table.append(&mut right_table);
        (
            TableWithJoins {
                relation: left_factor,
                joins: vec![right_factor_with_join],
            },
            left_table,
        )
    }

    fn gen_table_subquery(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (subquery, columns) = self.gen_local_query();
        let alias = self.gen_table_name_with_prefix("sq");
        let table = Table {
            name: alias.clone(),
            columns,
        };
        let relation = TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(subquery),
                alias: Some(TableAlias {
                    name: Ident::new(alias),
                    columns: vec![],
                }),
            },
            joins: vec![],
        };

        (relation, vec![table])
    }
}
