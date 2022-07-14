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

use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Ident;

use crate::binder::bind_context::LateralBindContext;
use crate::binder::Binder;
use crate::expr::{CorrelatedInputRef, ExprImpl, InputRef};

impl Binder {
    pub fn bind_column(&mut self, idents: &[Ident]) -> Result<ExprImpl> {
        // TODO: check quote style of `ident`.
        let (_schema_name, table_name, column_name) = match idents {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => {
                return Err(
                    ErrorCode::InternalError(format!("Too many idents: {:?}", idents)).into(),
                )
            }
        };

        if let Ok(index) = self
            .context
            .get_column_binding_index(table_name, column_name)
        {
            let column = &self.context.columns[index];
            return Ok(InputRef::new(column.index, column.field.data_type.clone()).into());
        }

        // Try to find a correlated column in `upper_contexts`, starting from the innermost context.
        let mut err = ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)).into();
        for (i, (context, lateral_contexts)) in
            self.upper_subquery_contexts.iter().rev().enumerate()
        {
            // `depth` starts from 1.
            let depth = i + 1;
            match context.get_column_binding_index(table_name, column_name) {
                Ok(index) => {
                    let column = &context.columns[index];
                    return Ok(CorrelatedInputRef::new(
                        column.index,
                        column.field.data_type.clone(),
                        depth,
                    )
                    .into());
                }
                Err(e) => {
                    err = e;
                }
            }
            for LateralBindContext {
                context,
                is_visible,
            } in lateral_contexts
            {
                if *is_visible {
                    match context.get_column_binding_index(table_name, column_name) {
                        Ok(index) => {
                            let column = &context.columns[index];
                            return Ok(CorrelatedInputRef::new(
                                column.index,
                                column.field.data_type.clone(),
                                depth,
                            )
                            .into());
                        }
                        Err(e) => {
                            err = e;
                        }
                    }
                }
            }
        }
        Err(err)
    }
}
