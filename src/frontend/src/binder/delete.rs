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

use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::{Expr, ObjectName};

use super::{Binder, BoundBaseTable, BoundTableSource};
use crate::expr::ExprImpl;

#[derive(Debug)]
pub struct BoundDelete {
    /// Used for injecting deletion chunks to the source.
    pub table_source: BoundTableSource,

    /// Used for scanning the records to delete with the `selection`.
    pub table: BoundBaseTable,

    pub selection: Option<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        source_name: ObjectName,
        selection: Option<Expr>,
    ) -> Result<BoundDelete> {
        let (schema_name, table_name) = Self::resolve_table_name(source_name.clone())?;
        let table_source = self.bind_table_source(source_name).map_err(|err| {
            if let Ok(table) = self.catalog.get_table_by_name(&self.db_name, &schema_name,
            &table_name)      && table.associated_source_id().is_none(){
                return RwError::from(ErrorCode::InvalidInputSyntax(
                    format!("cannot change materialized view {}",table_name)
                ));
            }
            err
        })?;
        if table_source.append_only {
            return Err(ErrorCode::BindError(
                "Append-only table source doesn't support delete".to_string(),
            )
            .into());
        }
        let table = self.bind_table(&schema_name, &table_name, None)?;
        let delete = BoundDelete {
            table_source,
            table,
            selection: selection.map(|expr| self.bind_expr(expr)).transpose()?,
        };
        Ok(delete)
    }
}
