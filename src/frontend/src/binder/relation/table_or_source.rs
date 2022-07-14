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

use std::sync::Arc;

use risingwave_common::catalog::{ColumnDesc, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::{ObjectName, TableAlias};

use crate::binder::{Binder, Relation};
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::system_catalog::SystemCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, TableId};

#[derive(Debug, Clone)]
pub struct BoundBaseTable {
    pub name: String, // explain-only
    pub table_id: TableId,
    pub table_catalog: TableCatalog,
    pub table_indexes: Vec<Arc<TableCatalog>>,
}

/// `BoundTableSource` is used by DML statement on table source like insert, update.
#[derive(Debug)]
pub struct BoundTableSource {
    pub name: String,       // explain-only
    pub source_id: TableId, // TODO: refactor to source id
    pub columns: Vec<ColumnDesc>,
    pub append_only: bool,
}

#[derive(Debug, Clone)]
pub struct BoundSystemTable {
    pub name: String, // explain-only
    pub table_id: TableId,
    pub sys_table_catalog: SystemCatalog,
}

#[derive(Debug, Clone)]
pub struct BoundSource {
    pub catalog: SourceCatalog,
}

impl From<&SourceCatalog> for BoundSource {
    fn from(s: &SourceCatalog) -> Self {
        Self { catalog: s.clone() }
    }
}

impl Binder {
    pub(super) fn bind_table_or_source(
        &mut self,
        schema_name: &str,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
        let (ret, columns) = {
            let catalog = &self.catalog;
            if schema_name == PG_CATALOG_SCHEMA_NAME {
                if let Ok(sys_table_catalog) =
                    catalog.get_sys_table_by_name(&self.db_name, schema_name, table_name)
                {
                    let table = BoundSystemTable {
                        table_id: sys_table_catalog.id(),
                        name: table_name.to_string(),
                        sys_table_catalog: sys_table_catalog.clone(),
                    };
                    (
                        Relation::SystemTable(Box::new(table)),
                        sys_table_catalog.columns.clone(),
                    )
                } else {
                    return Err(ErrorCode::NotImplemented(
                        format!(
                            r###"pg_catalog.{} is not supported, please use `SHOW` commands for now.
`SHOW TABLES`,
`SHOW MATERIALIZED VIEWS`,
`DESCRIBE <table>`,
`SHOW COLUMNS FROM [table]`
"###,
                            table_name
                        ),
                        1695.into(),
                    )
                    .into());
                }
            } else if let Ok(table_catalog) =
                catalog.get_table_by_name(&self.db_name, schema_name, table_name)
            {
                let table_id = table_catalog.id();
                let table_catalog = table_catalog.clone();
                let columns = table_catalog.columns.clone();
                let table_indexes = self.resolve_table_indexes(schema_name, table_id)?;

                let table = BoundBaseTable {
                    name: table_name.to_string(),
                    table_id,
                    table_catalog,
                    table_indexes,
                };

                (Relation::BaseTable(Box::new(table)), columns)
            } else if let Ok(s) = catalog.get_source_by_name(&self.db_name, schema_name, table_name)
            {
                (Relation::Source(Box::new(s.into())), s.columns.clone())
            } else {
                return Err(RwError::from(CatalogError::NotFound(
                    "table or source",
                    table_name.to_string(),
                )));
            }
        };

        self.bind_table_to_context(
            columns
                .iter()
                .map(|c| (c.is_hidden, (&c.column_desc).into())),
            table_name.to_string(),
            alias,
        )?;
        Ok(ret)
    }

    fn resolve_table_indexes(
        &mut self,
        schema_name: &str,
        table_id: TableId,
    ) -> Result<Vec<Arc<TableCatalog>>> {
        Ok(self
            .catalog
            .get_schema_by_name(&self.db_name, schema_name)?
            .iter_index()
            .filter(|x| x.is_index_on == Some(table_id))
            .map(|table| table.clone().into())
            .collect())
    }

    pub(crate) fn bind_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<BoundBaseTable> {
        let table_catalog = self
            .catalog
            .get_table_by_name(&self.db_name, schema_name, table_name)?
            .clone();

        let table_id = table_catalog.id();
        let table_indexes = self.resolve_table_indexes(schema_name, table_id)?;

        let columns = table_catalog.columns.clone();

        self.bind_table_to_context(
            columns
                .iter()
                .map(|c| (c.is_hidden, (&c.column_desc).into())),
            table_name.to_string(),
            alias,
        )?;

        Ok(BoundBaseTable {
            name: table_name.to_string(),
            table_id,
            table_catalog,
            table_indexes,
        })
    }

    pub(crate) fn bind_table_source(&mut self, name: ObjectName) -> Result<BoundTableSource> {
        let (schema_name, source_name) = Self::resolve_table_name(name)?;
        let source = self
            .catalog
            .get_source_by_name(&self.db_name, &schema_name, &source_name)?;

        let source_id = TableId::new(source.id);

        let append_only = source.append_only;
        let columns = source
            .columns
            .iter()
            .filter(|c| !c.is_hidden)
            .map(|c| c.column_desc.clone())
            .collect();

        // Note(bugen): do not bind context here.

        Ok(BoundTableSource {
            name: source_name,
            source_id,
            columns,
            append_only,
        })
    }
}
