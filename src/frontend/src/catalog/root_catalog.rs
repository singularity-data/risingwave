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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::catalog::{CatalogVersion, TableId, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::error::Result;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Sink as ProstSink, Source as ProstSource,
    Table as ProstTable,
};

use super::source_catalog::SourceCatalog;
use super::{CatalogError, SinkId, SourceId};
use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::SystemCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{pg_catalog, DatabaseId, SchemaId};

/// Root catalog of database catalog. Manage all database/schema/table in memory on frontend. it
/// is protected by a `RwLock`. only [`crate::observer::observer_manager::FrontendObserverNode`]
/// will get its mut reference and do write to sync with the meta catalog. Other situations it is
/// read only with a read guard.
///
/// - catalog (root catalog)
///   - database catalog
///     - schema catalog
///       - table catalog
///        - column catalog
pub struct Catalog {
    version: CatalogVersion,
    database_by_name: HashMap<String, DatabaseCatalog>,
    db_name_by_id: HashMap<DatabaseId, String>,
}

#[expect(clippy::derivable_impls)]
impl Default for Catalog {
    fn default() -> Self {
        Self {
            version: 0,
            database_by_name: HashMap::new(),
            db_name_by_id: HashMap::new(),
        }
    }
}

impl Catalog {
    fn get_database_mut(&mut self, db_id: DatabaseId) -> Option<&mut DatabaseCatalog> {
        let name = self.db_name_by_id.get(&db_id)?;
        self.database_by_name.get_mut(name)
    }

    pub fn clear(&mut self) {
        self.database_by_name.clear();
        self.db_name_by_id.clear();
    }

    pub fn create_database(&mut self, db: ProstDatabase) {
        let name = db.name.clone();
        let id = db.id;

        self.database_by_name
            .try_insert(name.clone(), (&db).into())
            .unwrap();
        self.db_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn create_schema(&mut self, proto: ProstSchema) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .create_schema(proto.clone());

        if proto.name == PG_CATALOG_SCHEMA_NAME {
            pg_catalog::get_all_pg_catalogs()
                .into_iter()
                .for_each(|sys_table| {
                    self.get_database_mut(proto.database_id)
                        .unwrap()
                        .get_schema_mut(proto.id)
                        .unwrap()
                        .create_sys_table(sys_table);
                });
        }
    }

    pub fn create_table(&mut self, proto: &ProstTable) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_table(proto);
    }

    pub fn create_source(&mut self, proto: ProstSource) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_source(proto);
    }

    pub fn create_sink(&mut self, proto: ProstSink) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_sink(proto);
    }

    pub fn drop_database(&mut self, db_id: DatabaseId) {
        let name = self.db_name_by_id.remove(&db_id).unwrap();
        let _database = self.database_by_name.remove(&name).unwrap();
    }

    pub fn drop_schema(&mut self, db_id: DatabaseId, schema_id: SchemaId) {
        self.get_database_mut(db_id).unwrap().drop_schema(schema_id);
    }

    pub fn drop_table(&mut self, db_id: DatabaseId, schema_id: SchemaId, tb_id: TableId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_table(tb_id);
    }

    pub fn update_table(&mut self, proto: &ProstTable) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .update_table(proto);
    }

    pub fn drop_source(&mut self, db_id: DatabaseId, schema_id: SchemaId, source_id: SourceId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_source(source_id);
    }

    pub fn drop_sink(&mut self, db_id: DatabaseId, schema_id: SchemaId, sink_id: SinkId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_sink(sink_id);
    }

    pub fn get_database_by_name(&self, db_name: &str) -> Result<&DatabaseCatalog> {
        self.database_by_name
            .get(db_name)
            .ok_or_else(|| CatalogError::NotFound("database", db_name.to_string()).into())
    }

    pub fn get_all_schema_names(&self, db_name: &str) -> Result<Vec<String>> {
        Ok(self.get_database_by_name(db_name)?.get_all_schema_names())
    }

    pub fn get_all_schema_info(&self, db_name: &str) -> Result<Vec<ProstSchema>> {
        Ok(self.get_database_by_name(db_name)?.get_all_schema_info())
    }

    pub fn get_all_database_names(&self) -> Vec<String> {
        self.database_by_name.keys().cloned().collect_vec()
    }

    pub fn get_schema_by_name(&self, db_name: &str, schema_name: &str) -> Result<&SchemaCatalog> {
        self.get_database_by_name(db_name)?
            .get_schema_by_name(schema_name)
            .ok_or_else(|| CatalogError::NotFound("schema", schema_name.to_string()).into())
    }

    pub fn get_table_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<&TableCatalog> {
        self.get_schema_by_name(db_name, schema_name)?
            .get_table_by_name(table_name)
            .ok_or_else(|| CatalogError::NotFound("table", table_name.to_string()).into())
    }

    pub fn get_sys_table_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<&SystemCatalog> {
        self.get_schema_by_name(db_name, schema_name)?
            .get_system_table_by_name(table_name)
            .ok_or_else(|| CatalogError::NotFound("table", table_name.to_string()).into())
    }

    pub fn get_source_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        source_name: &str,
    ) -> Result<&SourceCatalog> {
        self.get_schema_by_name(db_name, schema_name)?
            .get_source_by_name(source_name)
            .ok_or_else(|| CatalogError::NotFound("source", source_name.to_string()).into())
    }

    pub fn get_sink_id_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        sink_name: &str,
    ) -> Result<u32> {
        self.get_schema_by_name(db_name, schema_name)?
            .get_sink_id_by_name(sink_name)
            .ok_or_else(|| CatalogError::NotFound("sink", sink_name.to_string()).into())
    }

    /// Check the name if duplicated with existing table, materialized view or source.
    pub fn check_relation_name_duplicated(
        &self,
        db_name: &str,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<(DatabaseId, SchemaId)> {
        let db = self.get_database_by_name(db_name)?;
        let schema = self.get_schema_by_name(db_name, schema_name)?;

        // Resolve source first.
        if let Some(source) = schema.get_source_by_name(relation_name) {
            // TODO: check if it is a materialized source and improve the err msg
            match source.source_type {
                risingwave_pb::stream_plan::source_node::SourceType::Table => {
                    Err(CatalogError::Duplicated("table", relation_name.to_string()).into())
                }
                risingwave_pb::stream_plan::source_node::SourceType::Source => {
                    let table = self
                        .get_table_by_name(db_name, schema_name, relation_name)
                        .ok();
                    match table {
                        Some(_) => Err(CatalogError::Duplicated(
                            "materialized source",
                            relation_name.to_string(),
                        )
                        .into()),
                        None => Err(
                            CatalogError::Duplicated("source", relation_name.to_string()).into(),
                        ),
                    }
                }
            }
        } else if let Some(_table) = schema.get_table_by_name(relation_name) {
            Err(CatalogError::Duplicated("materialized view", relation_name.to_string()).into())
        } else {
            Ok((db.id(), schema.id()))
        }
    }

    /// Get the catalog cache's catalog version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Set the catalog cache's catalog version.
    pub fn set_version(&mut self, catalog_version: CatalogVersion) {
        self.version = catalog_version;
    }
}
