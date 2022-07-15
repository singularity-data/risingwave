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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::session::OptimizerContext;

pub async fn handle_drop_source(context: OptimizerContext, name: ObjectName) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, source_name) = Binder::resolve_table_name(name)?;

    let catalog_reader = session.env().catalog_reader();

    let source = catalog_reader
        .read_guard()
        .get_source_by_name(session.database(), &schema_name, &source_name)
        .map_err(|err| {
            if let Ok(table) =
            catalog_reader.read_guard().get_table_by_name(session.database(), &schema_name, &source_name)
            && table.associated_source_id().is_none(){
                return RwError::from(ErrorCode::InvalidInputSyntax(
                    "Use `DROP MATERIALIZED VIEW` to drop a materialized view.".to_owned(),
                ));
            }
            err
        })?
        .clone();
    match source.source_type {
        SourceType::Table => {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "Use `DROP TABLE` to drop a table.".to_owned(),
            )));
        }
        SourceType::Source => {
            let table = catalog_reader
                .read_guard()
                .get_table_by_name(session.database(), &schema_name, &source_name)
                .ok()
                .cloned();
            let catalog_writer = session.env().catalog_writer();
            if let Some(table) = table {
                // Dropping a materialized source.
                catalog_writer
                    .drop_materialized_source(source.id, table.id)
                    .await?;
            } else {
                catalog_writer.drop_source(source.id).await?;
            }
        }
    }

    Ok(PgResponse::empty_result(StatementType::DROP_SOURCE))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    async fn test_drop_source(materialized: bool) {
        let frontend = LocalFrontend::new(Default::default()).await;

        let materialized = if materialized { "MATERIALIZED " } else { "" };
        let sql = format!("CREATE {}SOURCE s ROW FORMAT JSON", materialized);
        frontend.run_sql(sql).await.unwrap();

        assert_eq!(
            "Invalid input syntax: Use `DROP SOURCE` to drop a source.".to_string(),
            frontend
                .run_sql("DROP TABLE s")
                .await
                .unwrap_err()
                .to_string()
        );

        assert_eq!(
            "Invalid input syntax: Use `DROP SOURCE` to drop a source.".to_string(),
            frontend
                .run_sql("DROP MATERIALIZED VIEW s")
                .await
                .unwrap_err()
                .to_string()
        );

        frontend.run_sql("DROP SOURCE s").await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_normal_source() {
        test_drop_source(false).await;
    }

    #[tokio::test]
    async fn test_drop_materialized_source() {
        test_drop_source(true).await;
    }
}
