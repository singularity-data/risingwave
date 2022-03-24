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

use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::plan::{TaskId, TaskOutputId};
use risingwave_rpc_client::{ComputeClient, ExchangeSource};
use risingwave_sqlparser::ast::Statement;
use uuid::Uuid;

use crate::binder::Binder;
use crate::handler::util::{to_pg_field, to_pg_rows};
use crate::planner::Planner;
use crate::session::QueryContext;

pub async fn handle_query(context: QueryContext, stmt: Statement) -> Result<PgResponse> {
    let stmt_type = to_statement_type(&stmt);

    let session = context.session_ctx.clone();
    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
        binder.bind(stmt)?
    };

    let (plan, pg_descs) = {
        // Subblock to make sure PlanRef (an Rc) is dropped before `await` below.
        let plan = Planner::new(Rc::new(RefCell::new(context)))
            .plan(bound)?
            .gen_batch_query_plan();

        let pg_descs = plan.schema().fields().iter().map(to_pg_field).collect();

        (plan.to_batch_prost(), pg_descs)
    };

    // Choose the first node by WorkerNodeManager.
    let manager = session.env().worker_node_manager();
    let address = manager
        .list_worker_nodes()
        .get(0)
        .ok_or_else(|| RwError::from(InternalError("No working node available".to_string())))?
        .host
        .as_ref()
        .ok_or_else(|| RwError::from(InternalError("host address not found".to_string())))?
        .into();
    let compute_client: ComputeClient = ComputeClient::new(address).await?;

    // Build task id and task sink id
    let task_id = TaskId {
        query_id: Uuid::new_v4().to_string(),
        stage_id: 0,
        task_id: 0,
    };
    let task_sink_id = TaskOutputId {
        task_id: Some(task_id.clone()),
        output_id: 0,
    };

    // Pin snapshot in meta. Single frontend for now. So context_id is always 0.
    // TODO: hummock snapshot should maintain as cache instead of RPC each query.
    let meta_client = session.env().meta_client();
    let epoch = meta_client.pin_snapshot().await?;

    // Create task.
    compute_client
        .create_task(task_id.clone(), plan, epoch)
        .await?;

    // Receive data.
    let mut source = compute_client.get_data(task_sink_id.clone()).await?;
    let mut rows = vec![];
    while let Some(chunk) = source.take_data().await? {
        rows.extend(to_pg_rows(chunk));
    }

    // Unpin corresponding snapshot.
    meta_client.unpin_snapshot(epoch).await?;

    let rows_count = match stmt_type {
        StatementType::SELECT => rows.len() as i32,

        // TODO(renjie): We need a better solution for this.
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let first_row = rows[0].values();
            let affected_rows_str = first_row[0]
                .as_ref()
                .expect("compute node should return affected rows in output");
            affected_rows_str.parse().unwrap_or_default()
        }

        _ => unreachable!(),
    };

    Ok(PgResponse::new(stmt_type, rows_count, rows, pg_descs))
}

fn to_statement_type(stmt: &Statement) -> StatementType {
    use StatementType::*;

    match stmt {
        Statement::Insert { .. } => INSERT,
        Statement::Delete { .. } => DELETE,
        Statement::Update { .. } => UPDATE,
        Statement::Query(_) => SELECT,
        _ => unreachable!(),
    }
}
