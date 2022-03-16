use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan::{TaskId, TaskOutputId};
use risingwave_rpc_client::{ComputeClient, ExchangeSource, GrpcExchangeSource};
use risingwave_sqlparser::ast::{Query, Statement};

use crate::binder::Binder;
use crate::handler::util::{get_pg_field_descs, to_pg_rows};
use crate::planner::Planner;
use crate::scheduler::schedule::WorkerNodeManager;
use crate::session::QueryContext;

pub async fn handle_query(context: QueryContext, query: Box<Query>) -> Result<PgResponse> {
    let session = context.session_ctx.clone();
    let catalog_mgr = session.env().catalog_mgr();
    let catalog = catalog_mgr
        .get_database_snapshot(session.database())
        .ok_or_else(|| ErrorCode::InternalError(String::from("catalog not found")))?;
    let mut binder = Binder::new(catalog.clone());
    let bound = binder.bind(Statement::Query(query))?;
    let plan = Planner::new(Rc::new(RefCell::new(context)))
        .plan(bound.clone())?
        .gen_batch_query_plan()
        .to_batch_prost();

    // Choose the first node by WorkerNodeManager.
    let manager = WorkerNodeManager::new(session.env().meta_client().clone()).await?;
    let address = manager
        .list_worker_nodes()
        .get(0)
        .ok_or_else(|| RwError::from(InternalError("No working node available".to_string())))?
        .host
        .as_ref()
        .ok_or_else(|| RwError::from(InternalError("host address not found".to_string())))?
        .to_socket_addr()?;
    let compute_client: ComputeClient = ComputeClient::new(&address).await?;

    // Build task id and task sink id
    let task_id = TaskId {
        query_id: "".to_string(),
        stage_id: 0,
        task_id: 0,
    };
    let task_sink_id = TaskOutputId {
        task_id: Some(task_id.clone()),
        output_id: 0,
    };

    let mut rows = vec![];
    compute_client.create_task(task_id.clone(), plan).await?;
    let mut source =
        GrpcExchangeSource::create_with_client(compute_client.clone(), task_sink_id.clone())
            .await?;
    while let Some(chunk) = source.take_data().await? {
        rows.append(&mut to_pg_rows(chunk));
    }

    let pg_descs = get_pg_field_descs(bound)?;

    Ok(PgResponse::new(
        StatementType::SELECT,
        rows.len() as i32,
        rows,
        pg_descs,
    ))
}
