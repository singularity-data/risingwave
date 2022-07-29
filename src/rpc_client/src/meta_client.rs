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
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use paste::paste;
use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::util::addr::HostAddr;
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableId, HummockVersionId, LocalSstableInfo};
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Sink as ProstSink, Source as ProstSource,
    Table as ProstTable,
};
use risingwave_pb::common::WorkerType;
use risingwave_pb::ddl_service::ddl_service_client::DdlServiceClient;
use risingwave_pb::ddl_service::*;
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::*;
use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
use risingwave_pb::meta::heartbeat_service_client::HeartbeatServiceClient;
use risingwave_pb::meta::list_table_fragments_response::TableFragmentInfo;
use risingwave_pb::meta::notification_service_client::NotificationServiceClient;
use risingwave_pb::meta::scale_service_client::ScaleServiceClient;
use risingwave_pb::meta::stream_manager_service_client::StreamManagerServiceClient;
use risingwave_pb::meta::*;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_pb::user::user_service_client::UserServiceClient;
use risingwave_pb::user::*;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tonic::transport::{Channel, Endpoint};
use tonic::{Status, Streaming};

use crate::error::Result;
use crate::hummock_meta_client::HummockMetaClient;

type DatabaseId = u32;
type SchemaId = u32;

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Clone, Debug)]
pub struct MetaClient {
    worker_id: Option<u32>,
    pub inner: GrpcMetaClient,
}

impl MetaClient {
    /// Connect to the meta server `addr`.
    pub async fn new(meta_addr: &str) -> Result<Self> {
        Ok(Self {
            inner: GrpcMetaClient::new(meta_addr).await?,
            worker_id: None,
        })
    }

    pub fn set_worker_id(&mut self, worker_id: u32) {
        self.worker_id = Some(worker_id);
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_id.expect("worker node id is not set.")
    }

    /// Subscribe to notification from meta.
    pub async fn subscribe(
        &self,
        addr: &HostAddr,
        worker_type: WorkerType,
    ) -> Result<Box<dyn NotificationStream>> {
        let request = SubscribeRequest {
            worker_type: worker_type as i32,
            host: Some(addr.to_protobuf()),
        };
        self.inner.subscribe(request).await
    }

    /// Register the current node to the cluster and set the corresponding worker id.
    pub async fn register(
        &mut self,
        worker_type: WorkerType,
        addr: &HostAddr,
        worker_node_parallelism: usize,
    ) -> Result<u32> {
        let request = AddWorkerNodeRequest {
            worker_type: worker_type as i32,
            host: Some(addr.to_protobuf()),
            worker_node_parallelism: worker_node_parallelism as u64,
        };
        let resp = self.inner.add_worker_node(request).await?;
        let worker_node = resp.node.expect("AddWorkerNodeResponse::node is empty");
        self.set_worker_id(worker_node.id);
        // unpin snapshot before MAX will create a new snapshot with last committed epoch and then
        //  we do not create snapshot during every pin_snapshot.
        self.pin_snapshot().await?;
        Ok(worker_node.id)
    }

    /// Activate the current node in cluster to confirm it's ready to serve.
    pub async fn activate(&self, addr: &HostAddr) -> Result<()> {
        let request = ActivateWorkerNodeRequest {
            host: Some(addr.to_protobuf()),
        };
        self.inner.activate_worker_node(request).await?;
        Ok(())
    }

    /// Send heartbeat signal to meta service.
    pub async fn send_heartbeat(&self, node_id: u32) -> Result<()> {
        let request = HeartbeatRequest { node_id };
        self.inner.heartbeat(request).await?;
        Ok(())
    }

    pub async fn create_database(&self, db: ProstDatabase) -> Result<(DatabaseId, CatalogVersion)> {
        let request = CreateDatabaseRequest { db: Some(db) };
        let resp = self.inner.create_database(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.database_id, resp.version))
    }

    pub async fn create_schema(&self, schema: ProstSchema) -> Result<(SchemaId, CatalogVersion)> {
        let request = CreateSchemaRequest {
            schema: Some(schema),
        };
        let resp = self.inner.create_schema(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.schema_id, resp.version))
    }

    pub async fn create_materialized_view(
        &self,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<(TableId, CatalogVersion)> {
        let request = CreateMaterializedViewRequest {
            materialized_view: Some(table),
            fragment_graph: Some(graph),
        };
        let resp = self.inner.create_materialized_view(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.table_id.into(), resp.version))
    }

    pub async fn drop_materialized_view(&self, table_id: TableId) -> Result<CatalogVersion> {
        let request = DropMaterializedViewRequest {
            table_id: table_id.table_id(),
        };

        let resp = self.inner.drop_materialized_view(request).await?;
        Ok(resp.version)
    }

    pub async fn create_source(&self, source: ProstSource) -> Result<(u32, CatalogVersion)> {
        let request = CreateSourceRequest {
            source: Some(source),
        };

        let resp = self.inner.create_source(request).await?;
        Ok((resp.source_id, resp.version))
    }

    pub async fn create_sink(
        &self,
        sink: ProstSink,
        graph: StreamFragmentGraph,
    ) -> Result<(u32, CatalogVersion)> {
        let request = CreateSinkRequest {
            sink: Some(sink),
            fragment_graph: Some(graph),
        };

        let resp = self.inner.create_sink(request).await?;
        Ok((resp.sink_id, resp.version))
    }

    pub async fn create_materialized_source(
        &self,
        source: ProstSource,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<(TableId, u32, CatalogVersion)> {
        let request = CreateMaterializedSourceRequest {
            materialized_view: Some(table),
            fragment_graph: Some(graph),
            source: Some(source),
        };
        let resp = self.inner.create_materialized_source(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.table_id.into(), resp.source_id, resp.version))
    }

    pub async fn drop_materialized_source(
        &self,
        source_id: u32,
        table_id: TableId,
    ) -> Result<CatalogVersion> {
        let request = DropMaterializedSourceRequest {
            source_id,
            table_id: table_id.table_id(),
        };

        let resp = self.inner.drop_materialized_source(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_source(&self, source_id: u32) -> Result<CatalogVersion> {
        let request = DropSourceRequest { source_id };
        let resp = self.inner.drop_source(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_sink(&self, sink_id: u32) -> Result<CatalogVersion> {
        let request = DropSinkRequest { sink_id };
        let resp = self.inner.drop_sink(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_database(&self, database_id: u32) -> Result<CatalogVersion> {
        let request = DropDatabaseRequest { database_id };
        let resp = self.inner.drop_database(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_schema(&self, schema_id: u32) -> Result<CatalogVersion> {
        let request = DropSchemaRequest { schema_id };
        let resp = self.inner.drop_schema(request).await?;
        Ok(resp.version)
    }

    // TODO: using UserInfoVersion instead as return type.
    pub async fn create_user(&self, user: UserInfo) -> Result<u64> {
        let request = CreateUserRequest { user: Some(user) };
        let resp = self.inner.create_user(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_user(&self, user_id: u32) -> Result<u64> {
        let request = DropUserRequest { user_id };
        let resp = self.inner.drop_user(request).await?;
        Ok(resp.version)
    }

    pub async fn update_user(&self, request: UpdateUserRequest) -> Result<u64> {
        let resp = self.inner.update_user(request).await?;
        Ok(resp.version)
    }

    pub async fn grant_privilege(
        &self,
        user_ids: Vec<u32>,
        privileges: Vec<GrantPrivilege>,
        with_grant_option: bool,
        granted_by: u32,
    ) -> Result<u64> {
        let request = GrantPrivilegeRequest {
            user_ids,
            privileges,
            with_grant_option,
            granted_by,
        };
        let resp = self.inner.grant_privilege(request).await?;
        Ok(resp.version)
    }

    pub async fn revoke_privilege(
        &self,
        user_ids: Vec<u32>,
        privileges: Vec<GrantPrivilege>,
        granted_by: Option<u32>,
        revoke_by: u32,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> Result<u64> {
        let granted_by = granted_by.unwrap_or_default();
        let request = RevokePrivilegeRequest {
            user_ids,
            privileges,
            granted_by,
            revoke_by,
            revoke_grant_option,
            cascade,
        };
        let resp = self.inner.revoke_privilege(request).await?;
        Ok(resp.version)
    }

    /// Unregister the current node to the cluster.
    pub async fn unregister(&self, addr: HostAddr) -> Result<()> {
        let request = DeleteWorkerNodeRequest {
            host: Some(addr.to_protobuf()),
        };
        self.inner.delete_worker_node(request).await?;
        Ok(())
    }

    pub fn start_heartbeat_loop(
        meta_client: MetaClient,
        min_interval: Duration,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval_ticker = tokio::time::interval(min_interval);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval_ticker.tick() => {},
                    // Shutdown
                    _ = &mut shutdown_rx => {
                        tracing::info!("Heartbeat loop is stopped");
                        return;
                    }
                }
                tracing::trace!(target: "events::meta::client_heartbeat", "heartbeat");
                match tokio::time::timeout(
                    // TODO: decide better min_interval for timeout
                    min_interval * 3,
                    meta_client.send_heartbeat(meta_client.worker_id()),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        tracing::warn!("Failed to send_heartbeat: error {}", err);
                        if err.to_string().contains("unknown worker") {
                            panic!("Already removed by the meta node. Need to restart the worker");
                        }
                    }
                    Err(err) => {
                        tracing::warn!("Failed to send_heartbeat: timeout {}", err);
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    pub async fn risectl_list_state_tables(&self) -> Result<Vec<ProstTable>> {
        let request = RisectlListStateTablesRequest {};
        let resp = self.inner.risectl_list_state_tables(request).await?;
        Ok(resp.tables)
    }

    pub async fn flush(&self) -> Result<()> {
        let request = FlushRequest::default();
        self.inner.flush(request).await?;
        Ok(())
    }

    pub async fn list_table_fragments(
        &self,
        table_ids: &[u32],
    ) -> Result<HashMap<u32, TableFragmentInfo>> {
        let request = ListTableFragmentsRequest {
            table_ids: table_ids.to_vec(),
        };
        let resp = self.inner.list_table_fragments(request).await?;
        Ok(resp.table_fragments)
    }

    pub async fn pause(&self) -> Result<()> {
        let request = PauseRequest {};
        let _resp = self.inner.pause(request).await?;
        Ok(())
    }

    pub async fn resume(&self) -> Result<()> {
        let request = ResumeRequest {};
        let _resp = self.inner.resume(request).await?;
        Ok(())
    }

    pub async fn get_cluster_info(&self) -> Result<GetClusterInfoResponse> {
        let request = GetClusterInfoRequest {};
        let resp = self.inner.get_cluster_info(request).await?;
        Ok(resp)
    }
}

#[async_trait]
impl HummockMetaClient for MetaClient {
    async fn pin_version(
        &self,
        last_pinned: HummockVersionId,
    ) -> Result<(bool, Vec<HummockVersionDelta>, Option<HummockVersion>)> {
        let req = PinVersionRequest {
            context_id: self.worker_id(),
            last_pinned,
        };
        let resp = self.inner.pin_version(req).await?;
        Ok((
            resp.is_delta_response,
            resp.version_deltas,
            resp.pinned_version,
        ))
    }

    async fn unpin_version(&self) -> Result<()> {
        let req = UnpinVersionRequest {
            context_id: self.worker_id(),
        };
        self.inner.unpin_version(req).await?;
        Ok(())
    }

    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()> {
        let req = UnpinVersionBeforeRequest {
            context_id: self.worker_id(),
            unpin_version_before,
        };
        self.inner.unpin_version_before(req).await?;
        Ok(())
    }

    async fn pin_snapshot(&self) -> Result<HummockEpoch> {
        let req = PinSnapshotRequest {
            context_id: self.worker_id(),
        };
        let resp = self.inner.pin_snapshot(req).await?;
        Ok(resp.snapshot.unwrap().epoch)
    }

    async fn get_epoch(&self) -> Result<HummockEpoch> {
        let req = GetEpochRequest {};
        let resp = self.inner.get_epoch(req).await?;
        Ok(resp.snapshot.unwrap().epoch)
    }

    async fn unpin_snapshot(&self) -> Result<()> {
        let req = UnpinSnapshotRequest {
            context_id: self.worker_id(),
        };
        self.inner.unpin_snapshot(req).await?;
        Ok(())
    }

    async fn unpin_snapshot_before(&self, pinned_epochs: HummockEpoch) -> Result<()> {
        let req = UnpinSnapshotBeforeRequest {
            context_id: self.worker_id(),
            // For unpin_snapshot_before, we do not care about snapshots list but only min epoch.
            min_snapshot: Some(HummockSnapshot {
                epoch: pinned_epochs,
            }),
        };
        self.inner.unpin_snapshot_before(req).await?;
        Ok(())
    }

    async fn get_new_table_id(&self) -> Result<HummockSstableId> {
        let resp = self.inner.get_new_table_id(GetNewTableIdRequest {}).await?;
        Ok(resp.table_id)
    }

    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()> {
        let req = ReportCompactionTasksRequest {
            context_id: self.worker_id(),
            compact_task: Some(compact_task),
        };
        self.inner.report_compaction_tasks(req).await?;
        Ok(())
    }

    async fn commit_epoch(
        &self,
        _epoch: HummockEpoch,
        _sstables: Vec<LocalSstableInfo>,
    ) -> Result<()> {
        panic!("Only meta service can commit_epoch in production.")
    }

    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>> {
        let req = SubscribeCompactTasksRequest {
            context_id: self.worker_id(),
        };
        self.inner.subscribe_compact_tasks(req).await
    }

    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        let req = ReportVacuumTaskRequest {
            vacuum_task: Some(vacuum_task),
        };
        self.inner.report_vacuum_task(req).await?;
        Ok(())
    }

    async fn get_compaction_groups(&self) -> Result<Vec<CompactionGroup>> {
        let req = GetCompactionGroupsRequest {};
        let resp = self.inner.get_compaction_groups(req).await?;
        Ok(resp.compaction_groups)
    }

    async fn trigger_manual_compaction(
        &self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
    ) -> Result<()> {
        // TODO: support key_range parameter
        let req = TriggerManualCompactionRequest {
            compaction_group_id,
            table_id, /* if table_id not exist, manual_compaction will include all the sst
                       * without check internal_table_id */
            level,
            ..Default::default()
        };

        self.inner.trigger_manual_compaction(req).await?;
        Ok(())
    }
}

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Debug, Clone)]
pub struct GrpcMetaClient {
    pub cluster_client: ClusterServiceClient<Channel>,
    pub heartbeat_client: HeartbeatServiceClient<Channel>,
    pub ddl_client: DdlServiceClient<Channel>,
    pub hummock_client: HummockManagerServiceClient<Channel>,
    pub notification_client: NotificationServiceClient<Channel>,
    pub stream_client: StreamManagerServiceClient<Channel>,
    pub user_client: UserServiceClient<Channel>,
    pub scale_client: ScaleServiceClient<Channel>,
}

impl GrpcMetaClient {
    // Retry base interval in ms for connecting to meta server.
    const CONN_RETRY_BASE_INTERVAL_MS: u64 = 100;
    // Max retry interval in ms for connecting to meta server.
    const CONN_RETRY_MAX_INTERVAL_MS: u64 = 5000;

    /// Connect to the meta server `addr`.
    pub async fn new(addr: &str) -> Result<Self> {
        let endpoint = Endpoint::from_shared(addr.to_string())?;
        let retry_strategy = ExponentialBackoff::from_millis(Self::CONN_RETRY_BASE_INTERVAL_MS)
            .max_delay(Duration::from_millis(Self::CONN_RETRY_MAX_INTERVAL_MS))
            .map(jitter);
        let channel = tokio_retry::Retry::spawn(retry_strategy, || async {
            let endpoint = endpoint.clone();
            endpoint
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .inspect_err(|e| {
                    tracing::warn!(
                        "Failed to connect to meta server {}, wait for online: {}",
                        addr,
                        e
                    );
                })
        })
        .await?;

        let cluster_client = ClusterServiceClient::new(channel.clone());
        let heartbeat_client = HeartbeatServiceClient::new(channel.clone());
        let ddl_client = DdlServiceClient::new(channel.clone());
        let hummock_client = HummockManagerServiceClient::new(channel.clone());
        let notification_client = NotificationServiceClient::new(channel.clone());
        let stream_client = StreamManagerServiceClient::new(channel.clone());
        let user_client = UserServiceClient::new(channel.clone());
        let scale_client = ScaleServiceClient::new(channel);
        Ok(Self {
            cluster_client,
            heartbeat_client,
            ddl_client,
            hummock_client,
            notification_client,
            stream_client,
            user_client,
            scale_client,
        })
    }
}

macro_rules! grpc_meta_client_impl {
    ([], $( { $client:ident, $fn_name:ident, $req:ty, $resp:ty }),*) => {
        $(paste! {
            impl GrpcMetaClient {
                pub async fn [<$fn_name>](&self, request: $req) -> Result<$resp> {
                    Ok(self
                        .$client
                        .to_owned()
                        .$fn_name(request)
                        .await?
                        .into_inner())
                }
            }
        })*
    }
}

macro_rules! for_all_meta_rpc {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ cluster_client, add_worker_node, AddWorkerNodeRequest, AddWorkerNodeResponse }
            ,{ cluster_client, activate_worker_node, ActivateWorkerNodeRequest, ActivateWorkerNodeResponse }
            ,{ cluster_client, delete_worker_node, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse }
            ,{ cluster_client, list_all_nodes, ListAllNodesRequest, ListAllNodesResponse }
            ,{ heartbeat_client, heartbeat, HeartbeatRequest, HeartbeatResponse }
            ,{ stream_client, flush, FlushRequest, FlushResponse }
            ,{ stream_client, list_table_fragments, ListTableFragmentsRequest, ListTableFragmentsResponse }
            ,{ ddl_client, create_materialized_source, CreateMaterializedSourceRequest, CreateMaterializedSourceResponse }
            ,{ ddl_client, create_materialized_view, CreateMaterializedViewRequest, CreateMaterializedViewResponse }
            ,{ ddl_client, create_source, CreateSourceRequest, CreateSourceResponse }
            ,{ ddl_client, create_sink, CreateSinkRequest, CreateSinkResponse }
            ,{ ddl_client, create_schema, CreateSchemaRequest, CreateSchemaResponse }
            ,{ ddl_client, create_database, CreateDatabaseRequest, CreateDatabaseResponse }
            ,{ ddl_client, drop_materialized_source, DropMaterializedSourceRequest, DropMaterializedSourceResponse }
            ,{ ddl_client, drop_materialized_view, DropMaterializedViewRequest, DropMaterializedViewResponse }
            ,{ ddl_client, drop_source, DropSourceRequest, DropSourceResponse }
            ,{ ddl_client, drop_sink, DropSinkRequest, DropSinkResponse }
            ,{ ddl_client, drop_database, DropDatabaseRequest, DropDatabaseResponse }
            ,{ ddl_client, drop_schema, DropSchemaRequest, DropSchemaResponse }
            ,{ ddl_client, risectl_list_state_tables, RisectlListStateTablesRequest, RisectlListStateTablesResponse }
            ,{ hummock_client, pin_version, PinVersionRequest, PinVersionResponse }
            ,{ hummock_client, unpin_version, UnpinVersionRequest, UnpinVersionResponse }
            ,{ hummock_client, unpin_version_before, UnpinVersionBeforeRequest, UnpinVersionBeforeResponse }
            ,{ hummock_client, pin_snapshot, PinSnapshotRequest, PinSnapshotResponse }
            ,{ hummock_client, get_epoch, GetEpochRequest, GetEpochResponse }
            ,{ hummock_client, unpin_snapshot, UnpinSnapshotRequest, UnpinSnapshotResponse }
            ,{ hummock_client, unpin_snapshot_before, UnpinSnapshotBeforeRequest, UnpinSnapshotBeforeResponse }
            ,{ hummock_client, report_compaction_tasks, ReportCompactionTasksRequest, ReportCompactionTasksResponse }
            ,{ hummock_client, get_new_table_id, GetNewTableIdRequest, GetNewTableIdResponse }
            ,{ hummock_client, subscribe_compact_tasks, SubscribeCompactTasksRequest, Streaming<SubscribeCompactTasksResponse> }
            ,{ hummock_client, report_vacuum_task, ReportVacuumTaskRequest, ReportVacuumTaskResponse }
            ,{ hummock_client, get_compaction_groups, GetCompactionGroupsRequest, GetCompactionGroupsResponse }
            ,{ hummock_client, trigger_manual_compaction, TriggerManualCompactionRequest, TriggerManualCompactionResponse }
            ,{ user_client, create_user, CreateUserRequest, CreateUserResponse }
            ,{ user_client, update_user, UpdateUserRequest, UpdateUserResponse }
            ,{ user_client, drop_user, DropUserRequest, DropUserResponse }
            ,{ user_client, grant_privilege, GrantPrivilegeRequest, GrantPrivilegeResponse }
            ,{ user_client, revoke_privilege, RevokePrivilegeRequest, RevokePrivilegeResponse }
            ,{ scale_client, pause, PauseRequest, PauseResponse }
            ,{ scale_client, resume, ResumeRequest, ResumeResponse }
            ,{ scale_client, get_cluster_info, GetClusterInfoRequest, GetClusterInfoResponse }
        }
    };
}

for_all_meta_rpc! { grpc_meta_client_impl }

impl GrpcMetaClient {
    pub async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<Box<dyn NotificationStream>> {
        Ok(Box::new(
            self.notification_client
                .to_owned()
                .subscribe(request)
                .await?
                .into_inner(),
        ))
    }
}

#[async_trait::async_trait]
pub trait NotificationStream: Send {
    /// Ok(Some) => receive a `SubscribeResponse`.
    /// Ok(None) => stream terminates.
    /// Err => error happens.
    async fn next(&mut self) -> Result<Option<SubscribeResponse>>;
}

#[async_trait::async_trait]
impl NotificationStream for Streaming<SubscribeResponse> {
    async fn next(&mut self) -> Result<Option<SubscribeResponse>> {
        self.message().await.map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl NotificationStream for Receiver<std::result::Result<SubscribeResponse, Status>> {
    async fn next(&mut self) -> Result<Option<SubscribeResponse>> {
        self.recv().await.transpose().map_err(Into::into)
    }
}
