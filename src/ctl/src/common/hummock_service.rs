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
use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::monitor::{
    HummockMetrics, MonitoredStateStore, ObjectStoreMetrics, StateStoreMetrics,
};
use risingwave_storage::StateStoreImpl;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::MetaServiceOpts;

pub struct HummockServiceOpts {
    pub meta_opts: MetaServiceOpts,
    pub hummock_url: String,

    heartbeat_handle: Option<JoinHandle<()>>,
    heartbeat_shutdown_sender: Option<Sender<()>>,
}

pub struct Metrics {
    pub hummock_metrics: Arc<HummockMetrics>,
    pub state_store_metrics: Arc<StateStoreMetrics>,
    pub object_store_metrics: Arc<ObjectStoreMetrics>,
}

impl HummockServiceOpts {
    /// Recover hummock service options from env variable
    ///
    /// Currently, we will read these variables for meta:
    ///
    /// * `RW_HUMMOCK_URL`: meta service address
    pub fn from_env() -> Result<Self> {
        let meta_opts = MetaServiceOpts::from_env()?;

        let hummock_url = match env::var("RW_HUMMOCK_URL") {
            Ok(url) => {
                tracing::info!("using Hummock URL from `RW_HUMMOCK_URL`: {}", url);
                url
            }
            Err(_) => {
                const MESSAGE: &str = "env variable `RW_HUMMOCK_URL` not found.

For `./risedev d` use cases, please do the following:
* use `./risedev d for-ctl` to start the cluster.
* use `./risedev ctl` to use risectl.

For `./risedev apply-compose-deploy` users,
* `RW_HUMMOCK_URL` will be printed out when deploying. Please copy the bash exports to your console.

risectl requires a full persistent cluster to operate. Please make sure you're not running in minimum mode.";
                bail!(MESSAGE);
            }
        };
        Ok(Self {
            meta_opts,
            hummock_url,
            heartbeat_handle: None,
            heartbeat_shutdown_sender: None,
        })
    }

    pub async fn create_hummock_store_with_metrics(
        &mut self,
    ) -> Result<(MetaClient, MonitoredStateStore<HummockStorage>, Metrics)> {
        let meta_client = self.meta_opts.create_meta_client().await?;

        let (heartbeat_handle, heartbeat_shutdown_sender) =
            MetaClient::start_heartbeat_loop(meta_client.clone(), Duration::from_millis(1000));
        self.heartbeat_handle = Some(heartbeat_handle);
        self.heartbeat_shutdown_sender = Some(heartbeat_shutdown_sender);

        // FIXME: allow specify custom config
        let config = StorageConfig {
            share_buffer_compaction_worker_threads_number: 0,
            ..Default::default()
        };

        tracing::info!("using Hummock config: {:#?}", config);

        let metrics = Metrics {
            hummock_metrics: Arc::new(HummockMetrics::unused()),
            state_store_metrics: Arc::new(StateStoreMetrics::unused()),
            object_store_metrics: Arc::new(ObjectStoreMetrics::unused()),
        };

        let table_id_to_slice_transform = Arc::new(RwLock::new(HashMap::new()));
        let state_store_impl = StateStoreImpl::new(
            &self.hummock_url,
            "none://",
            Arc::new(config),
            Arc::new(MonitoredHummockMetaClient::new(
                meta_client.clone(),
                metrics.hummock_metrics.clone(),
            )),
            metrics.state_store_metrics.clone(),
            metrics.object_store_metrics.clone(),
            table_id_to_slice_transform.clone(),
        )
        .await?;

        if let StateStoreImpl::HummockStateStore(hummock_state_store) = state_store_impl {
            Ok((meta_client, hummock_state_store, metrics))
        } else {
            Err(anyhow!("only Hummock state store is supported in risectl"))
        }
    }

    pub async fn create_hummock_store(
        &mut self,
    ) -> Result<(MetaClient, MonitoredStateStore<HummockStorage>)> {
        let (meta_client, hummock_client, _) = self.create_hummock_store_with_metrics().await?;
        Ok((meta_client, hummock_client))
    }

    pub async fn shutdown(&mut self) {
        if let (Some(sender), Some(handle)) = (
            self.heartbeat_shutdown_sender.take(),
            self.heartbeat_handle.take(),
        ) {
            if let Err(err) = sender.send(()) {
                tracing::warn!("Failed to send shutdown: {:?}", err);
            }
            if let Err(err) = handle.await {
                tracing::warn!("Failed to join shutdown: {:?}", err);
            }
        }
    }
}
