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
use std::time::Duration;

use anyhow::anyhow;
use moka::future::Cache;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;
use tonic::transport::{Channel, Endpoint};

use crate::error::{Result, RpcError};

pub type StreamClient = StreamServiceClient<Channel>;

pub type WorkerId = u32;

/// [`StreamClientPool`] maintains stream service clients to known compute nodes.
pub struct StreamClientPool {
    /// Stores the [`StreamClient`] mapping: `node_id` => client.
    clients: Cache<WorkerId, StreamClient>,
}

impl Default for StreamClientPool {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamClientPool {
    pub fn new() -> Self {
        Self {
            clients: Cache::new(u64::MAX),
        }
    }

    /// Get the stream service client for the given node. If the connection is not established, a
    /// new client will be created and returned.
    pub async fn get(&self, node: &WorkerNode) -> Result<StreamServiceClient<Channel>> {
        self.clients
            .try_get_with(node.id, async {
                let addr: HostAddr = node.get_host().unwrap().into();
                let endpoint = Endpoint::from_shared(format!("http://{}", addr))?;
                let client = StreamServiceClient::new(
                    endpoint
                        .connect_timeout(Duration::from_secs(5))
                        .connect()
                        .await?,
                );
                Ok::<_, RpcError>(client)
            })
            .await
            .map_err(|e| anyhow!("failed to create compute client: {:?}", e).into())
    }

    /// Invalidate the stream service client for the given node.
    pub async fn invalidate(&self, node: &WorkerNode) {
        self.clients.invalidate(&node.id).await
    }
}

pub type StreamClientPoolRef = Arc<StreamClientPool>;
