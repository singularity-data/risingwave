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

use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;

use risingwave_hummock_sdk::HummockSstableId;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::Mutex;

use crate::hummock::{HummockError, HummockResult};

pub type SstableIdManagerRef = Arc<SstableIdManager>;

/// 1. Caches SST ids fetched from meta.
/// 2. TODO: Maintains watermark SST ids. It will be used by SST full GC.
pub struct SstableIdManager {
    unused_sst_ids: Mutex<VecDeque<HummockSstableId>>,
    remote_fetch_number: u32,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl SstableIdManager {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>, remote_fetch_number: u32) -> Self {
        Self {
            unused_sst_ids: Default::default(),
            remote_fetch_number,
            hummock_meta_client,
        }
    }

    pub async fn get_next_sst_id(&self) -> HummockResult<HummockSstableId> {
        let mut guard = self.unused_sst_ids.lock().await;
        let unused_sst_ids = guard.deref_mut();
        // TODO: fetch in advance
        if unused_sst_ids.is_empty() {
            let new_sst_ids = self
                .hummock_meta_client
                .get_new_sst_ids(self.remote_fetch_number)
                .await
                .map_err(HummockError::meta_error)?;
            unused_sst_ids.append(&mut new_sst_ids.into());
        }
        unused_sst_ids
            .pop_front()
            .ok_or_else(|| HummockError::meta_error("get_new_sst_ids returns empty result"))
    }
}
