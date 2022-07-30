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

use std::iter;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_pb::hummock::VacuumTask;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::{
    default_builder_opt_for_test, gen_default_test_sstable,
};
use risingwave_storage::hummock::vacuum::Vacuum;

#[tokio::test]
async fn test_vacuum_tracked_data() {
    let sstable_store = mock_sstable_store().await;
    // Put some SSTs to object store
    let sst_ids = (1..10).collect_vec();
    let mut sstables = vec![];
    for sstable_id in &sst_ids {
        let sstable = gen_default_test_sstable(
            default_builder_opt_for_test(),
            *sstable_id,
            sstable_store.clone(),
        )
        .await;
        sstables.push(sstable);
    }

    // Delete all existent SSTs and a nonexistent SSTs. Trying to delete a nonexistent SST is
    // OK.
    let nonexistent_id = 11u64;
    let vacuum_task = VacuumTask {
        sstable_ids: sst_ids
            .into_iter()
            .chain(iter::once(nonexistent_id))
            .collect_vec(),
    };
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    Vacuum::vacuum(sstable_store, vacuum_task, mock_hummock_meta_client)
        .await
        .unwrap();
}
