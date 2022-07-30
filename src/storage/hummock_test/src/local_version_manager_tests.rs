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

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_pb::hummock::HummockVersion;
use risingwave_storage::hummock::conflict_detector::ConflictDetector;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use risingwave_storage::hummock::shared_buffer::UncommittedData;
use risingwave_storage::hummock::shared_buffer::UploadTaskType::SyncEpoch;
use risingwave_storage::hummock::test_utils::{
    default_config_for_test, gen_dummy_batch, gen_dummy_sst_info,
};
use risingwave_storage::storage_value::StorageValue;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_update_pinned_version() {
    let opt = Arc::new(default_config_for_test());
    let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
    let local_version_manager = LocalVersionManager::for_test(
        opt.clone(),
        mock_sstable_store().await,
        Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        )),
        ConflictDetector::new_from_config(opt),
    )
    .await;

    let pinned_version = local_version_manager.get_pinned_version();
    let initial_version_id = pinned_version.id();
    let initial_max_commit_epoch = pinned_version.max_committed_epoch();

    let epochs: Vec<u64> = vec![
        initial_max_commit_epoch + 1,
        initial_max_commit_epoch + 2,
        initial_max_commit_epoch + 3,
        initial_max_commit_epoch + 4,
    ];
    let batches: Vec<Vec<(Bytes, StorageValue)>> =
        epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

    // Fill shared buffer with a dummy empty batch in epochs[0] and epochs[1]
    for i in 0..2 {
        local_version_manager
            .write_shared_buffer(
                epochs[i],
                StaticCompactionGroupId::StateDefault.into(),
                batches[i].clone(),
                false,
                Default::default(),
            )
            .await
            .unwrap();
        let local_version = local_version_manager.get_local_version();
        assert_eq!(
            local_version
                .get_shared_buffer(epochs[i])
                .unwrap()
                .read()
                .size(),
            SharedBufferBatch::measure_batch_size(
                &LocalVersionManager::build_shared_buffer_item_batches(
                    batches[i].clone(),
                    epochs[i]
                )
            )
        );
    }

    // Update version for epochs[0]
    let version = HummockVersion {
        id: initial_version_id + 1,
        max_committed_epoch: epochs[0],
        ..Default::default()
    };
    local_version_manager.try_update_pinned_version(None, (false, vec![], Some(version)));
    let local_version = local_version_manager.get_local_version();
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert_eq!(
        local_version
            .get_shared_buffer(epochs[1])
            .unwrap()
            .read()
            .size(),
        SharedBufferBatch::measure_batch_size(
            &LocalVersionManager::build_shared_buffer_item_batches(batches[1].clone(), epochs[1])
        )
    );

    // Update version for epochs[1]
    let version = HummockVersion {
        id: initial_version_id + 2,
        max_committed_epoch: epochs[1],
        ..Default::default()
    };
    local_version_manager.try_update_pinned_version(None, (false, vec![], Some(version)));
    let local_version = local_version_manager.get_local_version();
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());
}

#[tokio::test]
async fn test_update_uncommitted_ssts() {
    let opt = Arc::new(default_config_for_test());
    let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
    let local_version_manager = LocalVersionManager::for_test(
        opt.clone(),
        mock_sstable_store().await,
        Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        )),
        ConflictDetector::new_from_config(opt),
    )
    .await;

    let pinned_version = local_version_manager.get_pinned_version();
    let max_commit_epoch = pinned_version.max_committed_epoch();
    let initial_id = pinned_version.id();
    let version = pinned_version.version();

    let epochs: Vec<u64> = vec![max_commit_epoch + 1, max_commit_epoch + 2];
    let kvs: Vec<Vec<(Bytes, StorageValue)>> = epochs.iter().map(|e| gen_dummy_batch(*e)).collect();
    let mut batches = Vec::with_capacity(kvs.len());

    // Fill shared buffer with dummy batches
    for i in 0..2 {
        local_version_manager
            .write_shared_buffer(
                epochs[i],
                StaticCompactionGroupId::StateDefault.into(),
                kvs[i].clone(),
                false,
                Default::default(),
            )
            .await
            .unwrap();
        let local_version = local_version_manager.get_local_version();
        let batch = SharedBufferBatch::new(
            LocalVersionManager::build_shared_buffer_item_batches(kvs[i].clone(), epochs[i]),
            epochs[i],
            mpsc::unbounded_channel().0,
            StaticCompactionGroupId::StateDefault.into(),
            Default::default(),
        );
        assert_eq!(
            local_version
                .get_shared_buffer(epochs[i])
                .unwrap()
                .read()
                .size(),
            batch.size(),
        );
        batches.push(batch);
    }

    // Update uncommitted sst for epochs[0]
    let sst1 = gen_dummy_sst_info(1, vec![batches[0].clone()]);
    {
        let local_version_guard = local_version_manager.local_version().read();
        let mut shared_buffer_guard = local_version_guard
            .get_shared_buffer(epochs[0])
            .unwrap()
            .write();
        let (task_id, payload, task_size) = shared_buffer_guard.new_upload_task(SyncEpoch).unwrap();
        {
            assert_eq!(1, payload.len());
            assert_eq!(1, payload[0].len());
            assert_eq!(payload[0][0], UncommittedData::Batch(batches[0].clone()));
            assert_eq!(task_size, batches[0].size());
        }
        shared_buffer_guard.succeed_upload_task(
            task_id,
            vec![(StaticCompactionGroupId::StateDefault.into(), sst1.clone())],
        );
    }

    let local_version = local_version_manager.get_local_version();
    // Check shared buffer
    assert_eq!(
        local_version
            .get_shared_buffer(epochs[0])
            .unwrap()
            .read()
            .size(),
        0
    );
    assert_eq!(
        local_version
            .get_shared_buffer(epochs[1])
            .unwrap()
            .read()
            .size(),
        batches[1].size(),
    );

    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);
    // Check uncommitted ssts
    assert_eq!(local_version.iter_shared_buffer().count(), 2);
    let epoch_uncommitted_ssts = local_version
        .get_shared_buffer(epochs[0])
        .unwrap()
        .read()
        .get_ssts_to_commit()
        .into_iter()
        .map(|(_, sst)| sst)
        .collect_vec();
    assert_eq!(epoch_uncommitted_ssts.len(), 1);
    assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst1);

    // Update uncommitted sst for epochs[1]
    let sst2 = gen_dummy_sst_info(2, vec![batches[1].clone()]);
    {
        let local_version_guard = local_version_manager.local_version().read();
        let mut shared_buffer_guard = local_version_guard
            .get_shared_buffer(epochs[1])
            .unwrap()
            .write();
        let (task_id, payload, task_size) = shared_buffer_guard.new_upload_task(SyncEpoch).unwrap();
        {
            assert_eq!(1, payload.len());
            assert_eq!(1, payload[0].len());
            assert_eq!(payload[0][0], UncommittedData::Batch(batches[1].clone()));
            assert_eq!(task_size, batches[1].size());
        }
        shared_buffer_guard.succeed_upload_task(
            task_id,
            vec![(StaticCompactionGroupId::StateDefault.into(), sst2.clone())],
        );
    }
    let local_version = local_version_manager.get_local_version();
    // Check shared buffer
    for epoch in &epochs {
        assert_eq!(
            local_version
                .get_shared_buffer(*epoch)
                .unwrap()
                .read()
                .size(),
            0
        );
    }
    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);
    // Check uncommitted ssts
    let epoch_uncommitted_ssts = local_version
        .get_shared_buffer(epochs[1])
        .unwrap()
        .read()
        .get_ssts_to_commit()
        .into_iter()
        .map(|(_, sst)| sst)
        .collect_vec();
    assert_eq!(epoch_uncommitted_ssts.len(), 1);
    assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst2);

    // Update version for epochs[0]
    let version = HummockVersion {
        id: initial_id + 1,
        max_committed_epoch: epochs[0],
        ..Default::default()
    };
    assert!(local_version_manager
        .try_update_pinned_version(None, (false, vec![], Some(version.clone()))));
    let local_version = local_version_manager.get_local_version();
    // Check shared buffer
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert_eq!(
        local_version
            .get_shared_buffer(epochs[1])
            .unwrap()
            .read()
            .size(),
        0
    );
    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);
    // Check uncommitted ssts
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    let epoch_uncommitted_ssts = local_version
        .get_shared_buffer(epochs[1])
        .unwrap()
        .read()
        .get_ssts_to_commit()
        .into_iter()
        .map(|(_, sst)| sst)
        .collect_vec();
    assert_eq!(epoch_uncommitted_ssts.len(), 1);
    assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst2);

    // Update version for epochs[1]
    let version = HummockVersion {
        id: initial_id + 2,
        max_committed_epoch: epochs[1],
        ..Default::default()
    };
    local_version_manager.try_update_pinned_version(None, (false, vec![], Some(version.clone())));
    let local_version = local_version_manager.get_local_version();
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());
    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);
    // Check uncommitted ssts
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());
}

#[tokio::test]
async fn test_clear_shared_buffer() {
    let opt = Arc::new(default_config_for_test());
    let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
    let local_version_manager = LocalVersionManager::for_test(
        opt.clone(),
        mock_sstable_store().await,
        Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        )),
        ConflictDetector::new_from_config(opt),
    )
    .await;

    let pinned_version = local_version_manager.get_pinned_version();
    let initial_max_commit_epoch = pinned_version.max_committed_epoch();

    let epochs: Vec<u64> = vec![initial_max_commit_epoch + 1, initial_max_commit_epoch + 2];
    let batches: Vec<Vec<(Bytes, StorageValue)>> =
        epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

    // Fill shared buffer with a dummy empty batch in epochs[0] and epochs[1]
    for i in 0..2 {
        local_version_manager
            .write_shared_buffer(
                epochs[i],
                StaticCompactionGroupId::StateDefault.into(),
                batches[i].clone(),
                false,
                Default::default(),
            )
            .await
            .unwrap();
        let local_version = local_version_manager.get_local_version();
        assert_eq!(
            local_version
                .get_shared_buffer(epochs[i])
                .unwrap()
                .read()
                .size(),
            SharedBufferBatch::measure_batch_size(
                &LocalVersionManager::build_shared_buffer_item_batches(
                    batches[i].clone(),
                    epochs[i]
                )
            )
        );
    }

    // Clear shared buffer and check
    local_version_manager.clear_shared_buffer().await;
    let local_version = local_version_manager.get_local_version();
    assert_eq!(local_version.iter_shared_buffer().count(), 0)
}
