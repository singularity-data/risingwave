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

use std::cmp::Ordering;

use itertools::Itertools;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
// use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{
    HummockContextId, HummockEpoch, HummockSstableId, HummockVersionId, FIRST_VERSION_ID,
};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, KeyRange,
};

use crate::hummock::compaction::ManualCompactionOption;
use crate::hummock::error::Error;
use crate::hummock::test_utils::*;
use crate::hummock::HummockManagerRef;
use crate::model::MetadataModel;
use crate::storage::MemStore;

fn pin_versions_sum(pin_versions: &[HummockPinnedVersion]) -> usize {
    pin_versions.iter().len()
}

fn pin_snapshots_epoch(pin_snapshots: &[HummockPinnedSnapshot]) -> Vec<u64> {
    pin_snapshots
        .iter()
        .map(|p| p.minimal_pinned_snapshot)
        .collect_vec()
}

#[tokio::test]
async fn test_hummock_pin_unpin() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let version_id = FIRST_VERSION_ID;
    let epoch = INVALID_EPOCH;

    assert!(HummockPinnedVersion::list(env.meta_store())
        .await
        .unwrap()
        .is_empty());
    for _ in 0..2 {
        let hummock_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap()
            .2
            .unwrap();
        let levels = hummock_version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into());
        assert_eq!(version_id, hummock_version.id);
        assert_eq!(6, levels.levels.len());
        assert_eq!(0, levels.levels[0].table_infos.len());

        let pinned_versions = HummockPinnedVersion::list(env.meta_store()).await.unwrap();
        assert_eq!(pin_versions_sum(&pinned_versions), 1);
        assert_eq!(pinned_versions[0].context_id, context_id);
    }

    // unpin one context will delete the whole version info of the context
    for _ in 0..3 {
        hummock_manager.unpin_version(context_id).await.unwrap();
    }

    assert!(HummockPinnedSnapshot::list(env.meta_store())
        .await
        .unwrap()
        .is_empty());
    for _ in 0..2 {
        let pin_result = hummock_manager.pin_snapshot(context_id).await.unwrap();
        assert_eq!(pin_result.epoch, epoch);
        let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
        assert_eq!(pin_snapshots_epoch(&pinned_snapshots), vec![epoch]);
        assert_eq!(pinned_snapshots[0].context_id, context_id);
    }
    // unpin nonexistent target will not return error
    hummock_manager.unpin_snapshot(context_id).await.unwrap();
    assert!(HummockPinnedSnapshot::list(env.meta_store())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_unpin_snapshot_before() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = 0;

    for _ in 0..2 {
        let pin_result = hummock_manager.pin_snapshot(context_id).await.unwrap();
        assert_eq!(pin_result.epoch, epoch);
        let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
        assert_eq!(pinned_snapshots[0].context_id, context_id);
        assert_eq!(
            pinned_snapshots[0].minimal_pinned_snapshot,
            pin_result.epoch
        );
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(context_id, HummockSnapshot { epoch })
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            vec![epoch]
        );
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(context_id, HummockSnapshot { epoch: epoch + 1 })
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            vec![epoch]
        );
    }
}

#[tokio::test]
async fn test_hummock_compaction_task() {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2usize;

    // Construct vnode mappings for generating compaction tasks.
    let parallel_units = cluster_manager.list_parallel_units().await;
    env.hash_mapping_manager()
        .build_fragment_hash_mapping(1, &parallel_units);
    for table_id in 1..sst_num + 2 {
        env.hash_mapping_manager()
            .set_fragment_state_table(1, table_id as u32);
    }

    // No compaction task available.
    let task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap();
    assert_eq!(task, None);

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
        .await
        .unwrap();

    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, context_id, async { true })
        .await
        .unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 2);

    // Cancel the task and succeed.
    compact_task.task_status = false;
    assert!(hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());
    // Cancel the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());

    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, context_id, async { true })
        .await
        .unwrap();
    assert_eq!(compact_task.get_task_id(), 3);
    // Finish the task and succeed.
    compact_task.task_status = true;

    assert!(hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());
    // Finish the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());
}

#[tokio::test]
async fn test_hummock_table() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
        .await
        .unwrap();

    let pinned_version = hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap()
        .2
        .unwrap();
    let levels =
        pinned_version.get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into());
    assert_eq!(
        Ordering::Equal,
        levels
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .chain(levels.levels.iter())
            .flat_map(|level| level.table_infos.iter())
            .map(|info| info.id)
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.id).sorted())
    );

    // Confirm tables got are equal to original tables
    assert_eq!(
        get_sorted_sstable_ids(&original_tables),
        get_sorted_committed_sstable_ids(&pinned_version)
    );
}

#[tokio::test]
async fn test_hummock_transaction() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let mut committed_tables = vec![];

    // Add and commit tables in epoch1.
    // BEFORE:  committed_epochs = []
    // AFTER:   committed_epochs = [epoch1]
    let epoch1: u64 = 1;
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            hummock_manager.compaction_group_manager_ref_for_test(),
            &tables_in_epoch1,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch1. No tables should be returned.
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap()
            .2
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, INVALID_EPOCH);
        assert!(get_sorted_committed_sstable_ids(&pinned_version).is_empty());

        hummock_manager.unpin_version(context_id).await.unwrap();

        // Commit epoch1
        hummock_manager
            .commit_epoch(epoch1, to_local_sstable_info(&tables_in_epoch1))
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap()
            .2
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );

        hummock_manager.unpin_version(context_id).await.unwrap();
    }

    // Add and commit tables in epoch2.
    // BEFORE:  committed_epochs = [epoch1]
    // AFTER:   committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1 + 1;
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            hummock_manager.compaction_group_manager_ref_for_test(),
            &tables_in_epoch2,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap()
            .2
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager.unpin_version(context_id).await.unwrap();

        // Commit epoch2
        hummock_manager
            .commit_epoch(epoch2, to_local_sstable_info(&tables_in_epoch2))
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap()
            .2
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager.unpin_version(context_id).await.unwrap();
    }
}

#[tokio::test]
async fn test_release_context_resource() {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
    let context_id_1 = worker_node.id;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_manager
        .add_worker_node(
            WorkerType::ComputeNode,
            fake_host_address_2,
            fake_parallelism,
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        0
    );
    hummock_manager
        .pin_version(context_id_1, u64::MAX)
        .await
        .unwrap();
    hummock_manager
        .pin_version(context_id_2, u64::MAX)
        .await
        .unwrap();
    hummock_manager.pin_snapshot(context_id_1).await.unwrap();
    hummock_manager.pin_snapshot(context_id_2).await.unwrap();
    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        2
    );
    assert_eq!(
        HummockPinnedSnapshot::list(env.meta_store())
            .await
            .unwrap()
            .len(),
        2
    );
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    let pinned_versions = HummockPinnedVersion::list(env.meta_store()).await.unwrap();
    assert_eq!(pin_versions_sum(&pinned_versions), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
    assert_eq!(pinned_snapshots[0].context_id, context_id_2);
    // it's OK to call again
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    hummock_manager
        .release_contexts(&vec![context_id_2])
        .await
        .unwrap();
    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        0
    );
}

#[tokio::test]
async fn test_context_id_validation() {
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let invalid_context_id = HummockContextId::MAX;
    let context_id = worker_node.id;

    // Invalid context id is rejected.
    let error = hummock_manager
        .pin_version(invalid_context_id, u64::MAX)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidContext(_)));

    // Valid context id is accepted.
    hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap();
    // Pin multiple times is OK.
    hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap();

    // Remove the node from cluster will invalidate context id.
    cluster_manager
        .delete_worker_node(worker_node.host.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_hummock_manager_basic() {
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
    let context_id_1 = worker_node.id;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_manager
        .add_worker_node(
            WorkerType::ComputeNode,
            fake_host_address_2,
            fake_parallelism,
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    // initial version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        FIRST_VERSION_ID
    );

    let mut epoch = 1;
    let commit_one = |epoch: HummockEpoch, hummock_manager: HummockManagerRef<MemStore>| async move {
        let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            hummock_manager.compaction_group_manager_ref_for_test(),
            &original_tables,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        hummock_manager
            .commit_epoch(epoch, to_local_sstable_info(&original_tables))
            .await
            .unwrap();
    };

    commit_one(epoch, hummock_manager.clone()).await;
    epoch += 1;

    // increased version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        FIRST_VERSION_ID + 1
    );

    // min pinned version id if no clients
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
    for _ in 0..2 {
        hummock_manager.unpin_version(context_id_1).await.unwrap();
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            HummockVersionId::MAX
        );

        // should pin latest because u64::MAX
        let version = hummock_manager
            .pin_version(context_id_1, HummockVersionId::MAX)
            .await
            .unwrap()
            .2
            .unwrap();
        assert_eq!(version.id, FIRST_VERSION_ID + 1);
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            FIRST_VERSION_ID + 1
        );
    }

    commit_one(epoch, hummock_manager.clone()).await;
    // epoch += 1;

    for _ in 0..2 {
        // should pin latest because deltas cannot contain INVALID_EPOCH
        let version = hummock_manager
            .pin_version(context_id_2, INVALID_EPOCH)
            .await
            .unwrap()
            .2
            .unwrap();
        assert_eq!(version.id, FIRST_VERSION_ID + 2);
        // pinned by context_id_1
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            FIRST_VERSION_ID + 1
        );
    }

    // ssts_to_delete is always empty because no compaction is ever invoked.
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        1
    );
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (1, 0)
    );

    hummock_manager.unpin_version(context_id_1).await.unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        FIRST_VERSION_ID + 2
    );
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        1
    );
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (1, 0)
    );

    hummock_manager.unpin_version(context_id_2).await.unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );

    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        0
    );
}

#[tokio::test]
async fn test_pin_snapshot_response_lost() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let mut epoch: u64 = 1;
    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ ] -> [ e0 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let mut epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);

    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0:pinned ] -> [ e0:pinned, e1 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Assume the response of the previous rpc is lost.
    // [ e0:pinned, e1 ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);

    // Assume the response of the previous rpc is lost.
    // [ e0, e1:pinned ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);

    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned ] -> [ e0, e1:pinned, e2 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Use correct snapshot id.
    // [ e0, e1:pinned, e2 ] -> [ e0, e1:pinned, e2:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);

    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned, e2:pinned ] -> [ e0, e1:pinned, e2:pinned, e3 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Use u64::MAX as epoch to pin greatest snapshot
    // [ e0, e1:pinned, e2:pinned, e3 ] -> [ e0, e1:pinned, e2:pinned, e3::pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);
}

#[tokio::test]
async fn test_print_compact_task() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );

    let s = compact_task_to_string(&compact_task);
    println!("{:?}", s);
    assert!(s.contains("Compaction task id: 1, target level: 0"));
}

// TODO #4081: reject SSTs based on its timestamp watermark
#[tokio::test]
#[ignore]
async fn test_invalid_sst_id() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    let epoch = 1;
    let ssts = generate_test_tables(epoch, vec![HummockSstableId::MAX]);
    register_sstable_infos_to_compaction_group(
        hummock_manager.compaction_group_manager_ref_for_test(),
        &ssts,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    let error = hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&ssts))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InternalError(_)));
}

#[tokio::test]
async fn test_trigger_manual_compaction() {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2usize;

    // Construct vnode mappings for generating compaction tasks.
    let parallel_units = cluster_manager.list_parallel_units().await;
    env.hash_mapping_manager()
        .build_fragment_hash_mapping(1, &parallel_units);
    for table_id in 1..sst_num + 2 {
        env.hash_mapping_manager()
            .set_fragment_state_table(1, table_id as u32);
    }

    {
        let option = ManualCompactionOption::default();
        // to check no compactor
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;

        assert_eq!(
            "internal error: trigger_manual_compaction No compactor is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager_ref_for_test();
    let receiver = compactor_manager_ref.add_compactor(context_id);
    {
        let option = ManualCompactionOption::default();
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert_eq!("internal error: trigger_manual_compaction No compaction_task is available. compaction_group 2", result.err().unwrap().to_string());
    }

    let _ = add_test_tables(&hummock_manager, context_id).await;
    {
        // to check compactor send task fail
        drop(receiver);
        {
            let option = ManualCompactionOption::default();
            let result = hummock_manager
                .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
                .await;
            assert!(result.is_err());
        }
    }

    compactor_manager_ref.remove_compactor(context_id);
    let _receiver = compactor_manager_ref.add_compactor(context_id);

    {
        let option = ManualCompactionOption {
            level: 6,
            key_range: KeyRange {
                inf: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_ok());
    }

    let task_id: u64 = 4;
    let compact_task = hummock_manager
        .compaction_task_from_assignment_for_test(task_id)
        .await
        .unwrap()
        .compact_task
        .unwrap();
    assert_eq!(task_id, compact_task.task_id);

    {
        let option = ManualCompactionOption::default();
        // all sst pending , test no compaction avail
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_err());
    }
}
