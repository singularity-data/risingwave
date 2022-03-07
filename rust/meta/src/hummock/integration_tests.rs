use std::iter::once;
use std::sync::Arc;

use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_storage::hummock::compactor::{Compactor, SubCompactContext};
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{HummockOptions, HummockStorage, SstableStore};
use risingwave_storage::monitor::DEFAULT_STATE_STORE_STATS;
use risingwave_storage::object::InMemObjectStore;

use crate::cluster::StoredClusterManager;
use crate::hummock::mock_hummock_meta_client::MockHummockMetaClient;
use crate::hummock::HummockManager;
use crate::manager::{MetaSrvEnv, NotificationManager};
use crate::rpc::metrics::MetaMetrics;

async fn get_hummock_meta_client() -> MockHummockMetaClient {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = Arc::new(
        HummockManager::new(env.clone(), Arc::new(MetaMetrics::new()))
            .await
            .unwrap(),
    );
    let notification_manager = Arc::new(NotificationManager::new());
    let cluster_manager =
        StoredClusterManager::new(env, Some(hummock_manager.clone()), notification_manager)
            .await
            .unwrap();
    let fake_host_address = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 80,
    };
    let (worker_node, _) = cluster_manager
        .add_worker_node(fake_host_address.clone(), WorkerType::ComputeNode)
        .await
        .unwrap();
    cluster_manager
        .activate_worker_node(fake_host_address)
        .await
        .unwrap();
    MockHummockMetaClient::new(hummock_manager, worker_node.id)
}

async fn get_hummock_storage() -> HummockStorage {
    let remote_dir = "hummock_001_test".to_string();
    let options = HummockOptions {
        sstable_size: 64,
        block_size: 1 << 10,
        bloom_false_positive: 0.1,
        data_directory: remote_dir.clone(),
        checksum_algo: ChecksumAlg::XxHash64,
    };
    let hummock_meta_client = Arc::new(get_hummock_meta_client().await);
    let obj_client = Arc::new(InMemObjectStore::new());
    let sstable_store = Arc::new(SstableStore::new(obj_client.clone(), remote_dir));
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    HummockStorage::with_default_stats(
        options.clone(),
        sstable_store,
        local_version_manager.clone(),
        hummock_meta_client.clone(),
    )
    .await
    .unwrap()
}

#[tokio::test]
#[ignore]
async fn test_compaction_basic() {
    todo!()
}

#[tokio::test]
async fn test_compaction_same_key_not_split() {
    let storage = get_hummock_storage().await;
    let sub_compact_context = SubCompactContext {
        options: storage.options().clone(),
        local_version_manager: storage.local_version_manager().clone(),
        sstable_store: storage.sstable_store(),
        hummock_meta_client: storage.hummock_meta_client().clone(),
        stats: DEFAULT_STATE_STORE_STATS.clone(),
        is_share_buffer_compact: false,
    };

    // 1. add sstables
    let kv_count = 128;
    let epoch: u64 = 1;
    for _ in 0..kv_count {
        storage
            .write_batch(
                once((b"same_key".to_vec(), HummockValue::Put(b"value".to_vec()))),
                epoch,
            )
            .await
            .unwrap();
        storage
            .shared_buffer_manager()
            .sync(Some(epoch))
            .await
            .unwrap();
    }

    // 2. commit epoch
    storage
        .hummock_meta_client()
        .commit_epoch(epoch)
        .await
        .unwrap();

    // 3. get compact task
    let mut compact_task = storage
        .hummock_meta_client()
        .get_compaction_task()
        .await
        .unwrap()
        .unwrap();

    // assert compact_task
    assert_eq!(
        compact_task
            .input_ssts
            .first()
            .unwrap()
            .level
            .as_ref()
            .unwrap()
            .table_ids
            .len(),
        kv_count
    );

    // 4. compact
    Compactor::run_compact(&sub_compact_context, &mut compact_task)
        .await
        .unwrap();

    let output_table_count = compact_task.sorted_output_ssts.len();
    // should not split into multiple tables
    assert_eq!(output_table_count, 1);

    let table = compact_task.sorted_output_ssts.get(0).unwrap();
    let table = storage
        .local_version_manager()
        .pick_few_tables(&[table.id])
        .await
        .unwrap()
        .first()
        .cloned()
        .unwrap();
    // assert that output table reaches the target size
    let target_table_size = storage.options().sstable_size;
    assert!(table.meta.estimated_size > target_table_size);

    // 5. get compact task
    let compact_task = storage
        .hummock_meta_client()
        .get_compaction_task()
        .await
        .unwrap();

    assert!(compact_task.is_none());
}
