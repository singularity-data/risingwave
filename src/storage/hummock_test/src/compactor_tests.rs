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

#[cfg(test)]
mod tests {

    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use bytes::Bytes;
    use itertools::Itertools;
    use parking_lot::RwLock;
    use rand::Rng;
    use risingwave_common::catalog::TableId;
    use risingwave_common::config::constant::hummock::CompactionFilterFlag;
    use risingwave_common::config::StorageConfig;
    use risingwave_common::util::epoch::Epoch;
    use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::get_table_id;
    use risingwave_meta::hummock::compaction::ManualCompactionOption;
    use risingwave_meta::hummock::test_utils::{
        register_table_ids_to_compaction_group, setup_compute_env,
        unregister_table_ids_from_compaction_group,
    };
    use risingwave_meta::hummock::MockHummockMetaClient;
    use risingwave_pb::hummock::{HummockVersion, TableOption};
    use risingwave_rpc_client::HummockMetaClient;
    use risingwave_storage::hummock::compaction_group_client::DummyCompactionGroupClient;
    use risingwave_storage::hummock::compactor::{Compactor, CompactorContext};
    use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
    use risingwave_storage::hummock::{HummockStorage, MemoryLimiter};
    use risingwave_storage::monitor::{StateStoreMetrics, StoreLocalStatistic};
    use risingwave_storage::storage_value::StorageValue;
    use risingwave_storage::store::{ReadOptions, WriteOptions};
    use risingwave_storage::{Keyspace, StateStore};

    async fn get_hummock_storage(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> HummockStorage {
        let remote_dir = "hummock_001_test".to_string();
        let options = Arc::new(StorageConfig {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store();

        HummockStorage::with_default_stats(
            options.clone(),
            sstable_store,
            hummock_meta_client.clone(),
            Arc::new(StateStoreMetrics::unused()),
            Arc::new(DummyCompactionGroupClient::new(
                StaticCompactionGroupId::StateDefault.into(),
            )),
        )
        .await
        .unwrap()
    }

    async fn prepare_test_put_data(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        key: &Bytes,
        value_size: usize,
        epochs: Vec<u64>,
    ) {
        // 1. add sstables
        let val = b"0"[..].repeat(value_size);
        for epoch in epochs {
            let mut new_val = val.clone();
            new_val.extend_from_slice(&epoch.to_be_bytes());
            storage
                .ingest_batch(
                    vec![(
                        key.clone(),
                        StorageValue::new_default_put(Bytes::from(new_val)),
                    )],
                    WriteOptions {
                        epoch,
                        table_id: Default::default(),
                    },
                )
                .await
                .unwrap();
            storage.sync(Some(epoch)).await.unwrap();
            hummock_meta_client
                .commit_epoch(
                    epoch,
                    storage.local_version_manager().get_uncommitted_ssts(epoch),
                )
                .await
                .unwrap();
        }
    }

    fn get_compactor_context(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
    ) -> CompactorContext {
        CompactorContext {
            options: storage.options().clone(),
            sstable_store: storage.sstable_store(),
            hummock_meta_client: hummock_meta_client.clone(),
            stats: Arc::new(StateStoreMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: None,
            table_id_to_slice_transform: Arc::new(RwLock::new(HashMap::new())),
            memory_limiter: Arc::new(MemoryLimiter::new(1024 * 1024 * 128)),
        }
    }

    #[tokio::test]
    async fn test_compaction_watermark() {
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let storage = get_hummock_storage(hummock_meta_client.clone()).await;
        let compact_ctx = get_compactor_context(&storage, &hummock_meta_client);

        // 1. add sstables
        let mut key = b"t".to_vec();
        key.extend_from_slice(&1u32.to_be_bytes());
        key.extend_from_slice(&0u64.to_be_bytes());
        let key = Bytes::from(key);

        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 10,
            (1..129).into_iter().map(|v| (v * 1000) << 16).collect_vec(),
        )
        .await;

        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::TTL;
        compact_task.watermark = (32 * 1000) << 16;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        compact_task.table_options = HashMap::from([(1, TableOption { ttl: 64 })]);
        compact_task.current_epoch_time = 0;
        let mut val = b"0"[..].repeat(1 << 10);
        val.extend_from_slice(&compact_task.watermark.to_be_bytes());

        hummock_manager_ref
            .assign_compaction_task(&compact_task, worker_node.id, async { true })
            .await
            .unwrap();

        // assert compact_task
        assert_eq!(compact_task.input_ssts.len(), 128);

        // 3. compact
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone()).await;

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_table_id = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .last()
            .unwrap()
            .table_infos
            .first()
            .unwrap()
            .id;
        storage
            .local_version_manager()
            .try_update_pinned_version(None, (false, vec![], Some(version)));
        let table = storage
            .sstable_store()
            .sstable(output_table_id, &mut StoreLocalStatistic::default())
            .await
            .unwrap();

        // we have removed these 31 keys before watermark 32.
        assert_eq!(table.value().meta.key_count, 97);

        let get_val = storage
            .get(
                &key,
                ReadOptions {
                    epoch: (32 * 1000) << 16,
                    table_id: Default::default(),
                    ttl: None,
                },
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec();

        assert_eq!(get_val, val);
        let ret = storage
            .get(
                &key,
                ReadOptions {
                    epoch: (31 * 1000) << 16,
                    table_id: Default::default(),
                    ttl: None,
                },
            )
            .await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    async fn test_compaction_same_key_not_split() {
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let storage = get_hummock_storage(hummock_meta_client.clone()).await;
        let compact_ctx = get_compactor_context(&storage, &hummock_meta_client);

        // 1. add sstables with 1MB value
        let key = Bytes::from(&b"same_key"[..]);
        let mut val = b"0"[..].repeat(1 << 20);
        val.extend_from_slice(&128u64.to_be_bytes());
        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 20,
            (1..129).into_iter().collect_vec(),
        )
        .await;

        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        compact_task.current_epoch_time = 0;

        hummock_manager_ref
            .assign_compaction_task(&compact_task, worker_node.id, async { true })
            .await
            .unwrap();

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            128
        );
        compact_task.target_level = 6;

        // 3. compact
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone()).await;

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_table_id = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap()
            .table_infos
            .first()
            .unwrap()
            .id;
        let table = storage
            .sstable_store()
            .sstable(output_table_id, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let target_table_size = storage.options().sstable_size_mb * (1 << 20);

        assert!(
            table.value().meta.estimated_size > target_table_size,
            "table.meta.estimated_size {} <= target_table_size {}",
            table.value().meta.estimated_size,
            target_table_size
        );

        // 5. storage get back the correct kv after compaction
        storage
            .local_version_manager()
            .try_update_pinned_version(None, (false, vec![], Some(version)));
        let get_val = storage
            .get(
                &key,
                ReadOptions {
                    epoch: 129,
                    table_id: Default::default(),
                    ttl: None,
                },
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec();
        assert_eq!(get_val, val);

        // 6. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap();

        assert!(compact_task.is_none());
    }

    #[tokio::test]
    async fn test_compaction_drop_all_key() {
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let storage = get_hummock_storage(hummock_meta_client.clone()).await;
        let compact_ctx = CompactorContext {
            options: storage.options().clone(),
            sstable_store: storage.sstable_store(),
            hummock_meta_client: hummock_meta_client.clone(),
            stats: Arc::new(StateStoreMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: None,
            table_id_to_slice_transform: Arc::new(RwLock::new(HashMap::new())),
            memory_limiter: Arc::new(MemoryLimiter::new(1024 * 1024 * 128)),
        };

        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value

        let existing_table_id: u32 = 1;
        let keyspace = Keyspace::table_root(storage.clone(), &TableId::new(existing_table_id));
        // Only registered table_ids are accepted in commit_epoch
        register_table_ids_to_compaction_group(
            hummock_manager_ref.compaction_group_manager_ref_for_test(),
            &[existing_table_id],
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;

        let kv_count = 128;
        let mut epoch: u64 = 1;
        for _ in 0..kv_count {
            epoch += 1;
            let mut write_batch = keyspace.state_store().start_write_batch(WriteOptions {
                epoch,
                table_id: existing_table_id.into(),
            });
            let mut local = write_batch.prefixify(&keyspace);

            let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
            local.put(ramdom_key, StorageValue::new_default_put(val.clone()));
            write_batch.ingest().await.unwrap();

            storage.sync(Some(epoch)).await.unwrap();
            let ssts = storage.local_version_manager().get_uncommitted_ssts(epoch);
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(
            hummock_manager_ref.compaction_group_manager_ref_for_test(),
            &[existing_table_id],
        )
        .await;

        // 2. get compact task
        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            128
        );

        // 3. compact
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone()).await;

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_level_info = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap();
        assert_eq!(0, output_level_info.total_file_size);

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap();

        assert!(compact_task.is_none());
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_existing_table_id() {
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let storage = get_hummock_storage(hummock_meta_client.clone()).await;
        let compact_ctx = CompactorContext {
            options: storage.options().clone(),
            sstable_store: storage.sstable_store(),
            hummock_meta_client: hummock_meta_client.clone(),
            stats: Arc::new(StateStoreMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: None,
            table_id_to_slice_transform: Arc::new(RwLock::new(HashMap::new())),
            memory_limiter: Arc::new(MemoryLimiter::new(1024 * 1024 * 128)),
        };

        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value

        let drop_table_id = 1;
        let existing_table_ids = 2;
        let kv_count: usize = 128;
        let mut epoch: u64 = 1;
        for index in 0..kv_count {
            let table_id = if index % 2 == 0 {
                drop_table_id
            } else {
                existing_table_ids
            };
            let keyspace = Keyspace::table_root(storage.clone(), &TableId::new(table_id));
            register_table_ids_to_compaction_group(
                hummock_manager_ref.compaction_group_manager_ref_for_test(),
                &[table_id],
                StaticCompactionGroupId::StateDefault.into(),
            )
            .await;
            epoch += 1;
            let mut write_batch = keyspace.state_store().start_write_batch(WriteOptions {
                epoch,
                table_id: TableId::from(table_id),
            });
            let mut local = write_batch.prefixify(&keyspace);

            let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
            local.put(ramdom_key, StorageValue::new_default_put(val.clone()));
            write_batch.ingest().await.unwrap();

            storage.sync(Some(epoch)).await.unwrap();
            hummock_meta_client
                .commit_epoch(
                    epoch,
                    storage.local_version_manager().get_uncommitted_ssts(epoch),
                )
                .await
                .unwrap();
        }

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(
            hummock_manager_ref.compaction_group_manager_ref_for_test(),
            &[drop_table_id],
        )
        .await;

        // 2. get compact task
        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();
        compact_task.existing_table_ids.push(2);
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();

        hummock_manager_ref
            .assign_compaction_task(&compact_task, worker_node.id, async { true })
            .await
            .unwrap();

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .filter(|level| level.level_idx != compact_task.target_level)
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count
        );

        // 3. compact
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone()).await;

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut table_ids_from_version = vec![];
        version.level_iter(StaticCompactionGroupId::StateDefault.into(), |level| {
            table_ids_from_version.extend(level.table_infos.iter().map(|table| table.id));
            true
        });

        let mut key_count = 0;
        for table_id in table_ids_from_version {
            key_count += storage
                .sstable_store()
                .sstable(table_id, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        assert_eq!((kv_count / 2) as u32, key_count);

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        storage
            .local_version_manager()
            .try_update_pinned_version(None, (false, vec![], Some(version)));

        // 6. scan kv to check key table_id
        let scan_result = storage
            .scan::<_, Vec<u8>>(
                ..,
                None,
                ReadOptions {
                    epoch,
                    table_id: Default::default(),
                    ttl: None,
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = get_table_id(&k).unwrap();
            assert_eq!(table_id, existing_table_ids);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_ttl() {
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let storage = get_hummock_storage(hummock_meta_client.clone()).await;
        let compact_ctx = CompactorContext {
            options: storage.options().clone(),
            sstable_store: storage.sstable_store(),
            hummock_meta_client: hummock_meta_client.clone(),
            stats: Arc::new(StateStoreMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: None,
            table_id_to_slice_transform: Arc::new(RwLock::new(HashMap::new())),
            memory_limiter: Arc::new(MemoryLimiter::new(1024 * 1024 * 128)),
        };

        // 1. add sstables
        let val = Bytes::from(b"0"[..].to_vec()); // 1 Byte value

        let existing_table_id = 2;
        let kv_count = 11;
        // let base_epoch = Epoch(0);
        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;
        let keyspace = Keyspace::table_root(storage.clone(), &TableId::new(existing_table_id));
        register_table_ids_to_compaction_group(
            hummock_manager_ref.compaction_group_manager_ref_for_test(),
            &[existing_table_id],
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        let mut epoch_set = BTreeSet::new();
        for _ in 0..kv_count {
            epoch += millisec_interval_epoch;
            epoch_set.insert(epoch);
            let mut write_batch = keyspace.state_store().start_write_batch(WriteOptions {
                epoch,
                table_id: TableId::from(existing_table_id),
            });
            let mut local = write_batch.prefixify(&keyspace);

            let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
            local.put(ramdom_key, StorageValue::new_default_put(val.clone()));
            write_batch.ingest().await.unwrap();
        }

        storage.sync(None).await.unwrap();
        for epoch in &epoch_set {
            hummock_meta_client
                .commit_epoch(
                    *epoch,
                    storage.local_version_manager().get_uncommitted_ssts(*epoch),
                )
                .await
                .unwrap();
        }

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        compact_task.existing_table_ids.push(existing_table_id);
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        let ttl_expire_second = 1;
        compact_task.table_options = HashMap::from_iter([(
            existing_table_id,
            TableOption {
                ttl: ttl_expire_second,
            },
        )]);
        compact_task.current_epoch_time = epoch;

        hummock_manager_ref
            .assign_compaction_task(&compact_task, worker_node.id, async { true })
            .await
            .unwrap();

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count,
        );

        // 3. compact
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone()).await;

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut table_ids_from_version = vec![];
        version.level_iter(StaticCompactionGroupId::StateDefault.into(), |level| {
            table_ids_from_version.extend(level.table_infos.iter().map(|table| table.id));
            true
        });

        let mut key_count = 0;
        for table_id in table_ids_from_version {
            key_count += storage
                .sstable_store()
                .sstable(table_id, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        let expect_count = kv_count as u32 - ttl_expire_second;
        assert_eq!(expect_count, key_count); // ttl will clean the key (which epoch < epoch - ttl)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        storage
            .local_version_manager()
            .try_update_pinned_version(None, (false, vec![], Some(version)));

        // 6. scan kv to check key table_id
        let scan_result = storage
            .scan::<_, Vec<u8>>(
                ..,
                None,
                ReadOptions {
                    epoch,
                    table_id: Default::default(),
                    ttl: None,
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = get_table_id(&k).unwrap();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }
}
