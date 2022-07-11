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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use dyn_clone::DynClone;
use futures::future::{try_join_all, BoxFuture};
use futures::{stream, FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::config::constant::hummock::{CompactionFilterFlag, TABLE_OPTION_DUMMY_TTL};
use risingwave_common::config::StorageConfig;
use risingwave_common::util::compress::decompress_data;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{
    extract_table_id_and_epoch, get_epoch, get_table_id, Epoch, FullKey,
};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{CompactionGroupId, HummockSSTableId, VersionedComparator};
use risingwave_pb::hummock::{CompactTask, SstableInfo, SubscribeCompactTasksResponse, VacuumTask};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::group_builder::{GroupedSstableBuilder, VirtualNodeGrouping};
use super::iterator::{BoxedForwardHummockIterator, ConcatIterator, MergeIterator};
use super::{
    CompressionAlgorithm, HummockResult, SSTableBuilder, SSTableBuilderOptions, SSTableIterator,
    SSTableIteratorType, Sstable,
};
use crate::hummock::compaction_executor::CompactionExecutor;
use crate::hummock::group_builder::KeyValueGrouping;
use crate::hummock::iterator::ReadOptions;
use crate::hummock::multi_builder::{do_upload, SealedSstableBuilder, UploadRequest};
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::shared_buffer::{build_ordered_merge_iter, UncommittedData};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::state_store::ForwardIter;
use crate::hummock::utils::can_concat;
use crate::hummock::vacuum::Vacuum;
use crate::hummock::{CachePolicy, HummockError, SstableStore};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

pub type SstableIdGenerator =
    Arc<dyn Fn() -> BoxFuture<'static, HummockResult<HummockSSTableId>> + Send + Sync>;

pub fn get_remote_sstable_id_generator(
    meta_client: Arc<dyn HummockMetaClient>,
) -> SstableIdGenerator {
    Arc::new(move || {
        let meta_client = meta_client.clone();
        async move {
            meta_client
                .get_new_table_id()
                .await
                .map_err(HummockError::meta_error)
        }
        .boxed()
    })
}

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct CompactorContext {
    /// Storage configurations.
    pub options: Arc<StorageConfig>,

    /// The meta client.
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,

    /// SSTable store that manages the sstables.
    pub sstable_store: SstableStoreRef,

    /// Statistics.
    pub stats: Arc<StateStoreMetrics>,

    /// True if it is a memory compaction (from shared buffer).
    pub is_share_buffer_compact: bool,

    pub sstable_id_generator: SstableIdGenerator,

    pub compaction_executor: Option<Arc<CompactionExecutor>>,
}

trait CompactionFilter: Send + DynClone {
    fn should_delete(&mut self, _: &[u8]) -> bool {
        false
    }
}

dyn_clone::clone_trait_object!(CompactionFilter);

#[derive(Clone)]
pub struct DummyCompactionFilter;

impl CompactionFilter for DummyCompactionFilter {}

#[derive(Clone)]
pub struct StateCleanUpCompactionFilter {
    existing_table_ids: HashSet<u32>,
    last_table: Option<(u32, bool)>,
}

impl StateCleanUpCompactionFilter {
    fn new(table_id_set: HashSet<u32>) -> Self {
        StateCleanUpCompactionFilter {
            existing_table_ids: table_id_set,
            last_table: None,
        }
    }
}

impl CompactionFilter for StateCleanUpCompactionFilter {
    fn should_delete(&mut self, key: &[u8]) -> bool {
        let table_id_option = get_table_id(key);
        match table_id_option {
            None => false,
            Some(table_id) => {
                if let Some((last_table_id, removed)) = self.last_table.as_ref() {
                    if *last_table_id == table_id {
                        return *removed;
                    }
                }
                let removed = !self.existing_table_ids.contains(&table_id);
                self.last_table = Some((table_id, removed));
                removed
            }
        }
    }
}

#[derive(Clone)]
pub struct TTLCompactionFilter {
    table_id_to_ttl: HashMap<u32, u32>,
    expire_epoch: u64,
    last_table_and_ttl: Option<(u32, u64)>,
}

impl CompactionFilter for TTLCompactionFilter {
    fn should_delete(&mut self, key: &[u8]) -> bool {
        pub use risingwave_common::util::epoch::Epoch;
        let (table_id, epoch) = extract_table_id_and_epoch(key);
        match table_id {
            Some(table_id) => {
                if let Some((last_table_id, ttl_mill)) = self.last_table_and_ttl.as_ref() {
                    if *last_table_id == table_id {
                        let min_epoch = Epoch(self.expire_epoch).subtract_ms(*ttl_mill);
                        return Epoch(epoch) <= min_epoch;
                    }
                }
                match self.table_id_to_ttl.get(&table_id) {
                    Some(ttl_second_u32) => {
                        assert!(*ttl_second_u32 != TABLE_OPTION_DUMMY_TTL);
                        // default to zero.
                        let ttl_mill = (*ttl_second_u32 * 1000) as u64;
                        let min_epoch = Epoch(self.expire_epoch).subtract_ms(ttl_mill);
                        self.last_table_and_ttl = Some((table_id, ttl_mill));
                        Epoch(epoch) <= min_epoch
                    }
                    None => false,
                }
            }
            None => false,
        }
    }
}

impl TTLCompactionFilter {
    fn new(table_id_to_ttl: HashMap<u32, u32>, expire: u64) -> Self {
        Self {
            table_id_to_ttl,
            expire_epoch: expire,
            last_table_and_ttl: None,
        }
    }
}

#[derive(Default, Clone)]
struct MultiCompactionFilter {
    filter_vec: Vec<Box<dyn CompactionFilter>>,
}

impl CompactionFilter for MultiCompactionFilter {
    fn should_delete(&mut self, key: &[u8]) -> bool {
        self.filter_vec
            .iter_mut()
            .any(|filter| filter.should_delete(key))
    }
}

impl MultiCompactionFilter {
    fn register(&mut self, filter: Box<dyn CompactionFilter>) {
        self.filter_vec.push(filter);
    }
}

#[derive(Clone)]
/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: Arc<CompactorContext>,

    /// A compaction task received from the hummock manager.
    /// When it's local compaction from memory, it uses a locally
    /// constructed compaction task.
    compact_task: CompactTask,
}

pub type CompactOutput = (usize, Vec<(Sstable, u64, Vec<u32>)>);

impl Compactor {
    /// Create a new compactor.
    pub fn new(context: Arc<CompactorContext>, compact_task: CompactTask) -> Self {
        Self {
            context,
            compact_task,
        }
    }

    /// Flush shared buffer to level0. Resulted SSTs are grouped by compaction group.
    pub async fn compact_shared_buffer_by_compaction_group(
        context: Arc<CompactorContext>,
        payload: UploadTaskPayload,
    ) -> HummockResult<Vec<(CompactionGroupId, Sstable, u64, Vec<u32>)>> {
        let mut grouped_payload: HashMap<CompactionGroupId, UploadTaskPayload> = HashMap::new();
        for uncommitted_list in payload {
            let mut next_inner = HashSet::new();
            for uncommitted in uncommitted_list {
                let compaction_group_id = match &uncommitted {
                    UncommittedData::Sst((compaction_group_id, _)) => *compaction_group_id,
                    UncommittedData::Batch(batch) => batch.compaction_group_id(),
                };
                let group = grouped_payload
                    .entry(compaction_group_id)
                    .or_insert_with(std::vec::Vec::new);
                if !next_inner.contains(&compaction_group_id) {
                    group.push(vec![]);
                    next_inner.insert(compaction_group_id);
                }
                group.last_mut().unwrap().push(uncommitted);
            }
        }

        let mut futures = vec![];
        for (id, group_payload) in grouped_payload {
            let id_copy = id;
            futures.push(
                Compactor::compact_shared_buffer(context.clone(), group_payload).map_ok(
                    move |results| {
                        results
                            .into_iter()
                            .map(move |result| (id_copy, result.0, result.1, result.2))
                            .collect_vec()
                    },
                ),
            );
        }
        // Note that the output is reordered compared with input `payload`.
        let result = try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .collect_vec();
        Ok(result)
    }

    /// For compaction from shared buffer to level 0, this is the only function gets called.
    pub async fn compact_shared_buffer(
        context: Arc<CompactorContext>,
        payload: UploadTaskPayload,
    ) -> HummockResult<Vec<(Sstable, u64, Vec<u32>)>> {
        let mut start_user_keys = payload
            .iter()
            .flat_map(|data_list| data_list.iter().map(UncommittedData::start_user_key))
            .collect_vec();
        start_user_keys.sort();
        start_user_keys.dedup();
        let mut splits = Vec::with_capacity(start_user_keys.len());
        splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
        let mut key_split_append = |key_before_last: &Bytes| {
            splits.last_mut().unwrap().right = key_before_last.clone();
            splits.push(KeyRange::new(key_before_last.clone(), Bytes::new()));
        };
        if start_user_keys.len() > 1 {
            let split_num = context.options.share_buffers_sync_parallelism as usize;
            let buffer_per_split = start_user_keys.len() / split_num;
            for i in 1..split_num {
                key_split_append(
                    &FullKey::from_user_key_slice(
                        start_user_keys[i * buffer_per_split],
                        Epoch::MAX,
                    )
                    .into_inner()
                    .into(),
                );
            }
        }

        // Local memory compaction looks at all key ranges.
        let compact_task = CompactTask {
            input_ssts: vec![],
            splits: splits.into_iter().map(|v| v.into()).collect_vec(),
            watermark: u64::MAX,
            sorted_output_ssts: vec![],
            task_id: 0,
            target_level: 0,
            gc_delete_keys: false,
            task_status: false,
            // VNode mappings are not required when compacting shared buffer to L0
            vnode_mappings: vec![],
            compaction_group_id: StaticCompactionGroupId::SharedBuffer.into(),
            existing_table_ids: vec![],
            target_file_size: context.options.sstable_size_mb as u64 * (1 << 20),
            compression_algorithm: 0,
            compaction_filter_mask: 0,
            table_options: HashMap::default(),
            current_epoch_time: 0,
        };

        let sstable_store = context.sstable_store.clone();
        let stats = context.stats.clone();

        let parallelism = compact_task.splits.len();
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let compactor = Compactor::new(context, compact_task.clone());

        let vnode2unit: Arc<HashMap<u32, Vec<u32>>> = Arc::new(HashMap::new());

        let mut local_stats = StoreLocalStatistic::default();
        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            let iter = build_ordered_merge_iter::<ForwardIter>(
                &payload,
                sstable_store.clone(),
                stats.clone(),
                &mut local_stats,
                Arc::new(ReadOptions::default()),
            )
            .await? as BoxedForwardHummockIterator;
            let vnode2unit = vnode2unit.clone();
            let compaction_executor = compactor.context.compaction_executor.as_ref().cloned();

            let split_task = async move {
                compactor
                    .compact_key_range(split_index, iter, vnode2unit)
                    .await
            };
            let rx = Compactor::request_execution(compaction_executor, split_task)?;
            compaction_futures.push(rx);
        }
        local_stats.report(stats.as_ref());

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        let mut err = None;
        while let Some(future_result) = buffered.next().await {
            match future_result.unwrap() {
                Ok((split_index, ssts)) => {
                    output_ssts.push((split_index, ssts));
                }
                Err(e) => {
                    compact_success = false;
                    tracing::warn!("Shared Buffer Compaction failed with error: {:#?}", e);
                    err = Some(e);
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        if compact_success {
            let mut level0 = Vec::with_capacity(parallelism);

            for (_, sst) in output_ssts {
                for (table, _, _) in &sst {
                    compactor
                        .context
                        .stats
                        .write_build_l0_bytes
                        .inc_by(table.meta.estimated_size as u64);
                }
                level0.extend(sst);
            }

            Ok(level0)
        } else {
            Err(err.unwrap())
        }
    }

    /// Tries to schedule on `compaction_executor` if `compaction_executor` is not None.
    ///
    /// Tries to schedule on current runtime if `compaction_executor` is None.
    fn request_execution(
        compaction_executor: Option<Arc<CompactionExecutor>>,
        split_task: impl Future<Output = HummockResult<CompactOutput>> + Send + 'static,
    ) -> HummockResult<JoinHandle<HummockResult<CompactOutput>>> {
        match compaction_executor {
            None => Ok(tokio::spawn(split_task)),
            Some(compaction_executor) => {
                let rx = compaction_executor
                    .send_request(split_task)
                    .map_err(HummockError::compaction_executor)?;
                Ok(tokio::spawn(async move {
                    match rx.await {
                        Ok(result) => result,
                        Err(err) => Err(HummockError::compaction_executor(err)),
                    }
                }))
            }
        }
    }

    /// Handle a compaction task and report its status to hummock manager.
    /// Always return `Ok` and let hummock manager handle errors.
    pub async fn compact(context: Arc<CompactorContext>, mut compact_task: CompactTask) -> bool {
        use risingwave_common::catalog::TableOption;
        tracing::info!("Ready to handle compaction task: {}", compact_task.task_id,);
        let group_label = compact_task.compaction_group_id.to_string();
        let cur_level_label = compact_task.input_ssts[0].level_idx.to_string();
        let compaction_read_bytes = compact_task.input_ssts[0]
            .table_infos
            .iter()
            .map(|t| t.file_size)
            .sum::<u64>();
        context
            .stats
            .compact_read_current_level
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc_by(compaction_read_bytes);
        context
            .stats
            .compact_read_sstn_current_level
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc_by(compact_task.input_ssts[0].table_infos.len() as u64);
        context
            .stats
            .compact_frequency
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc();

        if compact_task.input_ssts.len() > 1 {
            let sec_level_read_bytes: u64 = compact_task.input_ssts[1]
                .table_infos
                .iter()
                .map(|t| t.file_size)
                .sum();
            let next_level_label = compact_task.input_ssts[1].level_idx.to_string();
            context
                .stats
                .compact_read_next_level
                .with_label_values(&[group_label.as_str(), next_level_label.as_str()])
                .inc_by(sec_level_read_bytes);
            context
                .stats
                .compact_read_sstn_next_level
                .with_label_values(&[group_label.as_str(), next_level_label.as_str()])
                .inc_by(compact_task.input_ssts[1].table_infos.len() as u64);
        }

        let timer = context
            .stats
            .compact_task_duration
            .with_label_values(&[compact_task.input_ssts[0].level_idx.to_string().as_str()])
            .start_timer();

        // if disable_parallel_compact = true, use one splits to limit the parallelism of
        // compact_task
        if !compact_task.vnode_mappings.is_empty() {
            compact_task.splits = vec![risingwave_pb::hummock::KeyRange {
                left: vec![],
                right: vec![],
                inf: false,
            }];
        };

        // Number of splits (key ranges) is equal to number of compaction tasks
        let parallelism = compact_task.splits.len();
        assert_ne!(parallelism, 0, "splits cannot be empty");
        context.stats.compact_parallelism.inc_by(parallelism as u64);
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let mut compactor = Compactor::new(context, compact_task.clone());

        let vnode2unit: Arc<HashMap<u32, Vec<u32>>> = Arc::new(
            compact_task
                .vnode_mappings
                .into_iter()
                .map(|tar| {
                    (
                        tar.table_id,
                        decompress_data(tar.get_original_indices(), tar.get_data()),
                    )
                })
                .collect(),
        );

        let mut multi_filter = MultiCompactionFilter::default();
        let compaction_filter_flag =
            CompactionFilterFlag::from_bits(compact_task.compaction_filter_mask)
                .unwrap_or_default();
        if compaction_filter_flag.contains(CompactionFilterFlag::STATE_CLEAN) {
            let state_clean_up_filter = Box::new(StateCleanUpCompactionFilter::new(
                HashSet::from_iter(compact_task.existing_table_ids),
            ));

            multi_filter.register(state_clean_up_filter);
        }

        if compaction_filter_flag.contains(CompactionFilterFlag::TTL) {
            let id_to_ttl = compact_task
                .table_options
                .iter()
                .filter(|id_to_option| {
                    let table_option: TableOption = id_to_option.1.into();
                    table_option.ttl.is_some()
                })
                .map(|id_to_option| (*id_to_option.0, id_to_option.1.ttl))
                .collect();
            let ttl_filter = Box::new(TTLCompactionFilter::new(
                id_to_ttl,
                compact_task.current_epoch_time,
            ));
            multi_filter.register(ttl_filter);
        }

        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            let vnode2unit = vnode2unit.clone();
            let compaction_executor = compactor.context.compaction_executor.as_ref().cloned();
            let filter = multi_filter.clone();
            let split_task = async move {
                let merge_iter = compactor.build_sst_iter().await?;
                compactor
                    .compact_key_range_with_filter(split_index, merge_iter, vnode2unit, filter)
                    .await
            };
            let rx = match Compactor::request_execution(compaction_executor, split_task) {
                Ok(rx) => rx,
                Err(err) => {
                    tracing::warn!("Failed to schedule compaction execution: {:#?}", err);
                    return false;
                }
            };
            compaction_futures.push(rx);
        }

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        while let Some(future_result) = buffered.next().await {
            match future_result.unwrap() {
                Ok((split_index, ssts)) => {
                    output_ssts.push((split_index, ssts));
                }
                Err(e) => {
                    compact_success = false;
                    tracing::warn!(
                        "Compaction task {} failed with error: {:#?}",
                        compact_task.task_id,
                        e
                    );
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        // After a compaction is done, mutate the compaction task.
        compactor.compact_done(output_ssts, compact_success).await;
        let cost_time = timer.stop_and_record() * 1000.0;
        tracing::info!(
            "Finished compaction task in {:?}ms: \n{}",
            cost_time,
            compact_task_to_string(&compactor.compact_task)
        );
        compact_success
    }

    /// Fill in the compact task and let hummock manager know the compaction output ssts.
    async fn compact_done(&mut self, output_ssts: Vec<CompactOutput>, task_ok: bool) {
        self.compact_task.task_status = task_ok;
        self.compact_task
            .sorted_output_ssts
            .reserve(self.compact_task.splits.len());
        let mut compaction_write_bytes = 0;
        for (_, ssts) in output_ssts {
            for (sst, unit_id, table_ids) in ssts {
                let sst_info = SstableInfo {
                    id: sst.id,
                    key_range: Some(risingwave_pb::hummock::KeyRange {
                        left: sst.meta.smallest_key.clone(),
                        right: sst.meta.largest_key.clone(),
                        inf: false,
                    }),
                    file_size: sst.meta.estimated_size as u64,
                    table_ids,
                    unit_id,
                };
                compaction_write_bytes += sst_info.file_size;
                self.compact_task.sorted_output_ssts.push(sst_info);
            }
        }

        let group_label = self.compact_task.compaction_group_id.to_string();
        let level_label = self.compact_task.target_level.to_string();
        self.context
            .stats
            .compact_write_bytes
            .with_label_values(&[group_label.as_str(), level_label.as_str()])
            .inc_by(compaction_write_bytes);
        self.context
            .stats
            .compact_write_sstn
            .with_label_values(&[group_label.as_str(), level_label.as_str()])
            .inc_by(self.compact_task.sorted_output_ssts.len() as u64);

        if let Err(e) = self
            .context
            .hummock_meta_client
            .report_compaction_task(self.compact_task.to_owned())
            .await
        {
            tracing::warn!(
                "Failed to report compaction task: {}, error: {}",
                self.compact_task.task_id,
                e
            );
        }
    }

    async fn start_uploading_task_coordinator(
        sstable_store: Arc<SstableStore>,
        mut receiver: mpsc::UnboundedReceiver<UploadRequest>,
    ) {
        while let Some(request) = receiver.recv().await {
            let ret = do_upload(
                sstable_store.clone(),
                request.id,
                request.data,
                request.meta,
                request.policy,
            )
            .await;
            request.grant_sender.send(ret).unwrap();
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    async fn compact_key_range_impl(
        &self,
        split_index: usize,
        iter: BoxedForwardHummockIterator,
        vnode2unit: Arc<HashMap<u32, Vec<u32>>>,
        compaction_filter: impl CompactionFilter,
    ) -> HummockResult<CompactOutput> {
        let split = self.compact_task.splits[split_index].clone();
        let kr = KeyRange {
            left: Bytes::copy_from_slice(split.get_left()),
            right: Bytes::copy_from_slice(split.get_right()),
            inf: split.get_inf(),
        };

        let get_id_time = Arc::new(AtomicU64::new(0));
        let max_target_file_size = self.context.options.sstable_size_mb as usize * (1 << 20);
        let cache_policy = if !self.context.is_share_buffer_compact
            && (self.compact_task.target_file_size as usize) < max_target_file_size
        {
            CachePolicy::Fill
        } else {
            CachePolicy::NotFill
        };
        let target_file_size = std::cmp::min(
            self.compact_task.target_file_size as usize,
            max_target_file_size,
        );

        let (uploading_event_sender, uploading_event_receiver) = mpsc::unbounded_channel();
        tokio::spawn(Compactor::start_uploading_task_coordinator(
            self.context.sstable_store.clone(),
            uploading_event_receiver,
        ));

        // NOTICE: should be user_key overlap, NOT full_key overlap!
        let mut builder = GroupedSstableBuilder::new(
            || async {
                let timer = Instant::now();
                let table_id = (self.context.sstable_id_generator)().await?;
                let cost = (timer.elapsed().as_secs_f64() * 1000000.0).round() as u64;
                let mut options: SSTableBuilderOptions = self.context.options.as_ref().into();

                options.capacity = target_file_size;
                options.compression_algorithm = match self.compact_task.compression_algorithm {
                    0 => CompressionAlgorithm::None,
                    1 => CompressionAlgorithm::Lz4,
                    _ => CompressionAlgorithm::Zstd,
                };
                let builder = SSTableBuilder::new(table_id, options);
                get_id_time.fetch_add(cost, Ordering::Relaxed);
                Ok(builder)
            },
            VirtualNodeGrouping::new(vnode2unit),
            cache_policy,
            self.context.sstable_store.clone(),
            Some(uploading_event_sender),
        );

        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.context.is_share_buffer_compact {
            self.context.stats.write_build_l0_sst_duration.start_timer()
        } else {
            self.context.stats.compact_sst_duration.start_timer()
        };

        Compactor::compact_and_build_sst(
            &mut builder,
            kr,
            iter,
            self.compact_task.gc_delete_keys,
            self.compact_task.watermark,
            compaction_filter,
        )
        .await?;
        let builder_len = builder.len();
        let sealed_builders = builder.finish();
        compact_timer.observe_duration();

        let mut ssts = Vec::with_capacity(builder_len);
        let mut upload_join_handles = vec![];
        for SealedSstableBuilder {
            id: table_id,
            meta,
            table_ids,
            upload_join_handle,
            data_len,
            unit_id,
        } in sealed_builders
        {
            let sst = Sstable::new(table_id, meta);
            let len = data_len;
            ssts.push((sst, unit_id, table_ids));
            upload_join_handles.push(upload_join_handle);

            if self.context.is_share_buffer_compact {
                self.context
                    .stats
                    .shared_buffer_to_sstable_size
                    .observe(len as _);
            } else {
                self.context.stats.compaction_upload_sst_counts.inc();
            }
        }

        // Wait for all upload to finish
        try_join_all(upload_join_handles.into_iter().map(|join_handle| {
            join_handle.map(|result| match result {
                Ok(upload_result) => upload_result,
                Err(e) => Err(HummockError::other(format!(
                    "fail to receive from upload join handle: {:?}",
                    e
                ))),
            })
        }))
        .await?;

        self.context
            .stats
            .get_table_id_total_time_duration
            .observe(get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);
        Ok((split_index, ssts))
    }

    async fn compact_key_range(
        &self,
        split_index: usize,
        iter: BoxedForwardHummockIterator,
        vnode2unit: Arc<HashMap<u32, Vec<u32>>>,
    ) -> HummockResult<CompactOutput> {
        let dummy_compaction_filter = DummyCompactionFilter {};
        self.compact_key_range_impl(split_index, iter, vnode2unit, dummy_compaction_filter)
            .await
    }

    async fn compact_key_range_with_filter(
        &self,
        split_index: usize,
        iter: BoxedForwardHummockIterator,
        vnode2unit: Arc<HashMap<u32, Vec<u32>>>,
        compaction_filter: impl CompactionFilter,
    ) -> HummockResult<CompactOutput> {
        self.compact_key_range_impl(split_index, iter, vnode2unit, compaction_filter)
            .await
    }

    /// Build the merge iterator based on the given input ssts.
    async fn build_sst_iter(&self) -> HummockResult<BoxedForwardHummockIterator> {
        let mut table_iters: Vec<BoxedForwardHummockIterator> = Vec::new();
        let mut stats = StoreLocalStatistic::default();
        let read_options = Arc::new(ReadOptions { prefetch: true });

        // TODO: check memory limit
        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }
            // Do not need to filter the table because manager has done it.
            // let read_statistics: &mut TableSetStatistics = if *level_idx ==
            // compact_task.target_level {
            //     compact_task.metrics.as_mut().unwrap().read_level_nplus1.as_mut().unwrap()
            // } else {
            //     compact_task.metrics.as_mut().unwrap().read_level_n.as_mut().unwrap()
            // };
            // for table in &tables {
            //     read_statistics.size_gb += table.meta.estimated_size as f64 / (1024 * 1024 *
            // 1024) as f64;     read_statistics.cnt += 1;
            // }

            if can_concat(&level.table_infos.iter().collect_vec()) {
                table_iters.push(Box::new(ConcatIterator::new(
                    level.table_infos.clone(),
                    self.context.sstable_store.clone(),
                    read_options.clone(),
                )) as BoxedForwardHummockIterator);
            } else {
                for table_info in &level.table_infos {
                    let table = self
                        .context
                        .sstable_store
                        .load_table(table_info.id, true, &mut stats)
                        .await?;
                    table_iters.push(Box::new(SSTableIterator::create(
                        table,
                        self.context.sstable_store.clone(),
                        read_options.clone(),
                    )));
                }
            }
        }
        stats.report(self.context.stats.as_ref());
        Ok(Box::new(MergeIterator::new(
            table_iters,
            self.context.stats.clone(),
        )))
    }

    pub async fn try_vacuum(
        vacuum_task: Option<VacuumTask>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        if let Some(vacuum_task) = vacuum_task {
            tracing::info!("Try to vacuum SSTs {:?}", vacuum_task.sstable_ids);
            match Vacuum::vacuum(
                sstable_store.clone(),
                vacuum_task,
                hummock_meta_client.clone(),
            )
            .await
            {
                Ok(_) => {
                    tracing::info!("Finish vacuuming SSTs");
                }
                Err(e) => {
                    tracing::warn!("Failed to vacuum SSTs. {:#?}", e);
                }
            }
        }
    }

    /// The background compaction thread that receives compaction tasks from hummock compaction
    /// manager and runs compaction tasks.
    pub fn start_compactor(
        options: Arc<StorageConfig>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        compaction_executor: Option<Arc<CompactionExecutor>>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let compactor_context = Arc::new(CompactorContext {
            options,
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats,
            is_share_buffer_compact: false,
            sstable_id_generator: get_remote_sstable_id_generator(hummock_meta_client.clone()),
            compaction_executor,
        });
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let stream_retry_interval = Duration::from_secs(60);
        let join_handle = tokio::spawn(async move {
            let process_task = |compact_task,
                                vacuum_task,
                                compactor_context,
                                sstable_store,
                                hummock_meta_client| async {
                if let Some(compact_task) = compact_task {
                    Compactor::compact(compactor_context, compact_task).await;
                }

                Compactor::try_vacuum(vacuum_task, sstable_store, hummock_meta_client).await;
            };
            let mut min_interval = tokio::time::interval(stream_retry_interval);
            // This outer loop is to recreate stream.
            'start_stream: loop {
                tokio::select! {
                    // Wait for interval.
                    _ = min_interval.tick() => {},
                    // Shutdown compactor.
                    _ = &mut shutdown_rx => {
                        tracing::info!("Compactor is shutting down");
                        return;
                    }
                }

                let mut stream = match compactor_context
                    .hummock_meta_client
                    .subscribe_compact_tasks()
                    .await
                {
                    Ok(stream) => {
                        tracing::debug!("Succeeded subscribe_compact_tasks.");
                        stream
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Subscribing to compaction tasks failed with error: {}. Will retry.",
                            e
                        );
                        continue 'start_stream;
                    }
                };

                // This inner loop is to consume stream.
                loop {
                    let message = tokio::select! {
                        message = stream.message() => {
                            message
                        },
                        // Shutdown compactor
                        _ = &mut shutdown_rx => {
                            tracing::info!("Compactor is shutting down");
                            return
                        }
                    };
                    match message {
                        // The inner Some is the side effect of generated code.
                        Ok(Some(SubscribeCompactTasksResponse {
                            compact_task,
                            vacuum_task,
                        })) => {
                            tokio::spawn(process_task(
                                compact_task,
                                vacuum_task,
                                compactor_context.clone(),
                                sstable_store.clone(),
                                hummock_meta_client.clone(),
                            ));
                        }
                        Err(e) => {
                            tracing::warn!("Failed to consume stream. {}", e.message());
                            continue 'start_stream;
                        }
                        _ => {
                            // The stream is exhausted
                            continue 'start_stream;
                        }
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    async fn compact_and_build_sst<B, G, F>(
        sst_builder: &mut GroupedSstableBuilder<B, G>,
        kr: KeyRange,
        mut iter: BoxedForwardHummockIterator,
        gc_delete_keys: bool,
        watermark: Epoch,
        mut compaction_filter: impl CompactionFilter,
    ) -> HummockResult<()>
    where
        B: Clone + Fn() -> F,
        G: KeyValueGrouping,
        F: Future<Output = HummockResult<SSTableBuilder>>,
    {
        if !kr.left.is_empty() {
            iter.seek(&kr.left).await?;
        } else {
            iter.rewind().await?;
        }

        let mut last_key = BytesMut::new();
        let mut watermark_can_see_last_key = false;

        while iter.is_valid() {
            let iter_key = iter.key();

            let is_new_user_key =
                last_key.is_empty() || !VersionedComparator::same_user_key(iter_key, &last_key);

            let mut drop = false;
            let epoch = get_epoch(iter_key);
            if is_new_user_key {
                if !kr.right.is_empty()
                    && VersionedComparator::compare_key(iter_key, &kr.right)
                        != std::cmp::Ordering::Less
                {
                    break;
                }

                last_key.clear();
                last_key.extend_from_slice(iter_key);
                watermark_can_see_last_key = false;
            }

            // Among keys with same user key, only retain keys which satisfy `epoch` >= `watermark`.
            // If there is no keys whose epoch is equal than `watermark`, keep the latest key which
            // satisfies `epoch` < `watermark`
            // in our design, frontend avoid to access keys which had be deleted, so we dont
            // need to consider the epoch when the compaction_filter match (it
            // means that mv had drop)
            if (epoch <= watermark && gc_delete_keys && iter.value().is_delete())
                || (epoch < watermark && watermark_can_see_last_key)
            {
                drop = true;
            }

            if !drop && compaction_filter.should_delete(iter_key) {
                drop = true;
            }

            if epoch <= watermark {
                watermark_can_see_last_key = true;
            }

            if drop {
                iter.next().await?;
                continue;
            }

            // Don't allow two SSTs to share same user key
            sst_builder
                .add_full_key(FullKey::from_slice(iter_key), iter.value(), is_new_user_key)
                .await?;

            iter.next().await?;
        }
        Ok(())
    }
}
