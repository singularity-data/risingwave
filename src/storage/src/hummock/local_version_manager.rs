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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use futures::future::{join_all, try_join_all};
use itertools::Itertools;
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::slice_transform::SliceTransformImpl;
use risingwave_hummock_sdk::{CompactionGroupId, LocalSstableInfo};
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_retry::strategy::jitter;
use tracing::{error, info};

use super::local_version::{LocalVersion, PinnedVersion, ReadVersion};
use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::shared_buffer::shared_buffer_uploader::SharedBufferUploader;
use super::SstableStoreRef;
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferItem;
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::shared_buffer::UploadTaskType::{FlushWriteBatch, SyncEpoch};
use crate::hummock::shared_buffer::{
    KeyIndexedUncommittedData, OrderIndex, SharedBufferEvent, WriteRequest,
};
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockVersionId, INVALID_VERSION_ID,
};
use crate::monitor::StateStoreMetrics;
use crate::storage_value::StorageValue;

struct WorkerContext {
    version_update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
}

struct BufferTracker {
    flush_threshold: usize,
    block_write_threshold: usize,
    global_buffer_size: Arc<AtomicUsize>,
    global_upload_task_size: Arc<AtomicUsize>,

    buffer_event_sender: mpsc::UnboundedSender<SharedBufferEvent>,
}

impl BufferTracker {
    pub fn new(
        flush_threshold: usize,
        block_write_threshold: usize,
        buffer_event_sender: mpsc::UnboundedSender<SharedBufferEvent>,
    ) -> Self {
        assert!(
            flush_threshold <= block_write_threshold,
            "flush threshold {} is not less than block write threshold {}",
            flush_threshold,
            block_write_threshold
        );
        info!(
            "buffer tracker init: flush threshold {}, block write threshold {}",
            flush_threshold, block_write_threshold
        );
        Self {
            flush_threshold,
            block_write_threshold,
            global_buffer_size: Arc::new(AtomicUsize::new(0)),
            global_upload_task_size: Arc::new(AtomicUsize::new(0)),
            buffer_event_sender,
        }
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer_size.load(Relaxed)
    }

    pub fn get_upload_task_size(&self) -> usize {
        self.global_upload_task_size.load(Relaxed)
    }

    pub fn can_write(&self) -> bool {
        self.get_buffer_size() <= self.block_write_threshold
    }

    pub fn try_write(&self, size: usize) -> bool {
        loop {
            let current_size = self.global_buffer_size.load(Acquire);
            if current_size > self.block_write_threshold {
                return false;
            }
            if self
                .global_buffer_size
                .compare_exchange(current_size, current_size + size, Acquire, Acquire)
                .is_ok()
            {
                break true;
            }
        }
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_more_flush(&self) -> bool {
        self.get_buffer_size() > self.flush_threshold + self.get_upload_task_size()
    }

    pub fn send_event(&self, event: SharedBufferEvent) {
        self.buffer_event_sender.send(event).unwrap();
    }
}

/// The `LocalVersionManager` maintains a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `Sstables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    local_version: RwLock<LocalVersion>,
    worker_context: WorkerContext,
    buffer_tracker: BufferTracker,
    write_conflict_detector: Option<Arc<ConflictDetector>>,
    shared_buffer_uploader: Arc<SharedBufferUploader>,
}

impl LocalVersionManager {
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
        table_id_to_slice_transform: Arc<RwLock<HashMap<u32, SliceTransformImpl>>>,
    ) -> Arc<LocalVersionManager> {
        let (version_unpin_worker_tx, version_unpin_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (version_update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);

        let pinned_version = Self::pin_version_with_retry(
            hummock_meta_client.clone(),
            INVALID_VERSION_ID,
            10,
            // never break until max retry
            || false,
        )
        .await
        .expect("should be `Some` since `break_condition` is always false")
        .expect("should be able to pinned the first version")
        .2
        .unwrap();

        let (buffer_event_sender, buffer_event_receiver) = mpsc::unbounded_channel();

        let capacity = (options.shared_buffer_capacity_mb as usize) * (1 << 20);

        let local_version_manager = Arc::new(LocalVersionManager {
            local_version: RwLock::new(LocalVersion::new(pinned_version, version_unpin_worker_tx)),
            worker_context: WorkerContext {
                version_update_notifier_tx,
            },
            buffer_tracker: BufferTracker::new(
                // 0.8 * capacity
                // TODO: enable setting the ratio with config
                capacity * 4 / 5,
                capacity,
                buffer_event_sender,
            ),
            write_conflict_detector: write_conflict_detector.clone(),

            shared_buffer_uploader: Arc::new(SharedBufferUploader::new(
                options.clone(),
                sstable_store,
                hummock_meta_client.clone(),
                stats,
                write_conflict_detector,
                table_id_to_slice_transform.clone(),
            )),
        });

        // Pin and get the latest version.
        tokio::spawn(LocalVersionManager::start_pin_worker(
            Arc::downgrade(&local_version_manager),
            hummock_meta_client.clone(),
        ));

        // Unpin unused version.
        tokio::spawn(LocalVersionManager::start_unpin_worker(
            Arc::downgrade(&local_version_manager),
            version_unpin_worker_rx,
            hummock_meta_client,
        ));

        // Buffer size manager.
        tokio::spawn(LocalVersionManager::start_buffer_tracker_worker(
            local_version_manager.clone(),
            buffer_event_receiver,
        ));

        local_version_manager
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
    ) -> Arc<LocalVersionManager> {
        Self::new(
            options,
            sstable_store,
            Arc::new(StateStoreMetrics::unused()),
            hummock_meta_client,
            write_conflict_detector,
            Arc::new(RwLock::new(HashMap::new())),
        )
        .await
    }

    /// Updates cached version if the new version is of greater id.
    /// You shouldn't unpin even the method returns false, as it is possible `hummock_version` is
    /// being referenced by some readers.
    pub fn try_update_pinned_version(
        &self,
        last_pinned: Option<u64>,
        pin_resp: (bool, Vec<HummockVersionDelta>, Option<HummockVersion>),
    ) -> bool {
        let old_version = self.local_version.upgradable_read();
        if let Some(last_pinned_id) = last_pinned {
            if old_version.pinned_version().id() != last_pinned_id {
                return false;
            }
        }

        let new_version_id = if pin_resp.0 {
            match pin_resp.1.last() {
                Some(version_delta) => version_delta.id,
                None => old_version.pinned_version().id(),
            }
        } else {
            pin_resp.2.as_ref().unwrap().id
        };
        if old_version.pinned_version().id() >= new_version_id {
            return false;
        }

        let newly_pinned_version = if pin_resp.0 {
            let mut version_to_apply = old_version.pinned_version().version();
            for version_delta in pin_resp.1 {
                version_to_apply.apply_version_delta(&version_delta);
            }
            version_to_apply
        } else {
            pin_resp.2.unwrap()
        };
        for levels in newly_pinned_version.levels.values() {
            if validate_table_key_range(&levels.levels).is_err() {
                error!("invalid table key range: {:?}", levels.levels);
                return false;
            }
        }

        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.set_watermark(newly_pinned_version.max_committed_epoch);
        }

        let mut new_version = old_version.clone();
        new_version.set_pinned_version(newly_pinned_version);
        {
            let mut guard = RwLockUpgradableReadGuard::upgrade(old_version);
            *guard = new_version;
            RwLockWriteGuard::unlock_fair(guard);
        }

        self.worker_context
            .version_update_notifier_tx
            .send(new_version_id)
            .ok();
        true
    }

    /// Waits until the local hummock version contains the given committed epoch
    pub async fn wait_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        if epoch == HummockEpoch::MAX {
            panic!("epoch should not be u64::MAX");
        }
        let mut receiver = self.worker_context.version_update_notifier_tx.subscribe();
        loop {
            let (pinned_version_id, pinned_version_epoch) = {
                let current_version = self.local_version.read();
                if current_version.pinned_version().max_committed_epoch() >= epoch {
                    return Ok(());
                }
                (
                    current_version.pinned_version().id(),
                    current_version.pinned_version().max_committed_epoch(),
                )
            };
            match tokio::time::timeout(Duration::from_secs(10), receiver.changed()).await {
                Err(_) => {
                    // The reason that we need to retry here is batch scan in chain/rearrange_chain
                    // is waiting for an uncommitted epoch carried by the CreateMV barrier, which
                    // can take unbounded time to become committed and propagate
                    // to the CN. We should consider removing the retry as well as wait_epoch for
                    // chain/rearrange_chain if we enforce chain/rearrange_chain to be
                    // scheduled on the same CN with the same distribution as
                    // the upstream MV. See #3845 for more details.
                    tracing::warn!(
                        "wait_epoch {} timeout when waiting for version update. pinned_version_id {}, pinned_version_epoch {}.",
                        epoch, pinned_version_id, pinned_version_epoch
                    );
                    continue;
                }
                Ok(Err(_)) => {
                    return Err(HummockError::wait_epoch("tx dropped"));
                }
                Ok(Ok(_)) => {}
            }
        }
    }

    pub fn build_shared_buffer_item_batches(
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: HummockEpoch,
    ) -> Vec<SharedBufferItem> {
        kv_pairs
            .into_iter()
            .map(|(key, value)| {
                (
                    Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                    value.into(),
                )
            })
            .collect_vec()
    }

    pub async fn write_shared_buffer(
        &self,
        epoch: HummockEpoch,
        compaction_group_id: CompactionGroupId,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        is_remote_batch: bool,
        table_id: u32,
    ) -> HummockResult<usize> {
        let sorted_items = Self::build_shared_buffer_item_batches(kv_pairs, epoch);

        let batch = SharedBufferBatch::new(
            sorted_items,
            epoch,
            self.buffer_tracker.buffer_event_sender.clone(),
            compaction_group_id,
            table_id,
        );
        let batch_size = batch.size();
        if self.buffer_tracker.try_write(batch_size) {
            self.write_shared_buffer_inner(epoch, batch, is_remote_batch);
            self.buffer_tracker.send_event(SharedBufferEvent::MayFlush);
        } else {
            let (tx, rx) = oneshot::channel();
            self.buffer_tracker
                .send_event(SharedBufferEvent::WriteRequest(WriteRequest {
                    batch,
                    epoch,
                    is_remote_batch,
                    grant_sender: tx,
                }));
            rx.await.unwrap();
        }
        Ok(batch_size)
    }

    fn write_shared_buffer_inner(
        &self,
        epoch: HummockEpoch,
        batch: SharedBufferBatch,
        is_remote_batch: bool,
    ) {
        // Try get shared buffer with version read lock
        let shared_buffer = self.local_version.read().get_shared_buffer(epoch).cloned();

        // New a shared buffer with version write lock if shared buffer of the corresponding epoch
        // does not exist before
        let shared_buffer = shared_buffer.unwrap_or_else(|| {
            self.local_version
                .write()
                .new_shared_buffer(epoch, self.buffer_tracker.global_upload_task_size.clone())
        });

        // Write into shared buffer
        if is_remote_batch {
            // The batch won't be synced to S3 asynchronously if it is a remote batch
            shared_buffer.write().replicate_batch(batch);
        } else {
            // The batch will be synced to S3 asynchronously if it is a local batch
            shared_buffer.write().write_batch(batch);
        }

        // Notify the buffer tracker after the batch has been added to shared buffer.
        self.buffer_tracker.send_event(SharedBufferEvent::MayFlush);
    }

    /// Issue a concurrent upload task to flush some local shared buffer batch to object store.
    ///
    /// This method should only be called in the buffer tracker worker.
    ///
    /// Return:
    ///   - Some(task join handle) when there is new upload task
    ///   - None when there is no new task
    pub fn flush_shared_buffer(
        self: Arc<Self>,
        syncing_epoch: &HashSet<HummockEpoch>,
    ) -> Option<(HummockEpoch, JoinHandle<()>)> {
        // The current implementation is a trivial one, which issue only one flush task and wait for
        // the task to finish.
        let mut task = None;
        for (epoch, shared_buffer) in self.local_version.read().iter_shared_buffer() {
            // skip the epoch that is being synced
            if syncing_epoch.get(epoch).is_some() {
                continue;
            }
            if let Some(upload_task) = shared_buffer.write().new_upload_task(FlushWriteBatch) {
                task = Some((*epoch, upload_task));
                break;
            }
        }
        let (epoch, (order_index, payload, task_write_batch_size)) = match task {
            Some(task) => task,
            None => return None,
        };

        let join_handle = tokio::spawn(async move {
            info!(
                "running flush task in epoch {} of size {}",
                epoch, task_write_batch_size
            );
            // TODO: may apply different `is_local` according to whether local spill is enabled.
            let _ = self
                .run_upload_task(order_index, epoch, payload, true)
                .await
                .inspect_err(|err| {
                    error!(
                        "upload task fail. epoch: {}, order_index: {}. Err: {:?}",
                        epoch, order_index, err
                    );
                });
            info!(
                "flush task in epoch {} of size {} finished",
                epoch, task_write_batch_size
            );
        });
        Some((epoch, join_handle))
    }

    pub async fn sync_shared_buffer(&self, epoch_range: Option<(u64, u64)>) -> HummockResult<()> {
        let epochs = match epoch_range {
            Some(epoch_range) => self
                .local_version
                .read()
                .scan_shared_buffer(epoch_range)
                .map(|(epoch, _)| *epoch)
                .collect_vec(),
            None => self
                .local_version
                .read()
                .iter_shared_buffer()
                .map(|(epoch, _)| *epoch)
                .collect_vec(),
        };
        if epochs.is_empty() {
            return Ok(());
        }
        let epoch_range = epoch_range.unwrap_or_else(|| (0, *epochs.last().unwrap()));
        let (tx, rx) = oneshot::channel();
        self.buffer_tracker
            .send_event(SharedBufferEvent::SyncEpoch(epochs.clone(), tx));
        let join_handles = rx.await.unwrap();
        for result in join_all(join_handles).await {
            result.expect("should be able to join the flush handle")
        }

        tracing::info!("sync epoch {:?},epoch {:?}", epochs, epoch_range.1);
        self.sync_all_shared_buffer(epoch_range).await?;
        self.buffer_tracker
            .send_event(SharedBufferEvent::EpochSynced(epochs));
        Ok(())
    }

    /// Sync all shared buffer less than or equal to epoch
    pub async fn sync_all_shared_buffer(&self, epoch_range: (u64, u64)) -> HummockResult<()> {
        let epoch = epoch_range.1;
        let mut size = 0;
        let keyed_payload = self
            .local_version
            .read()
            .scan_shared_buffer(epoch_range)
            .map(|(key, value)| {
                if key != &epoch {
                    size += value.read().get_upload_size();
                }
                (*value).clone()
            })
            .flat_map(|value| value.write().get_uncommitted_data())
            .collect::<KeyIndexedUncommittedData>();
        let (order_index, task_payload, task_write_batch_size) = match self
            .local_version
            .write()
            .new_shared_buffer(epoch, Arc::new(AtomicUsize::new(0)))
            .write()
            .new_upload_task(SyncEpoch(Some((keyed_payload, size))))
        {
            Some(task) => task,
            None => {
                tracing::trace!("sync epoch {} has no more task to do", epoch);
                return Ok(());
            }
        };

        let ret = self
            .run_upload_task(order_index, epoch, task_payload, false)
            .await;
        tracing::trace!(
            "sync epoch {} finished. Task size {}",
            epoch,
            task_write_batch_size
        );
        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.archive_epoch(epoch);
        }
        ret
    }

    async fn run_upload_task(
        &self,
        order_index: OrderIndex,
        epoch: HummockEpoch,
        task_payload: UploadTaskPayload,
        is_local: bool,
    ) -> HummockResult<()> {
        let task_result = self
            .shared_buffer_uploader
            .flush(epoch, is_local, task_payload)
            .await;

        let local_version_guard = self.local_version.read();
        let mut shared_buffer_guard = local_version_guard
            .get_shared_buffer(epoch)
            .expect("shared buffer should exist since some uncommitted data is not committed yet")
            .write();

        let ret = match task_result {
            Ok(ssts) => {
                tracing::info!("test{:?}", epoch);
                shared_buffer_guard.succeed_upload_task(order_index, ssts);
                Ok(())
            }
            Err(e) => {
                shared_buffer_guard.fail_upload_task(order_index);
                Err(e)
            }
        };
        self.buffer_tracker.send_event(SharedBufferEvent::MayFlush);
        ret
    }

    pub fn read_version(self: &Arc<LocalVersionManager>, read_epoch: HummockEpoch) -> ReadVersion {
        LocalVersion::read_version(&self.local_version, read_epoch)
    }

    pub fn get_pinned_version(&self) -> Arc<PinnedVersion> {
        self.local_version.read().pinned_version().clone()
    }

    pub fn get_uncommitted_ssts(&self, epoch: HummockEpoch) -> Vec<LocalSstableInfo> {
        tracing::info!("batch epoch{}", epoch);
        self.local_version
            .read()
            .get_shared_buffer(epoch)
            .map(|shared_buffer| shared_buffer.read().get_ssts_to_commit())
            .unwrap_or_default()
    }

    /// Pin a version with retry.
    ///
    /// Return:
    ///   - `Some(Ok(pinned_version))` if success
    ///   - `Some(Err(err))` if exceed the retry limit
    ///   - `None` if meet the break condition
    async fn pin_version_with_retry(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        last_pinned: HummockVersionId,
        max_retry: usize,
        break_condition: impl Fn() -> bool,
    ) -> Option<HummockResult<(bool, Vec<HummockVersionDelta>, Option<HummockVersion>)>> {
        let max_retry_interval = Duration::from_secs(10);
        let mut retry_backoff = tokio_retry::strategy::ExponentialBackoff::from_millis(10)
            .max_delay(max_retry_interval)
            .map(jitter);

        let mut retry_count = 0;
        loop {
            if retry_count > max_retry {
                break Some(Err(HummockError::meta_error(format!(
                    "pin_version max retry reached: {}.",
                    max_retry
                ))));
            }
            if break_condition() {
                break None;
            }
            match hummock_meta_client.pin_version(last_pinned).await {
                Ok(version) => {
                    break Some(Ok(version));
                }
                Err(err) => {
                    let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                    tracing::warn!(
                        "Failed to pin version {:?}. Will retry after about {} milliseconds",
                        err,
                        retry_after.as_millis()
                    );
                    tokio::time::sleep(retry_after).await;
                }
            }
            retry_count += 1;
        }
    }

    async fn start_pin_worker(
        local_version_manager_weak: Weak<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let min_execute_interval = Duration::from_millis(100);
        let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
        min_execute_interval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            min_execute_interval_tick.tick().await;
            let local_version_manager = match local_version_manager_weak.upgrade() {
                None => {
                    tracing::info!("Shutdown hummock pin worker");
                    return;
                }
                Some(local_version_manager) => local_version_manager,
            };

            let last_pinned = local_version_manager
                .local_version
                .read()
                .pinned_version()
                .id();

            match Self::pin_version_with_retry(
                hummock_meta_client.clone(),
                last_pinned,
                usize::MAX,
                || {
                    // Should stop when the `local_version_manager` in this thread is the only
                    // strong reference to the object.
                    local_version_manager_weak.strong_count() == 1
                },
            )
            .await
            {
                Some(Ok(pinned_version)) => {
                    local_version_manager
                        .try_update_pinned_version(Some(last_pinned), pinned_version);
                }
                Some(Err(_)) => {
                    unreachable!(
                        "since the max_retry is `usize::MAX`, this should never return `Err`"
                    );
                }
                None => {
                    tracing::info!("Shutdown hummock pin worker");
                    return;
                }
            };
        }
    }
}

// concurrent worker thread of `LocalVersionManager`
impl LocalVersionManager {
    async fn start_unpin_worker(
        local_version_manager_weak: Weak<LocalVersionManager>,
        mut rx: UnboundedReceiver<HummockVersionId>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let min_execute_interval = Duration::from_millis(1000);
        let max_retry_interval = Duration::from_secs(10);
        let get_backoff_strategy = || {
            tokio_retry::strategy::ExponentialBackoff::from_millis(10)
                .max_delay(max_retry_interval)
                .map(jitter)
        };
        let mut retry_backoff = get_backoff_strategy();
        let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
        min_execute_interval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut need_unpin = false;
        // For each run in the loop, accumulate versions to unpin and call unpin RPC once.
        loop {
            min_execute_interval_tick.tick().await;
            // 1. Collect new versions to unpin.
            let mut versions_to_unpin = vec![];
            'collect: loop {
                match rx.try_recv() {
                    Ok(version) => {
                        versions_to_unpin.push(version);
                    }
                    Err(err) => match err {
                        TryRecvError::Empty => {
                            break 'collect;
                        }
                        TryRecvError::Disconnected => {
                            tracing::info!("Shutdown hummock unpin worker");
                            return;
                        }
                    },
                }
            }
            if !versions_to_unpin.is_empty() {
                need_unpin = true;
            }
            if !need_unpin {
                continue;
            }
            let local_version_manager = match local_version_manager_weak.upgrade() {
                None => {
                    tracing::info!("Shutdown hummock unpin worker");
                    return;
                }
                Some(local_version_manager) => local_version_manager,
            };
            let unpin_before = {
                let mut local_version = local_version_manager.local_version.write();
                for version in &versions_to_unpin {
                    local_version.version_ids_in_use.remove(version);
                }
                *local_version.version_ids_in_use.first().unwrap()
            };

            // 2. Call unpin RPC, including versions failed to unpin in previous RPC calls.
            match hummock_meta_client.unpin_version_before(unpin_before).await {
                Ok(_) => {
                    versions_to_unpin.clear();
                    need_unpin = false;
                    retry_backoff = get_backoff_strategy();
                }
                Err(err) => {
                    let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                    tracing::warn!(
                        "Failed to unpin version {:?}. Will retry after about {} milliseconds",
                        err,
                        retry_after.as_millis()
                    );
                    tokio::time::sleep(retry_after).await;
                }
            }
        }
    }

    pub async fn start_buffer_tracker_worker(
        local_version_manager: Arc<LocalVersionManager>,
        mut buffer_size_change_receiver: mpsc::UnboundedReceiver<SharedBufferEvent>,
    ) {
        let mut syncing_epoch = HashSet::new();
        let mut epoch_join_handle = HashMap::new();
        let try_flush_shared_buffer =
            |syncing_epoch: &HashSet<HummockEpoch>,
             epoch_join_handle: &mut HashMap<HummockEpoch, Vec<JoinHandle<()>>>| {
                // Keep issuing new flush task until flush is not needed or we can issue
                // no more task
                while local_version_manager.buffer_tracker.need_more_flush() {
                    if let Some((epoch, join_handle)) = local_version_manager
                        .clone()
                        .flush_shared_buffer(syncing_epoch)
                    {
                        epoch_join_handle
                            .entry(epoch)
                            .or_insert_with(Vec::new)
                            .push(join_handle);
                    } else {
                        break;
                    }
                }
            };

        let grant_write_request = |request| {
            let WriteRequest {
                batch,
                epoch,
                is_remote_batch,
                grant_sender: sender,
            } = request;
            let size = batch.size();
            local_version_manager.write_shared_buffer_inner(epoch, batch, is_remote_batch);
            local_version_manager
                .buffer_tracker
                .global_buffer_size
                .fetch_add(size, Relaxed);
            let _ = sender.send(()).inspect_err(|err| {
                error!("unable to send write request response: {:?}", err);
            });
        };

        let mut pending_write_requests: VecDeque<_> = VecDeque::new();

        // While the current Arc is not the only strong reference to the local version manager
        while Arc::strong_count(&local_version_manager) > 1 {
            if let Some(event) = buffer_size_change_receiver.recv().await {
                match event {
                    SharedBufferEvent::WriteRequest(request) => {
                        if local_version_manager.buffer_tracker.can_write() {
                            grant_write_request(request);
                            try_flush_shared_buffer(&syncing_epoch, &mut epoch_join_handle);
                        } else {
                            info!(
                                "write request is blocked: epoch {}, size: {}",
                                request.epoch,
                                request.batch.size()
                            );
                            pending_write_requests.push_back(request);
                        }
                    }
                    SharedBufferEvent::MayFlush => {
                        // Only check and flush shared buffer after batch has been added to shared
                        // buffer.
                        try_flush_shared_buffer(&syncing_epoch, &mut epoch_join_handle);
                    }
                    SharedBufferEvent::BufferRelease(size) => {
                        local_version_manager
                            .buffer_tracker
                            .global_buffer_size
                            .fetch_sub(size, Relaxed);
                        let mut has_granted = false;
                        while !pending_write_requests.is_empty()
                            && local_version_manager.buffer_tracker.can_write()
                        {
                            let request = pending_write_requests.pop_front().unwrap();
                            info!(
                                "write request is granted: epoch {}, size: {}",
                                request.epoch,
                                request.batch.size()
                            );
                            grant_write_request(request);
                            has_granted = true;
                        }
                        if has_granted {
                            try_flush_shared_buffer(&syncing_epoch, &mut epoch_join_handle);
                        }
                    }
                    SharedBufferEvent::SyncEpoch(epochs, join_handle_sender) => {
                        let join_handle = epochs
                            .into_iter()
                            .flat_map(|epoch| {
                                tracing::info!("sadsad{:?}", syncing_epoch);
                                assert!(
                                    syncing_epoch.insert(epoch),
                                    "epoch {} cannot be synced for twice",
                                    epoch
                                );
                                epoch_join_handle.remove(&epoch).unwrap_or_default()
                            })
                            .collect_vec();
                        let _ = join_handle_sender.send(join_handle).inspect_err(|e| {
                            error!("unable to send join handles Err {:?}", e);
                        });
                    }
                    SharedBufferEvent::EpochSynced(epochs) => {
                        epochs.into_iter().for_each(|epoch| {
                            assert!(
                                syncing_epoch.remove(&epoch),
                                "removing a previous not synced epoch {}",
                                epoch
                            )
                        });
                    }

                    SharedBufferEvent::Clear(notifier) => {
                        // Wait for all ongoing flush to finish.
                        let ongoing_flush_handles: Vec<_> =
                            epoch_join_handle.drain().flat_map(|e| e.1).collect();
                        if let Err(e) = try_join_all(ongoing_flush_handles).await {
                            error!("Failed to join flush handle {:?}", e)
                        }

                        // There cannot be any pending write requests since we should only clear
                        // shared buffer after all actors stop processing data.
                        assert!(pending_write_requests.is_empty());

                        // Clear shared buffer
                        local_version_manager
                            .local_version
                            .write()
                            .clear_shared_buffer();

                        // Notify completion of the Clear event.
                        notifier.send(()).unwrap();
                    }
                };
            } else {
                break;
            }
        }
    }

    pub async fn clear_shared_buffer(&self) {
        let (tx, rx) = oneshot::channel();
        self.buffer_tracker.send_event(SharedBufferEvent::Clear(tx));
        rx.await.unwrap();
    }
}

#[cfg(any(test, feature = "test"))]
// Some method specially for tests of `LocalVersionManager`
impl LocalVersionManager {
    pub async fn refresh_version(&self, hummock_meta_client: &dyn HummockMetaClient) -> bool {
        let last_pinned = self.get_pinned_version().id();
        let version = hummock_meta_client.pin_version(last_pinned).await.unwrap();
        self.try_update_pinned_version(Some(last_pinned), version)
    }

    pub fn local_version(&self) -> &RwLock<LocalVersion> {
        &self.local_version
    }

    pub fn get_local_version(&self) -> LocalVersion {
        self.local_version.read().clone()
    }

    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_buffer_size()
    }
}
