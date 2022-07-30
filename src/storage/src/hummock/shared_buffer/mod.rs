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

pub mod shared_buffer_batch;
#[expect(dead_code)]
pub mod shared_buffer_uploader;

use std::collections::{BTreeMap, HashMap};
use std::mem::swap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::key::user_key;
use risingwave_hummock_sdk::{is_remote_sst_id, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::{KeyRange, SstableInfo};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use self::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::iterator::{
    HummockIteratorDirection, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner,
};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchIterator;
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::state_store::HummockIteratorType;
use crate::hummock::utils::{filter_single_sst, range_overlap};
use crate::hummock::{HummockResult, SstableIteratorType, SstableStore};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

#[derive(Debug, Clone, PartialEq)]
pub enum UncommittedData {
    Sst(LocalSstableInfo),
    Batch(SharedBufferBatch),
}

fn get_sst_key_range(info: &SstableInfo) -> &KeyRange {
    let key_range = info
        .key_range
        .as_ref()
        .expect("local sstable should have key range");
    assert!(
        !key_range.inf,
        "local sstable should not have infinite key range. Sstable info: {:?}",
        info,
    );
    key_range
}

impl UncommittedData {
    pub fn start_user_key(&self) -> &[u8] {
        match self {
            UncommittedData::Sst((_, info)) => {
                let key_range = get_sst_key_range(info);
                user_key(key_range.left.as_slice())
            }
            UncommittedData::Batch(batch) => batch.start_user_key(),
        }
    }

    pub fn end_user_key(&self) -> &[u8] {
        match self {
            UncommittedData::Sst((_, info)) => {
                let key_range = get_sst_key_range(info);
                user_key(key_range.right.as_slice())
            }
            UncommittedData::Batch(batch) => batch.end_user_key(),
        }
    }
}

pub(crate) type OrderIndex = usize;
/// `{ (end key, order_id) -> batch }`
pub(crate) type KeyIndexedUncommittedData = BTreeMap<(Vec<u8>, OrderIndex), UncommittedData>;
/// uncommitted data sorted by order index in descending order. Data in the same inner list share
/// the same order index, which means their keys don't overlap.
pub(crate) type OrderSortedUncommittedData = Vec<Vec<UncommittedData>>;

pub(crate) fn to_order_sorted(
    key_indexed_data: &KeyIndexedUncommittedData,
) -> OrderSortedUncommittedData {
    let mut order_indexed_data = BTreeMap::new();
    for ((_, order_id), data) in key_indexed_data {
        order_indexed_data
            .entry(*order_id)
            .or_insert_with(Vec::new)
            .push(data.clone());
    }
    // Take rev here to ensure order index sorted in descending order.
    order_indexed_data.into_values().rev().collect()
}

#[allow(type_alias_bounds)]
pub type UncommittedDataIteratorType<
    D: HummockIteratorDirection,
    I: SstableIteratorType<Direction = D>,
> = HummockIteratorUnion<D, SharedBufferBatchIterator<D>, I>;

#[allow(type_alias_bounds)]
pub type SharedBufferIteratorType<
    D: HummockIteratorDirection,
    I: SstableIteratorType<Direction = D>,
> = OrderedMergeIteratorInner<
    HummockIteratorUnion<
        D,
        UncommittedDataIteratorType<D, I>,
        UnorderedMergeIteratorInner<UncommittedDataIteratorType<D, I>>,
    >,
>;

pub(crate) async fn build_ordered_merge_iter<T: HummockIteratorType>(
    uncommitted_data: &OrderSortedUncommittedData,
    sstable_store: Arc<SstableStore>,
    stats: Arc<StateStoreMetrics>,
    local_stats: &mut StoreLocalStatistic,
    read_options: Arc<SstableIteratorReadOptions>,
) -> HummockResult<SharedBufferIteratorType<T::Direction, T::SstableIteratorType>> {
    let mut ordered_iters = Vec::with_capacity(uncommitted_data.len());
    for data_list in uncommitted_data {
        let mut data_iters = Vec::new();
        for data in data_list {
            match data {
                UncommittedData::Batch(batch) => {
                    data_iters.push(UncommittedDataIteratorType::First(
                        batch.clone().into_directed_iter::<T::Direction>(),
                    ));
                }
                UncommittedData::Sst((_, table_info)) => {
                    let table = sstable_store.sstable(table_info.id, local_stats).await?;
                    data_iters.push(UncommittedDataIteratorType::Second(
                        T::SstableIteratorType::create(
                            table,
                            sstable_store.clone(),
                            read_options.clone(),
                        ),
                    ));
                }
            }
        }
        if data_iters.is_empty() {
            continue;
        } else if data_iters.len() == 1 {
            ordered_iters.push(HummockIteratorUnion::First(data_iters.pop().unwrap()));
        } else {
            ordered_iters.push(HummockIteratorUnion::Second(
                UnorderedMergeIteratorInner::new(data_iters, stats.clone()),
            ));
        }
    }
    Ok(OrderedMergeIteratorInner::new(ordered_iters, stats.clone()))
}

#[derive(Debug)]
pub struct SharedBuffer {
    uncommitted_data: KeyIndexedUncommittedData,
    replicate_batches: BTreeMap<Vec<u8>, SharedBufferBatch>,
    // OrderIndex -> (task payload, task write batch size)
    uploading_tasks: HashMap<OrderIndex, (KeyIndexedUncommittedData, usize)>,
    upload_batches_size: usize,
    replicate_batches_size: usize,

    global_upload_task_size: Arc<AtomicUsize>,

    next_order_index: usize,
}

pub enum UploadTaskType {
    FlushWriteBatch,
    SyncEpoch(Option<(KeyIndexedUncommittedData, usize)>),
}

#[derive(Debug)]
pub struct WriteRequest {
    pub batch: SharedBufferBatch,
    pub epoch: HummockEpoch,
    pub is_remote_batch: bool,
    pub grant_sender: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum SharedBufferEvent {
    /// Write request to shared buffer. The first parameter is the batch size and the second is the
    /// request permission notifier. After the write request is granted and notified, the size is
    /// already tracked.
    WriteRequest(WriteRequest),

    /// Notify that we may flush the shared buffer.
    MayFlush,

    /// A shared buffer batch is released. The parameter is the batch size.
    BufferRelease(usize),

    /// An epoch is going to be synced. Once the event is processed, there will be no more flush
    /// task on this epoch. Previous concurrent flush task join handle will be returned by the join
    /// handle sender.
    SyncEpoch(Vec<HummockEpoch>, oneshot::Sender<Vec<JoinHandle<()>>>),

    /// An epoch has been synced.
    EpochSynced(Vec<HummockEpoch>),

    /// Clear shared buffer and reset all states
    Clear(oneshot::Sender<()>),
}

impl SharedBuffer {
    pub fn new(global_upload_task_size: Arc<AtomicUsize>) -> Self {
        Self {
            uncommitted_data: Default::default(),
            replicate_batches: Default::default(),
            uploading_tasks: Default::default(),
            upload_batches_size: 0,
            replicate_batches_size: 0,
            global_upload_task_size,
            next_order_index: 0,
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::new(Arc::new(AtomicUsize::new(0)))
    }

    pub fn write_batch(&mut self, batch: SharedBufferBatch) {
        self.upload_batches_size += batch.size();
        let order_index = self.get_next_order_index();

        let insert_result = self.uncommitted_data.insert(
            (batch.end_user_key().to_vec(), order_index),
            UncommittedData::Batch(batch),
        );
        assert!(
            insert_result.is_none(),
            "duplicate end key and order index when inserting a write batch. \
            Order index: {}, previous data: {:?}",
            order_index,
            insert_result
        );
    }

    pub fn replicate_batch(&mut self, batch: SharedBufferBatch) {
        self.replicate_batches_size += batch.size();
        self.replicate_batches
            .insert(batch.end_user_key().to_vec(), batch);
    }

    /// Gets batches from shared buffer that overlap with the given key range.
    /// The return tuple is (replicated batches, uncommitted data).
    pub fn get_overlap_data<R, B>(
        &self,
        key_range: &R,
    ) -> (Vec<SharedBufferBatch>, OrderSortedUncommittedData)
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let replicated_batches = self
            .replicate_batches
            .range((
                key_range.start_bound().map(|b| b.as_ref().to_vec()),
                std::ops::Bound::Unbounded,
            ))
            .filter(|(_, batch)| {
                range_overlap(key_range, batch.start_user_key(), batch.end_user_key())
            })
            .map(|(_, batches)| batches.clone())
            .collect_vec();

        let range = (
            match key_range.start_bound() {
                Bound::Included(key) => Bound::Included((key.as_ref().to_vec(), OrderIndex::MIN)),
                Bound::Excluded(key) => Bound::Excluded((key.as_ref().to_vec(), OrderIndex::MAX)),
                Bound::Unbounded => Bound::Unbounded,
            },
            std::ops::Bound::Unbounded,
        );

        let local_data_iter = self
            .uncommitted_data
            .range(range.clone())
            .chain(
                self.uploading_tasks
                    .values()
                    .flat_map(|(payload, _)| payload.range(range.clone())),
            )
            .filter(|(_, data)| match data {
                UncommittedData::Batch(batch) => {
                    range_overlap(key_range, batch.start_user_key(), batch.end_user_key())
                }
                UncommittedData::Sst((_, info)) => filter_single_sst(info, key_range),
            })
            .map(|((_, order_index), data)| (*order_index, data.clone()));

        let mut uncommitted_data = BTreeMap::new();
        for (order_index, data) in local_data_iter {
            uncommitted_data
                .entry(order_index)
                .or_insert_with(Vec::new)
                .push(data);
        }

        (
            replicated_batches,
            uncommitted_data.into_values().rev().collect(),
        )
    }

    pub fn clear_replicate_batch(&mut self) {
        self.replicate_batches.clear();
        self.replicate_batches_size = 0;
    }

    pub fn get_uncommitted_data(&mut self) -> KeyIndexedUncommittedData {
        assert!(
            self.uploading_tasks.is_empty(),
            "when sync an epoch, there should not be any uploading task"
        );
        let mut keyed_payload = KeyIndexedUncommittedData::new();
        swap(&mut self.uncommitted_data, &mut keyed_payload);
        keyed_payload
    }

    /// Create a new upload task
    ///
    /// Return: (order index, task payload, task write batch size)
    pub fn new_upload_task(
        &mut self,
        task_type: UploadTaskType,
    ) -> Option<(OrderIndex, UploadTaskPayload, usize)> {
        let keyed_payload = match task_type {
            UploadTaskType::FlushWriteBatch => {
                // For flush write batch, currently we only flush the write batches. We first pick
                // the write batch with the smallest order index, and then start
                // from this order index, we iterate over all order indexes in
                // ascending order. We add the write batches to the task payload and
                // stop when we meet a sst.

                // Keep track of whether the data of an order index is non uploaded local batches.
                // The key is the order index. The value for sst and uploading tasks are `None`. For
                // write batches, their value is `Some((end_user_key, order index))`, which is the
                // key stored in `uncommitted_data`. We store the key in `uncommitted_data` so that
                // after we generate the upload task, we can remove the key from
                // `uncommitted_data`.
                let mut order_index_is_non_upload_batch = BTreeMap::new();
                for ((end_key, order_index), data) in &self.uncommitted_data {
                    if matches!(data, UncommittedData::Batch(_)) {
                        // Here we assume that for a write batch, no other uncommitted data will
                        // share the same order index with it, and therefore it's safe to insert
                        // into the map directly.
                        order_index_is_non_upload_batch
                            .insert(*order_index, Some((end_key, order_index)));
                    } else {
                        order_index_is_non_upload_batch.insert(*order_index, None);
                    }
                }
                for order_index in self.uploading_tasks.keys() {
                    order_index_is_non_upload_batch.insert(*order_index, None);
                }

                let mut payload_keys = Vec::new();
                // This will iterate over all order indexes in ascending order.
                for payload_keys_opt in order_index_is_non_upload_batch.values() {
                    match payload_keys_opt {
                        Some((end_key, order_index)) => {
                            payload_keys.push(((*end_key).clone(), **order_index));
                        }
                        None => {
                            if !payload_keys.is_empty() {
                                break;
                            }
                        }
                    }
                }

                let mut keyed_payload = KeyIndexedUncommittedData::new();
                for key in payload_keys {
                    let data = self.uncommitted_data.remove(&key).expect(
                        "the key to remote in the original non uploaded batches should exist",
                    );
                    keyed_payload.insert(key, data);
                }
                keyed_payload
            }
            UploadTaskType::SyncEpoch(keyed_payload) => match keyed_payload {
                Some((keyed_payload, size)) => {
                    self.upload_batches_size += size;
                    keyed_payload
                }
                None => self.get_uncommitted_data(),
            },
        };

        // The min order index in the task payload will be the order index of the payload.
        let min_order_index = keyed_payload
            .keys()
            .map(|(_, order_index)| order_index)
            .min()
            .cloned();

        if let Some(min_order_index) = min_order_index {
            let task_write_batch_size = keyed_payload
                .values()
                .map(|data| match data {
                    UncommittedData::Batch(batch) => batch.size(),
                    _ => 0,
                })
                .sum();
            self.global_upload_task_size
                .fetch_add(task_write_batch_size, Relaxed);
            let ret = Some((
                min_order_index,
                to_order_sorted(&keyed_payload),
                task_write_batch_size,
            ));
            self.uploading_tasks
                .insert(min_order_index, (keyed_payload, task_write_batch_size));
            ret
        } else {
            None
        }
    }

    pub fn fail_upload_task(&mut self, order_index: OrderIndex) {
        let (payload, task_write_batch_size) = self
            .uploading_tasks
            .remove(&order_index)
            .unwrap_or_else(|| {
                panic!(
                    "the order index should exist {} when fail an upload task",
                    order_index
                )
            });
        self.global_upload_task_size
            .fetch_sub(task_write_batch_size, Relaxed);
        self.uncommitted_data.extend(payload);
    }

    pub fn succeed_upload_task(
        &mut self,
        order_index: OrderIndex,
        new_sst: Vec<LocalSstableInfo>,
    ) -> Vec<LocalSstableInfo> {
        let (payload, task_write_batch_size) = self
            .uploading_tasks
            .remove(&order_index)
            .unwrap_or_else(|| {
                panic!(
                    "the order index should exist {} when succeed an upload task",
                    order_index
                )
            });
        self.global_upload_task_size
            .fetch_sub(task_write_batch_size, Relaxed);
        for sst in new_sst {
            let data = UncommittedData::Sst(sst);
            let insert_result = self
                .uncommitted_data
                .insert((data.end_user_key().to_vec(), order_index), data);
            assert!(
                insert_result.is_none(),
                "duplicate data end key and order index when inserting an SST. \
                Order index: {}. Previous data: {:?}",
                order_index,
                insert_result,
            );
        }
        let mut previous_sst = Vec::new();
        for data in payload.into_values() {
            match data {
                UncommittedData::Batch(batch) => {
                    self.upload_batches_size -= batch.size();
                }
                UncommittedData::Sst(sst) => {
                    previous_sst.push(sst);
                }
            }
        }
        // TODO: may want to delete the sst
        previous_sst
    }

    pub fn get_ssts_to_commit(&self) -> Vec<LocalSstableInfo> {
        assert!(
            self.uploading_tasks.is_empty(),
            "when committing sst there should not be uploading task"
        );
        let mut ret = Vec::new();
        for data in self.uncommitted_data.values() {
            match data {
                UncommittedData::Batch(_) => {
                    panic!("there should not be any batch when committing sst");
                }
                UncommittedData::Sst((compaction_group_id, sst)) => {
                    assert!(
                        is_remote_sst_id(sst.id),
                        "all sst should be remote when trying to get ssts to commit"
                    );
                    ret.push((*compaction_group_id, sst.clone()));
                }
            }
        }
        ret
    }

    pub fn size(&self) -> usize {
        self.upload_batches_size + self.replicate_batches_size
    }

    pub fn get_upload_size(&self) -> usize {
        self.upload_batches_size
    }

    fn get_next_order_index(&mut self) -> OrderIndex {
        let ret = self.next_order_index;
        self.next_order_index += 1;
        ret
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ops::DerefMut;

    use bytes::Bytes;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::{key_with_epoch, user_key};
    use tokio::sync::mpsc;

    use super::*;
    use crate::hummock::iterator::test_utils::iterator_test_value_of;
    use crate::hummock::shared_buffer::UploadTaskType::{FlushWriteBatch, SyncEpoch};
    use crate::hummock::test_utils::gen_dummy_sst_info;
    use crate::hummock::HummockValue;

    fn generate_and_write_batch(
        put_keys: &[Vec<u8>],
        delete_keys: &[Vec<u8>],
        epoch: u64,
        idx: &mut usize,
        shared_buffer: &mut SharedBuffer,
        is_replicate: bool,
    ) -> SharedBufferBatch {
        let mut shared_buffer_items = Vec::new();
        for key in put_keys {
            shared_buffer_items.push((
                Bytes::from(key_with_epoch(key.clone(), epoch)),
                HummockValue::put(iterator_test_value_of(*idx).into()),
            ));
            *idx += 1;
        }
        for key in delete_keys {
            shared_buffer_items.push((
                Bytes::from(key_with_epoch(key.clone(), epoch)),
                HummockValue::delete(),
            ));
        }
        shared_buffer_items.sort_by(|l, r| user_key(&l.0).cmp(&r.0));
        let batch = SharedBufferBatch::new(
            shared_buffer_items,
            epoch,
            mpsc::unbounded_channel().0,
            StaticCompactionGroupId::StateDefault.into(),
            Default::default(),
        );
        if is_replicate {
            shared_buffer.replicate_batch(batch.clone());
        } else {
            shared_buffer.write_batch(batch.clone());
        }
        batch
    }

    #[tokio::test]
    async fn test_get_overlap_batches() {
        let mut shared_buffer = SharedBuffer::for_test();
        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let large_key = format!("key_test_{:05}", 9).as_bytes().to_vec();
        let mut idx = 0;

        // Write two batches in epoch1
        let epoch1 = 1;

        // Write to upload buffer
        let shared_buffer_batch1 = generate_and_write_batch(
            &keys[0..3],
            &[],
            epoch1,
            &mut idx,
            &mut shared_buffer,
            false,
        );

        // Write to replicate buffer
        let shared_buffer_batch2 =
            generate_and_write_batch(&keys[0..3], &[], epoch1, &mut idx, &mut shared_buffer, true);

        // Get overlap batches and verify
        for key in &keys[0..3] {
            // Single key
            let (replicate_batches, overlap_data) =
                shared_buffer.get_overlap_data(&(key.clone()..=key.clone()));
            assert_eq!(overlap_data.len(), 1);
            assert_eq!(
                overlap_data[0],
                vec![UncommittedData::Batch(shared_buffer_batch1.clone())],
            );
            assert_eq!(replicate_batches.len(), 1);
            assert_eq!(replicate_batches[0], shared_buffer_batch2);

            // Forward key range
            let (replicate_batches, overlap_data) =
                shared_buffer.get_overlap_data(&(key.clone()..=keys[3].clone()));
            assert_eq!(overlap_data.len(), 1);
            assert_eq!(
                overlap_data[0],
                vec![UncommittedData::Batch(shared_buffer_batch1.clone())],
            );
            assert_eq!(replicate_batches.len(), 1);
            assert_eq!(replicate_batches[0], shared_buffer_batch2);
        }
        // Non-existent key
        let (replicate_batches, overlap_data) =
            shared_buffer.get_overlap_data(&(large_key.clone()..=large_key.clone()));
        assert!(replicate_batches.is_empty());
        assert!(overlap_data.is_empty());

        // Non-existent key range forward
        let (replicate_batches, overlap_data) =
            shared_buffer.get_overlap_data(&(keys[3].clone()..=large_key));
        assert!(replicate_batches.is_empty());
        assert!(overlap_data.is_empty());
    }

    #[tokio::test]
    async fn test_new_upload_task() {
        let shared_buffer = RefCell::new(SharedBuffer::for_test());
        let mut idx = 0;
        let mut generate_test_data = |key: &str| {
            generate_and_write_batch(
                &[key.as_bytes().to_vec()],
                &[],
                1,
                &mut idx,
                shared_buffer.borrow_mut().deref_mut(),
                false,
            )
        };

        let batch1 = generate_test_data("aa");
        let batch2 = generate_test_data("bb");

        let (order_index1, payload1, task_size) = shared_buffer
            .borrow_mut()
            .new_upload_task(FlushWriteBatch)
            .unwrap();
        assert_eq!(order_index1, 0);
        assert_eq!(2, payload1.len());
        assert_eq!(payload1[0].len(), 1);
        assert_eq!(payload1[0], vec![UncommittedData::Batch(batch2.clone())]);
        assert_eq!(payload1[1].len(), 1);
        assert_eq!(payload1[1], vec![UncommittedData::Batch(batch1.clone())]);
        assert_eq!(task_size, batch1.size() + batch2.size());

        let batch3 = generate_test_data("cc");
        let batch4 = generate_test_data("dd");

        let (order_index2, payload2, task_size) = shared_buffer
            .borrow_mut()
            .new_upload_task(FlushWriteBatch)
            .unwrap();
        assert_eq!(order_index2, 2);
        assert_eq!(2, payload2.len());
        assert_eq!(payload2[0].len(), 1);
        assert_eq!(payload2[0], vec![UncommittedData::Batch(batch4.clone())]);
        assert_eq!(payload2[1].len(), 1);
        assert_eq!(payload2[1], vec![UncommittedData::Batch(batch3.clone())]);
        assert_eq!(task_size, batch3.size() + batch4.size());

        shared_buffer.borrow_mut().fail_upload_task(order_index1);
        let (order_index1, payload1, task_size) = shared_buffer
            .borrow_mut()
            .new_upload_task(FlushWriteBatch)
            .unwrap();
        assert_eq!(order_index1, 0);
        assert_eq!(2, payload1.len());
        assert_eq!(payload1[0].len(), 1);
        assert_eq!(payload1[0], vec![UncommittedData::Batch(batch2.clone())]);
        assert_eq!(payload1[1].len(), 1);
        assert_eq!(payload1[1], vec![UncommittedData::Batch(batch1.clone())]);
        assert_eq!(task_size, batch1.size() + batch2.size());

        let sst1 = gen_dummy_sst_info(1, vec![batch1, batch2]);
        shared_buffer.borrow_mut().succeed_upload_task(
            order_index1,
            vec![(StaticCompactionGroupId::StateDefault.into(), sst1.clone())],
        );

        shared_buffer.borrow_mut().fail_upload_task(order_index2);

        let (order_index3, payload3, task_size) = shared_buffer
            .borrow_mut()
            .new_upload_task(SyncEpoch(None))
            .unwrap();

        assert_eq!(order_index3, 0);
        assert_eq!(3, payload3.len());
        assert_eq!(task_size, batch3.size() + batch4.size());
        assert_eq!(vec![UncommittedData::Batch(batch4)], payload3[0]);
        assert_eq!(vec![UncommittedData::Batch(batch3)], payload3[1]);
        assert_eq!(
            vec![UncommittedData::Sst((
                StaticCompactionGroupId::StateDefault.into(),
                sst1
            ))],
            payload3[2]
        );
    }
}
