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

use std::ops::Bound::{self, *};
use std::sync::Arc;

use risingwave_hummock_sdk::key::{get_epoch, key_with_epoch, user_key as to_user_key, Epoch};

use crate::hummock::iterator::merge_inner::UnorderedMergeIteratorInner;
use crate::hummock::iterator::{
    Backward, DirectedUserIterator, DirectedUserIteratorBuilder, HummockIterator,
    UserIteratorPayloadType,
};
use crate::hummock::local_version::PinnedVersion;
use crate::hummock::value::HummockValue;
use crate::hummock::{BackwardSstableIterator, HummockResult};
use crate::monitor::StateStoreMetrics;

/// [`BackwardUserIterator`] can be used by user directly.
pub struct BackwardUserIterator {
    /// Inner table iterator.
    iterator:
        UnorderedMergeIteratorInner<UserIteratorPayloadType<Backward, BackwardSstableIterator>>,

    /// We just met a new key
    just_met_new_key: bool,

    /// Last user key
    last_key: Vec<u8>,

    /// Last user value
    last_val: Vec<u8>,

    /// Last user key value is deleted
    last_delete: bool,

    /// Flag for whether the iterator reaches over the right end of the range.
    out_of_range: bool,

    /// Start and end bounds of user key.
    key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),

    /// Only reads values if `epoch <= self.read_epoch`.
    read_epoch: Epoch,

    /// Only reads values if `ts > self.min_epoch`. use for ttl
    min_epoch: Epoch,

    /// Ensures the SSTs needed by `iterator` won't be vacuumed.
    _version: Option<Arc<PinnedVersion>>,
}

impl BackwardUserIterator {
    /// Creates [`BackwardUserIterator`] with maximum epoch.
    #[cfg(test)]
    pub(crate) fn new(
        iterator: UnorderedMergeIteratorInner<
            UserIteratorPayloadType<Backward, BackwardSstableIterator>,
        >,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Self {
        Self::with_epoch(iterator, key_range, Epoch::MAX, 0, None)
    }

    /// Creates [`BackwardUserIterator`] with given `read_epoch`.
    pub(crate) fn with_epoch(
        iterator: UnorderedMergeIteratorInner<
            UserIteratorPayloadType<Backward, BackwardSstableIterator>,
        >,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_epoch: u64,
        min_epoch: u64,
        version: Option<Arc<PinnedVersion>>,
    ) -> Self {
        Self {
            iterator,
            out_of_range: false,
            key_range,
            just_met_new_key: false,
            last_key: Vec::new(),
            last_val: Vec::new(),
            last_delete: true,
            read_epoch,
            min_epoch,
            _version: version,
        }
    }

    fn out_of_range(&self, key: &[u8]) -> bool {
        match &self.key_range.0 {
            Included(begin_key) => key < begin_key.as_slice(),
            Excluded(begin_key) => key <= begin_key.as_slice(),
            Unbounded => false,
        }
    }

    fn reset(&mut self) {
        self.last_key.clear();
        self.just_met_new_key = false;
        self.last_delete = true;
        self.out_of_range = false;
    }

    /// Gets the iterator move to the next step.
    ///
    /// Returned result:
    /// - if `Ok(())` is returned, it means that the iterator successfully move to the next position
    ///   (may reach to the end and thus not valid)
    /// - if `Err(_) ` is returned, it means that some error happened.
    pub async fn next(&mut self) -> HummockResult<()> {
        // We need to deal with three cases:
        // 1. current key == last key.
        //    Since current key must have an epoch newer than the one of the last key,
        //    we assign current kv as the new last kv and also inherit its status of deletion, and
        // continue.
        //
        // 2. current key != last key.
        //    We have to make a decision for the last key.
        //    a. If it is not deleted, we stop.
        //    b. Otherwise, we continue to find the next new key.
        //
        // 3. `self.iterator` invalid. The case is the same as 2. However, option b is invalid now.
        // We just stop. Without further `next`, `BackwardUserIterator` is still valid.

        // We remark that whether `self.iterator` is valid and `BackwardUserIterator` is valid can
        // be different even if we leave `out_of_range` out of consideration. This diffs
        // from `UserIterator` because we always make a decision about the past key only
        // when we enter a new state, such as encountering a new key, or `self.iterator`
        // turning invalid.

        if !self.iterator.is_valid() {
            // We abuse `last_delete` to indicate that we are indeed invalid now, i.e. run out of kv
            // pairs.
            self.last_delete = true;
            return Ok(());
        }

        while self.iterator.is_valid() {
            let full_key = self.iterator.key();
            let epoch = get_epoch(full_key);
            let key = to_user_key(full_key);

            if epoch > self.min_epoch && epoch <= self.read_epoch {
                if self.just_met_new_key {
                    self.last_key.clear();
                    self.last_key.extend_from_slice(key);
                    self.just_met_new_key = false;
                    // If we encounter an out-of-range key, stop early.
                    if self.out_of_range(&self.last_key) {
                        self.out_of_range = true;
                        break;
                    }
                } else if self.last_key != key {
                    if !self.last_delete {
                        // We remark that we don't check `out_of_range` here as the other two cases
                        // covered all situation. 2(a)
                        self.just_met_new_key = true;
                        return Ok(());
                    } else {
                        // 2(b)
                        self.last_key.clear();
                        self.last_key.extend_from_slice(key);
                        // If we encounter an out-of-range key, stop early.
                        if self.out_of_range(&self.last_key) {
                            self.out_of_range = true;
                            break;
                        }
                    }
                }
                // TODO: Since the real world workload may follow power law or 20/80 rule, or
                // whatever name. We may directly seek to the next key if we have
                // been seeing the same key for too many times.

                // 1 and 2(a)
                match self.iterator.value() {
                    HummockValue::Put(val) => {
                        self.last_val.clear();
                        self.last_val.extend_from_slice(val);
                        self.last_delete = false;
                    }
                    HummockValue::Delete => {
                        self.last_delete = true;
                    }
                }
            }
            self.iterator.next().await?;
        }
        Ok(()) // not valid, EOF
    }

    /// Returns the key with the newest version. Thus no version in it, and only the `user_key` will
    /// be returned.
    ///
    /// The returned key is de-duplicated and thus it will not output the same key, unless the
    /// `rewind` or `seek` methods are called.
    ///
    /// Note: before call the function you need to ensure that the iterator is valid.
    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        self.last_key.as_slice()
    }

    /// The returned value is in the form of user value.
    ///
    /// Note: before calling the function you need to ensure that the iterator is valid.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        self.last_val.as_slice()
    }

    /// Resets the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        // Handle range scan
        match &self.key_range.1 {
            Included(end_key) => {
                let full_key = &key_with_epoch(end_key.clone(), 0);
                self.iterator.seek(full_key).await?;
            }
            Excluded(_) => unimplemented!("excluded begin key is not supported"),
            Unbounded => self.iterator.rewind().await?,
        };

        // Handle multi-version
        self.reset();
        // Handle range scan when key < begin_key
        self.next().await
    }

    /// Resets the iterating position to the first position where the key >= provided key.
    pub async fn seek(&mut self, user_key: &[u8]) -> HummockResult<()> {
        // Handle range scan when key > end_key
        let user_key = match &self.key_range.1 {
            Included(end_key) => {
                if end_key.as_slice() < user_key {
                    end_key.clone()
                } else {
                    Vec::from(user_key)
                }
            }
            Excluded(_) => unimplemented!("excluded begin key is not supported"),
            Unbounded => Vec::from(user_key),
        };
        let full_key = &key_with_epoch(user_key, 0);
        self.iterator.seek(full_key).await?;

        // Handle multi-version
        self.reset();
        // Handle range scan when key < begin_key
        self.next().await
    }

    /// Indicates whether the iterator can be used.
    pub fn is_valid(&self) -> bool {
        // Handle range scan
        // key <= end_key is guaranteed by seek/rewind function
        // We remark that there are only three cases out of four combinations:
        // (iterator valid && last_delete false) is impossible
        let has_enough_input = self.iterator.is_valid() || !self.last_delete;
        has_enough_input && (!self.out_of_range)
    }
}

impl DirectedUserIteratorBuilder for BackwardUserIterator {
    type Direction = Backward;
    type SstableIteratorType = BackwardSstableIterator;

    fn create(
        iterator_iter: impl IntoIterator<
            Item = UserIteratorPayloadType<Backward, BackwardSstableIterator>,
        >,
        stats: Arc<StateStoreMetrics>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_epoch: u64,
        min_epoch: u64,
        version: Option<Arc<PinnedVersion>>,
    ) -> DirectedUserIterator {
        let iterator = UnorderedMergeIteratorInner::new(iterator_iter, stats);
        DirectedUserIterator::Backward(BackwardUserIterator::with_epoch(
            iterator, key_range, read_epoch, min_epoch, version,
        ))
    }
}

#[expect(unused_variables)]
#[cfg(test)]
mod tests {
    use std::cmp::Reverse;
    use std::collections::BTreeMap;
    use std::ops::Bound::*;
    use std::sync::Arc;

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use risingwave_hummock_sdk::key::{prev_key, user_key};

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable_base,
        gen_iterator_test_sstable_from_kv_pair, gen_iterator_test_sstable_with_incr_epoch,
        iterator_test_key_of, iterator_test_key_of_epoch, iterator_test_value_of,
        mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::HummockIteratorUnion;
    use crate::hummock::sstable::Sstable;
    use crate::hummock::test_utils::{create_small_table_cache, gen_test_sstable};
    use crate::hummock::value::HummockValue;
    use crate::hummock::{BackwardSstableIterator, SstableStoreRef};
    use crate::monitor::StateStoreMetrics;

    #[tokio::test]
    async fn test_backward_user_basic() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let cache = create_small_table_cache();
        let handle0 = cache.insert(table0.id, table0.id, 1, Box::new(table0));
        let handle1 = cache.insert(table1.id, table1.id, 1, Box::new(table1));
        let handle2 = cache.insert(table2.id, table2.id, 1, Box::new(table2));

        let backward_iters = vec![
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
                handle1,
                sstable_store.clone(),
            )),
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
                handle2,
                sstable_store.clone(),
            )),
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(handle0, sstable_store)),
        ];

        let mi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));
        let mut ui = BackwardUserIterator::new(mi, (Unbounded, Unbounded));
        let mut i = 3 * TEST_KEYS_COUNT;
        ui.rewind().await.unwrap();
        while ui.is_valid() {
            let key = ui.key();
            let val = ui.value();
            assert_eq!(key, user_key(iterator_test_key_of(i).as_slice()));
            assert_eq!(val, iterator_test_value_of(i).as_slice());
            i -= 1;
            ui.next().await.unwrap();
            if i == 0 {
                assert!(!ui.is_valid());
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_backward_user_seek() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let cache = create_small_table_cache();
        let handle0 = cache.insert(table0.id, table0.id, 1, Box::new(table0));
        let handle1 = cache.insert(table1.id, table1.id, 1, Box::new(table1));
        let handle2 = cache.insert(table2.id, table2.id, 1, Box::new(table2));
        let backward_iters = vec![
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
                handle0,
                sstable_store.clone(),
            )),
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
                handle1,
                sstable_store.clone(),
            )),
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(handle2, sstable_store)),
        ];

        let bmi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));
        let mut bui = BackwardUserIterator::new(bmi, (Unbounded, Unbounded));

        // right edge case
        bui.seek(user_key(iterator_test_key_of(0).as_slice()))
            .await
            .unwrap();
        assert!(!bui.is_valid());

        // normal case
        bui.seek(user_key(
            iterator_test_key_of(TEST_KEYS_COUNT + 4).as_slice(),
        ))
        .await
        .unwrap();
        let k = bui.key();
        let v = bui.value();
        assert_eq!(v, iterator_test_value_of(TEST_KEYS_COUNT + 4).as_slice());
        assert_eq!(
            k,
            user_key(iterator_test_key_of(TEST_KEYS_COUNT + 4).as_slice())
        );
        bui.seek(user_key(
            iterator_test_key_of(2 * TEST_KEYS_COUNT + 5).as_slice(),
        ))
        .await
        .unwrap();
        let k = bui.key();
        let v = bui.value();
        assert_eq!(
            v,
            iterator_test_value_of(2 * TEST_KEYS_COUNT + 5).as_slice()
        );
        assert_eq!(
            k,
            user_key(iterator_test_key_of(2 * TEST_KEYS_COUNT + 5).as_slice())
        );

        // left edge case
        bui.seek(user_key(
            iterator_test_key_of(3 * TEST_KEYS_COUNT).as_slice(),
        ))
        .await
        .unwrap();
        let k = bui.key();
        let v = bui.value();
        assert_eq!(v, iterator_test_value_of(3 * TEST_KEYS_COUNT).as_slice());
        assert_eq!(
            k,
            user_key(iterator_test_key_of(3 * TEST_KEYS_COUNT).as_slice())
        );
    }

    #[tokio::test]
    async fn test_backward_user_delete() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (1, 300, HummockValue::delete()),
            (2, 100, HummockValue::put(iterator_test_value_of(2))),
        ];
        let table0 =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;

        let kv_pairs = vec![
            (1, 400, HummockValue::put(iterator_test_value_of(1))),
            (2, 200, HummockValue::delete()),
        ];
        let table1 =
            gen_iterator_test_sstable_from_kv_pair(1, kv_pairs, sstable_store.clone()).await;
        let cache = create_small_table_cache();
        let backward_iters = vec![
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
                cache.insert(table0.id, table0.id, 1, Box::new(table0)),
                sstable_store.clone(),
            )),
            HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
                cache.insert(table1.id, table1.id, 1, Box::new(table1)),
                sstable_store,
            )),
        ];
        let bmi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));
        let mut bui = BackwardUserIterator::new(bmi, (Unbounded, Unbounded));

        bui.rewind().await.unwrap();

        // verify
        let k = bui.key();
        let v = bui.value();

        assert_eq!(k, user_key(iterator_test_key_of(1).as_slice()));
        assert_eq!(v, iterator_test_value_of(1));

        // only one valid kv pair
        bui.next().await.unwrap();
        assert!(!bui.is_valid());
    }

    // left..=end
    #[tokio::test]
    async fn test_backward_user_range_inclusive() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 400, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::put(iterator_test_value_of(2))),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 300, HummockValue::put(iterator_test_value_of(7))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let sstable =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let cache = create_small_table_cache();
        let handle = cache.insert(sstable.id, sstable.id, 1, Box::new(sstable));
        let backward_iters = vec![HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
            handle,
            sstable_store,
        ))];
        let bmi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));

        let begin_key = Included(user_key(iterator_test_key_of_epoch(2, 0).as_slice()).to_vec());
        let end_key = Included(user_key(iterator_test_key_of_epoch(7, 0).as_slice()).to_vec());

        let mut bui = BackwardUserIterator::new(bmi, (begin_key, end_key));

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(7).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- after-end-range iterate -----
        bui.seek(user_key(iterator_test_key_of(8).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(7).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(user_key(iterator_test_key_of(7).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(7).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(user_key(iterator_test_key_of(2).as_slice()))
            .await
            .unwrap();
        assert!(!bui.is_valid());

        // ----- before-begin-range iterate -----
        bui.seek(user_key(iterator_test_key_of(1).as_slice()))
            .await
            .unwrap();
        assert!(!bui.is_valid());
    }

    // left..end
    #[tokio::test]
    async fn test_backward_user_range() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let sstable =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let cache = create_small_table_cache();
        let handle = cache.insert(sstable.id, sstable.id, 1, Box::new(sstable));
        let backward_iters = vec![HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
            handle,
            sstable_store,
        ))];
        let bmi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));

        let begin_key = Excluded(user_key(iterator_test_key_of_epoch(2, 0).as_slice()).to_vec());
        let end_key = Included(user_key(iterator_test_key_of_epoch(7, 0).as_slice()).to_vec());

        let mut bui = BackwardUserIterator::new(bmi, (begin_key, end_key));

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(7).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- after-bend-range iterate -----
        bui.seek(user_key(iterator_test_key_of(8).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(7).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(user_key(iterator_test_key_of(7).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(7).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(user_key(iterator_test_key_of(2).as_slice()))
            .await
            .unwrap();
        assert!(!bui.is_valid());

        // ----- begin-begin-range iterate -----
        bui.seek(user_key(iterator_test_key_of(1).as_slice()))
            .await
            .unwrap();
        assert!(!bui.is_valid());
    }

    // ..=right
    #[tokio::test]
    async fn test_backward_user_range_to_inclusive() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let sstable =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let cache = create_small_table_cache();
        let backward_iters = vec![HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
            cache.insert(sstable.id, sstable.id, 1, Box::new(sstable)),
            sstable_store,
        ))];
        let bmi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));
        let end_key = Included(user_key(iterator_test_key_of_epoch(7, 0).as_slice()).to_vec());

        let mut bui = BackwardUserIterator::new(bmi, (Unbounded, end_key));

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(1).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(user_key(iterator_test_key_of(7).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(1).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- in-range iterate -----
        bui.seek(user_key(iterator_test_key_of(6).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(1).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(user_key(iterator_test_key_of(0).as_slice()))
            .await
            .unwrap();
        assert!(!bui.is_valid());
    }

    // left..
    #[tokio::test]
    async fn test_backward_user_range_from() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let sstable =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let cache = create_small_table_cache();
        let handle = cache.insert(sstable.id, sstable.id, 1, Box::new(sstable));

        let backward_iters = vec![HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
            handle,
            sstable_store,
        ))];
        let bmi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));
        let begin_key = Included(user_key(iterator_test_key_of_epoch(2, 0).as_slice()).to_vec());

        let mut bui = BackwardUserIterator::new(bmi, (begin_key, Unbounded));

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(8).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(user_key(iterator_test_key_of(2).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- in-range iterate -----
        bui.seek(user_key(iterator_test_key_of(5).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(user_key(iterator_test_key_of(8).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(8).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- after-end-range iterate -----
        bui.seek(user_key(iterator_test_key_of(9).as_slice()))
            .await
            .unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(8).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(6).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(3).as_slice()));
        bui.next().await.unwrap();
        assert_eq!(bui.key(), user_key(iterator_test_key_of(2).as_slice()));
        bui.next().await.unwrap();
        assert!(!bui.is_valid());
    }

    fn key_from_num(num: usize) -> Vec<u8> {
        let width = 20;
        format!("{:0width$}", num, width = width)
            .as_bytes()
            .to_vec()
    }

    async fn chaos_test_case(
        sstable: Sstable,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
        truth: &ChaosTestTruth,
        sstable_store: SstableStoreRef,
    ) {
        let start_key = match &start_bound {
            Bound::Included(b) => prev_key(&b.clone()),
            Bound::Excluded(b) => b.clone(),
            Unbounded => key_from_num(0),
        };
        let end_key = match &end_bound {
            Bound::Included(b) => b.clone(),
            Unbounded => key_from_num(999999999999),
            _ => unimplemented!(),
        };
        let cache = create_small_table_cache();
        let handle = cache.insert(sstable.id, sstable.id, 1, Box::new(sstable));
        let backward_iters = vec![HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
            handle,
            sstable_store,
        ))];
        let bmi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));
        let mut bui = BackwardUserIterator::new(bmi, (start_bound, end_bound));
        let num_puts: usize = truth
            .iter()
            .map(|(key, inserts)| {
                if *key > end_key || *key <= start_key {
                    return 0;
                }
                match inserts.first_key_value().unwrap().1 {
                    HummockValue::Put(_) => 1,
                    HummockValue::Delete => 0,
                }
            })
            .reduce(|accum, item| accum + item)
            .unwrap();
        let mut num_kvs = 0;
        bui.rewind().await.unwrap();
        for (key, value) in truth.iter().rev() {
            if *key > end_key || *key <= start_key {
                continue;
            }
            let (time, value) = value.first_key_value().unwrap();
            if let HummockValue::Delete = value {
                continue;
            }
            assert!(bui.is_valid(), "num_kvs:{}", num_kvs);
            let full_key = key_with_epoch(key.clone(), time.0);
            assert_eq!(bui.key(), user_key(&full_key), "num_kvs:{}", num_kvs);
            if let HummockValue::Put(bytes) = &value {
                assert_eq!(bui.value(), bytes, "num_kvs:{}", num_kvs);
            }
            bui.next().await.unwrap();
            num_kvs += 1;
        }
        assert!(!bui.is_valid());
        assert_eq!(num_kvs, num_puts);
    }

    type ChaosTestTruth = BTreeMap<Vec<u8>, BTreeMap<Reverse<Epoch>, HummockValue<Vec<u8>>>>;

    async fn generate_chaos_test_data() -> (usize, Sstable, ChaosTestTruth, SstableStoreRef) {
        // We first generate the key value pairs.
        let mut rng = thread_rng();
        let mut truth: ChaosTestTruth = BTreeMap::new();
        let mut prev_key_number: usize = 1;
        let number_of_keys = 5000;
        for _ in 0..number_of_keys {
            let key: usize = rng.gen_range(prev_key_number..=(prev_key_number + 10));
            prev_key_number = key + 1;
            let key_bytes = key_from_num(key);
            let mut prev_time = 500;
            let num_updates = rng.gen_range(1..10usize);
            for _ in 0..num_updates {
                let time: Epoch = rng.gen_range(prev_time..=(prev_time + 1000));
                let is_delete = rng.gen_range(0..=1usize) < 1usize;
                match is_delete {
                    true => {
                        truth
                            .entry(key_bytes.clone())
                            .or_default()
                            .insert(Reverse(time), HummockValue::delete());
                    }
                    false => {
                        let value_size = rng.gen_range(100..=200);
                        let value: String = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(value_size)
                            .map(char::from)
                            .collect();
                        truth
                            .entry(key_bytes.clone())
                            .or_default()
                            .insert(Reverse(time), HummockValue::put(value.into_bytes()));
                    }
                }
                prev_time = time + 1;
            }
        }
        let sstable_store = mock_sstable_store().await;
        let sst = gen_test_sstable(
            default_builder_opt_for_test(),
            0,
            truth.iter().flat_map(|(key, inserts)| {
                inserts.iter().map(|(time, value)| {
                    let full_key = key_with_epoch(key.clone(), time.0);
                    (full_key, value.clone())
                })
            }),
            sstable_store.clone(),
        )
        .await;

        (prev_key_number, sst, truth, sstable_store)
    }

    #[tokio::test]
    async fn test_backward_user_chaos_unbounded_unbounded() {
        let (prev_key_number, sst, truth, sstable_store) = generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key);
            chaos_test_case(
                clone_sst(&sst),
                Unbounded,
                Unbounded,
                &truth,
                sstable_store.clone(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_unbounded_included() {
        let (prev_key_number, sst, truth, sstable_store) = generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key);
            chaos_test_case(
                clone_sst(&sst),
                Unbounded,
                Included(end_key_bytes.clone()),
                &truth,
                sstable_store.clone(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_included_unbounded() {
        let (prev_key_number, sst, truth, sstable_store) = generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key);
            chaos_test_case(
                clone_sst(&sst),
                Included(begin_key_bytes.clone()),
                Unbounded,
                &truth,
                sstable_store.clone(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_excluded_unbounded() {
        let (prev_key_number, sst, truth, sstable_store) = generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key);
            chaos_test_case(
                clone_sst(&sst),
                Excluded(begin_key_bytes.clone()),
                Unbounded,
                &truth,
                sstable_store.clone(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_included_included() {
        let (prev_key_number, sst, truth, sstable_store) = generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key);
            chaos_test_case(
                clone_sst(&sst),
                Included(begin_key_bytes.clone()),
                Included(end_key_bytes.clone()),
                &truth,
                sstable_store.clone(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_excluded_included() {
        let (prev_key_number, sst, truth, sstable_store) = generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key);
            chaos_test_case(
                clone_sst(&sst),
                Excluded(begin_key_bytes),
                Included(end_key_bytes),
                &truth,
                sstable_store.clone(),
            )
            .await;
        }
    }

    fn clone_sst(sst: &Sstable) -> Sstable {
        Sstable {
            id: sst.id,
            meta: sst.meta.clone(),
            blocks: vec![],
        }
    }

    #[tokio::test]
    async fn test_min_epoch() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_with_incr_epoch(
            0,
            default_builder_opt_for_test(),
            |x| x * 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
            1,
        )
        .await;

        let cache = create_small_table_cache();
        let handle0 = cache.insert(table0.id, table0.id, 1, Box::new(table0));

        let backward_iters = vec![HummockIteratorUnion::Fourth(BackwardSstableIterator::new(
            handle0,
            sstable_store,
        ))];

        let min_epoch = (TEST_KEYS_COUNT / 5) as u64;
        let mi =
            UnorderedMergeIteratorInner::new(backward_iters, Arc::new(StateStoreMetrics::unused()));
        let mut ui =
            BackwardUserIterator::with_epoch(mi, (Unbounded, Unbounded), u64::MAX, min_epoch, None);
        ui.rewind().await.unwrap();

        let mut i = 0;
        while ui.is_valid() {
            let key = ui.key();
            let key_epoch = get_epoch(key);
            assert!(key_epoch > min_epoch);

            i += 1;
            ui.next().await.unwrap();
        }

        let expect_count = TEST_KEYS_COUNT - min_epoch as usize;
        assert_eq!(i, expect_count);
    }
}
