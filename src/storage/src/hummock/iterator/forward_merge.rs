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
mod test {
    use std::sync::Arc;

    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable_base,
        gen_merge_iterator_interleave_test_sstable_iters, iterator_test_key_of,
        iterator_test_value_of, mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::{
        Forward, HummockIterator, HummockIteratorUnion, OrderedMergeIteratorInner,
        UnorderedMergeIteratorInner,
    };
    use crate::hummock::sstable::{
        SstableIterator, SstableIteratorReadOptions, SstableIteratorType,
    };
    use crate::hummock::test_utils::{create_small_table_cache, gen_test_sstable};
    use crate::hummock::value::HummockValue;
    use crate::monitor::StateStoreMetrics;

    #[tokio::test]
    async fn test_merge_basic() {
        let mut unordered_iter: HummockIteratorUnion<
            Forward,
            UnorderedMergeIteratorInner<SstableIterator>,
            OrderedMergeIteratorInner<SstableIterator>,
        > = HummockIteratorUnion::First(UnorderedMergeIteratorInner::new(
            gen_merge_iterator_interleave_test_sstable_iters(TEST_KEYS_COUNT, 3).await,
            Arc::new(StateStoreMetrics::unused()),
        ));
        let mut ordered_iter: HummockIteratorUnion<
            Forward,
            UnorderedMergeIteratorInner<SstableIterator>,
            OrderedMergeIteratorInner<SstableIterator>,
        > = HummockIteratorUnion::Second(OrderedMergeIteratorInner::new(
            gen_merge_iterator_interleave_test_sstable_iters(TEST_KEYS_COUNT, 3).await,
            Arc::new(StateStoreMetrics::unused()),
        ));

        // Test both ordered and unordered iterators
        let test_iters = vec![&mut unordered_iter, &mut ordered_iter];
        for iter in test_iters {
            let mut i = 0;
            iter.rewind().await.unwrap();
            while iter.is_valid() {
                let key = iter.key();
                let val = iter.value();
                assert_eq!(key, iterator_test_key_of(i).as_slice());
                assert_eq!(
                    val.into_user_value().unwrap(),
                    iterator_test_value_of(i).as_slice()
                );
                i += 1;
                iter.next().await.unwrap();
                if i == TEST_KEYS_COUNT * 3 {
                    assert!(!iter.is_valid());
                    break;
                }
            }
            assert!(i >= TEST_KEYS_COUNT * 3);
        }
    }

    #[tokio::test]
    async fn test_merge_seek() {
        let mut unordered_iter: HummockIteratorUnion<
            Forward,
            UnorderedMergeIteratorInner<SstableIterator>,
            OrderedMergeIteratorInner<SstableIterator>,
        > = HummockIteratorUnion::First(UnorderedMergeIteratorInner::new(
            gen_merge_iterator_interleave_test_sstable_iters(TEST_KEYS_COUNT, 3).await,
            Arc::new(StateStoreMetrics::unused()),
        ));
        let mut ordered_iter: HummockIteratorUnion<
            Forward,
            UnorderedMergeIteratorInner<SstableIterator>,
            OrderedMergeIteratorInner<SstableIterator>,
        > = HummockIteratorUnion::Second(OrderedMergeIteratorInner::new(
            gen_merge_iterator_interleave_test_sstable_iters(TEST_KEYS_COUNT, 3).await,
            Arc::new(StateStoreMetrics::unused()),
        ));

        // Test both ordered and unordered iterators
        let test_iters = vec![&mut unordered_iter, &mut ordered_iter];

        for iter in test_iters {
            // right edge case
            iter.seek(iterator_test_key_of(TEST_KEYS_COUNT * 3).as_slice())
                .await
                .unwrap();
            assert!(!iter.is_valid());

            // normal case
            iter.seek(iterator_test_key_of(TEST_KEYS_COUNT * 2 + 5).as_slice())
                .await
                .unwrap();
            let k = iter.key();
            let v = iter.value();
            assert_eq!(
                v.into_user_value().unwrap(),
                iterator_test_value_of(TEST_KEYS_COUNT * 2 + 5).as_slice()
            );
            assert_eq!(k, iterator_test_key_of(TEST_KEYS_COUNT * 2 + 5).as_slice());

            iter.seek(iterator_test_key_of(17).as_slice())
                .await
                .unwrap();
            let k = iter.key();
            let v = iter.value();
            assert_eq!(
                v.into_user_value().unwrap(),
                iterator_test_value_of(TEST_KEYS_COUNT + 7).as_slice()
            );
            assert_eq!(k, iterator_test_key_of(TEST_KEYS_COUNT + 7).as_slice());

            // left edge case
            iter.seek(iterator_test_key_of(0).as_slice()).await.unwrap();
            let k = iter.key();
            let v = iter.value();
            assert_eq!(
                v.into_user_value().unwrap(),
                iterator_test_value_of(0).as_slice()
            );
            assert_eq!(k, iterator_test_key_of(0).as_slice());
        }
    }

    #[tokio::test]
    async fn test_merge_invalidate_reset() {
        let sstable_store = mock_sstable_store().await;
        let read_options = Arc::new(SstableIteratorReadOptions::default());
        let table0 = Box::new(
            gen_iterator_test_sstable_base(
                0,
                default_builder_opt_for_test(),
                |x| x,
                sstable_store.clone(),
                TEST_KEYS_COUNT,
            )
            .await,
        );
        let table1 = Box::new(
            gen_iterator_test_sstable_base(
                1,
                default_builder_opt_for_test(),
                |x| TEST_KEYS_COUNT + x,
                sstable_store.clone(),
                TEST_KEYS_COUNT,
            )
            .await,
        );

        let cache = create_small_table_cache();

        let mut unordered_iter: HummockIteratorUnion<
            Forward,
            UnorderedMergeIteratorInner<SstableIterator>,
            OrderedMergeIteratorInner<SstableIterator>,
        > = HummockIteratorUnion::First(UnorderedMergeIteratorInner::new(
            vec![
                SstableIterator::create(
                    cache.insert(table0.id, table0.id, 1, table0),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
                SstableIterator::create(
                    cache.insert(table1.id, table1.id, 1, table1),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
            ],
            Arc::new(StateStoreMetrics::unused()),
        ));
        let mut ordered_iter: HummockIteratorUnion<
            Forward,
            UnorderedMergeIteratorInner<SstableIterator>,
            OrderedMergeIteratorInner<SstableIterator>,
        > = HummockIteratorUnion::Second(OrderedMergeIteratorInner::new(
            vec![
                SstableIterator::create(
                    cache.lookup(0, &0).unwrap(),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
                SstableIterator::create(
                    cache.lookup(1, &1).unwrap(),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
            ],
            Arc::new(StateStoreMetrics::unused()),
        ));

        // Test both ordered and unordered iterators
        let test_iters = vec![&mut unordered_iter, &mut ordered_iter];

        for iter in test_iters {
            iter.rewind().await.unwrap();
            let mut count = 0;
            while iter.is_valid() {
                count += 1;
                iter.next().await.unwrap();
            }
            assert_eq!(count, TEST_KEYS_COUNT * 2);

            iter.rewind().await.unwrap();
            let mut count = 0;
            while iter.is_valid() {
                count += 1;
                iter.next().await.unwrap();
            }
            assert_eq!(count, TEST_KEYS_COUNT * 2);
        }
    }

    #[tokio::test]
    async fn test_ordered_merge_iter() {
        let sstable_store = mock_sstable_store().await;
        let read_options = Arc::new(SstableIteratorReadOptions::default());

        let non_overlapped_sstable = Box::new(
            gen_test_sstable(
                default_builder_opt_for_test(),
                0,
                (0..TEST_KEYS_COUNT).filter(|x| x % 3 == 0).map(|x| {
                    (
                        iterator_test_key_of(x),
                        HummockValue::put(format!("non_overlapped_{}", x).as_bytes().to_vec()),
                    )
                }),
                sstable_store.clone(),
            )
            .await,
        );

        let overlapped_old_sstable = Box::new(
            gen_test_sstable(
                default_builder_opt_for_test(),
                1,
                (0..TEST_KEYS_COUNT).filter(|x| x % 3 != 0).map(|x| {
                    (
                        iterator_test_key_of(x),
                        HummockValue::put(format!("overlapped_old_{}", x).as_bytes().to_vec()),
                    )
                }),
                sstable_store.clone(),
            )
            .await,
        );

        let overlapped_new_sstable = Box::new(
            gen_test_sstable(
                default_builder_opt_for_test(),
                2,
                (0..TEST_KEYS_COUNT).filter(|x| x % 3 == 1).map(|x| {
                    (
                        iterator_test_key_of(x),
                        HummockValue::put(format!("overlapped_new_{}", x).as_bytes().to_vec()),
                    )
                }),
                sstable_store.clone(),
            )
            .await,
        );
        let cache = create_small_table_cache();

        let mut iter = OrderedMergeIteratorInner::new(
            vec![
                SstableIterator::create(
                    cache.insert(
                        non_overlapped_sstable.id,
                        non_overlapped_sstable.id,
                        1,
                        non_overlapped_sstable,
                    ),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
                SstableIterator::create(
                    cache.insert(
                        overlapped_new_sstable.id,
                        overlapped_new_sstable.id,
                        1,
                        overlapped_new_sstable,
                    ),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
                SstableIterator::create(
                    cache.insert(
                        overlapped_old_sstable.id,
                        overlapped_old_sstable.id,
                        1,
                        overlapped_old_sstable,
                    ),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
            ],
            Arc::new(StateStoreMetrics::unused()),
        );

        iter.rewind().await.unwrap();

        let mut count = 0;

        while iter.is_valid() {
            assert_eq!(iter.key(), iterator_test_key_of(count));
            let expected_value = match count % 3 {
                0 => format!("non_overlapped_{}", count).as_bytes().to_vec(),
                1 => format!("overlapped_new_{}", count).as_bytes().to_vec(),
                2 => format!("overlapped_old_{}", count).as_bytes().to_vec(),
                _ => unreachable!(),
            };
            assert_eq!(iter.value(), HummockValue::put(expected_value.as_slice()));
            count += 1;
            iter.next().await.unwrap();
        }

        assert_eq!(count, TEST_KEYS_COUNT);
    }
}
