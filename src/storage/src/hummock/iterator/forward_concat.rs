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

use crate::hummock::iterator::concat_inner::ConcatIteratorInner;
use crate::hummock::SstableIterator;

/// Iterates on multiple non-overlapping tables.
pub type ConcatIterator = ConcatIteratorInner<SstableIterator>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable_base, iterator_test_key_of,
        iterator_test_value_of, mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::sstable::SstableIteratorReadOptions;

    #[tokio::test]
    async fn test_concat_iterator() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT * 2 + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = ConcatIterator::new(
            vec![
                table0.get_sstable_info(),
                table1.get_sstable_info(),
                table2.get_sstable_info(),
            ],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );
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

        iter.rewind().await.unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(0).as_slice());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(0).as_slice()
        );
    }

    #[tokio::test]
    async fn test_concat_seek() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT * 2 + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = ConcatIterator::new(
            vec![
                table0.get_sstable_info(),
                table1.get_sstable_info(),
                table2.get_sstable_info(),
            ],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );

        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT + 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT + 1).as_slice());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT + 1).as_slice()
        );

        // Left edge case
        iter.seek(iterator_test_key_of(0).as_slice()).await.unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(0).as_slice());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(0).as_slice()
        );

        // Right edge case
        iter.seek(iterator_test_key_of(3 * TEST_KEYS_COUNT - 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(
            key,
            iterator_test_key_of(3 * TEST_KEYS_COUNT - 1).as_slice()
        );
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(3 * TEST_KEYS_COUNT - 1).as_slice()
        );

        // Right overflow case
        iter.seek(iterator_test_key_of(3 * TEST_KEYS_COUNT).as_slice())
            .await
            .unwrap();
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_concat_seek_not_exists() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| (TEST_KEYS_COUNT + x) * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| (2 * TEST_KEYS_COUNT + x) * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = ConcatIterator::new(
            vec![
                table0.get_sstable_info(),
                table1.get_sstable_info(),
                table2.get_sstable_info(),
            ],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );

        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT + 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT + 2).as_slice());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT + 2).as_slice()
        );

        // seek the last of table1
        iter.seek(iterator_test_key_of((TEST_KEYS_COUNT + 9) * 2 + 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT * 4).as_slice());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT * 4).as_slice()
        );
    }
}
