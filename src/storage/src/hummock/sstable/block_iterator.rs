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
use std::ops::Range;

use bytes::BytesMut;
use risingwave_hummock_sdk::VersionedComparator;

use super::KeyPrefix;
use crate::hummock::BlockHolder;

/// [`BlockIterator`] is used to read kv pairs in a block.
pub struct BlockIterator {
    /// Block that iterates on.
    block: BlockHolder,
    /// Current restart point index.
    restart_point_index: usize,
    /// Current offset.
    offset: usize,
    /// Current key.
    key: BytesMut,
    /// Current value.
    value_range: Range<usize>,
    /// Current entry len.
    entry_len: usize,
}

impl BlockIterator {
    pub fn new(block: BlockHolder) -> Self {
        Self {
            block,
            offset: usize::MAX,
            restart_point_index: usize::MAX,
            key: BytesMut::default(),
            value_range: 0..0,
            entry_len: 0,
        }
    }

    pub fn next(&mut self) {
        assert!(self.is_valid());
        self.next_inner();
    }

    pub fn prev(&mut self) {
        assert!(self.is_valid());
        self.prev_inner();
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.key[..]
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.block.data()[self.value_range.clone()]
    }

    pub fn is_valid(&self) -> bool {
        self.offset < self.block.len()
    }

    pub fn seek_to_first(&mut self) {
        self.seek_restart_point_by_index(0);
    }

    pub fn seek_to_last(&mut self) {
        self.seek_restart_point_by_index(self.block.restart_point_len() - 1);
        self.next_until_prev_offset(self.block.len());
    }

    pub fn seek(&mut self, key: &[u8]) {
        self.seek_restart_point_by_key(key);
        self.next_until_key(key);
    }

    pub fn seek_le(&mut self, key: &[u8]) {
        self.seek_restart_point_by_key(key);
        self.next_until_key(key);
        if !self.is_valid() {
            self.seek_to_last();
        }
        self.prev_until_key(key);
    }
}

impl BlockIterator {
    /// Invalidates current state after reaching a invalid state.
    fn invalidate(&mut self) {
        self.offset = self.block.len();
        self.restart_point_index = self.block.restart_point_len();
        self.key.clear();
        self.value_range = 0..0;
        self.entry_len = 0;
    }

    /// Moves to the next entry.
    ///
    /// Note: Ensures that the current state is valid.
    fn next_inner(&mut self) {
        let offset = self.offset + self.entry_len;
        if offset >= self.block.len() {
            self.invalidate();
            return;
        }
        let prefix = self.decode_prefix_at(offset);
        self.key.truncate(prefix.overlap_len());
        self.key
            .extend_from_slice(&self.block.data()[prefix.diff_key_range()]);
        self.value_range = prefix.value_range();
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        if self.restart_point_index + 1 < self.block.restart_point_len()
            && self.offset >= self.block.restart_point(self.restart_point_index + 1) as usize
        {
            self.restart_point_index += 1;
        }
    }

    /// Moves forward until reaching the first that equals or larger than the given `key`.
    fn next_until_key(&mut self, key: &[u8]) {
        while self.is_valid()
            && VersionedComparator::compare_key(&self.key[..], key) == Ordering::Less
        {
            self.next_inner();
        }
    }

    /// Moves backward until reaching the first key that equals or smaller than the given `key`.
    fn prev_until_key(&mut self, key: &[u8]) {
        while self.is_valid()
            && VersionedComparator::compare_key(&self.key[..], key) == Ordering::Greater
        {
            self.prev_inner();
        }
    }

    /// Moves forward until the position reaches the previous position of the given `next_offset` or
    /// the last valid position if exists.
    fn next_until_prev_offset(&mut self, offset: usize) {
        while self.offset + self.entry_len < std::cmp::min(self.block.len(), offset) {
            self.next_inner();
        }
    }

    /// Moves to the previous entry.
    ///
    /// Note: Ensure that the current state is valid.
    fn prev_inner(&mut self) {
        if self.offset == 0 {
            self.invalidate();
            return;
        }
        if self.block.restart_point(self.restart_point_index) as usize == self.offset {
            self.restart_point_index -= 1;
        }
        let origin_offset = self.offset;
        self.seek_restart_point_by_index(self.restart_point_index);
        self.next_until_prev_offset(origin_offset);
    }

    /// Decodes [`KeyPrefix`] at given offset.
    fn decode_prefix_at(&self, offset: usize) -> KeyPrefix {
        KeyPrefix::decode(&mut &self.block.data()[offset..], offset)
    }

    /// Searches the restart point index that the given `key` belongs to.
    fn search_restart_point_index_by_key(&self, key: &[u8]) -> usize {
        // Find the largest restart point that restart key equals or less than the given key.
        self.block
            .search_restart_partition_point(|&probe| {
                let prefix = self.decode_prefix_at(probe as usize);
                let probe_key = &self.block.data()[prefix.diff_key_range()];
                match VersionedComparator::compare_key(probe_key, key) {
                    Ordering::Less | Ordering::Equal => true,
                    Ordering::Greater => false,
                }
            })
            .saturating_sub(1) // Prevent from underflowing when given is smaller than ther first.
    }

    /// Seeks to the restart point that the given `key` belongs to.
    fn seek_restart_point_by_key(&mut self, key: &[u8]) {
        let index = self.search_restart_point_index_by_key(key);
        self.seek_restart_point_by_index(index)
    }

    /// Seeks to the restart point by given restart point index.
    fn seek_restart_point_by_index(&mut self, index: usize) {
        let offset = self.block.restart_point(index) as usize;
        let prefix = self.decode_prefix_at(offset);
        self.key = BytesMut::from(&self.block.data()[prefix.diff_key_range()]);
        self.value_range = prefix.value_range();
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        self.restart_point_index = index;
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes};

    use super::*;
    use crate::hummock::{Block, BlockBuilder, BlockBuilderOptions};

    fn build_iterator_for_test() -> BlockIterator {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k01", 1), b"v01");
        builder.add(&full_key(b"k02", 2), b"v02");
        builder.add(&full_key(b"k04", 4), b"v04");
        builder.add(&full_key(b"k05", 5), b"v05");
        let buf = builder.build().to_vec();
        BlockIterator::new(BlockHolder::from_owned_block(Box::new(
            Block::decode(Bytes::from(buf)).unwrap(),
        )))
    }

    #[test]
    fn test_seek_first() {
        let mut it = build_iterator_for_test();
        it.seek_to_first();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());
    }

    #[test]
    fn test_seek_last() {
        let mut it = build_iterator_for_test();
        it.seek_to_last();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn test_seek_none_front() {
        let mut it = build_iterator_for_test();
        it.seek(&full_key(b"k00", 0)[..]);
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        let mut it = build_iterator_for_test();

        it.seek_le(&full_key(b"k00", 0)[..]);
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_none_back() {
        let mut it = build_iterator_for_test();
        it.seek(&full_key(b"k06", 6)[..]);
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test();
        it.seek_le(&full_key(b"k06", 6)[..]);
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn bi_direction_seek() {
        let mut it = build_iterator_for_test();
        it.seek(&full_key(b"k03", 3)[..]);
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());

        it.seek_le(&full_key(b"k03", 3)[..]);
        assert_eq!(&full_key(format!("k{:02}", 2).as_bytes(), 2)[..], it.key());
    }

    #[test]
    fn test_forward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek_to_first();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
        assert_eq!(b"v02", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], it.key());
        assert_eq!(b"v04", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());

        it.next();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek_to_last();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], it.key());
        assert_eq!(b"v04", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
        assert_eq!(b"v02", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        it.prev();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(&full_key(b"k03", 3)[..]);
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());

        it.prev();
        assert_eq!(&full_key(format!("k{:02}", 2).as_bytes(), 2)[..], it.key());

        it.next();
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());
    }

    pub fn full_key(user_key: &[u8], epoch: u64) -> Bytes {
        let mut buf = BytesMut::with_capacity(user_key.len() + 8);
        buf.put_slice(user_key);
        buf.put_u64(!epoch);
        buf.freeze()
    }
}
