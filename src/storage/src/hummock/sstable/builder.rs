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

use std::collections::BTreeSet;

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::key::{get_table_id, user_key};

use super::bloom::Bloom;
use super::utils::CompressionAlgorithm;
use super::{
    BlockBuilder, BlockBuilderOptions, BlockMeta, SstableMeta, DEFAULT_BLOCK_SIZE,
    DEFAULT_ENTRY_SIZE, DEFAULT_RESTART_INTERVAL, VERSION,
};
use crate::hummock::value::HummockValue;

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.1;

#[derive(Clone, Debug)]
pub struct SstableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// Restart point interval.
    pub restart_interval: usize,
    /// False positive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
}

impl From<&StorageConfig> for SstableBuilderOptions {
    fn from(options: &StorageConfig) -> SstableBuilderOptions {
        SstableBuilderOptions {
            capacity: (options.sstable_size_mb as usize) * (1 << 20),
            block_capacity: (options.block_size_kb as usize) * (1 << 10),
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            compression_algorithm: CompressionAlgorithm::None,
        }
    }
}

impl Default for SstableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorithm::None,
        }
    }
}

pub struct SstableBuilder {
    /// Options.
    options: SstableBuilderOptions,
    /// Write buffer.
    buf: BytesMut,
    /// Current block builder.
    block_builder: Option<BlockBuilder>,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,
    /// `table_id` of added keys.
    table_ids: BTreeSet<u32>,
    /// Hashes of user keys.
    user_key_hashes: Vec<u32>,
    /// Last added full key.
    last_full_key: Bytes,
    key_count: usize,
    sstable_id: u64,
}

impl SstableBuilder {
    pub fn new(sstable_id: u64, options: SstableBuilderOptions) -> Self {
        Self {
            options: options.clone(),
            buf: BytesMut::with_capacity(options.capacity),
            block_builder: None,
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            table_ids: BTreeSet::new(),
            user_key_hashes: Vec::with_capacity(options.capacity / DEFAULT_ENTRY_SIZE + 1),
            last_full_key: Bytes::default(),
            key_count: 0,
            sstable_id,
        }
    }

    /// Add kv pair to sstable.
    pub fn add(&mut self, full_key: &[u8], value: HummockValue<&[u8]>) {
        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_none() {
            self.last_full_key.clear();
            self.block_builder = Some(BlockBuilder::new(BlockBuilderOptions {
                capacity: self.options.block_capacity,
                restart_interval: self.options.restart_interval,
                compression_algorithm: self.options.compression_algorithm,
            }));
            self.block_metas.push(BlockMeta {
                offset: self.buf.len() as u32,
                len: 0,
                smallest_key: vec![],
            })
        }

        let block_builder = self.block_builder.as_mut().unwrap();

        // TODO: refine me
        let mut raw_value = BytesMut::default();
        value.encode(&mut raw_value);
        if let Some(table_id) = get_table_id(full_key) {
            self.table_ids.insert(table_id);
        }
        let raw_value = raw_value.freeze();

        block_builder.add(full_key, &raw_value);

        let user_key = user_key(full_key);
        self.user_key_hashes.push(farmhash::fingerprint32(user_key));

        if self.last_full_key.is_empty() {
            self.block_metas.last_mut().unwrap().smallest_key = full_key.to_vec();
        }
        self.last_full_key = Bytes::copy_from_slice(full_key);

        if block_builder.approximate_len() >= self.options.block_capacity {
            self.build_block();
        }
        self.key_count += 1;
    }

    /// Finish building sst.
    ///
    /// Unlike most LSM-Tree implementations, sstable meta and data are encoded separately.
    /// Both meta and data has its own object (file).
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub fn finish(mut self) -> (u64, Bytes, SstableMeta, Vec<u32>) {
        let smallest_key = self.block_metas[0].smallest_key.clone();
        let largest_key = self.last_full_key.to_vec();
        self.build_block();
        self.buf.put_u32_le(self.block_metas.len() as u32);

        let meta = SstableMeta {
            block_metas: self.block_metas,
            bloom_filter: if self.options.bloom_false_positive > 0.0 {
                let bits_per_key = Bloom::bloom_bits_per_key(
                    self.user_key_hashes.len(),
                    self.options.bloom_false_positive,
                );
                Bloom::build_from_key_hashes(&self.user_key_hashes, bits_per_key).to_vec()
            } else {
                vec![]
            },
            estimated_size: self.buf.len() as u32,
            key_count: self.key_count as u32,
            smallest_key,
            largest_key,
            version: VERSION,
        };

        (
            self.sstable_id,
            self.buf.freeze(),
            meta,
            self.table_ids.into_iter().collect(),
        )
    }

    pub fn approximate_len(&self) -> usize {
        self.buf.len() + 4
    }

    fn build_block(&mut self) {
        // Skip empty block.
        if self.block_builder.is_none() {
            return;
        }
        let mut block_meta = self.block_metas.last_mut().unwrap();
        let block = self.block_builder.take().unwrap().build();
        self.buf.put_slice(&block);
        block_meta.len = self.buf.len() as u32 - block_meta.offset;
    }

    pub fn len(&self) -> usize {
        self.user_key_hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.user_key_hashes.is_empty()
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        self.approximate_len() >= self.options.capacity
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_default_test_sstable, test_key_of, test_value_of,
        TEST_KEYS_COUNT,
    };

    #[test]
    #[should_panic]
    fn test_empty() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };

        let b = SstableBuilder::new(0, opt);

        b.finish();
    }

    #[test]
    fn test_smallest_key_and_largest_key() {
        let mut b = SstableBuilder::new(0, default_builder_opt_for_test());

        for i in 0..TEST_KEYS_COUNT {
            b.add(&test_key_of(i), HummockValue::put(&test_value_of(i)));
        }

        let (_, _, meta, _) = b.finish();

        assert_eq!(test_key_of(0), meta.smallest_key);
        assert_eq!(test_key_of(TEST_KEYS_COUNT - 1), meta.largest_key);
    }

    async fn test_with_bloom_filter(with_blooms: bool) {
        let key_count = 1000;

        let opts = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            compression_algorithm: CompressionAlgorithm::None,
        };

        // build remote table
        let sstable_store = mock_sstable_store().await;
        let table = gen_default_test_sstable(opts, 0, sstable_store).await;

        assert_eq!(table.has_bloom_filter(), with_blooms);
        for i in 0..key_count {
            let full_key = test_key_of(i);
            assert!(!table.surely_not_have_user_key(user_key(full_key.as_slice())));
        }
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        test_with_bloom_filter(false).await;
        test_with_bloom_filter(true).await;
    }
}
