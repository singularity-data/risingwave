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

//! Hummock state store's SST builder, format and iterator

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
mod block;

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub use block::*;
mod block_iterator;
pub use block_iterator::*;
mod bloom;
use bloom::Bloom;
pub mod builder;
pub use builder::*;
mod forward_sstable_iterator;
pub mod multi_builder;
use bytes::{Buf, BufMut, Bytes};
use fail::fail_point;
pub use forward_sstable_iterator::*;
mod backward_sstable_iterator;
pub use backward_sstable_iterator::*;
use risingwave_hummock_sdk::HummockSstableId;
#[cfg(test)]
use risingwave_pb::hummock::{KeyRange, SstableInfo};

mod sstable_id_manager;
mod utils;
pub use sstable_id_manager::*;
pub use utils::CompressionAlgorithm;
use utils::{get_length_prefixed_slice, put_length_prefixed_slice};

use self::utils::{xxhash64_checksum, xxhash64_verify};
use super::{HummockError, HummockResult};

const DEFAULT_META_BUFFER_CAPACITY: usize = 4096;
const MAGIC: u32 = 0x5785ab73;
const VERSION: u32 = 1;

/// [`Sstable`] is a handle for accessing SST.
pub struct Sstable {
    pub id: HummockSstableId,
    pub meta: SstableMeta,
    pub blocks: Vec<Arc<Block>>,
}

impl Debug for Sstable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcExchangeSource")
            .field("id", &self.id)
            .field("meta", &self.meta)
            .finish()
    }
}

impl Sstable {
    pub fn new(id: HummockSstableId, meta: SstableMeta) -> Self {
        Self {
            id,
            meta,
            blocks: vec![],
        }
    }

    pub fn new_with_data(
        id: HummockSstableId,
        meta: SstableMeta,
        data: Bytes,
    ) -> HummockResult<Self> {
        let mut blocks = vec![];
        for block_meta in &meta.block_metas {
            let end_offset = (block_meta.offset + block_meta.len) as usize;
            let block = Block::decode(data.slice(block_meta.offset as usize..end_offset))?;
            blocks.push(Arc::new(block));
        }
        Ok(Self { id, meta, blocks })
    }

    pub fn has_bloom_filter(&self) -> bool {
        !self.meta.bloom_filter.is_empty()
    }

    pub fn surely_not_have_user_key(&self, user_key: &[u8]) -> bool {
        let enable_bloom_filter: fn() -> bool = || {
            fail_point!("disable_bloom_filter", |_| false);
            true
        };
        if enable_bloom_filter() && self.has_bloom_filter() {
            let hash = farmhash::fingerprint32(user_key);
            let bloom = Bloom::new(&self.meta.bloom_filter);
            bloom.surely_not_have_hash(hash)
        } else {
            false
        }
    }

    pub fn block_count(&self) -> usize {
        self.meta.block_metas.len()
    }

    #[inline]
    pub fn encoded_size(&self) -> usize {
        8 /* id */ + self.meta.encoded_size() + self.blocks.iter().map(|block|block.data().len()).sum::<usize>()
    }

    #[cfg(test)]
    pub fn get_sstable_info(&self) -> SstableInfo {
        SstableInfo {
            id: self.id,
            key_range: Some(KeyRange {
                left: self.meta.smallest_key.clone(),
                right: self.meta.largest_key.clone(),
                inf: false,
            }),
            file_size: self.meta.estimated_size as u64,
            table_ids: vec![],
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlockMeta {
    pub smallest_key: Vec<u8>,
    pub offset: u32,
    pub len: u32,
}

impl BlockMeta {
    /// Format:
    ///
    /// ```plain
    /// | offset (4B) | len (4B) | smallest key len (4B) | smallest key |
    /// ```
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.len);
        put_length_prefixed_slice(buf, &self.smallest_key);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let offset = buf.get_u32_le();
        let len = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        Self {
            smallest_key,
            offset,
            len,
        }
    }

    #[inline]
    pub fn encoded_size(&self) -> usize {
        12 /* offset + len + key len */ + self.smallest_key.len()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SstableMeta {
    pub block_metas: Vec<BlockMeta>,
    pub bloom_filter: Vec<u8>,
    pub estimated_size: u32,
    pub key_count: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    /// Format version, for further compatibility.
    pub version: u32,
}

impl SstableMeta {
    /// Format:
    ///
    /// ```plain
    /// | N (4B) |
    /// | block meta 0 | ... | block meta N-1 |
    /// | bloom filter len (4B) | bloom filter |
    /// | estimated size (4B) | key count (4B) |
    /// | smallest key len (4B) | smallest key |
    /// | largest key len (4B) | largest key |
    /// | checksum (8B) | version (4B) | magic (4B) |
    /// ```
    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(DEFAULT_META_BUFFER_CAPACITY);
        buf.put_u32_le(self.block_metas.len() as u32);
        for block_meta in &self.block_metas {
            block_meta.encode(&mut buf);
        }
        put_length_prefixed_slice(&mut buf, &self.bloom_filter);
        buf.put_u32_le(self.estimated_size as u32);
        buf.put_u32_le(self.key_count as u32);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        put_length_prefixed_slice(&mut buf, &self.largest_key);
        let checksum = xxhash64_checksum(&buf);
        buf.put_u64_le(checksum);
        buf.put_u32_le(VERSION);
        buf.put_u32_le(MAGIC);
        buf
    }

    pub fn decode(buf: &mut &[u8]) -> HummockResult<Self> {
        let mut cursor = buf.len();

        cursor -= 4;
        let magic = (&buf[cursor..]).get_u32_le();
        if magic != MAGIC {
            return Err(HummockError::magic_mismatch(MAGIC, magic));
        }

        cursor -= 4;
        let version = (&buf[cursor..cursor + 4]).get_u32_le();
        if version != VERSION {
            return Err(HummockError::invalid_format_version(version));
        }

        cursor -= 8;
        let checksum = (&buf[cursor..cursor + 8]).get_u64_le();
        let buf = &mut &buf[..cursor];
        xxhash64_verify(buf, checksum)?;

        let block_meta_count = buf.get_u32_le() as usize;
        let mut block_metas = Vec::with_capacity(block_meta_count);
        for _ in 0..block_meta_count {
            block_metas.push(BlockMeta::decode(buf));
        }
        let bloom_filter = get_length_prefixed_slice(buf);
        let estimated_size = buf.get_u32_le();
        let key_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        let largest_key = get_length_prefixed_slice(buf);

        Ok(Self {
            block_metas,
            bloom_filter,
            estimated_size,
            key_count,
            smallest_key,
            largest_key,
            version,
        })
    }

    #[inline]
    pub fn encoded_size(&self) -> usize {
        4 // block meta count
            + self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.encoded_size())
            .sum::<usize>()
            + 4 // bloom filter len
            + self.bloom_filter.len()
            + 4 // estimated size
            + 4 // key count
            + 4 // key len
            + self.smallest_key.len()
            + 4 // key len
            + self.largest_key.len()
            + 8 // checksum
            + 4 // version
            + 4 // magic
    }
}

#[derive(Default)]
pub struct SstableIteratorReadOptions {
    pub prefetch: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_sstable_meta_enc_dec() {
        let meta = SstableMeta {
            block_metas: vec![
                BlockMeta {
                    smallest_key: b"0-smallest-key".to_vec(),
                    offset: 0,
                    len: 100,
                },
                BlockMeta {
                    smallest_key: b"5-some-key".to_vec(),
                    offset: 100,
                    len: 100,
                },
            ],
            bloom_filter: b"0123456789".to_vec(),
            estimated_size: 123,
            key_count: 123,
            smallest_key: b"0-smallest-key".to_vec(),
            largest_key: b"9-largest-key".to_vec(),
            version: VERSION,
        };
        let buf = meta.encode_to_bytes();
        let decoded_meta = SstableMeta::decode(&mut &buf[..]).unwrap();
        assert_eq!(decoded_meta, meta);
    }
}
