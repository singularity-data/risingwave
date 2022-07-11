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

use std::clone::Clone;
use std::sync::Arc;

use bytes::Bytes;
use fail::fail_point;
use risingwave_hummock_sdk::{is_remote_sst_id, HummockSSTableId};
use risingwave_object_store::object::{get_local_path, BlockLocation, ObjectStore, ObjectStoreRef};

use super::{Block, BlockCache, Sstable, SstableMeta};
use crate::hummock::{BlockHolder, CachableEntry, HummockError, HummockResult, LruCache};
use crate::monitor::StoreLocalStatistic;

const MAX_META_CACHE_SHARD_BITS: usize = 2;
const MAX_CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.
const MIN_BUFFER_SIZE_PER_SHARD: usize = 256 * 1024 * 1024; // 256MB

pub type TableHolder = CachableEntry<HummockSSTableId, Box<Sstable>>;

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill,
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: Arc<LruCache<HummockSSTableId, Box<Sstable>>>,
}

impl SstableStore {
    pub fn new(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
    ) -> Self {
        let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
        while (meta_cache_capacity >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0 {
            shard_bits -= 1;
        }
        let meta_cache = Arc::new(LruCache::new(shard_bits, meta_cache_capacity));
        Self {
            path,
            store,
            block_cache: BlockCache::new(block_cache_capacity, MAX_CACHE_SHARD_BITS),
            meta_cache,
        }
    }

    /// For compactor, we do not need a high concurrency load for cache. Instead, we need the cache
    ///  can be evict more effective.
    pub fn for_compactor(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
    ) -> Self {
        let meta_cache = Arc::new(LruCache::new(0, meta_cache_capacity));
        Self {
            path,
            store,
            block_cache: BlockCache::new(block_cache_capacity, 2),
            meta_cache,
        }
    }

    pub async fn put(&self, sst: Sstable, data: Bytes, policy: CachePolicy) -> HummockResult<()> {
        self.put_sst_data(sst.id, data.clone()).await?;

        fail_point!("metadata_upload_err");
        if let Err(e) = self.put_meta(&sst).await {
            self.delete_sst_data(sst.id).await?;
            return Err(e);
        }

        if let CachePolicy::Fill = policy {
            self.meta_cache
                .insert(sst.id, sst.id, sst.encoded_size(), Box::new(sst));
        }

        Ok(())
    }

    async fn put_meta(&self, sst: &Sstable) -> HummockResult<()> {
        let meta_path = self.get_sst_meta_path(sst.id);
        let meta = Bytes::from(sst.meta.encode_to_bytes());
        self.store
            .upload(&meta_path, meta)
            .await
            .map_err(HummockError::object_io_error)
    }

    async fn put_sst_data(&self, sst_id: HummockSSTableId, data: Bytes) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(sst_id);
        self.store
            .upload(&data_path, data)
            .await
            .map_err(HummockError::object_io_error)
    }

    async fn delete_sst_data(&self, sst_id: HummockSSTableId) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(sst_id);
        self.store
            .delete(&data_path)
            .await
            .map_err(HummockError::object_io_error)
    }

    pub fn add_block_cache(
        &self,
        sst_id: HummockSSTableId,
        block_idx: u64,
        block_data: Bytes,
    ) -> HummockResult<()> {
        let block = Box::new(Block::decode(block_data)?);
        self.block_cache.insert(sst_id, block_idx, block);
        Ok(())
    }

    pub async fn get(
        &self,
        sst: &Sstable,
        block_index: u64,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        stats.cache_data_block_total += 1;
        let fetch_block = async {
            stats.cache_data_block_miss += 1;
            let block_meta = sst
                .meta
                .block_metas
                .get(block_index as usize)
                .ok_or_else(HummockError::invalid_block)?;
            let block_loc = BlockLocation {
                offset: block_meta.offset as usize,
                size: block_meta.len as usize,
            };
            let data_path = self.get_sst_data_path(sst.id);
            let block_data = self
                .store
                .read(&data_path, Some(block_loc))
                .await
                .map_err(HummockError::object_io_error)?;
            let block = Block::decode(block_data)?;
            Ok(Box::new(block))
        };

        let disable_cache: fn() -> bool = || {
            fail_point!("disable_block_cache", |_| true);
            false
        };

        let policy = if disable_cache() {
            CachePolicy::Disable
        } else {
            policy
        };

        match policy {
            CachePolicy::Fill => {
                self.block_cache
                    .get_or_insert_with(sst.id, block_index, fetch_block)
                    .await
            }
            CachePolicy::NotFill => match self.block_cache.get(sst.id, block_index) {
                Some(block) => Ok(block),
                None => fetch_block.await.map(BlockHolder::from_owned_block),
            },
            CachePolicy::Disable => fetch_block.await.map(BlockHolder::from_owned_block),
        }
    }

    pub fn get_sst_meta_path(&self, sst_id: HummockSSTableId) -> String {
        let mut ret = format!("{}/{}.meta", self.path, sst_id);
        if !is_remote_sst_id(sst_id) {
            ret = get_local_path(&ret);
        }
        ret
    }

    pub fn get_sst_data_path(&self, sst_id: HummockSSTableId) -> String {
        let mut ret = format!("{}/{}.data", self.path, sst_id);
        if !is_remote_sst_id(sst_id) {
            ret = get_local_path(&ret);
        }
        ret
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }

    pub fn get_meta_cache(&self) -> Arc<LruCache<HummockSSTableId, Box<Sstable>>> {
        self.meta_cache.clone()
    }

    pub fn get_block_cache(&self) -> BlockCache {
        self.block_cache.clone()
    }

    #[cfg(test)]
    pub fn clear_block_cache(&self) {
        self.block_cache.clear();
    }

    pub async fn load_table(
        &self,
        sst_id: HummockSSTableId,
        load_data: bool,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        let mut meta_data = None;
        loop {
            stats.cache_meta_block_total += 1;
            let entry = self
                .meta_cache
                .lookup_with_request_dedup::<_, HummockError, _>(sst_id, sst_id, || async {
                    stats.cache_meta_block_miss += 1;
                    let meta = match meta_data {
                        Some(data) => data,
                        None => {
                            let path = self.get_sst_meta_path(sst_id);
                            let buf = self
                                .store
                                .read(&path, None)
                                .await
                                .map_err(HummockError::object_io_error)?;
                            SstableMeta::decode(&mut &buf[..])?
                        }
                    };
                    let mut size = meta.encoded_size();
                    let sst = if load_data {
                        size = meta.estimated_size as usize;
                        let data_path = self.get_sst_data_path(sst_id);
                        let block_data = self
                            .store
                            .read(&data_path, None)
                            .await
                            .map_err(HummockError::object_io_error)?;
                        Sstable::new_with_data(sst_id, meta, block_data)?
                    } else {
                        Sstable::new(sst_id, meta)
                    };
                    Ok((Box::new(sst), size))
                })
                .await
                .map_err(|e| {
                    HummockError::other(format!(
                        "meta cache lookup request dedup get cancel: {:?}",
                        e,
                    ))
                })??;
            if !load_data || !entry.value().blocks.is_empty() {
                return Ok(entry);
            }
            // remove sst from cache to avoid multiple thread acquire the same sstable.
            meta_data = Some(entry.value().meta.clone());
            drop(entry);
            self.meta_cache.erase(sst_id, &sst_id);
        }
    }

    pub async fn sstable(
        &self,
        sst_id: HummockSSTableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        self.load_table(sst_id, false, stats).await
    }
}

pub type SstableStoreRef = Arc<SstableStore>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::iterator::{HummockIterator, ReadOptions};
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable_data};
    use crate::hummock::value::HummockValue;
    use crate::hummock::{CachePolicy, SSTableIterator, Sstable};
    use crate::monitor::StoreLocalStatistic;

    #[tokio::test]
    async fn test_read_whole_data_object() {
        let sstable_store = mock_sstable_store();
        let (data, meta, _) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            (0..100).map(|x| {
                (
                    iterator_test_key_of(x),
                    HummockValue::put(format!("overlapped_new_{}", x).as_bytes().to_vec()),
                )
            }),
        );
        let table = Sstable::new(1, meta.clone());
        sstable_store
            .put(table, data, CachePolicy::Fill)
            .await
            .unwrap();
        let mut stats = StoreLocalStatistic::default();
        let holder = sstable_store.sstable(1, &mut stats).await.unwrap();
        assert_eq!(holder.value().meta, meta);
        assert!(holder.value().blocks.is_empty());
        let holder = sstable_store.load_table(1, true, &mut stats).await.unwrap();
        assert_eq!(holder.value().meta, meta);
        assert_eq!(
            holder.value().meta.block_metas.len(),
            holder.value().blocks.len()
        );
        assert!(!holder.value().blocks.is_empty());
        let mut iter =
            SSTableIterator::new(holder, sstable_store, Arc::new(ReadOptions::default()));
        iter.rewind().await.unwrap();
        for i in 0..100 {
            let key = iter.key();
            assert_eq!(key, iterator_test_key_of(i).as_slice());
            iter.next().await.unwrap();
        }
    }
}
