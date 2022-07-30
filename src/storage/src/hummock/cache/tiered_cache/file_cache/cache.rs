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

use std::collections::hash_map::RandomState;
use std::sync::Arc;
use std::u8;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::cache::LruCache;
use tokio::sync::Notify;

use super::buffer::TwoLevelBuffer;
use super::coding::HashBuilder;
use super::error::Result;
use super::meta::SlotId;
use super::store::{Store, StoreOptions, StoreRef};
use super::{utils, LRU_SHARD_BITS};
use crate::hummock::TieredCacheKey;

pub struct FileCacheOptions {
    pub dir: String,
    pub capacity: usize,
    pub total_buffer_capacity: usize,
    pub cache_file_fallocate_unit: usize,

    pub flush_buffer_hooks: Vec<Arc<dyn FlushBufferHook>>,
}

#[async_trait]
pub trait FlushBufferHook: Send + Sync + 'static {
    async fn pre_flush(&self) -> Result<()> {
        Ok(())
    }

    async fn post_flush(&self, _bytes: usize) -> Result<()> {
        Ok(())
    }
}

struct BufferFlusher<K, S>
where
    K: TieredCacheKey,
    S: HashBuilder,
{
    buffer: TwoLevelBuffer<K>,
    store: StoreRef<K>,
    indices: Arc<LruCache<K, SlotId>>,
    notifier: Arc<Notify>,

    hash_builder: S,

    hooks: Vec<Arc<dyn FlushBufferHook>>,
}

impl<K, S> BufferFlusher<K, S>
where
    K: TieredCacheKey,
    S: HashBuilder,
{
    async fn run(&self) -> Result<()> {
        loop {
            self.notifier.notified().await;

            for hook in &self.hooks {
                hook.pre_flush().await?;
            }

            let frozen = self.buffer.frozen();

            let mut batch = Vec::new();
            // TODO(MrCroxx): Avoid clone here?
            frozen.for_all(|key, value| batch.push((key.clone(), value.clone())));

            let mut bytes = 0;
            if batch.is_empty() {
                // Avoid allocate a new buffer.
                self.buffer.swap();
            } else {
                let slots = self.store.insert(&batch).await?;

                for ((key, value), slot) in batch.into_iter().zip_eq(slots.into_iter()) {
                    let hash = self.hash_builder.hash_one(&key);
                    self.indices.insert(
                        key,
                        hash,
                        utils::align_up(self.store.block_size(), value.len()),
                        slot,
                    );
                    bytes += value.len();
                }

                self.buffer.rotate();
            }

            for hook in &self.hooks {
                hook.post_flush(bytes).await?;
            }
        }
    }
}

#[derive(Clone)]
pub struct FileCache<K, S = RandomState>
where
    K: TieredCacheKey,
    S: HashBuilder,
{
    hash_builder: S,

    indices: Arc<LruCache<K, SlotId>>,

    store: StoreRef<K>,

    buffer: TwoLevelBuffer<K>,
    buffer_flusher_notifier: Arc<Notify>,
}

impl<K> FileCache<K, RandomState>
where
    K: TieredCacheKey,
{
    pub async fn open(options: FileCacheOptions) -> Result<Self> {
        let hash_builder = RandomState::new();
        Self::open_with_hasher(options, hash_builder).await
    }
}

impl<K, S> FileCache<K, S>
where
    K: TieredCacheKey,
    S: HashBuilder,
{
    pub async fn open_with_hasher(options: FileCacheOptions, hash_builder: S) -> Result<Self> {
        let buffer_capacity = options.total_buffer_capacity / 2;

        let store = Store::open(StoreOptions {
            dir: options.dir,
            capacity: options.capacity,
            buffer_capacity,
            cache_file_fallocate_unit: options.cache_file_fallocate_unit,
        })
        .await?;
        let store = Arc::new(store);

        let indices = Arc::new(LruCache::with_event_listener(
            LRU_SHARD_BITS,
            options.capacity,
            Some(store.clone()),
        ));
        store.restore(&indices, &hash_builder)?;

        let buffer = TwoLevelBuffer::new(buffer_capacity);
        let buffer_flusher_notifier = Arc::new(Notify::new());

        let buffer_flusher = BufferFlusher {
            buffer: buffer.clone(),
            store: store.clone(),
            indices: indices.clone(),
            notifier: buffer_flusher_notifier.clone(),

            hash_builder: hash_builder.clone(),

            hooks: options.flush_buffer_hooks,
        };
        // TODO(MrCroxx): Graceful shutdown.
        let _handle = tokio::task::spawn(async move {
            if let Err(e) = buffer_flusher.run().await {
                panic!("error raised within file cache buffer flusher: {}", e);
            }
        });

        Ok(Self {
            hash_builder,

            indices,

            store,

            buffer,
            buffer_flusher_notifier,
        })
    }

    pub fn insert(&self, key: K, value: Vec<u8>) -> Result<()> {
        let hash = self.hash_builder.hash_one(&key);
        self.buffer.insert(hash, key, value.len(), value);

        self.buffer_flusher_notifier.notify_one();
        Ok(())
    }

    pub async fn get(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let hash = self.hash_builder.hash_one(key);
        if let Some(value) = self.buffer.get(hash, key) {
            return Ok(Some(value));
        }

        if let Some(entry) = self.indices.lookup(hash, key) {
            let slot = *entry.value();
            let value = self.store.get(slot).await?;
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn erase(&self, key: &K) -> Result<()> {
        let hash = self.hash_builder.hash_one(key);
        self.buffer.erase(hash, key);

        if let Some(entry) = self.indices.lookup(hash, key) {
            let slot = *entry.value();
            self.indices.erase(hash, key);
            self.store.erase(slot).unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::path::Path;

    use super::super::test_utils::{datasize, key, FlushHolder, ModuloHasherBuilder, TestCacheKey};
    use super::super::utils;
    use super::*;

    const SHARDS: usize = 1 << LRU_SHARD_BITS;
    const SHARDSU8: u8 = SHARDS as u8;
    const SHARDSU64: u64 = SHARDS as u64;

    const BS: usize = 4096 * 4;
    const CAPACITY: usize = 4 * SHARDS * BS;
    const BUFFER_CAPACITY: usize = SHARDS * BS;
    const FALLOCATE_UNIT: usize = 2 * SHARDS * BS;

    const LOOPS: usize = 8;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCache<TestCacheKey>>();
    }

    fn tempdir() -> tempfile::TempDir {
        let ci: bool = std::env::var("RISINGWAVE_CI")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .expect("env $RISINGWAVE_CI must be 'true' or 'false'");

        if ci {
            tempfile::Builder::new().tempdir_in("/risingwave").unwrap()
        } else {
            tempfile::tempdir().unwrap()
        }
    }

    async fn create_file_cache_manager_for_test(
        dir: impl AsRef<Path>,
        flush_buffer_hooks: Vec<Arc<dyn FlushBufferHook>>,
    ) -> FileCache<TestCacheKey, ModuloHasherBuilder<SHARDSU8>> {
        let options = FileCacheOptions {
            dir: dir.as_ref().to_str().unwrap().to_string(),
            capacity: CAPACITY,
            total_buffer_capacity: 2 * BUFFER_CAPACITY,
            cache_file_fallocate_unit: FALLOCATE_UNIT,

            flush_buffer_hooks,
        };
        FileCache::open_with_hasher(options, ModuloHasherBuilder)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_cache_curd() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;

        cache.insert(key(1), vec![b'1'; 1234]).unwrap();

        // active
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![b'1'; 1234]));
        // frozen
        holder.trigger();
        holder.wait().await;
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![b'1'; 1234]));
        // cache file
        cache.buffer_flusher_notifier.notify_one();
        holder.trigger();
        holder.wait().await;
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![b'1'; 1234]));

        cache.erase(&key(1)).unwrap();
        assert_eq!(cache.get(&key(1)).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_cache_grow() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;

        for i in 0..SHARDSU64 * 2 {
            cache.insert(key(i), vec![b'x'; BS]).unwrap();
            assert_eq!(cache.get(&key(i)).await.unwrap(), Some(vec![b'x'; BS]));
        }

        for i in 0..SHARDSU64 {
            assert_eq!(cache.get(&key(i)).await.unwrap(), None, "i: {}", i);
        }
        for i in SHARDSU64..SHARDSU64 * 2 {
            assert_eq!(
                cache.get(&key(i)).await.unwrap(),
                Some(vec![b'x'; BS]),
                "i: {}",
                i
            );
        }

        assert_eq!(cache.store.cache_file_len(), 0);

        holder.trigger();
        holder.wait().await;
        cache.buffer_flusher_notifier.notify_one();
        holder.trigger();
        holder.wait().await;

        assert_eq!(cache.store.cache_file_len(), SHARDS * BS);
        assert_eq!(
            datasize(cache.store.cache_file_path()).unwrap(),
            utils::align_up(FALLOCATE_UNIT, SHARDS * BS)
        );

        for i in 0..SHARDSU64 {
            assert_eq!(cache.get(&key(i)).await.unwrap(), None);
        }
        for i in SHARDSU64..SHARDSU64 * 2 {
            assert_eq!(cache.get(&key(i)).await.unwrap(), Some(vec![b'x'; BS]));
        }

        for l in 0..LOOPS as u64 {
            for i in (l + 2) * SHARDSU64..(l + 3) * SHARDSU64 {
                cache.insert(key(i), vec![b'x'; BS]).unwrap();
                assert_eq!(cache.get(&key(i)).await.unwrap(), Some(vec![b'x'; BS]));
            }
            holder.trigger();
            holder.wait().await;
            cache.buffer_flusher_notifier.notify_one();
            holder.trigger();
            holder.wait().await;
        }

        assert_eq!(cache.store.cache_file_len(), 9 * SHARDS * BS);
        assert_eq!(
            datasize(cache.store.cache_file_path()).unwrap(),
            // TODO(MrCroxx): For inserting performs "append -> insert indices & punch hole",
            // the maximum file size may exceed the capacity by at most an alloc unit.
            // May refactor it later.
            utils::align_up(FALLOCATE_UNIT, CAPACITY) + FALLOCATE_UNIT - BUFFER_CAPACITY
        );

        for l in 0..2 + LOOPS as u64 - 4 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                assert_eq!(cache.get(&key(i)).await.unwrap(), None, "i: {}", i);
            }
        }

        for l in 2 + LOOPS as u64 - 4..2 + LOOPS as u64 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                assert_eq!(cache.get(&key(i)).await.unwrap(), Some(vec![b'x'; BS]));
            }
        }
    }

    #[tokio::test]
    async fn test_recovery() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;

        for l in 0..LOOPS as u64 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                cache.insert(key(i), vec![b'x'; BS]).unwrap();
                assert_eq!(cache.get(&key(i)).await.unwrap(), Some(vec![b'x'; BS]));
            }
            holder.trigger();
            holder.wait().await;
            cache.buffer_flusher_notifier.notify_one();
            holder.trigger();
            holder.wait().await;
        }

        let mut map = HashMap::new();
        for l in 0..LOOPS as u64 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                map.insert(key(i), cache.get(&key(i)).await.unwrap());
            }
        }

        drop(cache);

        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;
        for (key, slot) in map {
            assert_eq!(cache.get(&key).await.unwrap(), slot);
        }
    }
}
