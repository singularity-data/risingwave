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

use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::cache::LruCache;

use super::LRU_SHARD_BITS;
use crate::hummock::TieredCacheKey;

pub type Buffer<K> = Arc<LruCache<K, Vec<u8>>>;

struct TwoLevelBufferCore<K>
where
    K: TieredCacheKey,
{
    active_buffer: Buffer<K>,
    frozen_buffer: Buffer<K>,
}

impl<K> TwoLevelBufferCore<K>
where
    K: TieredCacheKey,
{
    fn swap(&mut self) {
        // Swap fields of `&mut self` to avoid the borrow checker complaining.
        std::mem::swap(&mut self.active_buffer, &mut self.frozen_buffer);
    }
}

#[derive(Clone)]
pub struct TwoLevelBuffer<K>
where
    K: TieredCacheKey,
{
    capacity: usize,
    core: Arc<RwLock<TwoLevelBufferCore<K>>>,
}

impl<K> TwoLevelBuffer<K>
where
    K: TieredCacheKey,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            core: Arc::new(RwLock::new(TwoLevelBufferCore {
                active_buffer: Arc::new(LruCache::new(LRU_SHARD_BITS, capacity)),
                frozen_buffer: Arc::new(LruCache::new(LRU_SHARD_BITS, capacity)),
            })),
        }
    }

    pub fn insert(&self, hash: u64, key: K, charge: usize, value: Vec<u8>) {
        let core = self.core.read();
        core.active_buffer.insert(key, hash, charge, value);
    }

    pub fn get(&self, hash: u64, key: &K) -> Option<Vec<u8>> {
        let core = self.core.read();
        if let Some(entry) = core.active_buffer.lookup(hash, key) {
            return Some(entry.value().clone());
        }
        if let Some(entry) = core.frozen_buffer.lookup(hash, key) {
            return Some(entry.value().clone());
        }
        None
    }

    pub fn erase(&self, hash: u64, key: &K) {
        let core = self.core.read();
        core.active_buffer.erase(hash, key);
        core.frozen_buffer.erase(hash, key);
    }

    pub fn active(&self) -> Buffer<K> {
        self.core.read().active_buffer.clone()
    }

    pub fn frozen(&self) -> Buffer<K> {
        self.core.read().frozen_buffer.clone()
    }

    pub fn swap(&self) {
        self.core.write().swap();
    }

    pub fn rotate(&self) -> Buffer<K> {
        let mut buffer = Arc::new(LruCache::new(LRU_SHARD_BITS, self.capacity));
        let mut core = self.core.write();
        std::mem::swap(&mut buffer, &mut core.active_buffer);
        std::mem::swap(&mut buffer, &mut core.frozen_buffer);
        buffer
    }
}
