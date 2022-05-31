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

use std::cmp::Eq;
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};

use lru::{DefaultHasher, LruCache};

/// A wrapper for [`LruCache`] which provides manual eviction.
pub struct EvictableHashMap<K, V, S = DefaultHasher> {
    inner: LruCache<K, V, S>,

    /// Target capacity to keep when calling `evict_to_target_cap`.
    target_cap: usize,
}

impl<K: Hash + Eq, V> EvictableHashMap<K, V> {
    /// Create a [`EvictableHashMap`] with the given target capacity.
    pub fn new(target_cap: usize) -> EvictableHashMap<K, V> {
        EvictableHashMap::with_hasher(target_cap, DefaultHasher::new())
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> EvictableHashMap<K, V, S> {
    /// Create a [`EvictableHashMap`] with the given target capacity and haser.
    pub fn with_hasher(target_cap: usize, hasher: S) -> Self {
        Self {
            inner: LruCache::unbounded_with_hasher(hasher),
            target_cap,
        }
    }

    pub fn target_cap(&self) -> usize {
        self.target_cap
    }

    /// Returns a mutable reference to the value of the key, or put with `construct` if it is not
    /// present.
    pub fn get_or_put<'a, I>(&'a mut self, key: &K, construct: I) -> &'a mut V
    where
        I: FnOnce() -> V,
        K: ToOwned<Owned = K>,
    {
        if !self.inner.contains(key) {
            let value = construct();
            self.inner.put(key.to_owned(), value);
        }
        self.inner.get_mut(key).unwrap()
    }

    /// Evict items in the map and only keep up-to `target_cap` items.
    pub fn evict_to_target_cap(&mut self) {
        self.inner.resize(self.target_cap);
        self.inner.resize(usize::MAX);
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_k, v)| v)
    }

    /// An iterator visiting all values mutably in most-recently used order. The iterator element
    /// type is &mut V.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.iter_mut().map(|(_k, v)| v)
    }

    pub fn pop(&mut self, k: &K) -> Option<V> {
        self.inner.pop(k)
    }

    pub fn push(&mut self, k: K, v: V) {
        self.inner.push(k, v);
    }
}

impl<K, V, S> Deref for EvictableHashMap<K, V, S> {
    type Target = LruCache<K, V, S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S> DerefMut for EvictableHashMap<K, V, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_len_after_evict() {
        let target_cap = 114;
        let items_count = 514;
        let mut map = EvictableHashMap::new(target_cap);

        for i in 0..items_count {
            map.put(i, ());
        }
        assert_eq!(map.len(), items_count);

        map.evict_to_target_cap();
        assert_eq!(map.len(), target_cap);

        assert!(map.get(&(items_count - target_cap - 1)).is_none());
        assert!(map.get(&(items_count - target_cap)).is_some());
    }
}
