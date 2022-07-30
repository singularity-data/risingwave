//  Copyright 2022 Singularity Data
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

//! `LruCache` implementation port from github.com/facebook/rocksdb. The class `LruCache` is
//! thread-safe, because every operation on cache will be protected by a spin lock.
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::hash::Hash;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::{channel, Receiver, Sender};

const IN_CACHE: u8 = 1;
const REVERSE_IN_CACHE: u8 = !IN_CACHE;

#[cfg(debug_assertions)]
const IN_LRU: u8 = 2;
#[cfg(debug_assertions)]
const REVERSE_IN_LRU: u8 = !IN_LRU;

pub trait LruKey: Eq + Send + Hash {}
impl<T: Eq + Send + Hash> LruKey for T {}

pub trait LruValue: Send + Sync {}
impl<T: Send + Sync> LruValue for T {}

/// An entry is a variable length heap-allocated structure.
/// Entries are referenced by cache and/or by any external entity.
/// The cache keeps all its entries in a hash table. Some elements
/// are also stored on LRU list.
///
/// `LruHandle` can be in these states:
/// 1. Referenced externally AND in hash table.
///    In that case the entry is *not* in the LRU list
///    (`refs` >= 1 && `in_cache` == true)
/// 2. Not referenced externally AND in hash table.
///    In that case the entry is in the LRU list and can be freed.
///    (`refs` == 0 && `in_cache` == true)
/// 3. Referenced externally AND not in hash table.
///    In that case the entry is not in the LRU list and not in hash table.
///    The entry can be freed when refs becomes 0.
///    (`refs` >= 1 && `in_cache` == false)
///
/// All newly created `LruHandle`s are in state 1. If you call
/// `LruCacheShard::release` on entry in state 1, it will go into state 2.
/// To move from state 1 to state 3, either call `LruCacheShard::erase` or
/// `LruCacheShard::insert` with the same key (but possibly different value).
/// To move from state 2 to state 1, use `LruCacheShard::lookup`.
/// Before destruction, make sure that no handles are in state 1. This means
/// that any successful `LruCacheShard::lookup/LruCacheShard::insert` have a
/// matching `LruCache::release` (to move into state 2) or `LruCacheShard::erase`
/// (to move into state 3).
pub struct LruHandle<K: LruKey, T: LruValue> {
    /// next element in the linked-list of hash bucket, only used by hash-table.
    next_hash: *mut LruHandle<K, T>,

    /// next element in LRU linked list
    next: *mut LruHandle<K, T>,

    /// prev element in LRU linked list
    prev: *mut LruHandle<K, T>,

    /// When the handle is on-use, the fields is `Some(...)`, while the handle is cleared up and
    /// recycled, the field is `None`.
    kv: Option<(K, T)>,
    hash: u64,
    charge: usize,

    /// The count for external references. If `refs > 0`, the handle is not in the lru cache, and
    /// when `refs == 0`, the handle must either be in LRU cache or has been recycled.
    refs: u32,
    flags: u8,
}

impl<K: LruKey, T: LruValue> Default for LruHandle<K, T> {
    fn default() -> Self {
        Self {
            next_hash: null_mut(),
            next: null_mut(),
            prev: null_mut(),
            kv: None,
            hash: 0,
            charge: 0,
            refs: 0,
            flags: 0,
        }
    }
}

impl<K: LruKey, T: LruValue> LruHandle<K, T> {
    pub fn new(key: K, value: T, hash: u64, charge: usize) -> Self {
        let mut ret = Self::default();
        ret.init(key, value, hash, charge);
        ret
    }

    pub fn init(&mut self, key: K, value: T, hash: u64, charge: usize) {
        self.next_hash = null_mut();
        self.prev = null_mut();
        self.next = null_mut();
        self.kv = Some((key, value));
        self.hash = hash;
        self.charge = charge;
        self.flags = 0;
        self.refs = 0;
    }

    /// Set the `in_cache` bit in the flag
    ///
    /// Since only `in_cache` reflects whether the handle is present in the hash table, this method
    /// should only be called in the method of hash table. Whenever the handle enters the hash
    /// table, we should call `set_in_cache(true)`, and whenever the handle leaves the hash table,
    /// we should call `set_in_cache(false)`
    fn set_in_cache(&mut self, in_cache: bool) {
        if in_cache {
            self.flags |= IN_CACHE;
        } else {
            self.flags &= REVERSE_IN_CACHE;
        }
    }

    fn add_ref(&mut self) {
        self.refs += 1;
    }

    fn unref(&mut self) -> bool {
        debug_assert!(self.refs > 0);
        self.refs -= 1;
        self.refs == 0
    }

    fn has_refs(&self) -> bool {
        self.refs > 0
    }

    /// Test whether the handle is in cache. `in cache` is equivalent to that the handle is in the
    /// hash table.
    fn is_in_cache(&self) -> bool {
        (self.flags & IN_CACHE) > 0
    }

    unsafe fn get_key(&self) -> &K {
        debug_assert!(self.kv.is_some());
        &self.kv.as_ref().unwrap_unchecked().0
    }

    unsafe fn get_value(&self) -> &T {
        debug_assert!(self.kv.is_some());
        &self.kv.as_ref().unwrap_unchecked().1
    }

    unsafe fn is_same_key(&self, key: &K) -> bool {
        debug_assert!(self.kv.is_some());
        self.kv.as_ref().unwrap_unchecked().0.eq(key)
    }

    unsafe fn take_kv(&mut self) -> (K, T) {
        debug_assert!(self.kv.is_some());
        self.kv.take().unwrap_unchecked()
    }

    #[cfg(debug_assertions)]
    fn is_in_lru(&self) -> bool {
        (self.flags & IN_LRU) > 0
    }

    #[cfg(debug_assertions)]
    fn set_in_lru(&mut self, in_lru: bool) {
        if in_lru {
            self.flags |= IN_LRU;
        } else {
            self.flags &= REVERSE_IN_LRU;
        }
    }
}

unsafe impl<K: LruKey, T: LruValue> Send for LruHandle<K, T> {}

pub struct LruHandleTable<K: LruKey, T: LruValue> {
    list: Vec<*mut LruHandle<K, T>>,
    elems: usize,
}

impl<K: LruKey, T: LruValue> LruHandleTable<K, T> {
    fn new() -> Self {
        Self {
            list: vec![null_mut(); 16],
            elems: 0,
        }
    }

    // A util method that is only used internally in this struct.
    unsafe fn find_pointer(
        &self,
        idx: usize,
        key: &K,
    ) -> (*mut LruHandle<K, T>, *mut LruHandle<K, T>) {
        let mut ptr = self.list[idx];
        let mut prev = null_mut();
        while !ptr.is_null() && !(*ptr).is_same_key(key) {
            prev = ptr;
            ptr = (*ptr).next_hash;
        }
        (prev, ptr)
    }

    unsafe fn remove(&mut self, hash: u64, key: &K) -> *mut LruHandle<K, T> {
        debug_assert!(self.list.len().is_power_of_two());
        let idx = (hash as usize) & (self.list.len() - 1);
        let (mut prev, ptr) = self.find_pointer(idx, key);
        if ptr.is_null() {
            return null_mut();
        }
        debug_assert!((*ptr).is_in_cache());
        (*ptr).set_in_cache(false);
        if prev.is_null() {
            self.list[idx] = (*ptr).next_hash;
        } else {
            (*prev).next_hash = (*ptr).next_hash;
        }
        self.elems -= 1;
        ptr
    }

    /// Insert a handle into the hash table. Return the handle of the previous value if the key
    /// exists.
    unsafe fn insert(&mut self, hash: u64, h: *mut LruHandle<K, T>) -> *mut LruHandle<K, T> {
        debug_assert!(!h.is_null());
        debug_assert!(!(*h).is_in_cache());
        (*h).set_in_cache(true);
        debug_assert!(self.list.len().is_power_of_two());
        let idx = (hash as usize) & (self.list.len() - 1);
        let (mut prev, ptr) = self.find_pointer(idx, (*h).get_key());
        if prev.is_null() {
            self.list[idx] = h;
        } else {
            (*prev).next_hash = h;
        }

        if !ptr.is_null() {
            debug_assert!((*ptr).is_same_key((*h).get_key()));
            debug_assert!((*ptr).is_in_cache());
            // The handle to be removed is set not in cache.
            (*ptr).set_in_cache(false);
            (*h).next_hash = (*ptr).next_hash;
            return ptr;
        }

        (*h).next_hash = ptr;

        self.elems += 1;
        if self.elems > self.list.len() {
            self.resize();
        }
        null_mut()
    }

    unsafe fn lookup(&self, hash: u64, key: &K) -> *mut LruHandle<K, T> {
        debug_assert!(self.list.len().is_power_of_two());
        let idx = (hash as usize) & (self.list.len() - 1);
        let (_, ptr) = self.find_pointer(idx, key);
        ptr
    }

    unsafe fn resize(&mut self) {
        let mut l = std::cmp::max(16, self.list.len());
        let next_capacity = self.elems * 3 / 2;
        while l < next_capacity {
            l <<= 1;
        }
        let mut count = 0;
        let mut new_list = vec![null_mut(); l];
        for head in self.list.drain(..) {
            let mut handle = head;
            while !handle.is_null() {
                let idx = (*handle).hash as usize & (l - 1);
                let next = (*handle).next_hash;
                (*handle).next_hash = new_list[idx];
                new_list[idx] = handle;
                handle = next;
                count += 1;
            }
        }
        assert_eq!(count, self.elems);
        self.list = new_list;
    }

    unsafe fn for_all<F>(&self, f: &mut F)
    where
        F: FnMut(&K, &T),
    {
        for idx in 0..self.list.len() {
            let mut ptr = self.list[idx];
            while !ptr.is_null() {
                f((*ptr).get_key(), (*ptr).get_value());
                ptr = (*ptr).next_hash;
            }
        }
    }
}

type RequestQueue<K, T> = Vec<Sender<CachableEntry<K, T>>>;
pub struct LruCacheShard<K: LruKey, T: LruValue> {
    /// The dummy header node of a ring linked list. The linked list is a LRU list, holding the
    /// cache handles that are not used externally.
    lru: Box<LruHandle<K, T>>,
    table: LruHandleTable<K, T>,
    // TODO: may want to use an atomic object linked list shared by all shards.
    object_pool: Vec<Box<LruHandle<K, T>>>,
    write_request: HashMap<K, RequestQueue<K, T>>,
    lru_usage: Arc<AtomicUsize>,
    usage: Arc<AtomicUsize>,
    capacity: usize,
}

unsafe impl<K: LruKey, T: LruValue> Send for LruCacheShard<K, T> {}

impl<K: LruKey, T: LruValue> LruCacheShard<K, T> {
    fn new(capacity: usize, object_capacity: usize) -> Self {
        let mut lru = Box::new(LruHandle::default());
        lru.prev = lru.as_mut();
        lru.next = lru.as_mut();
        let mut object_pool = Vec::with_capacity(object_capacity);
        for _ in 0..object_capacity {
            object_pool.push(Box::new(LruHandle::default()));
        }
        Self {
            capacity,
            lru_usage: Arc::new(AtomicUsize::new(0)),
            usage: Arc::new(AtomicUsize::new(0)),
            object_pool,
            lru,
            table: LruHandleTable::new(),
            write_request: HashMap::with_capacity(16),
        }
    }

    unsafe fn lru_remove(&mut self, e: *mut LruHandle<K, T>) {
        debug_assert!(!e.is_null());
        #[cfg(debug_assertions)]
        {
            assert!((*e).is_in_lru());
            (*e).set_in_lru(false);
        }

        (*(*e).next).prev = (*e).prev;
        (*(*e).prev).next = (*e).next;
        (*e).prev = null_mut();
        (*e).next = null_mut();
        self.lru_usage.fetch_sub((*e).charge, Ordering::Relaxed);
    }

    // insert entry in the end of the linked-list
    unsafe fn lru_insert(&mut self, e: *mut LruHandle<K, T>) {
        debug_assert!(!e.is_null());
        #[cfg(debug_assertions)]
        {
            assert!(!(*e).is_in_lru());
            (*e).set_in_lru(true);
        }

        (*e).next = self.lru.as_mut();
        (*e).prev = self.lru.prev;
        (*(*e).prev).next = e;
        (*(*e).next).prev = e;
        self.lru_usage.fetch_add((*e).charge, Ordering::Relaxed);
    }

    unsafe fn evict_from_lru(&mut self, charge: usize, last_reference_list: &mut Vec<(K, T)>) {
        // TODO: may want to optimize by only loading at the beginning and storing at the end for
        // only once.
        while self.usage.load(Ordering::Relaxed) + charge > self.capacity
            && !std::ptr::eq(self.lru.next, self.lru.as_mut())
        {
            let old_ptr = self.lru.next;
            self.table.remove((*old_ptr).hash, (*old_ptr).get_key());
            self.lru_remove(old_ptr);
            let (key, value) = self.clear_handle(old_ptr);
            last_reference_list.push((key, value));
        }
    }

    /// Clear a currently used handle and recycle it if possible
    unsafe fn clear_handle(&mut self, h: *mut LruHandle<K, T>) -> (K, T) {
        debug_assert!(!h.is_null());
        debug_assert!((*h).kv.is_some());
        #[cfg(debug_assertions)]
        assert!(!(*h).is_in_lru());
        debug_assert!(!(*h).is_in_cache());
        debug_assert!(!(*h).has_refs());
        self.usage.fetch_sub((*h).charge, Ordering::Relaxed);
        let (key, value) = (*h).take_kv();
        self.try_recycle_handle_object(h);
        (key, value)
    }

    /// Try to recycle a handle object if the object pool is not full.
    ///
    /// The handle should already cleared its kv.
    unsafe fn try_recycle_handle_object(&mut self, h: *mut LruHandle<K, T>) {
        let mut node = Box::from_raw(h);
        if self.object_pool.len() < self.object_pool.capacity() {
            node.next_hash = null_mut();
            node.next = null_mut();
            node.prev = null_mut();
            debug_assert!(node.kv.is_none());
            self.object_pool.push(node);
        }
    }

    /// insert a new key value in the cache. The handle for the new key value is returned.
    unsafe fn insert(
        &mut self,
        key: K,
        hash: u64,
        charge: usize,
        value: T,
        last_reference_list: &mut Vec<(K, T)>,
    ) -> *mut LruHandle<K, T> {
        self.evict_from_lru(charge, last_reference_list);

        let handle = if let Some(mut h) = self.object_pool.pop() {
            h.init(key, value, hash, charge);
            h
        } else {
            Box::new(LruHandle::new(key, value, hash, charge))
        };

        let ptr = Box::into_raw(handle);
        let old = self.table.insert(hash, ptr);
        if !old.is_null() {
            if let Some(data) = self.try_remove_cache_handle(old) {
                last_reference_list.push(data);
            }
        }
        self.usage.fetch_add(charge, Ordering::Relaxed);
        (*ptr).add_ref();
        ptr
    }

    /// Release the usage on a handle.
    ///
    /// Return: `Some(value)` if the handle is released, and `None` if the value is still in use.
    unsafe fn release(&mut self, h: *mut LruHandle<K, T>) -> Option<(K, T)> {
        debug_assert!(!h.is_null());
        // The handle should not be in lru before calling this method.
        #[cfg(debug_assertions)]
        assert!(!(*h).is_in_lru());
        let last_reference = (*h).unref();
        // If the handle is still referenced by someone else, do nothing and return.
        if !last_reference {
            return None;
        }

        // Keep the handle in lru list if it is still in the cache and the cache is not over-sized.
        if (*h).is_in_cache() {
            if self.usage.load(Ordering::Relaxed) <= self.capacity {
                self.lru_insert(h);
                return None;
            }
            // Remove the handle from table.
            self.table.remove((*h).hash, (*h).get_key());
        }

        // Since the released handle was previously used externally, it must not be in LRU, and we
        // don't need to remove it from lru.
        #[cfg(debug_assertions)]
        assert!(!(*h).is_in_lru());

        let (key, value) = self.clear_handle(h);
        Some((key, value))
    }

    unsafe fn lookup(&mut self, hash: u64, key: &K) -> *mut LruHandle<K, T> {
        let e = self.table.lookup(hash, key);
        if !e.is_null() {
            // If the handle previously has not ref, it must exist in the lru. And therefore we are
            // safe to remove it from lru.
            if !(*e).has_refs() {
                self.lru_remove(e);
            }
            (*e).add_ref();
        }
        e
    }

    /// Erase a key from the cache.
    unsafe fn erase(&mut self, hash: u64, key: &K) -> Option<(K, T)> {
        let h = self.table.remove(hash, key);
        if !h.is_null() {
            self.try_remove_cache_handle(h)
        } else {
            None
        }
    }

    /// Try removing the handle from the cache if the handle is not used externally any more.
    ///
    /// This method can only be called on the handle that just removed from the hash table.
    unsafe fn try_remove_cache_handle(&mut self, h: *mut LruHandle<K, T>) -> Option<(K, T)> {
        debug_assert!(!h.is_null());
        if !(*h).has_refs() {
            // Since the handle is just removed from the hash table, it should either be in lru or
            // referenced externally. Since we have checked that it is not referenced externally, it
            // must be in the LRU, and therefore we are safe to call `lru_remove`.
            self.lru_remove(h);
            let (key, value) = self.clear_handle(h);
            return Some((key, value));
        }
        None
    }

    // Clears the content of the cache.
    // This method is safe to use only if no cache entries are referenced outside.
    unsafe fn clear(&mut self) {
        while !std::ptr::eq(self.lru.next, self.lru.as_mut()) {
            let handle = self.lru.next;
            // `listener` should not be trigged here, for it doesn't listen to `clear`.
            self.erase((*handle).hash, (*handle).get_key());
        }
    }

    fn for_all<F>(&self, f: &mut F)
    where
        F: FnMut(&K, &T),
    {
        unsafe { self.table.for_all(f) };
    }
}

impl<K: LruKey, T: LruValue> Drop for LruCacheShard<K, T> {
    fn drop(&mut self) {
        // Since the shard is being drop, there must be no cache entries referenced outside. So we
        // are safe to call clear.
        unsafe {
            self.clear();
        }
    }
}

pub trait LruCacheEventListener: Send + Sync {
    type K: LruKey;
    type T: LruValue;

    /// `on_release` is called when a cache entry is erased or evicted by a new inserted entry.
    ///
    /// Note:
    /// `on_release` will not be triggered when the `LruCache` and its inner entries are dropped.
    fn on_release(&self, _key: Self::K, _value: Self::T) {}
}

pub struct LruCache<K: LruKey, T: LruValue> {
    shards: Vec<Mutex<LruCacheShard<K, T>>>,
    shard_usages: Vec<Arc<AtomicUsize>>,
    shard_lru_usages: Vec<Arc<AtomicUsize>>,

    listener: Option<Arc<dyn LruCacheEventListener<K = K, T = T>>>,
}

// we only need a small object pool because when the cache reach the limit of capacity, it will
// always release some object after insert a new block.
const DEFAULT_OBJECT_POOL_SIZE: usize = 1024;

impl<K: LruKey, T: LruValue> LruCache<K, T> {
    pub fn new(num_shard_bits: usize, capacity: usize) -> Self {
        Self::with_event_listener(num_shard_bits, capacity, None)
    }

    pub fn with_event_listener(
        num_shard_bits: usize,
        capacity: usize,
        listener: Option<Arc<dyn LruCacheEventListener<K = K, T = T>>>,
    ) -> Self {
        let num_shards = 1 << num_shard_bits;
        let mut shards = Vec::with_capacity(num_shards);
        let per_shard = capacity / num_shards;
        let mut shard_usages = Vec::with_capacity(num_shards);
        let mut shard_lru_usages = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            let shard = LruCacheShard::new(per_shard, DEFAULT_OBJECT_POOL_SIZE);
            shard_usages.push(shard.usage.clone());
            shard_lru_usages.push(shard.lru_usage.clone());
            shards.push(Mutex::new(shard));
        }
        Self {
            shards,
            shard_usages,
            shard_lru_usages,

            listener,
        }
    }

    pub fn lookup(self: &Arc<Self>, hash: u64, key: &K) -> Option<CachableEntry<K, T>> {
        let mut shard = self.shards[self.shard(hash)].lock();
        unsafe {
            let ptr = shard.lookup(hash, key);
            if ptr.is_null() {
                return None;
            }
            let entry = CachableEntry {
                cache: self.clone(),
                handle: ptr,
            };
            Some(entry)
        }
    }

    pub fn lookup_for_request(self: &Arc<Self>, hash: u64, key: K) -> LookupResult<K, T> {
        let mut shard = self.shards[self.shard(hash)].lock();
        unsafe {
            let ptr = shard.lookup(hash, &key);
            if !ptr.is_null() {
                return LookupResult::Cached(CachableEntry {
                    cache: self.clone(),
                    handle: ptr,
                });
            }
            if let Some(que) = shard.write_request.get_mut(&key) {
                let (tx, recv) = channel();
                que.push(tx);
                return LookupResult::WaitPendingRequest(recv);
            }
            shard.write_request.insert(key, vec![]);
            LookupResult::Miss
        }
    }

    unsafe fn release(&self, handle: *mut LruHandle<K, T>) {
        debug_assert!(!handle.is_null());
        let data = {
            let mut shard = self.shards[self.shard((*handle).hash)].lock();
            shard.release(handle)
        };
        // do not deallocate data with holding mutex.
        if let Some((key,value)) = data && let Some(listener) = &self.listener {
            listener.on_release(key, value);
        }
    }

    pub fn insert(
        self: &Arc<Self>,
        key: K,
        hash: u64,
        charge: usize,
        value: T,
    ) -> CachableEntry<K, T> {
        let mut to_delete = vec![];
        let handle = unsafe {
            let mut shard = self.shards[self.shard(hash)].lock();
            let pending_request = shard.write_request.remove(&key);
            let ptr = shard.insert(key, hash, charge, value, &mut to_delete);
            debug_assert!(!ptr.is_null());
            if let Some(que) = pending_request {
                for sender in que {
                    (*ptr).add_ref();
                    let _ = sender.send(CachableEntry {
                        cache: self.clone(),
                        handle: ptr,
                    });
                }
            }
            CachableEntry {
                cache: self.clone(),
                handle: ptr,
            }
        };
        // do not deallocate data with holding mutex.
        if let Some(listener) = &self.listener {
            for (key, value) in to_delete {
                listener.on_release(key, value);
            }
        }
        handle
    }

    pub fn clear_pending_request(&self, key: &K, hash: u64) {
        let mut shard = self.shards[self.shard(hash)].lock();
        shard.write_request.remove(key);
    }

    pub fn erase(&self, hash: u64, key: &K) {
        let data = unsafe {
            let mut shard = self.shards[self.shard(hash)].lock();
            shard.erase(hash, key)
        };
        // do not deallocate data with holding mutex.
        if let Some((key,value)) = data && let Some(listener) = &self.listener {
            listener.on_release(key, value);
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        self.shard_usages
            .iter()
            .map(|x| x.load(Ordering::Relaxed))
            .sum()
    }

    pub fn get_lru_usage(&self) -> usize {
        self.shard_lru_usages
            .iter()
            .map(|x| x.load(Ordering::Relaxed))
            .sum()
    }

    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.shards.len()
    }

    /// # Safety
    ///
    /// This method is used for read-only [`LruCache`]. It locks one shard per loop to prevent the
    /// iterating progress from blocking reads among all shards.
    ///
    /// If there is another thread inserting entries at the same time, there will be data
    /// inconsistency.
    pub fn for_all<F>(&self, mut f: F)
    where
        F: FnMut(&K, &T),
    {
        for shard in &self.shards {
            let shard = shard.lock();
            shard.for_all(&mut f);
        }
    }

    /// # Safety
    ///
    /// This method can only be called when no cache entry are referenced outside.
    pub fn clear(&self) {
        for shard in &self.shards {
            unsafe {
                let mut shard = shard.lock();
                shard.clear();
            }
        }
    }
}

/// Only implement `lookup_with_request_dedup` for static values, as they can be sent across tokio
/// spawned futures.
impl<K: LruKey + Clone + 'static, T: LruValue + 'static> LruCache<K, T> {
    pub async fn lookup_with_request_dedup<F, E, VC>(
        self: &Arc<Self>,
        hash: u64,
        key: K,
        fetch_value: F,
    ) -> Result<Result<CachableEntry<K, T>, E>, RecvError>
    where
        F: FnOnce() -> VC,
        E: Error + Send + 'static,
        VC: Future<Output = Result<(T, usize), E>> + Send + 'static,
    {
        match self.lookup_for_request(hash, key.clone()) {
            LookupResult::Cached(entry) => Ok(Ok(entry)),
            LookupResult::WaitPendingRequest(recv) => {
                let entry = recv.await?;
                Ok(Ok(entry))
            }
            LookupResult::Miss => {
                let this = self.clone();
                let fetch_value = fetch_value();
                tokio::spawn(async move {
                    match fetch_value.await {
                        Ok((value, charge)) => {
                            let entry = this.insert(key, hash, charge, value);
                            Ok(Ok(entry))
                        }
                        Err(e) => {
                            this.clear_pending_request(&key, hash);
                            Ok(Err(e))
                        }
                    }
                })
                .await
                .unwrap()
            }
        }
    }
}

pub struct CachableEntry<K: LruKey, T: LruValue> {
    cache: Arc<LruCache<K, T>>,
    handle: *mut LruHandle<K, T>,
}

pub enum LookupResult<K: LruKey, T: LruValue> {
    Cached(CachableEntry<K, T>),
    Miss,
    WaitPendingRequest(Receiver<CachableEntry<K, T>>),
}

unsafe impl<K: LruKey, T: LruValue> Send for CachableEntry<K, T> {}
unsafe impl<K: LruKey, T: LruValue> Sync for CachableEntry<K, T> {}

impl<K: LruKey, T: LruValue> CachableEntry<K, T> {
    pub fn value(&self) -> &T {
        unsafe { (*self.handle).get_value() }
    }
}

impl<K: LruKey, T: LruValue> Drop for CachableEntry<K, T> {
    fn drop(&mut self) {
        unsafe {
            self.cache.release(self.handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;

    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};
    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;

    pub struct Block {
        pub offset: u64,
        pub sst: u64,
    }

    #[test]
    fn test_cache_handle_basic() {
        let mut h = Box::new(LruHandle::new(1, 2, 0, 0));
        h.set_in_cache(true);
        assert!(h.is_in_cache());
        h.set_in_cache(false);
        assert!(!h.is_in_cache());
    }

    #[test]
    fn test_cache_shard() {
        let cache = Arc::new(LruCache::<(u64, u64), Block>::new(2, 256));
        assert_eq!(cache.shard(0), 0);
        assert_eq!(cache.shard(1), 1);
        assert_eq!(cache.shard(10), 2);
    }

    #[test]
    fn test_cache_basic() {
        let cache = Arc::new(LruCache::<(u64, u64), Block>::new(2, 256));
        let seed = 10244021u64;
        let mut rng = SmallRng::seed_from_u64(seed);
        for _ in 0..100000 {
            let block_offset = rng.next_u64() % 1024;
            let sst = rng.next_u64() % 1024;
            let mut hasher = DefaultHasher::new();
            sst.hash(&mut hasher);
            block_offset.hash(&mut hasher);
            let h = hasher.finish();
            if let Some(block) = cache.lookup(h, &(sst, block_offset)) {
                assert_eq!(block.value().offset, block_offset);
                drop(block);
                continue;
            }
            cache.insert(
                (sst, block_offset),
                h,
                1,
                Block {
                    offset: block_offset,
                    sst,
                },
            );
        }
        assert_eq!(256, cache.get_memory_usage());
    }

    fn validate_lru_list(cache: &mut LruCacheShard<String, String>, keys: Vec<&str>) {
        unsafe {
            let mut lru: *mut LruHandle<String, String> = cache.lru.as_mut();
            for k in keys {
                lru = (*lru).next;
                assert!(
                    (*lru).is_same_key(&k.to_string()),
                    "compare failed: {} vs {}, get value: {:?}",
                    (*lru).get_key(),
                    k,
                    (*lru).get_value()
                );
            }
        }
    }

    fn create_cache(capacity: usize) -> LruCacheShard<String, String> {
        LruCacheShard::new(capacity, capacity)
    }

    fn lookup(cache: &mut LruCacheShard<String, String>, key: &str) -> bool {
        unsafe {
            let h = cache.lookup(0, &key.to_string());
            let exist = !h.is_null();
            if exist {
                assert!((*h).is_same_key(&key.to_string()));
                cache.release(h);
            }
            exist
        }
    }

    fn insert(cache: &mut LruCacheShard<String, String>, key: &str, value: &str) {
        let mut free_list = vec![];
        unsafe {
            let handle = cache.insert(
                key.to_string(),
                0,
                value.len(),
                value.to_string(),
                &mut free_list,
            );
            cache.release(handle);
        }
        free_list.clear();
    }

    #[test]
    fn test_basic_lru() {
        let mut cache = create_cache(5);
        let keys = vec!["a", "b", "c", "d", "e"];
        for &k in &keys {
            insert(&mut cache, k, k);
        }
        validate_lru_list(&mut cache, keys);
        for k in ["x", "y", "z"] {
            insert(&mut cache, k, k);
        }
        validate_lru_list(&mut cache, vec!["d", "e", "x", "y", "z"]);
        assert!(!lookup(&mut cache, "b"));
        assert!(lookup(&mut cache, "e"));
        validate_lru_list(&mut cache, vec!["d", "x", "y", "z", "e"]);
        assert!(lookup(&mut cache, "z"));
        validate_lru_list(&mut cache, vec!["d", "x", "y", "e", "z"]);
        unsafe {
            let h = cache.erase(0, &"x".to_string());
            assert!(h.is_some());
            validate_lru_list(&mut cache, vec!["d", "y", "e", "z"]);
        }
        assert!(lookup(&mut cache, "d"));
        validate_lru_list(&mut cache, vec!["y", "e", "z", "d"]);
        insert(&mut cache, "u", "u");
        validate_lru_list(&mut cache, vec!["y", "e", "z", "d", "u"]);
        insert(&mut cache, "v", "v");
        validate_lru_list(&mut cache, vec!["e", "z", "d", "u", "v"]);
    }

    #[test]
    fn test_reference_and_usage() {
        let mut cache = create_cache(5);
        insert(&mut cache, "k1", "a");
        assert_eq!(cache.usage.load(Ordering::Relaxed), 1);
        insert(&mut cache, "k0", "aa");
        assert_eq!(cache.usage.load(Ordering::Relaxed), 3);
        insert(&mut cache, "k1", "aa");
        assert_eq!(cache.usage.load(Ordering::Relaxed), 4);
        insert(&mut cache, "k2", "aa");
        assert_eq!(cache.usage.load(Ordering::Relaxed), 4);
        let mut free_list = vec![];
        validate_lru_list(&mut cache, vec!["k1", "k2"]);
        unsafe {
            let h1 = cache.lookup(0, &"k1".to_string());
            assert!(!h1.is_null());
            let h2 = cache.lookup(0, &"k2".to_string());
            assert!(!h2.is_null());

            let h3 = cache.insert("k3".to_string(), 0, 2, "bb".to_string(), &mut free_list);
            assert_eq!(cache.usage.load(Ordering::Relaxed), 6);
            assert!(!h3.is_null());
            let h4 = cache.lookup(0, &"k1".to_string());
            assert!(!h4.is_null());

            cache.release(h1);
            assert_eq!(cache.usage.load(Ordering::Relaxed), 6);
            cache.release(h4);
            assert_eq!(cache.usage.load(Ordering::Relaxed), 4);

            cache.release(h3);
            cache.release(h2);

            validate_lru_list(&mut cache, vec!["k3", "k2"]);
        }
    }

    #[test]
    fn test_update_referenced_key() {
        unsafe {
            let mut to_delete = vec![];
            let mut cache = create_cache(5);
            let insert_handle = cache.insert(
                "key".to_string(),
                0,
                1,
                "old_value".to_string(),
                &mut to_delete,
            );
            let old_entry = cache.lookup(0, &"key".to_string());
            assert!(!old_entry.is_null());
            assert_eq!((*old_entry).get_value(), &"old_value".to_string());
            assert_eq!((*old_entry).refs, 2);
            cache.release(insert_handle);
            assert_eq!((*old_entry).refs, 1);
            let insert_handle = cache.insert(
                "key".to_string(),
                0,
                1,
                "new_value".to_string(),
                &mut to_delete,
            );
            assert!(!(*old_entry).is_in_cache());
            let new_entry = cache.lookup(0, &"key".to_string());
            assert!(!new_entry.is_null());
            assert_eq!((*new_entry).get_value(), &"new_value".to_string());
            assert_eq!((*new_entry).refs, 2);
            cache.release(insert_handle);
            assert_eq!((*new_entry).refs, 1);

            assert!(!old_entry.is_null());
            assert_eq!((*old_entry).get_value(), &"old_value".to_string());
            assert_eq!((*old_entry).refs, 1);

            cache.release(new_entry);
            assert!((*new_entry).is_in_cache());
            #[cfg(debug_assertions)]
            assert!((*new_entry).is_in_lru());

            // assert old value unchanged.
            assert!(!old_entry.is_null());
            assert_eq!((*old_entry).get_value(), &"old_value".to_string());
            assert_eq!((*old_entry).refs, 1);

            cache.release(old_entry);
            assert!(!(*old_entry).is_in_cache());
            assert!((*new_entry).is_in_cache());
            #[cfg(debug_assertions)]
            {
                assert!(!(*old_entry).is_in_lru());
                assert!((*new_entry).is_in_lru());
            }
        }
    }

    #[test]
    fn test_release_stale_value() {
        unsafe {
            let mut to_delete = vec![];
            // The cache can only hold one handle
            let mut cache = create_cache(1);
            let insert_handle = cache.insert(
                "key".to_string(),
                0,
                1,
                "old_value".to_string(),
                &mut to_delete,
            );
            cache.release(insert_handle);
            let old_entry = cache.lookup(0, &"key".to_string());
            assert!(!old_entry.is_null());
            assert_eq!((*old_entry).get_value(), &"old_value".to_string());
            assert_eq!((*old_entry).refs, 1);

            let insert_handle = cache.insert(
                "key".to_string(),
                0,
                1,
                "new_value".to_string(),
                &mut to_delete,
            );
            assert!(!(*old_entry).is_in_cache());
            let new_entry = cache.lookup(0, &"key".to_string());
            assert!(!new_entry.is_null());
            assert_eq!((*new_entry).get_value(), &"new_value".to_string());
            assert_eq!((*new_entry).refs, 2);
            cache.release(insert_handle);
            assert_eq!((*new_entry).refs, 1);

            // The handle for new and old value are both referenced.
            assert_eq!(2, cache.usage.load(Relaxed));
            assert_eq!(0, cache.lru_usage.load(Relaxed));

            // Release the old handle, it will be cleared since the cache capacity is 1
            cache.release(old_entry);
            assert_eq!(1, cache.usage.load(Relaxed));
            assert_eq!(0, cache.lru_usage.load(Relaxed));

            let new_entry_again = cache.lookup(0, &"key".to_string());
            assert!(!new_entry_again.is_null());
            assert_eq!((*new_entry_again).get_value(), &"new_value".to_string());
            assert_eq!((*new_entry_again).refs, 2);

            cache.release(new_entry);
            cache.release(new_entry_again);

            assert_eq!(1, cache.usage.load(Relaxed));
            assert_eq!(1, cache.lru_usage.load(Relaxed));
        }
    }

    #[test]
    fn test_write_request_pending() {
        let cache = Arc::new(LruCache::new(0, 5));
        {
            let mut shard = cache.shards[0].lock();
            insert(&mut shard, "a", "v1");
            assert!(lookup(&mut shard, "a"));
        }
        let ret = cache.lookup_for_request(0, "a".to_string());
        match ret {
            LookupResult::Cached(_) => (),
            _ => panic!(),
        }
        let ret1 = cache.lookup_for_request(0, "b".to_string());
        match ret1 {
            LookupResult::Miss => (),
            _ => panic!(),
        }
        let ret2 = cache.lookup_for_request(0, "b".to_string());
        match ret2 {
            LookupResult::WaitPendingRequest(mut recv) => {
                assert!(matches!(recv.try_recv(), Err(TryRecvError::Empty)));
                cache.insert("b".to_string(), 0, 1, "v2".to_string());
                let v = recv.try_recv().unwrap();
                assert_eq!(v.value(), "v2");
            }
            _ => panic!(),
        }
    }

    #[derive(Default, Debug)]
    struct TestLruCacheEventListener {
        released: Arc<Mutex<HashMap<String, String>>>,
    }

    impl LruCacheEventListener for TestLruCacheEventListener {
        type K = String;
        type T = String;

        fn on_release(&self, key: Self::K, value: Self::T) {
            self.released.lock().insert(key, value);
        }
    }

    #[test]
    fn test_event_listener() {
        let listener = Arc::new(TestLruCacheEventListener::default());
        let cache = Arc::new(LruCache::with_event_listener(0, 2, Some(listener.clone())));

        // full-fill cache
        let h = cache.insert("k1".to_string(), 0, 1, "v1".to_string());
        drop(h);
        let h = cache.insert("k2".to_string(), 0, 1, "v2".to_string());
        drop(h);
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().is_empty());

        // test evict
        let h = cache.insert("k3".to_string(), 0, 1, "v3".to_string());
        drop(h);
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().remove("k1").is_some());

        // test erase
        cache.erase(0, &"k2".to_string());
        assert_eq!(cache.get_memory_usage(), 1);
        assert!(listener.released.lock().remove("k2").is_some());

        // test refill
        let h = cache.insert("k4".to_string(), 0, 1, "v4".to_string());
        drop(h);
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().is_empty());

        // test release after full
        // 1. full-fill cache but not release
        let h1 = cache.insert("k5".to_string(), 0, 1, "v5".to_string());
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().remove("k3").is_some());
        let h2 = cache.insert("k6".to_string(), 0, 1, "v6".to_string());
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().remove("k4").is_some());

        // 2. insert one more entry after cache is full, cache will be oversized
        let h3 = cache.insert("k7".to_string(), 0, 1, "v7".to_string());
        assert_eq!(cache.get_memory_usage(), 3);
        assert!(listener.released.lock().is_empty());

        // 3. release one entry, and it will be evicted immediately bucause cache is oversized
        drop(h1);
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().remove("k5").is_some());

        // 4. release other entries, no entry will be evicted
        drop(h2);
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().is_empty());
        drop(h3);
        assert_eq!(cache.get_memory_usage(), 2);
        assert!(listener.released.lock().is_empty());

        // assert listener won't listen clear
        drop(cache);
        assert!(listener.released.lock().is_empty());
    }
}
