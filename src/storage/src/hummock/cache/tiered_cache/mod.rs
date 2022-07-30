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

use std::hash::Hash;

pub trait TieredCacheKey: Eq + Send + Sync + Hash + Clone + 'static + std::fmt::Debug {
    fn encoded_len() -> usize;

    fn encode(&self, buf: &mut [u8]);

    fn decode(buf: &[u8]) -> Self;
}

#[cfg(target_os = "linux")]
pub mod file_cache;

use std::marker::PhantomData;

#[derive(thiserror::Error, Debug)]
pub enum TieredCacheError {
    #[cfg(target_os = "linux")]
    #[error("file cache error: {0}")]
    FileCache(#[from] file_cache::error::Error),
}

pub type Result<T> = core::result::Result<T, TieredCacheError>;

pub enum TieredCacheOptions {
    NoneCache,
    #[cfg(target_os = "linux")]
    FileCache(file_cache::cache::FileCacheOptions),
}

#[derive(Clone)]
pub enum TieredCache<K: TieredCacheKey> {
    NoneCache(PhantomData<K>),
    #[cfg(target_os = "linux")]
    FileCache(file_cache::cache::FileCache<K>),
}

impl<K: TieredCacheKey> TieredCache<K> {
    #[allow(clippy::unused_async)]
    pub async fn open(options: TieredCacheOptions) -> Result<Self> {
        match options {
            TieredCacheOptions::NoneCache => Ok(Self::NoneCache(PhantomData::default())),
            #[cfg(target_os = "linux")]
            TieredCacheOptions::FileCache(options) => {
                let file_cache = file_cache::cache::FileCache::open(options).await?;
                Ok(Self::FileCache(file_cache))
            }
        }
    }

    #[allow(unused_variables)]
    pub fn insert(&self, key: K, value: Vec<u8>) -> Result<()> {
        match self {
            TieredCache::NoneCache(_) => Ok(()),
            #[cfg(target_os = "linux")]
            TieredCache::FileCache(file_cache) => {
                file_cache.insert(key, value)?;
                Ok(())
            }
        }
    }

    #[allow(unused_variables)]
    pub fn erase(&self, key: &K) -> Result<()> {
        match self {
            TieredCache::NoneCache(_) => Ok(()),
            #[cfg(target_os = "linux")]
            TieredCache::FileCache(file_cache) => {
                file_cache.erase(key)?;
                Ok(())
            }
        }
    }

    #[allow(unused_variables, clippy::unused_async)]
    pub async fn get(&self, key: &K) -> Result<Option<Vec<u8>>> {
        match self {
            TieredCache::NoneCache(_) => Ok(None),
            #[cfg(target_os = "linux")]
            TieredCache::FileCache(file_cache) => {
                let value = file_cache.get(key).await?;
                Ok(value)
            }
        }
    }
}
