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

use std::path::PathBuf;
use std::sync::Arc;

use itertools::Itertools;
use nix::sys::statfs::{statfs, FsType as NixFsType, EXT4_SUPER_MAGIC};
use parking_lot::RwLock;
use risingwave_common::cache::{LruCache, LruCacheEventListener};

use super::coding::HashBuilder;
use super::error::{Error, Result};
use super::file::{CacheFile, CacheFileOptions};
use super::meta::{BlockLoc, MetaFile, SlotId};
use super::{utils, DioBuffer, DIO_BUFFER_ALLOCATOR};
use crate::hummock::TieredCacheKey;

const META_FILE_FILENAME: &str = "meta";
const CACHE_FILE_FILENAME: &str = "cache";

#[derive(Clone, Copy, Debug)]
pub enum FsType {
    Ext4,
    Xfs,
}

pub struct StoreOptions {
    pub dir: String,
    pub capacity: usize,
    pub buffer_capacity: usize,
    pub cache_file_fallocate_unit: usize,
}

pub struct Store<K>
where
    K: TieredCacheKey,
{
    dir: String,
    _capacity: usize,

    _fs_type: FsType,
    _fs_block_size: usize,
    block_size: usize,
    buffer_capacity: usize,

    meta_file: Arc<RwLock<MetaFile<K>>>,
    cache_file: CacheFile,
}

impl<K> Store<K>
where
    K: TieredCacheKey,
{
    pub async fn open(options: StoreOptions) -> Result<Self> {
        if !PathBuf::from(options.dir.as_str()).exists() {
            std::fs::create_dir_all(options.dir.as_str())?;
        }

        // Get file system type and block size by `statfs(2)`.
        let fs_stat = statfs(options.dir.as_str())?;
        let fs_type = match fs_stat.filesystem_type() {
            EXT4_SUPER_MAGIC => FsType::Ext4,
            // FYI: https://github.com/nix-rust/nix/issues/1742
            NixFsType(libc::XFS_SUPER_MAGIC) => FsType::Xfs,
            nix_fs_type => return Err(Error::UnsupportedFilesystem(nix_fs_type.0)),
        };
        let fs_block_size = fs_stat.block_size() as usize;

        let cf_opts = CacheFileOptions {
            // TODO: Make it configurable.
            block_size: fs_block_size,
            fallocate_unit: options.cache_file_fallocate_unit,
        };

        let mf = MetaFile::open(PathBuf::from(&options.dir).join(META_FILE_FILENAME))?;

        let cf = CacheFile::open(
            PathBuf::from(&options.dir).join(CACHE_FILE_FILENAME),
            cf_opts,
        )
        .await?;

        Ok(Self {
            dir: options.dir,
            _capacity: options.capacity,

            _fs_type: fs_type,
            _fs_block_size: fs_block_size,
            // TODO: Make it configurable.
            block_size: fs_block_size,
            buffer_capacity: options.buffer_capacity,

            meta_file: Arc::new(RwLock::new(mf)),
            cache_file: cf,
        })
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn size(&self) -> usize {
        self.cache_file.size() + self.meta_file.read().size()
    }

    pub fn meta_file_size(&self) -> usize {
        self.meta_file.read().size()
    }

    pub fn cache_file_size(&self) -> usize {
        self.cache_file.size()
    }

    pub fn cache_file_len(&self) -> usize {
        self.cache_file.len()
    }

    pub fn meta_file_path(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(META_FILE_FILENAME)
    }

    pub fn cache_file_path(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(CACHE_FILE_FILENAME)
    }

    pub fn restore<S: HashBuilder>(
        &self,
        indices: &Arc<LruCache<K, SlotId>>,
        hash_builder: &S,
    ) -> Result<()> {
        let slots = self.meta_file.read().slots();

        for slot in 0..slots {
            // Wrap the read guard, or there will be deadlock when evicting entries.
            let res = { self.meta_file.read().get(slot) };
            if let Some((block_loc, key)) = res {
                indices.insert(
                    key.clone(),
                    hash_builder.hash_one(&key),
                    utils::align_up(self.block_size, block_loc.len as usize),
                    slot,
                );
            }
        }

        Ok(())
    }

    pub async fn insert(&self, batch: &[(K, Vec<u8>)]) -> Result<Vec<SlotId>> {
        debug_assert!(!batch.is_empty());
        let mut buf = DioBuffer::with_capacity_in(self.buffer_capacity, &DIO_BUFFER_ALLOCATOR);
        let mut blocs = Vec::with_capacity(batch.len());
        let mut slots = Vec::with_capacity(batch.len());

        for (_key, value) in batch {
            debug_assert!(!value.is_empty());
            let bloc = BlockLoc {
                bidx: buf.len() as u32 / self.block_size as u32,
                len: value.len() as u32,
            };
            blocs.push(bloc);
            buf.extend_from_slice(value);
            buf.resize(utils::align_up(self.block_size, buf.len()), 0);
        }

        let boff = self.cache_file.append(buf).await? as u32 / self.block_size as u32;

        for bloc in &mut blocs {
            bloc.bidx += boff;
        }

        let mut mf = self.meta_file.write();
        for ((key, _value), bloc) in batch.iter().zip_eq(blocs.iter()) {
            slots.push(mf.insert(key, bloc)?);
        }

        Ok(slots)
    }

    pub async fn get(&self, slot: SlotId) -> Result<Vec<u8>> {
        let (bloc, _key) = self
            .meta_file
            .read()
            .get(slot)
            .ok_or(Error::InvalidSlot(slot))?;
        let offset = bloc.bidx as u64 * self.block_size as u64;
        let blen = bloc.blen(self.block_size as u32) as usize;
        let buf = self.cache_file.read(offset, blen).await?;
        Ok(buf[..bloc.len as usize].to_vec())
    }

    pub fn erase(&self, slot: SlotId) -> Result<()> {
        self.free(slot)
    }

    fn free(&self, slot: SlotId) -> Result<()> {
        let bloc = match self.meta_file.write().free(slot) {
            None => return Ok(()),
            Some(bloc) => bloc,
        };
        let offset = bloc.bidx as u64 * self.block_size as u64;
        let len = bloc.blen(self.block_size as u32) as usize;
        self.cache_file.punch_hole(offset, len)
    }
}

impl<K> LruCacheEventListener for Store<K>
where
    K: TieredCacheKey,
{
    type K = K;
    type T = SlotId;

    fn on_release(&self, _key: Self::K, slot: Self::T) {
        // TODO: Throw warning log instead?
        self.free(slot).unwrap();
    }
}

pub type StoreRef<K> = Arc<Store<K>>;

#[cfg(test)]
mod tests {

    use super::super::test_utils::TestCacheKey;
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<StoreRef<TestCacheKey>>();
    }
}
