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

use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use risingwave_common::cache::{CachableEntry, LruCache};
use tokio::io::AsyncWriteExt;

use crate::object::{BlockLocation, ObjectError, ObjectMetadata, ObjectResult, ObjectStore};

pub(super) mod utils {
    use std::fs::Metadata;
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use tokio::fs::{create_dir_all, OpenOptions};
    use tokio::task::spawn_blocking;

    use super::OpenReadFileHolder;
    use crate::object::{ObjectError, ObjectResult};

    pub async fn ensure_file_dir_exists(path: &Path) -> ObjectResult<()> {
        if let Some(dir) = path.parent() {
            // `create_dir_all` will not error even if the directory already exists.
            create_dir_all(dir).await.map_err(|e| {
                ObjectError::disk(format!("unable to create dir: {:?}.", dir.to_str(),), e)
            })?;
        }
        Ok(())
    }

    pub async fn open_file(
        path: &Path,
        enable_read: bool,
        enable_write: bool,
        create_new: bool,
    ) -> ObjectResult<tokio::fs::File> {
        ensure_file_dir_exists(path).await?;
        OpenOptions::new()
            .read(enable_read)
            .write(enable_write)
            .create_new(create_new)
            .open(path)
            .await
            .map_err(|err| {
                ObjectError::disk(format!("Failed to open file {:?}", path.to_str(),), err)
            })
    }

    pub async fn asyncify<F, T>(f: F) -> ObjectResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> ObjectResult<T> + Send + 'static,
    {
        spawn_blocking(f).await.map_err(|e| {
            ObjectError::internal(format!("Fail to join a blocking-spawned task: {}", e))
        })?
    }

    pub async fn get_metadata(file: OpenReadFileHolder) -> ObjectResult<Metadata> {
        asyncify(move || {
            file.value()
                .metadata()
                .map_err(|err| ObjectError::disk("Failed to get metadata.".to_string(), err))
        })
        .await
    }

    pub fn get_last_modified_timestamp_since_unix_epoch(
        metadata: &Metadata,
    ) -> ObjectResult<Duration> {
        metadata
            .modified()
            .map_err(|err| {
                ObjectError::disk("Last modification metadata not available".to_string(), err)
            })?
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(ObjectError::internal)
    }

    pub fn get_path_str(path: &Path) -> ObjectResult<String> {
        path.to_str()
            .map(|s| s.to_owned())
            .ok_or_else(|| ObjectError::internal("Don't support non-UTF-8 in path"))
    }
}

pub type OpenReadFileHolder = Arc<CachableEntry<PathBuf, File>>;

pub struct DiskObjectStore {
    path_prefix: String,
    opened_read_file_cache: Arc<LruCache<PathBuf, File>>,
}

const OPENED_FILE_CACHE_DEFAULT_NUM_SHARD_BITS: usize = 2;
const OPENED_FILE_CACHE_DEFAULT_CAPACITY: usize = 1024;

impl DiskObjectStore {
    pub fn new(path_prefix: &str) -> DiskObjectStore {
        DiskObjectStore {
            path_prefix: path_prefix.to_string(),
            opened_read_file_cache: Arc::new(LruCache::new(
                OPENED_FILE_CACHE_DEFAULT_NUM_SHARD_BITS,
                OPENED_FILE_CACHE_DEFAULT_CAPACITY,
            )),
        }
    }

    pub fn new_file_path(&self, path: &str) -> ObjectResult<PathBuf> {
        if path.starts_with('/') {
            return Err(ObjectError::disk(
                "".to_string(),
                std::io::Error::new(std::io::ErrorKind::Other, "path should not start with /"),
            ));
        };
        let mut ret = PathBuf::from(&self.path_prefix);
        ret.push(path);
        Ok(ret)
    }

    pub async fn get_read_file(&self, path: &str) -> ObjectResult<OpenReadFileHolder> {
        let path = self.new_file_path(path)?;
        let hash = {
            let mut hasher = DefaultHasher::default();
            path.hash(&mut hasher);
            hasher.finish()
        };
        let path_when_err = path.clone();
        let entry = self
            .opened_read_file_cache
            .lookup_with_request_dedup::<_, ObjectError, _>(
                hash,
                path.clone(),
                move || async move {
                    let file = utils::open_file(&path, true, false, false)
                        .await?
                        .into_std()
                        .await;
                    Ok((file, 1))
                },
            )
            .await
            .map_err(|e| {
                ObjectError::internal(format!(
                    "open file cache request dedup get canceled {:?}. Err{:?}",
                    path_when_err.to_str(),
                    e
                ))
            })??;
        Ok(Arc::new(entry))
    }
}

#[async_trait::async_trait]
impl ObjectStore for DiskObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        let mut file =
            utils::open_file(self.new_file_path(path)?.as_path(), false, true, true).await?;
        file.write_all(&obj)
            .await
            .map_err(|e| ObjectError::disk(format!("failed to write {}", path), e))?;
        file.flush()
            .await
            .map_err(|e| ObjectError::disk(format!("failed to flush {}", path), e))?;
        Ok(())
    }

    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        match block_loc {
            Some(block_loc) => Ok(self.readv(path, &[block_loc]).await?.pop().unwrap()),
            None => {
                let file_holder = self.get_read_file(path).await?;
                let metadata = utils::get_metadata(file_holder.clone()).await?;
                let path_owned = path.to_owned();
                utils::asyncify(move || {
                    let mut buf = vec![0; metadata.len() as usize];
                    file_holder
                        .value()
                        .read_exact_at(&mut buf, 0)
                        .map_err(|e| {
                            ObjectError::disk(
                                format!("failed to read the whole file {}", path_owned),
                                e,
                            )
                        })?;
                    Ok(Bytes::from(buf))
                })
                .await
            }
        }
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        let file_holder = self.get_read_file(path).await?;
        let metadata = utils::get_metadata(file_holder.clone()).await?;
        for block_loc in block_locs {
            if block_loc.offset + block_loc.size > metadata.len() as usize {
                return Err(ObjectError::disk(
                    "".to_string(),
                    Error::new(
                        ErrorKind::Other,
                        format!(
                            "block location {:?} is out of bounds for file of len {}",
                            block_loc,
                            metadata.len()
                        ),
                    ),
                ));
            }
        }
        let mut ret = Vec::with_capacity(block_locs.len());
        for block_loc_ref in block_locs {
            let file_holder = file_holder.clone();
            let path_owned = path.to_owned();
            let block_loc = *block_loc_ref;
            let future = utils::asyncify(move || {
                let mut buf = vec![0; block_loc.size as usize];
                file_holder
                    .value()
                    .read_exact_at(&mut buf, block_loc.offset as u64)
                    .map_err(|e| {
                        ObjectError::disk(
                            format!(
                                "failed to read  file {} at offset {} for size {}",
                                path_owned, block_loc.offset, block_loc.size
                            ),
                            e,
                        )
                    })?;
                Ok(Bytes::from(buf))
            });
            ret.push(future)
        }

        try_join_all(ret).await
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let file_holder = self.get_read_file(path).await?;
        let metadata = utils::get_metadata(file_holder).await?;
        Ok(ObjectMetadata {
            key: path.to_owned(),
            last_modified: utils::get_last_modified_timestamp_since_unix_epoch(&metadata)?
                .as_secs_f64(),
            total_size: metadata.len() as usize,
        })
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        tokio::fs::remove_file(self.new_file_path(path)?.as_path())
            .await
            .map_err(|e| ObjectError::disk(format!("failed to delete {}", path), e))?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        let mut list_result = vec![];
        let mut path_to_walk = vec![];
        let common_prefix = {
            let mut common_prefix = PathBuf::from(&self.path_prefix);
            common_prefix.push(prefix.trim_start_matches(std::path::MAIN_SEPARATOR));
            utils::get_path_str(common_prefix.as_path())?.to_owned()
        };
        path_to_walk.push(PathBuf::from(&self.path_prefix));
        while let Some(path) = path_to_walk.pop() {
            let mut entries = tokio::fs::read_dir(path.as_path()).await.map_err(|err| {
                ObjectError::disk(
                    format!("Failed to read dir {}", path.to_str().unwrap_or("")),
                    err,
                )
            })?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|err| ObjectError::disk(format!("Failed to list {}", prefix), err))?
            {
                let metadata = entry
                    .metadata()
                    .await
                    .map_err(|err| ObjectError::disk("Failed to get metadata.".to_string(), err))?;
                let entry_path = utils::get_path_str(entry.path().as_path())?;
                if metadata.is_symlink() {
                    tracing::warn!("Skip symlink {}", entry_path);
                    continue;
                }
                if metadata.is_dir() {
                    if entry_path.starts_with(&common_prefix)
                        || common_prefix.starts_with(&entry_path)
                    {
                        path_to_walk.push(entry.path());
                    }
                    continue;
                }
                if !entry_path.starts_with(&common_prefix) {
                    continue;
                }
                list_result.push(ObjectMetadata {
                    key: entry_path
                        .trim_start_matches(&self.path_prefix)
                        .trim_start_matches(std::path::MAIN_SEPARATOR)
                        .to_owned(),
                    last_modified: utils::get_last_modified_timestamp_since_unix_epoch(&metadata)?
                        .as_secs_f64(),
                    total_size: metadata.len() as usize,
                });
            }
        }
        list_result.sort_by(|a, b| Ord::cmp(&a.key, &b.key));
        Ok(list_result)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::path::PathBuf;

    use bytes::Bytes;
    use itertools::{enumerate, Itertools};
    use tempfile::TempDir;

    use crate::object::disk::DiskObjectStore;
    use crate::object::{BlockLocation, ObjectStore};

    fn gen_test_payload() -> Vec<u8> {
        let mut ret = Vec::new();
        for i in 0..100000 {
            ret.extend(format!("{:05}", i).as_bytes());
        }
        ret
    }

    fn check_payload(payload: &[u8], path: &str) {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        assert_eq!(payload.len(), file.metadata().unwrap().len() as usize);
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(payload, &buf[..]);
    }

    #[tokio::test]
    async fn test_simple_upload() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);

        let mut path = PathBuf::from(test_root_path);
        path.push("test.obj");
        check_payload(&payload, path.to_str().unwrap());
    }

    #[tokio::test]
    async fn test_multi_level_dir_upload() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("1/2/test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("1/2/test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);

        let mut path = PathBuf::from(test_root_path);
        path.push("1/2/test.obj");
        check_payload(&payload, path.to_str().unwrap());
    }

    #[tokio::test]
    async fn test_read_all() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);
        let read_data = store.read("test.obj", None).await.unwrap();
        assert_eq!(payload, &read_data[..]);
    }

    #[tokio::test]
    async fn test_read_partial() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);
        let read_data = store
            .read(
                "test.obj",
                Some(BlockLocation {
                    offset: 10000,
                    size: 1000,
                }),
            )
            .await
            .unwrap();
        assert_eq!(&payload[10000..11000], &read_data[..]);
    }

    #[tokio::test]
    async fn test_read_multi_block() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);
        let test_loc = vec![(0, 1000), (10000, 1000), (20000, 1000)];
        let read_data = store
            .readv(
                "test.obj",
                &test_loc
                    .iter()
                    .map(|(offset, size)| BlockLocation {
                        offset: *offset,
                        size: *size,
                    })
                    .collect_vec(),
            )
            .await
            .unwrap();
        assert_eq!(test_loc.len(), read_data.len());
        for (i, (offset, size)) in test_loc.iter().enumerate() {
            assert_eq!(&payload[*offset..(*offset + *size)], &read_data[i][..]);
        }
    }

    #[tokio::test]
    async fn test_delete() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let mut path = PathBuf::from(test_root_path);
        path.push("test.obj");
        assert!(path.exists());
        store.delete("test.obj").await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn test_read_not_exists() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);

        assert!(store.read("non-exist.obj", None).await.is_err());
    }

    #[tokio::test]
    async fn test_read_out_of_range() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        assert_eq!(payload.len(), 500000);
        assert!(store
            .read(
                "test.obj",
                Some(BlockLocation {
                    offset: 499999,
                    size: 1,
                })
            )
            .await
            .is_ok());
        assert!(store
            .read(
                "test.obj",
                Some(BlockLocation {
                    offset: 499999,
                    size: 2,
                })
            )
            .await
            .is_err());
        assert!(store
            .readv(
                "test.obj",
                &[
                    BlockLocation {
                        offset: 499999,
                        size: 2,
                    },
                    BlockLocation {
                        offset: 10000,
                        size: 2,
                    }
                ]
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_invalid_path() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        // path is not allowed to be started with '/'
        assert!(store
            .upload("/test.obj", Bytes::from(payload))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = DiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        assert!(store.list("").await.unwrap().is_empty());
        assert!(store.list("/").await.unwrap().is_empty());

        let paths = vec!["001/002/test.obj", "001/003/test.obj"];
        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(store.list("").await.unwrap().len(), i);
            store
                .upload(path, Bytes::from(payload.clone()))
                .await
                .unwrap();
            assert_eq!(store.list("").await.unwrap().len(), i + 1);
        }

        let list_path = store
            .list("")
            .await
            .unwrap()
            .iter()
            .map(|p| p.key.clone())
            .collect_vec();
        assert_eq!(list_path, paths);

        for i in 0..=5 {
            assert_eq!(store.list(&paths[0][0..=i]).await.unwrap().len(), 2);
        }
        for i in 6..=paths[0].len() - 1 {
            assert_eq!(store.list(&paths[0][0..=i]).await.unwrap().len(), 1);
        }
        assert!(store.list("003").await.unwrap().is_empty());
        assert_eq!(store.list("/").await.unwrap().len(), 2);

        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(store.list("").await.unwrap().len(), paths.len() - i);
            store.delete(path).await.unwrap();
            assert_eq!(store.list("").await.unwrap().len(), paths.len() - i - 1);
        }
    }
}
