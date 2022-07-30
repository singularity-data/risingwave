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

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use tokio::sync::Mutex;

use super::{ObjectError, ObjectResult};
use crate::object::{BlockLocation, ObjectMetadata, ObjectStore};

/// In-memory object storage, useful for testing.
#[derive(Default)]
pub struct InMemObjectStore {
    objects: Mutex<HashMap<String, (ObjectMetadata, Bytes)>>,
}

#[async_trait::async_trait]
impl ObjectStore for InMemObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("mem_upload_err", |_| Err(ObjectError::internal(
            "mem upload error"
        )));
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            let metadata = ObjectMetadata {
                key: path.to_owned(),
                last_modified: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(ObjectError::internal)?
                    .as_secs_f64(),
                total_size: obj.len(),
            };
            self.objects
                .lock()
                .await
                .insert(path.into(), (metadata, obj));
            Ok(())
        }
    }

    async fn read(&self, path: &str, block: Option<BlockLocation>) -> ObjectResult<Bytes> {
        fail_point!("mem_read_err", |_| Err(ObjectError::internal(
            "mem read error"
        )));
        if let Some(loc) = block {
            self.get_object(path, |obj| find_block(obj, loc)).await?
        } else {
            self.get_object(path, |obj| Ok(obj.clone())).await?
        }
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        let futures = block_locs
            .iter()
            .map(|block_loc| self.read(path, Some(*block_loc)))
            .collect_vec();
        try_join_all(futures).await
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        self.objects
            .lock()
            .await
            .get(path)
            .map(|(metadata, _)| metadata)
            .cloned()
            .ok_or_else(|| ObjectError::internal(format!("no object at path '{}'", path)))
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("mem_delete_err", |_| Err(ObjectError::internal(
            "mem delete error"
        )));
        self.objects.lock().await.remove(path);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        Ok(self
            .objects
            .lock()
            .await
            .iter()
            .filter_map(|(path, (metadata, _))| {
                if path.starts_with(prefix) {
                    return Some(metadata.clone());
                }
                None
            })
            .sorted_by(|a, b| Ord::cmp(&a.key, &b.key))
            .collect_vec())
    }
}

impl InMemObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }

    async fn get_object<R, F>(&self, path: &str, f: F) -> ObjectResult<R>
    where
        F: Fn(&Bytes) -> R,
    {
        self.objects
            .lock()
            .await
            .get(path)
            .map(|(_, obj)| obj)
            .ok_or_else(|| ObjectError::internal(format!("no object at path '{}'", path)))
            .map(f)
    }
}

fn find_block(obj: &Bytes, block: BlockLocation) -> ObjectResult<Bytes> {
    if block.offset + block.size > obj.len() {
        Err(ObjectError::internal("bad block offset and size"))
    } else {
        Ok(obj.slice(block.offset..(block.offset + block.size)))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::enumerate;

    use super::*;

    #[tokio::test]
    async fn test_upload() {
        let block = Bytes::from("123456");

        let s3 = InMemObjectStore::new();
        s3.upload("/abc", block).await.unwrap();

        // No such object.
        s3.read("/ab", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();

        let bytes = s3
            .read("/abc", Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // Overflow.
        s3.read("/abc", Some(BlockLocation { offset: 4, size: 4 }))
            .await
            .unwrap_err();

        s3.delete("/abc").await.unwrap();

        // No such object.
        s3.read("/abc", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_metadata() {
        let block = Bytes::from("123456");

        let obj_store = InMemObjectStore::new();
        obj_store.upload("/abc", block).await.unwrap();

        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
    }

    #[tokio::test]
    async fn test_list() {
        let payload = Bytes::from("123456");
        let store = InMemObjectStore::new();
        assert!(store.list("").await.unwrap().is_empty());

        let paths = vec!["001/002/test.obj", "001/003/test.obj"];
        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(store.list("").await.unwrap().len(), i);
            store.upload(path, payload.clone()).await.unwrap();
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

        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(store.list("").await.unwrap().len(), paths.len() - i);
            store.delete(path).await.unwrap();
            assert_eq!(store.list("").await.unwrap().len(), paths.len() - i - 1);
        }
    }
}
