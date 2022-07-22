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

use aws_sdk_s3::client::fluent_builders::GetObject;
use aws_sdk_s3::{Client, Endpoint, Region};
use aws_smithy_http::body::SdkBody;
use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use tokio::io::AsyncRead;

use super::{BlockLocation, ObjectError, ObjectMetadata};
use crate::object::{Bytes, ObjectResult, ObjectStore};

/// Object store with S3 backend
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("s3_upload_err", |_| Err(ObjectError::internal(
            "s3 upload error"
        )));
        self.client
            .put_object()
            .bucket(&self.bucket)
            .body(SdkBody::from(obj).into())
            .key(path)
            .send()
            .await?;
        Ok(())
    }

    /// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        fail_point!("s3_read_err", |_| Err(ObjectError::internal(
            "s3 read error"
        )));
        let req = self.obj_store_request(path, block_loc);
        let resp = req.send().await?;
        let val = resp.body.collect().await?.into_bytes();

        if block_loc.is_some() && block_loc.as_ref().unwrap().size != val.len() {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                block_loc.as_ref().unwrap().size,
                val.len(),
                path,
                block_loc.as_ref().unwrap()
            )));
        }
        Ok(val)
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        let futures = block_locs
            .iter()
            .map(|block_loc| self.read(path, Some(*block_loc)))
            .collect_vec();
        try_join_all(futures).await
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        fail_point!("s3_metadata_err", |_| Err(ObjectError::internal(
            "s3 metadata error"
        )));
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(ObjectMetadata {
            total_size: resp.content_length as usize,
        })
    }

    async fn streaming_read(
        &self,
        path: &str,
        block_loc: Option<BlockLocation>,
    ) -> ObjectResult<Box<dyn AsyncRead + Unpin>> {
        fail_point!("s3_streaming_read_err", |_| Err(ObjectError::internal(
            "s3 streaming read error"
        )));
        let req = self.obj_store_request(path, block_loc);

        let resp = req.send().await?;
        Ok(Box::new(resp.body.into_async_read()))

        // TODO: we may wrap stream to check an `expected_bytes: Option<usize>` parameter.
        //
        // if block_loc.is_some() && block_loc.as_ref().unwrap().size != val.len() {
        //     return Err(ObjectError::internal(format!(
        //         "mismatched size: expected {}, found {} when reading {} at {:?}",
        //         block_loc.as_ref().unwrap().size,
        //         val.len(),
        //         path,
        //         block_loc.as_ref().unwrap()
        //     )));
        // }
    }

    /// Permanently deletes the whole object.
    /// According to Amazon S3, this will simply return Ok if the object does not exist.
    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("s3_delete_err", |_| Err(ObjectError::internal(
            "s3 delete error"
        )));
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(())
    }
}

impl S3ObjectStore {
    /// Creates an S3 object store from environment variable.
    ///
    /// See [AWS Docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) on how to provide credentials and region from env variable. If you are running compute-node on EC2, no configuration is required.
    pub async fn new(bucket: String) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let client = Client::new(&shared_config);

        Self { client, bucket }
    }

    /// Creates a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub async fn with_minio(server: &str) -> Self {
        let server = server.strip_prefix("minio://").unwrap();
        let (access_key_id, rest) = server.split_once(':').unwrap();
        let (secret_access_key, rest) = rest.split_once('@').unwrap();
        let (address, bucket) = rest.split_once('/').unwrap();

        let loader = aws_config::ConfigLoader::default();
        let builder = aws_sdk_s3::config::Builder::from(&loader.load().await);
        let builder = builder.region(Region::new("custom"));
        let builder = builder.endpoint_resolver(Endpoint::immutable(
            format!("http://{}", address).try_into().unwrap(),
        ));
        let builder = builder.credentials_provider(aws_sdk_s3::Credentials::from_keys(
            access_key_id,
            secret_access_key,
            None,
        ));
        let config = builder.build();
        let client = Client::from_conf(config);
        Self {
            client,
            bucket: bucket.to_string(),
        }
    }

    fn obj_store_request(&self, path: &str, block_loc: Option<BlockLocation>) -> GetObject {
        let req = self.client.get_object().bucket(&self.bucket).key(path);

        let range = match block_loc.as_ref() {
            None => None,
            Some(block_location) => block_location.byte_range_specifier(),
        };

        if let Some(range) = range {
            req.range(range)
        } else {
            req
        }
    }
}
