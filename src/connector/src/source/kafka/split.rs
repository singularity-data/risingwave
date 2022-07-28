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

use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::source::SplitMetaData;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct KafkaSplit {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) start_offset: Option<i64>,
    pub(crate) stop_offset: Option<i64>,
}

impl SplitMetaData for KafkaSplit {
    fn id(&self) -> String {
        format!("{}", self.partition)
    }

    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

impl KafkaSplit {
    pub fn new(
        partition: i32,
        start_offset: Option<i64>,
        stop_offset: Option<i64>,
        topic: String,
    ) -> KafkaSplit {
        KafkaSplit {
            topic,
            partition,
            start_offset,
            stop_offset,
        }
    }

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        Self::new(
            self.partition,
            Some(start_offset.as_str().parse::<i64>().unwrap()),
            self.stop_offset,
            self.topic.clone(),
        )
    }

    pub fn get_topic_and_partition(&self) -> (String, i32) {
        (self.topic.clone(), self.partition)
    }
}
