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

use anyhow::anyhow;
use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};

use crate::source::base::SplitEnumerator;
use crate::source::kafka::split::KafkaSplit;
use crate::source::kafka::{KafkaProperties, KAFKA_SYNC_CALL_TIMEOUT};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum KafkaEnumeratorOffset {
    Earliest,
    Latest,
    Offset(i64),
    Timestamp(i64),
    None,
}

pub struct KafkaSplitEnumerator {
    broker_address: String,
    topic: String,
    admin_client: BaseConsumer,
    start_offset: KafkaEnumeratorOffset,

    // maybe used in the future for batch processing
    stop_offset: KafkaEnumeratorOffset,
}

impl KafkaSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for KafkaSplitEnumerator {
    type Properties = KafkaProperties;
    type Split = KafkaSplit;

    async fn new(properties: KafkaProperties) -> anyhow::Result<KafkaSplitEnumerator> {
        let broker_address = properties.brokers;
        let topic = properties.topic;

        let mut scan_start_offset = match properties
            .scan_startup_mode
            .map(|s| s.to_lowercase())
            .as_deref()
        {
            Some("earliest") => KafkaEnumeratorOffset::Earliest,
            Some("latest") => KafkaEnumeratorOffset::Latest,
            None => KafkaEnumeratorOffset::Earliest,
            _ => {
                return Err(anyhow!(
                    "properties `scan_startup_mode` only support earliest and latest or leave it empty"
                ));
            }
        };

        if let Some(s) = properties.time_offset {
            let time_offset = s.parse::<i64>().map_err(|e| anyhow!(e))?;
            scan_start_offset = KafkaEnumeratorOffset::Timestamp(time_offset)
        }

        let client: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &broker_address)
            .create_with_context(DefaultConsumerContext)
            .map_err(|e| anyhow!(e))?;

        Ok(Self {
            broker_address,
            topic,
            admin_client: client,
            start_offset: scan_start_offset,
            stop_offset: KafkaEnumeratorOffset::None,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<KafkaSplit>> {
        let topic_partitions = self.fetch_topic_partition().map_err(|e| {
            anyhow!(
                "failed to fetch metadata from kafka ({}): {}",
                self.broker_address,
                e
            )
        })?;

        let mut start_offsets = self
            .fetch_start_offset(topic_partitions.as_ref())
            .map_err(|e| anyhow!("{}", e))?;

        let mut stop_offsets = self
            .fetch_stop_offset(topic_partitions.as_ref())
            .map_err(|e| anyhow!("{}", e))?;

        let ret = topic_partitions
            .into_iter()
            .map(|partition| KafkaSplit {
                topic: self.topic.clone(),
                partition,
                start_offset: start_offsets.remove(&partition).unwrap(),
                stop_offset: stop_offsets.remove(&partition).unwrap(),
            })
            .collect();

        Ok(ret)
    }
}

impl KafkaSplitEnumerator {
    fn fetch_stop_offset(&self, partitions: &[i32]) -> KafkaResult<HashMap<i32, Option<i64>>> {
        match self.stop_offset {
            KafkaEnumeratorOffset::Earliest => unreachable!(),
            KafkaEnumeratorOffset::Latest => partitions
                .iter()
                .map(|partition| {
                    self.admin_client
                        .fetch_watermarks(self.topic.as_str(), *partition, KAFKA_SYNC_CALL_TIMEOUT)
                        .map(|watermark| (*partition, Some(watermark.1)))
                })
                .collect(),
            KafkaEnumeratorOffset::Offset(offset) => partitions
                .iter()
                .map(|partition| Ok((*partition, Some(offset))))
                .collect(),
            KafkaEnumeratorOffset::Timestamp(time) => self.fetch_offset_for_time(partitions, time),
            KafkaEnumeratorOffset::None => partitions
                .iter()
                .map(|partition| Ok((*partition, None)))
                .collect(),
        }
    }

    fn fetch_start_offset(&self, partitions: &[i32]) -> KafkaResult<HashMap<i32, Option<i64>>> {
        match self.start_offset {
            KafkaEnumeratorOffset::Earliest | KafkaEnumeratorOffset::Latest => partitions
                .iter()
                .map(|partition| {
                    self.admin_client
                        .fetch_watermarks(self.topic.as_str(), *partition, KAFKA_SYNC_CALL_TIMEOUT)
                        .map(|watermark| match self.start_offset {
                            KafkaEnumeratorOffset::Earliest => Some(watermark.0),
                            KafkaEnumeratorOffset::Latest => Some(watermark.1),
                            _ => unreachable!(),
                        })
                        .map(|offset| (*partition, offset))
                })
                .collect(),

            KafkaEnumeratorOffset::Offset(offset) => partitions
                .iter()
                .map(|partition| Ok((*partition, Some(offset))))
                .collect(),

            KafkaEnumeratorOffset::Timestamp(time) => self.fetch_offset_for_time(partitions, time),

            KafkaEnumeratorOffset::None => partitions
                .iter()
                .map(|partition| Ok((*partition, None)))
                .collect(),
        }
    }

    fn fetch_offset_for_time(
        &self,
        partitions: &[i32],
        time: i64,
    ) -> KafkaResult<HashMap<i32, Option<i64>>> {
        let mut tpl = TopicPartitionList::new();

        for partition in partitions {
            tpl.add_partition_offset(self.topic.as_str(), *partition, Offset::Offset(time))?;
        }

        let offsets = self
            .admin_client
            .offsets_for_times(tpl, KAFKA_SYNC_CALL_TIMEOUT)?;

        let mut result = HashMap::with_capacity(partitions.len());

        for elem in offsets.elements_for_topic(self.topic.as_str()) {
            match elem.offset() {
                Offset::Offset(offset) => {
                    result.insert(elem.partition(), Some(offset));
                }
                _ => {
                    let (_, high_watermark) = self.admin_client.fetch_watermarks(
                        self.topic.as_str(),
                        elem.partition(),
                        KAFKA_SYNC_CALL_TIMEOUT,
                    )?;
                    result.insert(elem.partition(), Some(high_watermark));
                }
            }
        }

        Ok(result)
    }

    fn fetch_topic_partition(&mut self) -> anyhow::Result<Vec<i32>> {
        // for now, we only support one topic
        let metadata = self
            .admin_client
            .fetch_metadata(Some(self.topic.as_str()), KAFKA_SYNC_CALL_TIMEOUT)?;

        let topic_meta = match metadata.topics() {
            [meta] => meta,
            _ => return Err(anyhow!("topic {} not found", self.topic)),
        };

        if topic_meta.partitions().is_empty() {
            return Err(anyhow!("topic {} not found", self.topic));
        }

        Ok(topic_meta
            .partitions()
            .iter()
            .map(|partition| partition.id())
            .collect())
    }
}
