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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::dummy_connector::DummySplitReader;
use crate::kafka::enumerator::KafkaSplitEnumerator;
use crate::kafka::source::KafkaSplitReader;
use crate::kafka::KafkaSplit;
use crate::kinesis::enumerator::client::KinesisSplitEnumerator;
use crate::kinesis::source::reader::KinesisMultiSplitReader;
use crate::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::nexmark::source::reader::NexmarkSplitReader;
use crate::nexmark::{NexmarkSplit, NexmarkSplitEnumerator};
use crate::pulsar::source::reader::PulsarSplitReader;
use crate::pulsar::{PulsarEnumeratorOffset, PulsarSplit, PulsarSplitEnumerator};
use crate::ConnectorProperties;

pub type DataType = risingwave_common::types::DataType;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

const KAFKA_SOURCE: &str = "kafka";
const KINESIS_SOURCE: &str = "kinesis";
const PULSAR_SOURCE: &str = "pulsar";
const NEXMARK_SOURCE: &str = "nexmark";

const PULSAR_SPLIT_TYPE: &str = "pulsar";
const S3_SPLIT_TYPE: &str = "s3";
const KINESIS_SPLIT_TYPE: &str = "kinesis";
const KAFKA_SPLIT_TYPE: &str = "kafka";
const NEXMARK_SPLIT_TYPE: &str = "nexmark";

/// The message pumped from the external source service.
/// The third-party message structs will eventually be transformed into this struct.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceMessage {
    pub payload: Option<Bytes>,
    pub offset: String,
    pub split_id: String,
}

/// The metadata of a split.
pub trait SplitMetaData: Sized {
    fn id(&self) -> String;
    fn encode_to_bytes(&self) -> Bytes;
    fn restore_from_bytes(bytes: &[u8]) -> Result<Self>;
}

/// `SplitEnumerator` fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator {
    type Split: SplitMetaData + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

#[async_trait]
pub trait SplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>>;
}

/// The persistent state of the connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorState {
    pub identifier: Bytes,
    pub start_offset: String,
    pub end_offset: String,
}

impl ConnectorState {
    pub fn from_hashmap(state: HashMap<String, String>) -> Vec<Self> {
        if state.is_empty() {
            return vec![];
        }
        let mut connector_states: Vec<Self> = Vec::with_capacity(state.len());
        connector_states.extend(state.iter().map(|(split, offset)| Self {
            identifier: Bytes::from(split.to_owned()),
            start_offset: offset.clone(),
            end_offset: "".to_string(),
        }));
        connector_states
    }
}

impl From<SplitImpl> for ConnectorState {
    fn from(split: SplitImpl) -> Self {
        match split {
            SplitImpl::Kafka(kafka) => Self {
                identifier: Bytes::from(kafka.partition.to_string()),
                start_offset: kafka.start_offset.unwrap().to_string(),
                end_offset: if let Some(end_offset) = kafka.stop_offset {
                    end_offset.to_string()
                } else {
                    "".to_string()
                },
            },
            SplitImpl::Kinesis(kinesis) => Self {
                identifier: Bytes::from(kinesis.shard_id),
                start_offset: match kinesis.start_position {
                    KinesisOffset::SequenceNumber(s) => s,
                    _ => "".to_string(),
                },
                end_offset: match kinesis.end_position {
                    KinesisOffset::SequenceNumber(s) => s,
                    _ => "".to_string(),
                },
            },
            SplitImpl::Pulsar(pulsar) => Self {
                identifier: Bytes::from(pulsar.topic.to_string()),
                start_offset: match pulsar.start_offset {
                    PulsarEnumeratorOffset::MessageId(id) => id,
                    _ => "".to_string(),
                },
                end_offset: "".to_string(),
            },
            SplitImpl::Nexmark(nex_mark) => Self {
                identifier: Bytes::from(nex_mark.id()),
                start_offset: match nex_mark.start_offset {
                    Some(s) => s.to_string(),
                    _ => "".to_string(),
                },
                end_offset: "".to_string(),
            },
        }
    }
}

impl SplitMetaData for ConnectorState {
    fn id(&self) -> String {
        String::from_utf8(self.identifier.to_vec()).unwrap()
    }

    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

#[derive(Debug, Clone)]
pub enum ConnectorStateV2 {
    // ConnectorState should change to Vec<ConnectorState> because there can be multiple readers
    // in a source executor
    State(ConnectorState),
    Splits(Vec<SplitImpl>),
    None,
}

pub enum SplitReaderImpl {
    Kinesis(Box<KinesisMultiSplitReader>),
    Kafka(Box<KafkaSplitReader>),
    Dummy(Box<DummySplitReader>),
    Nexmark(Box<NexmarkSplitReader>),
    Pulsar(Box<PulsarSplitReader>),
}

impl SplitReaderImpl {
    pub async fn create(
        config: ConnectorProperties,
        state: ConnectorStateV2,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        if let ConnectorStateV2::Splits(s) = &state {
            if s.is_empty() {
                return Ok(Self::Dummy(Box::new(DummySplitReader {})));
            }
        }

        let connector = match config {
            ConnectorProperties::Kafka(props) => {
                Self::Kafka(Box::new(KafkaSplitReader::new(props, state).await?))
            }
            ConnectorProperties::Kinesis(props) => {
                Self::Kinesis(Box::new(KinesisMultiSplitReader::new(props, state).await?))
            }
            ConnectorProperties::Nexmark(props) => {
                Self::Nexmark(Box::new(NexmarkSplitReader::new(*props, state).await?))
            }
            ConnectorProperties::Pulsar(props) => {
                Self::Pulsar(Box::new(PulsarSplitReader::new(props, state).await?))
            }
            _other => {
                todo!()
            }
        };
        Ok(connector)
    }
}

macro_rules! impl_split_reader {
    ([], $({ $variant_name:ident, $split_name:ident, $split:ty} ),*) => {
        impl SplitReaderImpl {
            pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
                match self {
                    $( Self::$variant_name(inner) => inner.next().await, )*
                }
            }
        }
    }
}

impl_split_reader! {
            [ ] ,
            { Kafka, KAFKA_SPLIT_TYPE, KafkaSplit },
            { Pulsar, PULSAR_SPLIT_TYPE, PulsarSplit },
            { Kinesis, KINESIS_SPLIT_TYPE, KinesisSplit },
            { Nexmark, NEXMARK_SPLIT_TYPE, NexmarkSplit },
            { Dummy, NEXMARK_SPLIT_TYPE, NexmarkSplit }
}

pub enum SplitEnumeratorImpl {
    Kafka(KafkaSplitEnumerator),
    Pulsar(PulsarSplitEnumerator),
    Kinesis(KinesisSplitEnumerator),
    Nexmark(NexmarkSplitEnumerator),
}

impl SplitEnumeratorImpl {
    pub async fn create(properties: ConnectorProperties) -> Result<Self> {
        match properties {
            ConnectorProperties::Kafka(props) => KafkaSplitEnumerator::new(props).map(Self::Kafka),
            ConnectorProperties::Pulsar(props) => {
                PulsarSplitEnumerator::new(props).map(Self::Pulsar)
            }
            ConnectorProperties::Kinesis(props) => {
                KinesisSplitEnumerator::new(props).await.map(Self::Kinesis)
            }
            ConnectorProperties::Nexmark(props) => {
                NexmarkSplitEnumerator::new(props.as_ref()).map(Self::Nexmark)
            }
            ConnectorProperties::S3(_) => todo!(),
        }
    }
}

macro_rules! impl_split_enumerator {
    ([], $({ $variant_name:ident, $split_name:ident, $split:ty} ),*) => {
        impl SplitEnumeratorImpl {
             pub async fn list_splits(&mut self) -> Result<Vec<SplitImpl>> {
                match self {
                    $( Self::$variant_name(inner) => inner.list_splits().await.map(|ss| ss.into_iter().map(SplitImpl::$variant_name).collect_vec()), )*
                }
             }
        }
    }
}

impl_split_enumerator! {
            [ ] ,
            { Kafka, KAFKA_SPLIT_TYPE, KafkaSplit },
            { Pulsar, PULSAR_SPLIT_TYPE, PulsarSplit },
            { Kinesis, KINESIS_SPLIT_TYPE, KinesisSplit },
            { Nexmark, NEXMARK_SPLIT_TYPE, NexmarkSplit }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SplitImpl {
    Kafka(KafkaSplit),
    Pulsar(PulsarSplit),
    Kinesis(KinesisSplit),
    Nexmark(NexmarkSplit),
}

macro_rules! impl_split {
    ([], $({ $variant_name:ident, $split_name:ident, $split:ty} ),*) => {
        impl SplitImpl {
            pub fn id(&self) -> String {
                match self {
                    $( Self::$variant_name(inner) => inner.id(), )*
                }
            }

            pub fn to_json_bytes(&self) -> Bytes {
                match self {
                    $( Self::$variant_name(inner) => inner.encode_to_bytes(), )*
                }
            }
            //
            pub fn get_type(&self) -> String {
                match self {
                    $( Self::$variant_name(_) => $split_name, )*
                }
                    .to_string()
            }

            pub fn restore_from_bytes(split_type: String, bytes: &[u8]) -> Result<Self> {
                match split_type.as_str() {
                    $( $split_name => <$split>::restore_from_bytes(bytes).map(Self::$variant_name), )*
                    other => Err(anyhow!("split type {} not supported", other)),
                }
            }
        }
    }
}

impl_split! {
            [ ] ,
            { Kafka, KAFKA_SPLIT_TYPE, KafkaSplit },
            { Pulsar, PULSAR_SPLIT_TYPE, PulsarSplit },
            { Kinesis, KINESIS_SPLIT_TYPE, KinesisSplit },
            { Nexmark, NEXMARK_SPLIT_TYPE, NexmarkSplit }
}