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

#[macro_export]
macro_rules! impl_split_enumerator {
    ([], $({ $variant_name:ident, $split_enumerator_name:ident} ),*) => {
        impl SplitEnumeratorImpl {

             pub async fn create(properties: ConnectorProperties) -> Result<Self> {
                match properties {
                    $( ConnectorProperties::$variant_name(props) => $split_enumerator_name::new(props).await.map(Self::$variant_name), )*
                    other => Err(anyhow!(
                        "split enumerator type for config {:?} is not supported",
                        other
                    )),
                }
             }

             pub async fn list_splits(&mut self) -> Result<Vec<SplitImpl>> {
                match self {
                    $( Self::$variant_name(inner) => inner
                        .list_splits()
                        .await
                        .map(|ss| {
                            ss.into_iter()
                                .map(SplitImpl::$variant_name)
                                .collect_vec()
                        })
                        .map_err(|e| ErrorCode::ConnectorError(e.to_string()).into()),
                    )*
                }
             }
        }
    }
}

#[macro_export]
macro_rules! impl_split {
    ([], $({ $variant_name:ident, $connector_name:ident, $split:ty} ),*) => {
        impl From<&SplitImpl> for ConnectorSplit {
            fn from(split: &SplitImpl) -> Self {
                match split {
                    $( SplitImpl::$variant_name(inner) => ConnectorSplit { split_type: String::from($connector_name), encoded_split: inner.encode_to_bytes().to_vec() }, )*
                }
            }
        }

        impl TryFrom<&ConnectorSplit> for SplitImpl {
            type Error = anyhow::Error;

            fn try_from(split: &ConnectorSplit) -> std::result::Result<Self, Self::Error> {
                match split.split_type.to_lowercase().as_str() {
                    $( $connector_name => <$split>::restore_from_bytes(split.encoded_split.as_ref()).map(SplitImpl::$variant_name), )*
                        other => {
                    Err(anyhow!("connector '{}' is not supported", other))
                    }
                }
            }
        }

        impl SplitMetaData for SplitImpl {
            fn id(&self) -> String {
                match self {
                    $( Self::$variant_name(inner) => inner.id(), )*
                }
            }

            fn encode_to_bytes(&self) -> Bytes {
                Bytes::from(ConnectorSplit::from(self).encode_to_vec())
            }

            fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
                SplitImpl::try_from(&ConnectorSplit::decode(bytes)?)
            }
        }

        impl SplitImpl {
             pub fn get_type(&self) -> String {
                match self {
                    $( Self::$variant_name(_) => $connector_name, )*
                }
                    .to_string()
            }

            pub fn update(&self, start_offset: String) -> Self {
                match self {
                    $( Self::$variant_name(inner) => Self::$variant_name(inner.copy_with_offset(start_offset)), )*
                }
            }
        }
    }
}

#[macro_export]
macro_rules! impl_split_reader {
    ([], $({ $variant_name:ident, $split_reader_name:ident} ),*) => {
        impl SplitReaderImpl {
            pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
                match self {
                    $( Self::$variant_name(inner) => inner.next().await, )*
                }
            }

             pub async fn create(
                config: ConnectorProperties,
                state: ConnectorState,
                columns: Option<Vec<Column>>,
            ) -> Result<Self> {
                if state.is_none() {
                    return Ok(Self::Dummy(Box::new(DummySplitReader {})));
                }

                let connector = match config {
                     $( ConnectorProperties::$variant_name(props) => Self::$variant_name(Box::new($split_reader_name::new(props, state, columns).await?)), )*
                    _ => todo!()
                };

                Ok(connector)
            }
        }
    }
}

#[macro_export]
macro_rules! impl_connector_properties {
    ([], $({ $variant_name:ident, $connector_name:ident } ),*) => {
        impl ConnectorProperties {
            pub fn extract(mut props: HashMap<String, String>) -> Result<Self> {
                const UPSTREAM_SOURCE_KEY: &str = "connector";
                let connector = props.remove(UPSTREAM_SOURCE_KEY).ok_or_else(|| anyhow!("Must specify 'connector' in WITH clause"))?;
                let json_value = serde_json::to_value(props).map_err(|e| anyhow!(e))?;
                match connector.to_lowercase().as_str() {
                    $(
                        $connector_name => {
                            serde_json::from_value(json_value).map_err(|e| anyhow!(e.to_string())).map(Self::$variant_name)
                        },
                    )*
                    _ => {
                        Err(anyhow!("connector '{}' is not supported", connector,))
                    }
                }
            }
        }
    }
}
