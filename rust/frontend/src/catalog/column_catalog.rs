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
//

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnCatalog as ProstColumnCatalog;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnCatalog {
    pub column_desc: ColumnDesc,
    pub is_hidden: bool,
    pub catalogs: Vec<ColumnCatalog>,
    pub type_name: String,
}

impl ColumnCatalog {
    /// Get the column catalog's is hidden.
    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    /// Get a reference to the column desc's data type.
    pub fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    /// Get the column desc's column id.
    pub fn column_id(&self) -> ColumnId {
        self.column_desc.column_id
    }

    /// Get a reference to the column desc's name.
    pub fn name(&self) -> &str {
        self.column_desc.name.as_ref()
    }

    // Get all column descs by recursion
    pub fn get_column_descs(&self) -> Vec<ColumnDesc> {
        let mut descs = vec![self.column_desc.clone()];
        for catalog in &self.catalogs {
            descs.append(&mut catalog.get_column_descs());
        }
        descs
    }
}

impl From<ProstColumnCatalog> for ColumnCatalog {
    // If the DataType is struct, the column_catalog need to rebuild DataType Struct fields
    // according to its catalogs
    fn from(prost: ProstColumnCatalog) -> Self {
        let mut column_desc: ColumnDesc = prost.column_desc.unwrap().into();
        if let DataType::Struct { .. } = column_desc.data_type {
            let catalogs: Vec<ColumnCatalog> = prost
                .catalogs
                .into_iter()
                .map(ColumnCatalog::from)
                .collect();
            column_desc.data_type = DataType::Struct {
                fields: catalogs
                    .clone()
                    .into_iter()
                    .map(|c| c.data_type().clone())
                    .collect_vec()
                    .into(),
            };
            Self {
                column_desc,
                is_hidden: prost.is_hidden,
                catalogs,
                type_name: prost.type_name,
            }
        } else {
            Self {
                column_desc,
                is_hidden: prost.is_hidden,
                catalogs: vec![],
                type_name: prost.type_name,
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::*;
    use risingwave_pb::plan::{ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc};

    use crate::catalog::column_catalog::ColumnCatalog;
    pub fn build_prost_catalog() -> ProstColumnCatalog {
        let city = vec![
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(DataType::Varchar.to_protobuf()),
                    name: "country.city.address".to_string(),
                    column_id: 2,
                }),
                is_hidden: false,
                catalogs: vec![],
                ..Default::default()
            },
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(DataType::Varchar.to_protobuf()),
                    name: "country.city.zipcode".to_string(),
                    column_id: 3,
                }),
                is_hidden: false,
                catalogs: vec![],
                ..Default::default()
            },
        ];
        let country = vec![
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(DataType::Varchar.to_protobuf()),
                    name: "country.address".to_string(),
                    column_id: 1,
                }),
                is_hidden: false,
                catalogs: vec![],
                ..Default::default()
            },
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(
                        DataType::Struct {
                            fields: vec![].into(),
                        }
                        .to_protobuf(),
                    ),
                    name: "country.city".to_string(),
                    column_id: 4,
                }),
                is_hidden: false,
                catalogs: city,
                type_name: ".test.City".to_string(),
            },
        ];
        ProstColumnCatalog {
            column_desc: Some(ProstColumnDesc {
                column_type: Some(
                    DataType::Struct {
                        fields: vec![].into(),
                    }
                    .to_protobuf(),
                ),
                name: "country".to_string(),
                column_id: 5,
            }),
            is_hidden: false,
            catalogs: country,
            type_name: ".test.Country".to_string(),
        }
    }

    pub fn build_catalog() -> ColumnCatalog {
        let city = vec![
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Varchar,
                    name: "country.city.address".to_string(),
                    column_id: ColumnId::new(2),
                },
                is_hidden: false,
                catalogs: vec![],
                type_name: String::new(),
            },
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Varchar,
                    name: "country.city.zipcode".to_string(),
                    column_id: ColumnId::new(3),
                },
                is_hidden: false,
                catalogs: vec![],
                type_name: String::new(),
            },
        ];
        let data_type = vec![DataType::Varchar, DataType::Varchar];
        let country = vec![
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Varchar,
                    name: "country.address".to_string(),
                    column_id: ColumnId::new(1),
                },
                is_hidden: false,
                catalogs: vec![],
                type_name: String::new(),
            },
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Struct {
                        fields: data_type.clone().into(),
                    },
                    name: "country.city".to_string(),
                    column_id: ColumnId::new(4),
                },
                is_hidden: false,
                catalogs: city,
                type_name: ".test.City".to_string(),
            },
        ];

        ColumnCatalog {
            column_desc: ColumnDesc {
                data_type: DataType::Struct {
                    fields: vec![
                        DataType::Varchar,
                        DataType::Struct {
                            fields: data_type.into(),
                        },
                    ]
                    .into(),
                },
                column_id: ColumnId::new(5),
                name: "country".to_string(),
            },
            is_hidden: false,
            catalogs: country,
            type_name: ".test.Country".to_string(),
        }
    }
    #[test]
    fn test_into_column_catalog() {
        let catalog: ColumnCatalog = build_prost_catalog().into();
        assert_eq!(catalog, build_catalog());
    }
}
