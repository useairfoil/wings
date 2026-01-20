// This code originally comes from the DataFusion project.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
pub mod conversion;
pub mod error;
pub mod from_proto;
pub mod to_proto;

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
pub use error::{Result, SchemaError};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

pub mod pb {
    tonic::include_proto!("wings.schema");
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub id: u64,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self {
            fields,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn fields_iter(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
    }

    pub fn arrow_schema(&self) -> ArrowSchema {
        self.clone().into()
    }
}

impl Field {
    pub fn new(name: impl Into<String>, id: u64, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            id,
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn into_arrow_field(self) -> ArrowField {
        self.into()
    }
}

impl From<Field> for ArrowField {
    fn from(f: Field) -> Self {
        let mut metadata = f.metadata;
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), f.id.to_string());
        ArrowField::new(f.name, f.data_type, f.nullable).with_metadata(metadata)
    }
}

impl From<Schema> for ArrowSchema {
    fn from(s: Schema) -> Self {
        let fields: Vec<ArrowField> = s.fields.into_iter().map(Into::into).collect();
        ArrowSchema::new(fields)
    }
}
