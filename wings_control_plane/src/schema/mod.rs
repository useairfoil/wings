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
mod conversion;
mod data_type;
mod datum;
mod delta;
pub mod error;
mod from_proto;
mod to_proto;
pub mod visitor;

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::schema::visitor::visit_field;

pub use self::{
    data_type::{DataType, TimeUnit},
    datum::Datum,
    error::{Result, SchemaError},
    visitor::{SchemaVisitor, visit_schema},
};

pub mod pb {
    tonic::include_proto!("wings.schema");
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub fields: Fields,
    pub metadata: HashMap<String, String>,
    field_id_to_field: HashMap<u64, FieldRef>,
}

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub id: u64,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

pub type FieldRef = Arc<Field>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fields(Arc<[FieldRef]>);

#[derive(Debug)]
pub struct SchemaBuilder {
    fields: Fields,
    metadata: HashMap<String, String>,
}

impl Schema {
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn fields_iter(&self) -> impl Iterator<Item = &Field> {
        self.fields.0.iter().map(AsRef::as_ref)
    }

    pub fn field_by_id(&self, id: u64) -> Option<&FieldRef> {
        self.field_id_to_field.get(&id)
    }

    pub fn arrow_schema(&self) -> ArrowSchema {
        let fields = self
            .fields
            .0
            .iter()
            .map(|f| f.to_arrow_field())
            .collect::<Vec<_>>();
        ArrowSchema::new(fields)
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    pub fn into_arrow_field(self) -> ArrowField {
        let mut metadata = self.metadata;
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), self.id.to_string());
        ArrowField::new(self.name, self.data_type.into(), self.nullable).with_metadata(metadata)
    }

    pub fn to_arrow_field(&self) -> ArrowField {
        self.clone().into_arrow_field()
    }
}

impl SchemaBuilder {
    pub fn new(fields: impl Into<Fields>) -> Self {
        Self {
            fields: fields.into(),
            metadata: Default::default(),
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn build(self) -> Result<Schema> {
        let mut visitor = FieldIdMapBuilder::default();
        for field in self.fields.iter() {
            visit_field(field, &mut visitor)?;
        }

        Ok(Schema {
            fields: self.fields,
            metadata: self.metadata,
            field_id_to_field: visitor.field_id_to_field,
        })
    }
}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Field {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for Field {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);

        // ensure deterministic key order
        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(state);
            self.metadata.get(k).expect("key valid").hash(state);
        }
    }
}

#[derive(Debug, Default)]
struct FieldIdMapBuilder {
    field_id_to_field: HashMap<u64, FieldRef>,
}

impl SchemaVisitor for FieldIdMapBuilder {
    type Error = SchemaError;

    fn field(&mut self, field: &FieldRef) -> Result<()> {
        let Some(existing) = self.field_id_to_field.insert(field.id, field.clone()) else {
            return Ok(());
        };

        Err(SchemaError::DuplicateFieldId {
            id: field.id,
            f1_name: existing.name().to_string(),
            f2_name: field.name().to_string(),
        })
    }

    fn before_list_element(&mut self, _field: &FieldRef) -> Result<()> {
        Ok(())
    }

    fn after_list_element(&mut self, _field: &FieldRef) -> Result<()> {
        Ok(())
    }

    fn before_struct_field(&mut self, _field: &FieldRef) -> Result<()> {
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &FieldRef) -> Result<()> {
        Ok(())
    }
}

impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<FieldRef> for Fields {
    fn from_iter<T: IntoIterator<Item = FieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Field>> for Fields {
    fn from(value: Vec<Field>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<FieldRef>> for Fields {
    fn from(value: Vec<FieldRef>) -> Self {
        Self(value.into())
    }
}

impl From<&[FieldRef]> for Fields {
    fn from(value: &[FieldRef]) -> Self {
        Self(value.into())
    }
}

impl<const N: usize> From<[FieldRef; N]> for Fields {
    fn from(value: [FieldRef; N]) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for Fields {
    type Target = [FieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a Fields {
    type Item = &'a FieldRef;
    type IntoIter = std::slice::Iter<'a, FieldRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
