use sea_orm::{Condition, entity::prelude::*};
use wings_control_plane_core::log_metadata::{FolioLocation, LogLocation};

use crate::db::{Error, PartitionKey};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "partition_locations")]
pub struct Model {
    // Add the id because it's required by sea_orm.
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: u32,
    pub tenant_id: String,
    pub namespace_id: String,
    pub topic_id: String,
    pub partition_value: Vec<u8>,
    pub start_offset: u32,
    pub end_offset: u32,
    pub file_ref: String,
    pub num_rows: u32,
    pub location_type: LocationType,
    pub folio_offset_bytes: Option<u32>,
    pub folio_size_bytes: Option<u32>,
    pub folio_batches_pb: Option<Vec<u8>>,
    pub parquet_metadata_pb: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(1))")]
pub enum LocationType {
    #[sea_orm(string_value = "F")]
    Folio,
    #[sea_orm(string_value = "L")]
    DataLake,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "super::topic::Entity")]
    Topic,
}

impl ActiveModelBehavior for ActiveModel {}

impl Related<super::topic::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Topic.def()
    }
}

pub fn topic_partition_condition(key: PartitionKey) -> Condition {
    Condition::all()
        .add(Column::TenantId.eq(key.tenant_id))
        .add(Column::NamespaceId.eq(key.namespace_id))
        .add(Column::TopicId.eq(key.topic_id))
        .add(Column::PartitionValue.eq(key.partition_value))
}

impl TryFrom<Model> for LogLocation {
    type Error = Error;

    fn try_from(model: Model) -> Result<Self, Self::Error> {
        match model.location_type {
            LocationType::Folio => {
                use prost::Message;
                use wings_control_plane_core::pb::CommittedBatches;

                let folio_batches_pb = model.folio_batches_pb.ok_or_else(|| Error::Internal {
                    message: "incosistent db state: folio_batches_pb missing".to_string(),
                })?;

                let committed_batches = CommittedBatches::decode(folio_batches_pb.as_ref())?;

                let batches = committed_batches
                    .batches
                    .into_iter()
                    .map(|batch| batch.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                let offset_bytes = model.folio_offset_bytes.ok_or_else(|| Error::Internal {
                    message: "incosistent db state: folio_offset_bytes missing".to_string(),
                })?;

                let size_bytes = model.folio_size_bytes.ok_or_else(|| Error::Internal {
                    message: "incosistent db state: folio_size_bytes missing".to_string(),
                })?;

                let inner = FolioLocation {
                    file_ref: model.file_ref,
                    offset_bytes: offset_bytes as _,
                    size_bytes: size_bytes as _,
                    num_rows: model.num_rows as _,
                    batches,
                };
                Ok(LogLocation::Folio(inner))
            }
            LocationType::DataLake => {
                todo!();
            }
        }
    }
}
