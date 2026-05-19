use std::time::{Duration, SystemTime};

use sea_orm::{Condition, entity::prelude::*};
use wings_control_plane_core::table_metadata::{PartitionMetadata, SeqNum};
use wings_resources::{PartitionValue, TableName};

use super::error::Error;
use crate::db::PartitionKey;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "partition_states")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tenant_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub namespace_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub partition_value: Vec<u8>,
    pub next_seqnum: u32,
    pub last_timestamp_ms: u32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "super::table::Entity")]
    Table,
}

impl ActiveModelBehavior for ActiveModel {}

impl Related<super::table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Table.def()
    }
}

pub fn table_condition(table_name: &TableName) -> Condition {
    let table_id = table_name.id();
    let namespace_id = table_name.parent().id();
    let tenant_id = table_name.parent().parent().id();

    Condition::all()
        .add(Column::TenantId.eq(tenant_id))
        .add(Column::NamespaceId.eq(namespace_id))
        .add(Column::TableId.eq(table_id))
}

impl Model {
    pub fn next_seqnum(&self) -> SeqNum {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_millis(self.last_timestamp_ms as _);
        SeqNum {
            seqnum: self.next_seqnum as _,
            timestamp,
        }
    }
}

impl From<PartitionKey> for <PrimaryKey as PrimaryKeyTrait>::ValueType {
    fn from(pk: PartitionKey) -> Self {
        (
            pk.tenant_id,
            pk.namespace_id,
            pk.table_id,
            pk.partition_value,
        )
    }
}

impl TryFrom<Model> for PartitionMetadata {
    type Error = Error;

    fn try_from(model: Model) -> Result<Self, Self::Error> {
        use prost::Message;
        use wings_control_plane_core::pb::PartitionValue as Proto;

        let partition_value: Option<PartitionValue> = if model.partition_value.is_empty() {
            None
        } else {
            let proto = Proto::decode(model.partition_value.as_slice())?;
            Some(proto.try_into()?)
        };

        Ok(Self {
            partition_value,
            end_seqnum: model.next_seqnum(),
        })
    }
}
