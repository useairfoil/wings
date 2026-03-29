use std::time::{Duration, SystemTime};

use sea_orm::{Condition, entity::prelude::*};
use wings_control_plane_core::log_metadata::{LogOffset, PartitionMetadata};
use wings_resources::{PartitionValue, TopicName};

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
    pub topic_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub partition_value: Vec<u8>,
    pub next_offset: u32,
    pub last_timestamp_ms: u32,
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

pub fn topic_condition(topic_name: &TopicName) -> Condition {
    let topic_id = topic_name.id();
    let namespace_id = topic_name.parent().id();
    let tenant_id = topic_name.parent().parent().id();

    Condition::all()
        .add(Column::TenantId.eq(tenant_id))
        .add(Column::NamespaceId.eq(namespace_id))
        .add(Column::TopicId.eq(topic_id))
}

impl Model {
    pub fn next_log_offset(&self) -> LogOffset {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_millis(self.last_timestamp_ms as _);
        LogOffset {
            offset: self.next_offset as _,
            timestamp,
        }
    }
}

impl From<PartitionKey> for <PrimaryKey as PrimaryKeyTrait>::ValueType {
    fn from(pk: PartitionKey) -> Self {
        (
            pk.tenant_id,
            pk.namespace_id,
            pk.topic_id,
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
            end_offset: model.next_log_offset(),
        })
    }
}
