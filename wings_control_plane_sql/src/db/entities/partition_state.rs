use std::time::{Duration, SystemTime};

use sea_orm::entity::prelude::*;
use wings_control_plane_core::log_metadata::LogOffset;
use wings_resources::{PartitionValue, TopicName};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionKey {
    pub tenant_id: String,
    pub namespace_id: String,
    pub topic_id: String,
    pub partition_value: Vec<u8>,
}

impl PartitionKey {
    pub fn new(name: &TopicName, partition_value: Option<PartitionValue>) -> Self {
        use prost::Message;
        use wings_control_plane_core::pb::PartitionValue as Proto;

        let namespace = name.parent();
        let tenant = namespace.parent();

        let pv = if let Some(ref pv) = partition_value {
            let pv: Proto = pv.into();
            pv.encode_to_vec()
        } else {
            Vec::default()
        };

        Self {
            tenant_id: tenant.id().to_owned(),
            namespace_id: namespace.id().to_owned(),
            topic_id: name.id().to_owned(),
            partition_value: pv,
        }
    }
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
