use std::collections::HashMap;

use sea_orm::{DatabaseTransaction, entity::prelude::*};
use snafu::ResultExt;
use wings_resources::{CompactionConfiguration, NamespaceName, TenantName, Topic, TopicName};
use wings_schema::{FieldRef, Fields, SchemaBuilder};

use crate::db::error::InvalidResourceNameSnafu;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "topics")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tenant_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub namespace_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub schema_fields: Json,
    pub schema_metadata: Json,
    pub partition_key: Option<u32>,
    pub description: Option<String>,
    pub compaction_freshness_ms: u32,
    pub compaction_ttl_ms: Option<u32>,
    pub compaction_target_file_size_bytes: u32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "super::namespace::Entity")]
    Namespace,
}

impl ActiveModelBehavior for ActiveModel {}

impl Related<super::namespace::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Namespace.def()
    }
}

impl TryFrom<Model> for Topic {
    type Error = crate::db::error::Error;

    fn try_from(model: Model) -> Result<Self, Self::Error> {
        let tenant_name = TenantName::new(model.tenant_id.clone())
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;
        let namespace_name = NamespaceName::new(model.namespace_id.clone(), tenant_name.clone())
            .context(InvalidResourceNameSnafu {
                resource: "namespace",
            })?;
        let name = TopicName::new(model.id, namespace_name)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let fields: Vec<FieldRef> = serde_json::from_value(model.schema_fields)?;
        let fields = Fields::from(fields);

        let metadata: HashMap<String, String> = serde_json::from_value(model.schema_metadata)?;

        let schema = SchemaBuilder::new(fields).with_metadata(metadata).build()?;

        let partition_key = model.partition_key.map(|id| id as u64);

        let compaction = {
            let freshness = std::time::Duration::from_millis(model.compaction_freshness_ms as u64);
            let ttl = model
                .compaction_ttl_ms
                .map(|ms| std::time::Duration::from_millis(ms as u64));
            let target_file_size =
                bytesize::ByteSize::b(model.compaction_target_file_size_bytes as u64);

            CompactionConfiguration {
                freshness,
                ttl,
                target_file_size,
            }
        };

        Ok(Topic {
            name,
            schema,
            partition_key,
            description: model.description,
            compaction,
            status: None,
        })
    }
}

pub async fn expect_exists(
    tx: &DatabaseTransaction,
    name: &TopicName,
) -> Result<(), crate::db::Error> {
    let tenant_id = name.parent().parent().id().to_owned();
    let namespace_id = name.parent().id().to_owned();
    let id = name.id.to_owned();
    let existing = Entity::find_by_id((tenant_id, namespace_id, id))
        .one(tx)
        .await?;

    if existing.is_none() {
        return Err(crate::db::Error::NotFound {
            resource: "topic",
            message: format!("name={name}"),
        });
    }

    Ok(())
}
