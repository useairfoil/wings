use sea_orm::{
    ActiveValue::Set, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use wings_control_plane_core::cluster_metadata::{
    ListTopicsRequest, ListTopicsResponse, TopicView,
};
use wings_resources::{Topic, TopicName, TopicOptions, validate_compaction};

use crate::{
    Database,
    db::{
        entities,
        error::{Error, Result},
    },
};

impl Database {
    pub async fn create_topic(&self, name: TopicName, options: TopicOptions) -> Result<Topic> {
        let namespace_name = name.parent().clone();
        let tenant_id = namespace_name.parent().id().to_owned();
        let namespace_id = namespace_name.id().to_owned();
        let id = name.id().to_owned();

        // Validate partition key exists in schema
        if let Some(partition_key) = options.partition_key
            && !options
                .schema
                .fields_iter()
                .any(|field| field.id == partition_key)
        {
            return Err(Error::InvalidArgument {
                resource: "topic",
                message: format!("no field with id {partition_key} found in schema"),
            });
        }

        // Validate compaction configuration
        if let Err(errors) = validate_compaction(&options.compaction) {
            let message = errors.join(", ");
            return Err(Error::InvalidArgument {
                resource: "topic",
                message: format!("compaction configuration is invalid: {message}"),
            });
        }

        let schema_fields = serde_json::to_value(&options.schema.fields)?;
        let schema_metadata = serde_json::to_value(&options.schema.metadata)?;

        let topic = entities::topic::ActiveModel {
            tenant_id: Set(tenant_id.clone()),
            namespace_id: Set(namespace_id.clone()),
            id: Set(id.clone()),
            schema_fields: Set(schema_fields),
            schema_metadata: Set(schema_metadata),
            partition_key: Set(options.partition_key.map(|id| id as u32)),
            description: Set(options.description.clone()),
            compaction_freshness_ms: Set(options.compaction.freshness.as_millis() as u32),
            compaction_ttl_ms: Set(options.compaction.ttl.map(|ttl| ttl.as_millis() as u32)),
            compaction_target_file_size_bytes: Set(
                options.compaction.target_file_size.as_u64() as u32
            ),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::namespace::expect_exists(tx, &namespace_name).await?;

                let existing = entities::topic::Entity::find_by_id((
                    tenant_id.clone(),
                    namespace_id.clone(),
                    id.clone(),
                ))
                .one(tx)
                .await?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists {
                        resource: "topic",
                        message: format!("name={name}"),
                    });
                }

                let entity = entities::topic::Entity::insert(topic)
                    .exec_with_returning(tx)
                    .await?;

                entity.try_into()
            })
        })
        .await
    }

    pub async fn get_topic(&self, name: TopicName, _view: TopicView) -> Result<Topic> {
        let tenant_id = name.parent().parent().id().to_owned();
        let namespace_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        let existing = entities::topic::Entity::find_by_id((tenant_id, namespace_id, id))
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into(),
            None => Err(Error::NotFound {
                resource: "topic",
                message: format!("name={name}"),
            }),
        }
    }

    pub async fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        let tenant_id = request.parent.parent().id().to_owned();
        let namespace_id = request.parent.id().to_owned();
        let page_size = request.page_size.unwrap_or(100).clamp(1, 1_000) as u64;

        let mut query = entities::topic::Entity::find()
            .filter(entities::topic::Column::TenantId.eq(&tenant_id))
            .filter(entities::topic::Column::NamespaceId.eq(&namespace_id))
            .order_by_asc(entities::topic::Column::Id);

        if let Some(id) = request.page_token {
            query = query.filter(entities::topic::Column::Id.gt(id));
        }

        let entities = query.paginate(&self.pool, page_size).fetch().await?;

        let has_more = entities.len() == page_size as usize;

        let next_page_token = if has_more {
            let last_entity_index = page_size as usize - 1;
            Some(entities[last_entity_index].id.clone())
        } else {
            None
        };

        let topics = entities
            .into_iter()
            .map(|entity| entity.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(ListTopicsResponse {
            topics,
            next_page_token,
        })
    }

    pub async fn delete_topic(&self, name: TopicName, _force: bool) -> Result<()> {
        let tenant_id = name.parent().parent().id().to_owned();
        let namespace_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::topic::expect_exists(tx, &name).await?;

                let result = entities::topic::Entity::delete_by_id((
                    tenant_id.clone(),
                    namespace_id.clone(),
                    id.clone(),
                ))
                .exec(tx)
                .await?;

                if result.rows_affected == 0 {
                    return Err(Error::NotFound {
                        resource: "topic",
                        message: format!("name={name}"),
                    });
                }

                Ok(())
            })
        })
        .await
    }
}
