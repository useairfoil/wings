use sea_orm::{
    ActiveValue::Set, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use wings_control_plane_core::cluster_metadata::{
    ListObjectStoresRequest, ListObjectStoresResponse,
};
use wings_resources::{ObjectStore, ObjectStoreConfiguration, ObjectStoreName};

use crate::{
    Database,
    db::{
        entities,
        error::{Error, Result},
    },
};

impl Database {
    pub async fn create_object_store(
        &self,
        name: ObjectStoreName,
        config: ObjectStoreConfiguration,
    ) -> Result<ObjectStore> {
        let tenant_name = name.parent().clone();
        let tenant_id = tenant_name.id().to_owned();
        let id = name.id().to_owned();

        let config_json = serde_json::to_value(&config)?;

        let object_store = entities::object_store::ActiveModel {
            id: Set(id.clone()),
            tenant_id: Set(tenant_id.clone()),
            config: Set(config_json),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::tenant::expect_exists(tx, &tenant_name).await?;

                let existing = entities::object_store::Entity::find_by_id((tenant_id, id))
                    .one(tx)
                    .await?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists {
                        resource: "object_store",
                        message: format!("name={name}"),
                    });
                }

                let entity = entities::object_store::Entity::insert(object_store)
                    .exec_with_returning(tx)
                    .await?;

                entity.try_into()
            })
        })
        .await
    }

    pub async fn get_object_store(&self, name: ObjectStoreName) -> Result<ObjectStore> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        let existing = entities::object_store::Entity::find_by_id((tenant_id, id))
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into(),
            None => Err(Error::NotFound {
                resource: "object_store",
                message: format!("name={name}"),
            }),
        }
    }

    pub async fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse> {
        let tenant_id = request.parent.id().to_owned();
        let page_size = request.page_size.unwrap_or(100).clamp(1, 1_000) as u64;

        let mut query = entities::object_store::Entity::find()
            .filter(entities::object_store::Column::TenantId.eq(&tenant_id))
            .order_by_asc(entities::object_store::Column::Id);

        if let Some(id) = request.page_token {
            query = query.filter(entities::object_store::Column::Id.gt(id));
        }

        let entities = query.paginate(&self.pool, page_size).fetch().await?;

        let has_more = entities.len() == page_size as usize;

        let next_page_token = if has_more {
            let last_entity_index = page_size as usize - 1;
            Some(entities[last_entity_index].id.clone())
        } else {
            None
        };

        let object_stores = entities
            .into_iter()
            .map(|entity| entity.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(ListObjectStoresResponse {
            object_stores,
            next_page_token,
        })
    }

    pub async fn delete_object_store(&self, name: ObjectStoreName) -> Result<()> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::object_store::expect_exists(tx, &name).await?;

                let result = entities::object_store::Entity::delete_by_id((tenant_id, id))
                    .exec(tx)
                    .await?;

                if result.rows_affected == 0 {
                    return Err(Error::NotFound {
                        resource: "object_store",
                        message: format!("name={name}"),
                    });
                }

                Ok(())
            })
        })
        .await
    }
}
