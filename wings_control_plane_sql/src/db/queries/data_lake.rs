use sea_orm::{
    ActiveValue::Set, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use wings_control_plane_core::cluster_metadata::{ListDataLakesRequest, ListDataLakesResponse};
use wings_resources::{DataLake, DataLakeConfiguration, DataLakeName};

use crate::{
    Database,
    db::{
        entities,
        error::{Error, Result},
    },
};

impl Database {
    pub async fn create_data_lake(
        &self,
        name: DataLakeName,
        config: DataLakeConfiguration,
    ) -> Result<DataLake> {
        let tenant_name = name.parent().clone();
        let tenant_id = tenant_name.id().to_owned();
        let id = name.id().to_owned();

        let config_json = serde_json::to_value(&config)?;

        let data_lake = entities::data_lake::ActiveModel {
            id: Set(id.clone()),
            tenant_id: Set(tenant_id.clone()),
            config: Set(config_json),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::tenant::expect_exists(tx, &tenant_name).await?;

                let existing = entities::data_lake::Entity::find_by_id((tenant_id, id))
                    .one(tx)
                    .await?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists {
                        resource: "data_lake",
                        message: format!("name={name}"),
                    });
                }

                let entity = entities::data_lake::Entity::insert(data_lake)
                    .exec_with_returning(tx)
                    .await?;

                entity.try_into()
            })
        })
        .await
    }

    pub async fn get_data_lake(&self, name: DataLakeName) -> Result<DataLake> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        let existing = entities::data_lake::Entity::find_by_id((tenant_id, id))
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into(),
            None => Err(Error::NotFound {
                resource: "data-lake",
                message: format!("name={name}"),
            }),
        }
    }

    pub async fn list_data_lakes(
        &self,
        request: ListDataLakesRequest,
    ) -> Result<ListDataLakesResponse> {
        let tenant_id = request.parent.id().to_owned();
        let page_size = request.page_size.unwrap_or(100).clamp(1, 1_000) as u64;

        let mut query = entities::data_lake::Entity::find()
            .filter(entities::data_lake::Column::TenantId.eq(&tenant_id))
            .order_by_asc(entities::data_lake::Column::Id);

        if let Some(id) = request.page_token {
            query = query.filter(entities::data_lake::Column::Id.gt(id));
        }

        let entities = query.paginate(&self.pool, page_size).fetch().await?;

        let has_more = entities.len() == page_size as usize;

        let next_page_token = if has_more {
            let last_entity_index = page_size as usize - 1;
            Some(entities[last_entity_index].id.clone())
        } else {
            None
        };

        let data_lakes = entities
            .into_iter()
            .map(|entity| entity.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(ListDataLakesResponse {
            data_lakes,
            next_page_token,
        })
    }

    pub async fn delete_data_lake(&self, name: DataLakeName) -> Result<()> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::data_lake::expect_exists(tx, &name).await?;

                let result = entities::data_lake::Entity::delete_by_id((tenant_id, id))
                    .exec(tx)
                    .await?;

                if result.rows_affected == 0 {
                    return Err(Error::NotFound {
                        resource: "data-lake",
                        message: format!("name={name}"),
                    });
                }

                Ok(())
            })
        })
        .await
    }
}
