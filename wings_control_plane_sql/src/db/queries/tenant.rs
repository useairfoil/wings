use sea_orm::{
    ActiveValue::Set, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use time::OffsetDateTime;
use wings_control_plane_core::cluster_metadata::{ListTenantsRequest, ListTenantsResponse};
use wings_resources::{Tenant, TenantName};

use crate::{
    Database,
    db::{
        entities,
        error::{Error, Result},
    },
};

impl Database {
    pub async fn create_tenant(&self, name: TenantName) -> Result<Tenant> {
        let id = name.id().to_owned();
        let tenant = entities::tenant::ActiveModel {
            id: Set(id.clone()),
            created_at: Set(OffsetDateTime::now_utc()),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
                let existing = entities::tenant::Entity::find_by_id(&id).one(tx).await?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists {
                        resource: "tenant",
                        message: format!("name={name}"),
                    });
                }

                let entity = entities::tenant::Entity::insert(tenant)
                    .exec_with_returning(tx)
                    .await?;

                entity.try_into()
            })
        })
        .await
    }

    pub async fn get_tenant(&self, name: TenantName) -> Result<Tenant> {
        let id = name.id();

        let existing = entities::tenant::Entity::find_by_id(id)
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into(),
            None => Err(Error::NotFound {
                resource: "tenant",
                message: format!("name={name}"),
            }),
        }
    }

    pub async fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        let page_size = request.page_size.unwrap_or(100).clamp(1, 1_000) as u64;

        let mut query = entities::tenant::Entity::find().order_by_asc(entities::tenant::Column::Id);

        if let Some(id) = request.page_token {
            query = query.filter(entities::tenant::Column::Id.gt(id));
        }

        let entities = query.paginate(&self.pool, page_size).fetch().await?;

        let has_more = entities.len() == page_size as usize;

        let next_page_token = if has_more {
            let last_entity_index = page_size as usize - 1;
            Some(entities[last_entity_index].id.clone())
        } else {
            None
        };

        let tenants = entities
            .into_iter()
            .map(|entity| entity.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(ListTenantsResponse {
            tenants,
            next_page_token,
        })
    }

    pub async fn delete_tenant(&self, name: TenantName) -> Result<()> {
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::tenant::expect_exists(tx, &name).await?;

                let has_namespaces = entities::namespace::Entity::find()
                    .filter(entities::namespace::Column::TenantId.eq(&id))
                    .one(tx)
                    .await?
                    .is_some();

                if has_namespaces {
                    return Err(Error::InvalidArgument {
                        resource: "tenant",
                        message: format!("{name} has namespaces and cannot be deleted"),
                    });
                }

                let result = entities::tenant::Entity::delete_by_id(&id).exec(tx).await?;

                if result.rows_affected == 0 {
                    return Err(Error::NotFound {
                        resource: "tenant",
                        message: format!("name={name}"),
                    });
                }

                Ok(())
            })
        })
        .await
    }
}
