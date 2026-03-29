use sea_orm::{
    ActiveValue::Set, ColumnTrait, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use snafu::Snafu;
use time::OffsetDateTime;
use wings_control_plane_core::{
    ClusterMetadataError,
    cluster_metadata::{ListTenantsRequest, ListTenantsResponse},
};
use wings_resources::{Tenant, TenantName};

use crate::{Database, db::entities};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("tenant {name} already exists"))]
    AlreadyExists { name: TenantName },
    #[snafu(display("tenant {name} not found"))]
    NotFound { name: TenantName },
    #[snafu(display("tenant {name} has {namespaces_count} namespaces and cannot be deleted"))]
    NotEmpty {
        name: TenantName,
        namespaces_count: u64,
    },
    #[snafu(transparent)]
    Entity { source: entities::Error },
    #[snafu(transparent)]
    Db { source: DbErr },
}

impl Database {
    pub async fn create_tenant(&self, name: TenantName) -> Result<Tenant, Error> {
        let id = name.id().to_owned();
        let tenant = entities::tenant::ActiveModel {
            id: Set(id.clone()),
            created_at: Set(OffsetDateTime::now_utc()),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
                let existing = entities::tenant::Entity::find_by_id(&id).one(tx).await?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists { name });
                }

                let entity = entities::tenant::Entity::insert(tenant)
                    .exec_with_returning(tx)
                    .await?;

                entity.try_into().map_err(Into::into)
            })
        })
        .await
    }

    pub async fn get_tenant(&self, name: TenantName) -> Result<Tenant, Error> {
        let id = name.id();

        let existing = entities::tenant::Entity::find_by_id(id)
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into().map_err(Into::into),
            None => Err(Error::NotFound { name }),
        }
    }

    pub async fn list_tenants(
        &self,
        request: ListTenantsRequest,
    ) -> Result<ListTenantsResponse, Error> {
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
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ListTenantsResponse {
            tenants,
            next_page_token,
        })
    }

    pub async fn delete_tenant(&self, name: TenantName) -> Result<(), Error> {
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::tenant::expect_exists(tx, &name).await?;

                let namespaces_count = entities::namespace::Entity::find()
                    .filter(entities::namespace::Column::TenantId.eq(&id))
                    .count(tx)
                    .await?;

                if namespaces_count > 0 {
                    return Err(Error::NotEmpty {
                        name,
                        namespaces_count,
                    });
                }

                let result = entities::tenant::Entity::delete_by_id(&id).exec(tx).await?;

                if result.rows_affected == 0 {
                    return Err(Error::NotFound { name });
                }

                Ok(())
            })
        })
        .await
    }
}

impl From<Error> for ClusterMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::AlreadyExists { name } => ClusterMetadataError::AlreadyExists {
                resource: "tenant".to_string(),
                name: name.to_string(),
            },
            Error::NotFound { name } => ClusterMetadataError::NotFound {
                resource: "tenant".to_string(),
                name: name.to_string(),
            },
            Error::NotEmpty { .. } => ClusterMetadataError::FailedPrecondition {
                message: err.to_string(),
            },
            Error::Entity { source } => source.into(),
            Error::Db { source } => ClusterMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
