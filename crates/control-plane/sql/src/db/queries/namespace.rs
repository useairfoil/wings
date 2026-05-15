use sea_orm::{
    ActiveValue::Set, ColumnTrait, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use snafu::Snafu;
use wings_control_plane_core::{
    ClusterMetadataError,
    cluster_metadata::{ListNamespacesRequest, ListNamespacesResponse},
};
use wings_resources::{Namespace, NamespaceName, NamespaceOptions};

use crate::{Database, db::entities};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("namespace {name} already exists"))]
    AlreadyExists { name: NamespaceName },
    #[snafu(display("namespace {name} not found"))]
    NotFound { name: NamespaceName },
    #[snafu(display("{resource} must have the same parent as the namespace"))]
    InvalidParent { resource: &'static str },
    #[snafu(display("namespace {name} has {tables_count} tables and cannot be deleted"))]
    NotEmpty {
        name: NamespaceName,
        tables_count: u64,
    },
    #[snafu(transparent)]
    Entity { source: entities::Error },
    #[snafu(transparent)]
    Db { source: DbErr },
}

impl Database {
    pub async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace, Error> {
        let tenant_name = name.parent().clone();
        let tenant_id = tenant_name.id().to_owned();
        let id = name.id().to_owned();

        if options.data_lake.parent() != name.parent() {
            return Err(Error::InvalidParent {
                resource: "namespace",
            });
        }

        if options.object_store.parent() != name.parent() {
            return Err(Error::InvalidParent {
                resource: "namespace",
            });
        }

        let namespace = entities::namespace::ActiveModel {
            id: Set(id.clone()),
            tenant_id: Set(tenant_id.clone()),
            flush_size_bytes: Set(options.flush_size.as_u64() as _),
            flush_interval_ms: Set(options.flush_interval.as_millis() as _),
            object_store_id: Set(options.object_store.id.clone()),
            data_lake_id: Set(options.data_lake.id.clone()),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::tenant::expect_exists(tx, &tenant_name).await?;
                entities::data_lake::expect_exists(tx, &options.data_lake).await?;
                entities::object_store::expect_exists(tx, &options.object_store).await?;

                let existing = entities::namespace::Entity::find_by_id((tenant_id, id))
                    .one(tx)
                    .await?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists { name });
                }

                let entity = entities::namespace::Entity::insert(namespace)
                    .exec_with_returning(tx)
                    .await?;

                entity.try_into().map_err(Into::into)
            })
        })
        .await
    }

    pub async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace, Error> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        let existing = entities::namespace::Entity::find_by_id((tenant_id, id))
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into().map_err(Into::into),
            None => Err(Error::NotFound { name }),
        }
    }

    pub async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse, Error> {
        let tenant_id = request.parent.id().to_owned();
        let page_size = request.page_size.unwrap_or(100).clamp(1, 1_000) as u64;

        let mut query = entities::namespace::Entity::find()
            .filter(entities::namespace::Column::TenantId.eq(&tenant_id))
            .order_by_asc(entities::namespace::Column::Id);

        if let Some(id) = request.page_token {
            query = query.filter(entities::namespace::Column::Id.gt(id));
        }

        let entities = query.paginate(&self.pool, page_size).fetch().await?;

        let has_more = entities.len() == page_size as usize;

        let next_page_token = if has_more {
            let last_entity_index = page_size as usize - 1;
            Some(entities[last_entity_index].id.clone())
        } else {
            None
        };

        let namespaces = entities
            .into_iter()
            .map(|entity| entity.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ListNamespacesResponse {
            namespaces,
            next_page_token,
        })
    }

    pub async fn delete_namespace(&self, name: NamespaceName) -> Result<(), Error> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::namespace::expect_exists(tx, &name).await?;

                // Check if namespace has any tables
                let tables_count = entities::table::Entity::find()
                    .filter(entities::table::Column::TenantId.eq(&tenant_id))
                    .filter(entities::table::Column::NamespaceId.eq(&id))
                    .count(tx)
                    .await?;

                if tables_count > 0 {
                    return Err(Error::NotEmpty { name, tables_count });
                }

                let result = entities::namespace::Entity::delete_by_id((tenant_id, id))
                    .exec(tx)
                    .await?;

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
                resource: "namespace".to_string(),
                name: name.to_string(),
            },
            Error::NotFound { name } => ClusterMetadataError::NotFound {
                resource: "namespace".to_string(),
                name: name.to_string(),
            },
            Error::InvalidParent { .. } => ClusterMetadataError::InvalidArgument {
                resource: "namespace".to_string(),
                message: err.to_string(),
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
