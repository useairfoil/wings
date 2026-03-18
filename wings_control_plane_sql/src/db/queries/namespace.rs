use sea_orm::{
    ActiveValue::Set, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use wings_control_plane_core::cluster_metadata::{ListNamespacesRequest, ListNamespacesResponse};
use wings_resources::{Namespace, NamespaceName, NamespaceOptions};

use crate::{
    Database,
    db::{
        entities,
        error::{Error, Result},
    },
};

impl Database {
    pub async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        let tenant_name = name.parent().clone();
        let tenant_id = tenant_name.id().to_owned();
        let id = name.id().to_owned();

        if options.data_lake.parent() != name.parent() {
            return Err(Error::InvalidArgument {
                resource: "namespace",
                message: "data lake must have the same parent as the namespace".to_string(),
            });
        }

        if options.object_store.parent() != name.parent() {
            return Err(Error::InvalidArgument {
                resource: "namespace",
                message: "object store must have the same parent as the namespace".to_string(),
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
                    return Err(Error::AlreadyExists {
                        resource: "namespace",
                        message: format!("name={name}"),
                    });
                }

                let entity = entities::namespace::Entity::insert(namespace)
                    .exec_with_returning(tx)
                    .await?;

                entity.try_into()
            })
        })
        .await
    }

    pub async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        let existing = entities::namespace::Entity::find_by_id((tenant_id, id))
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into(),
            None => Err(Error::NotFound {
                resource: "namespace",
                message: format!("name={name}"),
            }),
        }
    }

    pub async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
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
            .collect::<Result<Vec<_>>>()?;

        Ok(ListNamespacesResponse {
            namespaces,
            next_page_token,
        })
    }

    pub async fn delete_namespace(&self, name: NamespaceName) -> Result<()> {
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::namespace::expect_exists(tx, &name).await?;

                // Check if namespace has any topics
                let topics_count = entities::topic::Entity::find()
                    .filter(entities::topic::Column::TenantId.eq(&tenant_id))
                    .filter(entities::topic::Column::NamespaceId.eq(&id))
                    .count(tx)
                    .await?;

                if topics_count > 0 {
                    return Err(Error::InvalidArgument {
                        resource: "namespace",
                        message: format!(
                            "namespace has {topics_count} topics and cannot be deleted"
                        ),
                    });
                }

                let result = entities::namespace::Entity::delete_by_id((tenant_id, id))
                    .exec(tx)
                    .await?;

                if result.rows_affected == 0 {
                    return Err(Error::NotFound {
                        resource: "namespace",
                        message: format!("name={name}"),
                    });
                }

                Ok(())
            })
        })
        .await
    }
}
