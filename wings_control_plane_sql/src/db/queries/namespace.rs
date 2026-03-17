use sea_orm::{ActiveValue::Set, EntityTrait};
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
        let tenant_id = name.parent().id().to_owned();
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
}
