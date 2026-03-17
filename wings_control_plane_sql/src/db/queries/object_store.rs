use sea_orm::{ActiveValue::Set, EntityTrait};
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
        let tenant_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        let config_json = serde_json::to_value(&config)?;

        let object_store = entities::object_store::ActiveModel {
            id: Set(id.clone()),
            tenant_id: Set(tenant_id.clone()),
            config: Set(config_json),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
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
}
