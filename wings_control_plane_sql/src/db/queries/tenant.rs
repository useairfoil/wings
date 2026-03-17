use sea_orm::{ActiveValue::Set, EntityTrait};
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
}
