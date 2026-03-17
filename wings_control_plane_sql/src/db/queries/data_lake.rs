use sea_orm::{ActiveValue::Set, EntityTrait};
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
}
