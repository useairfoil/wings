use sea_orm::{DatabaseTransaction, entity::prelude::*};
use snafu::ResultExt;
use wings_resources::{DataLake, DataLakeName, TenantName};

use crate::db::error::InvalidResourceNameSnafu;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "data_lakes")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tenant_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub config: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "super::tenant::Entity")]
    Tenant,
}

impl ActiveModelBehavior for ActiveModel {}

impl Related<super::tenant::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Tenant.def()
    }
}

impl TryFrom<Model> for DataLake {
    type Error = crate::db::error::Error;

    fn try_from(model: Model) -> Result<Self, Self::Error> {
        let tenant_name = TenantName::new(model.tenant_id)
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;
        let name = DataLakeName::new(model.id, tenant_name).context(InvalidResourceNameSnafu {
            resource: "data-lake",
        })?;

        let data_lake = serde_json::from_value(model.config)?;

        Ok(DataLake { name, data_lake })
    }
}

pub async fn expect_exists(
    tx: &DatabaseTransaction,
    name: &DataLakeName,
) -> Result<(), crate::db::Error> {
    let tenant_id = name.parent().id().to_owned();
    let id = name.id.to_owned();
    let existing = Entity::find_by_id((tenant_id, id)).one(tx).await?;

    if existing.is_none() {
        return Err(crate::db::Error::NotFound {
            resource: "data-lake",
            message: format!("name={name}"),
        });
    }

    Ok(())
}
