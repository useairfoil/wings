use std::time::Duration;

use bytesize::ByteSize;
use sea_orm::{DatabaseTransaction, entity::prelude::*};
use snafu::ResultExt;
use wings_resources::{DataLakeName, Namespace, NamespaceName, ObjectStoreName, TenantName};

use crate::db::error::InvalidResourceNameSnafu;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "namespaces")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tenant_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub flush_size_bytes: u32,  // sqlite is limited to u32
    pub flush_interval_ms: u32, // sqlite is limited to u32
    pub object_store_id: String,
    pub data_lake_id: String,
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

impl TryFrom<Model> for Namespace {
    type Error = crate::db::error::Error;

    fn try_from(model: Model) -> Result<Self, Self::Error> {
        let tenant_name = TenantName::new(model.tenant_id)
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;

        let name = NamespaceName::new(model.id, tenant_name.clone()).context(
            InvalidResourceNameSnafu {
                resource: "namespace",
            },
        )?;

        let data_lake = DataLakeName::new(model.data_lake_id, tenant_name.clone()).context(
            InvalidResourceNameSnafu {
                resource: "data-lake",
            },
        )?;

        let object_store = ObjectStoreName::new(model.object_store_id, tenant_name).context(
            InvalidResourceNameSnafu {
                resource: "object-store",
            },
        )?;

        let flush_size = ByteSize::b(model.flush_size_bytes as _);
        let flush_interval = Duration::from_millis(model.flush_interval_ms as _);

        Ok(Namespace {
            name,
            flush_size,
            flush_interval,
            object_store,
            data_lake,
        })
    }
}

pub async fn expect_exists(
    tx: &DatabaseTransaction,
    name: &NamespaceName,
) -> Result<(), crate::db::Error> {
    let tenant_id = name.parent().id().to_owned();
    let id = name.id.to_owned();
    let existing = Entity::find_by_id((tenant_id, id)).one(tx).await?;

    if existing.is_none() {
        return Err(crate::db::Error::NotFound {
            resource: "namespace",
            message: format!("name={name}"),
        });
    }

    Ok(())
}
