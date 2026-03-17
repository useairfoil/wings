use sea_orm::entity::prelude::*;
use snafu::ResultExt;
use wings_resources::{Tenant, TenantName};

use crate::db::error::InvalidResourceNameSnafu;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "tenants")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::object_store::Entity")]
    ObjectStore,
    #[sea_orm(has_many = "super::data_lake::Entity")]
    DataLake,
    #[sea_orm(has_many = "super::namespace::Entity")]
    Namespace,
}

impl ActiveModelBehavior for ActiveModel {}

impl Related<super::object_store::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ObjectStore.def()
    }
}

impl Related<super::data_lake::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::DataLake.def()
    }
}

impl Related<super::namespace::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Namespace.def()
    }
}

impl TryFrom<Model> for Tenant {
    type Error = crate::db::error::Error;

    fn try_from(model: Model) -> Result<Self, Self::Error> {
        let name =
            TenantName::new(model.id).context(InvalidResourceNameSnafu { resource: "tenant" })?;
        Ok(Tenant { name })
    }
}
