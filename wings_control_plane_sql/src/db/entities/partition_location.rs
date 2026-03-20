use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "partition_locations")]
pub struct Model {
    // Add the id because it's required by sea_orm.
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: u32,
    pub tenant_id: String,
    pub namespace_id: String,
    pub topic_id: String,
    pub partition_value: Vec<u8>,
    pub start_offset: u32,
    pub end_offset: u32,
    pub file_ref: String,
    pub num_rows: u32,
    pub location_type: LocationType,
    pub folio_offset_bytes: Option<u32>,
    pub folio_size_bytes: Option<u32>,
    pub folio_batches_pb: Vec<u8>,
    pub parquet_metadata_pb: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(1))")]
pub enum LocationType {
    #[sea_orm(string_value = "F")]
    Folio,
    #[sea_orm(string_value = "P")]
    Parquet,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "super::topic::Entity")]
    Topic,
}

impl ActiveModelBehavior for ActiveModel {}

impl Related<super::topic::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Topic.def()
    }
}
