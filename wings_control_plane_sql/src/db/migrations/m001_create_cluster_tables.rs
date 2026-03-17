use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("tenants")
                    .if_not_exists()
                    .col(ColumnDef::new("id").text().primary_key())
                    .col(
                        ColumnDef::new("created_at")
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("object_stores")
                    .if_not_exists()
                    .col(ColumnDef::new("id").text())
                    .col(ColumnDef::new("tenant_id").text())
                    .col(ColumnDef::new("config").json())
                    .primary_key(Index::create().col("tenant_id").col("id"))
                    .foreign_key(
                        ForeignKey::create()
                            .name("tenant_fk")
                            .to("tenants", "id")
                            .from("object_stores", "tenant_id")
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("data_lakes")
                    .if_not_exists()
                    .col(ColumnDef::new("id").text())
                    .col(ColumnDef::new("tenant_id").text())
                    .col(ColumnDef::new("config").json())
                    .primary_key(Index::create().col("tenant_id").col("id"))
                    .foreign_key(
                        ForeignKey::create()
                            .name("tenant_fk")
                            .to("tenants", "id")
                            .from("data_lakes", "tenant_id")
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("namespaces")
                    .if_not_exists()
                    .col(ColumnDef::new("id").text())
                    .col(ColumnDef::new("tenant_id").text())
                    .col(ColumnDef::new("flush_size_bytes").integer())
                    .col(ColumnDef::new("flush_interval_ms").integer())
                    .col(ColumnDef::new("object_store_id").text())
                    .col(ColumnDef::new("data_lake_id").text())
                    .primary_key(Index::create().col("tenant_id").col("id"))
                    .foreign_key(
                        ForeignKey::create()
                            .name("tenant_fk")
                            .to("tenants", "id")
                            .from("namespaces", "tenant_id")
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("data_lake_fk")
                            .to("data_lakes", ("tenant_id", "id"))
                            .from("namespaces", ("tenant_id", "data_lake_id"))
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("object_store_fk")
                            .to("object_stores", ("tenant_id", "id"))
                            .from("namespaces", ("tenant_id", "object_store_id"))
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        panic!("Migration cannot be rolled back")
    }
}
