use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                TableCreateStatement::new()
                    .table("partition_states")
                    .if_not_exists()
                    .col(ColumnDef::new("tenant_id").text())
                    .col(ColumnDef::new("namespace_id").text())
                    .col(ColumnDef::new("table_id").text())
                    .col(ColumnDef::new("partition_value").binary())
                    .col(ColumnDef::new("next_seqnum").integer().not_null())
                    .col(ColumnDef::new("last_timestamp_ms").integer().not_null())
                    .primary_key(
                        Index::create()
                            .col("tenant_id")
                            .col("namespace_id")
                            .col("table_id")
                            .col("partition_value"),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("table_fk")
                            .to("tables", ("tenant_id", "namespace_id", "id"))
                            .from(
                                "partition_states",
                                ("tenant_id", "namespace_id", "table_id"),
                            )
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                TableCreateStatement::new()
                    .table("partition_locations")
                    .if_not_exists()
                    .col(
                        ColumnDef::new("id")
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(ColumnDef::new("tenant_id").text())
                    .col(ColumnDef::new("namespace_id").text())
                    .col(ColumnDef::new("table_id").text())
                    .col(ColumnDef::new("partition_value").text())
                    .col(ColumnDef::new("start_seqnum").integer().not_null())
                    .col(ColumnDef::new("end_seqnum").integer().not_null())
                    .col(ColumnDef::new("file_ref").text().not_null())
                    .col(ColumnDef::new("num_rows").integer().not_null())
                    // either F (folio) or P (parquet)
                    .col(ColumnDef::new("location_type").string_len(1).not_null())
                    // folio-specific columns
                    .col(ColumnDef::new("folio_offset_bytes").integer())
                    .col(ColumnDef::new("folio_size_bytes").integer())
                    .col(ColumnDef::new("folio_batches_pb").binary())
                    // parquet-specific columns
                    .col(ColumnDef::new("parquet_metadata_pb").binary())
                    .foreign_key(
                        ForeignKey::create()
                            .name("table_fk")
                            .to("tables", ("tenant_id", "namespace_id", "id"))
                            .from(
                                "partition_locations",
                                ("tenant_id", "namespace_id", "table_id"),
                            )
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                TableCreateStatement::new()
                    .table("tasks")
                    .if_not_exists()
                    .col(ColumnDef::new("id").text().primary_key())
                    .col(
                        ColumnDef::new("created_at")
                            .timestamp_with_time_zone()
                            .default(Expr::current_timestamp()),
                    )
                    //  'Q' - queued
                    //  'P' - processing
                    //  'C' - completed (success)
                    //  'F' - failed
                    .col(ColumnDef::new("status").string_len(1).not_null())
                    .col(
                        ColumnDef::new("run_at")
                            .timestamp_with_time_zone()
                            .default(Expr::current_timestamp()),
                    )
                    .col(ColumnDef::new("task_type_url").text().not_null())
                    // Protobuf-serialized task payload
                    .col(ColumnDef::new("task_payload_pb").binary().not_null())
                    .col(ColumnDef::new("updated_at").timestamp_with_time_zone())
                    .col(ColumnDef::new("attempts").integer().default(Expr::value(0)))
                    .col(
                        ColumnDef::new("max_attempts")
                            .integer()
                            .default(Expr::value(5)),
                    )
                    .col(ColumnDef::new("error_message").text())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                IndexCreateStatement::new()
                    .name("tasks_fetch_idx")
                    .table("tasks")
                    .col("run_at")
                    .and_where(Expr::col("status").eq("Q"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
