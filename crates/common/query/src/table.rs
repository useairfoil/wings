use async_trait::async_trait;
use datafusion::{
    datasource::provider_as_source,
    error::DataFusionError,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::{SessionContext, and, col, lit},
};
use snafu::Snafu;
use wings_resources::{PartitionValue, Table};
use wings_schema::SchemaError;

#[derive(Debug, Snafu)]
pub enum TableLogicalPlanError {
    #[snafu(display("Missing required partition value for field '{field}'"))]
    MissingPartitionValue { field: String },
    #[snafu(transparent)]
    DataFusion { source: DataFusionError },
    #[snafu(transparent)]
    Schema { source: SchemaError },
}

type Result<T, E = TableLogicalPlanError> = std::result::Result<T, E>;

#[async_trait]
pub trait TopicLogicalPlanExt {
    async fn logical_plan(
        &self,
        ctx: &SessionContext,
        start_seqnum: u64,
        end_seqnum: u64,
        partition_value: Option<PartitionValue>,
    ) -> Result<LogicalPlan>;
}

#[async_trait]
impl TopicLogicalPlanExt for Table {
    async fn logical_plan(
        &self,
        ctx: &SessionContext,
        start_seqnum: u64,
        end_seqnum: u64,
        partition_value: Option<PartitionValue>,
    ) -> Result<LogicalPlan> {
        let seqnum_expr = col("__seqnum__").between(lit(start_seqnum), lit(end_seqnum));

        let filter = if let Some(field) = self.partition_field() {
            let Some(value) = partition_value else {
                return Err(TableLogicalPlanError::MissingPartitionValue {
                    field: field.name().to_string(),
                });
            };

            let partition_expr = col(field.name()).eq(value.into_lit());
            and(seqnum_expr, partition_expr)
        } else {
            seqnum_expr
        };

        let sort = col("__seqnum__").sort(true, false);

        let table_provider = ctx.table_provider(self.name.id()).await?;
        let table_source = provider_as_source(table_provider);

        let plan = LogicalPlanBuilder::scan(self.name.id(), table_source, None)?
            .filter(filter)?
            .sort([sort])?
            .build()?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::RecordBatch,
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        },
        datasource::MemTable,
    };
    use wings_resources::{NamespaceName, TableName, TableOptions, TenantName};
    use wings_schema::{DataType, Field as WingsField, SchemaBuilder};

    use super::*;

    fn create_test_table_without_partition() -> Table {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let schema = SchemaBuilder::new(vec![
            WingsField::new("id", 1, DataType::Int32, false),
            WingsField::new("name", 2, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new(schema);
        Table::new(table_name, options)
    }

    fn create_test_table_with_partition() -> Table {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("partitioned-table", namespace_name).unwrap();
        let schema = SchemaBuilder::new(vec![
            WingsField::new("region_id", 0, DataType::Int64, false),
            WingsField::new("id", 1, DataType::Int32, false),
            WingsField::new("name", 2, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        Table::new(table_name, options)
    }

    async fn create_session_with_table(table: &Table) -> SessionContext {
        let ctx = SessionContext::new();

        // Create an Arrow schema that includes the table's columns plus the __seqnum__ column
        let arrow_schema = table.arrow_schema();
        let mut fields = arrow_schema.fields().to_vec();
        fields.push(Arc::new(ArrowField::new(
            "__seqnum__",
            ArrowDataType::UInt64,
            true,
        )));
        let full_schema = Arc::new(ArrowSchema::new(fields));

        // Create empty record batch to initialize the table
        let batch = RecordBatch::new_empty(full_schema.clone());
        let mem_table = MemTable::try_new(full_schema, vec![vec![batch]]).unwrap();

        // Register the table with the table's ID
        ctx.register_table(table.name.id(), Arc::new(mem_table))
            .expect("register table");

        ctx
    }

    #[tokio::test]
    async fn test_logical_plan_non_partitioned_table() {
        // Arrange
        let table = create_test_table_without_partition();
        let ctx = create_session_with_table(&table).await;
        let start_seqnum = 0u64;
        let end_seqnum = 100u64;

        // Act
        let plan = table
            .logical_plan(&ctx, start_seqnum, end_seqnum, None)
            .await
            .expect("logical_plan should succeed");

        // Assert
        insta::assert_snapshot!(plan.display_indent(), @r"
        Sort: test-table.__seqnum__ ASC NULLS LAST
          Filter: test-table.__seqnum__ BETWEEN UInt64(0) AND UInt64(100)
            TableScan: test-table
        ");
    }

    #[tokio::test]
    async fn test_logical_plan_partitioned_table_with_value() {
        // Arrange
        let table = create_test_table_with_partition();
        let ctx = create_session_with_table(&table).await;
        let start_seqnum = 10u64;
        let end_seqnum = 50u64;
        let partition_value = Some(PartitionValue::Int64(42));

        // Act
        let plan = table
            .logical_plan(&ctx, start_seqnum, end_seqnum, partition_value)
            .await
            .expect("logical_plan should succeed");

        // Assert
        insta::assert_snapshot!(plan.display_indent(), @r"
        Sort: partitioned-table.__seqnum__ ASC NULLS LAST
          Filter: partitioned-table.__seqnum__ BETWEEN UInt64(10) AND UInt64(50) AND partitioned-table.region_id = Int64(42)
            TableScan: partitioned-table
        ");
    }

    #[tokio::test]
    async fn test_logical_plan_partitioned_table_missing_value() {
        // Arrange
        let table = create_test_table_with_partition();
        let ctx = create_session_with_table(&table).await;
        let start_seqnum = 10u64;
        let end_seqnum = 50u64;

        // Act
        let result = table
            .logical_plan(&ctx, start_seqnum, end_seqnum, None)
            .await;

        // Assert
        insta::assert_debug_snapshot!(result.unwrap_err(), @r#"
        MissingPartitionValue {
            field: "region_id",
        }
        "#);
    }

    #[tokio::test]
    async fn test_logical_plan_different_partition_value_types() {
        // Arrange
        let table = create_test_table_with_partition();
        let ctx = create_session_with_table(&table).await;

        // Test with different partition value types
        let test_values = vec![
            PartitionValue::Int8(1),
            PartitionValue::Int16(100),
            PartitionValue::Int32(1000),
            PartitionValue::Int64(10000),
            PartitionValue::UInt8(1),
            PartitionValue::UInt16(100),
            PartitionValue::UInt32(1000),
            PartitionValue::UInt64(10000),
            PartitionValue::String("test".to_string()),
            PartitionValue::Boolean(true),
        ];

        for value in test_values {
            let result = table
                .logical_plan(&ctx, 0u64, 100u64, Some(value.clone()))
                .await;
            assert!(
                result.is_ok(),
                "logical_plan should succeed for partition value {:?}",
                value
            );
        }
    }

    #[tokio::test]
    async fn test_logical_plan_table_not_found_error() {
        // Arrange - create a table but don't register a table
        let table = create_test_table_without_partition();
        let ctx = SessionContext::new(); // Empty context without the table

        // Act
        let result = table.logical_plan(&ctx, 0u64, 100u64, None).await;

        // Assert - should return DataFusion error for missing table
        insta::assert_debug_snapshot!(result.unwrap_err(), @r#"
        DataFusion {
            source: Plan(
                "No table named 'test-table'",
            ),
        }
        "#);
    }
}
