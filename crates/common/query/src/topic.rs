use async_trait::async_trait;
use datafusion::{
    datasource::provider_as_source,
    error::DataFusionError,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::{SessionContext, and, col, lit},
};
use snafu::Snafu;
use wings_resources::{PartitionValue, Topic};
use wings_schema::SchemaError;

#[derive(Debug, Snafu)]
pub enum TopicLogicalPlanError {
    #[snafu(display("Missing required partition value for field '{field}'"))]
    MissingPartitionValue { field: String },
    #[snafu(transparent)]
    DataFusion { source: DataFusionError },
    #[snafu(transparent)]
    Schema { source: SchemaError },
}

type Result<T, E = TopicLogicalPlanError> = std::result::Result<T, E>;

#[async_trait]
pub trait TopicLogicalPlanExt {
    async fn logical_plan(
        &self,
        ctx: &SessionContext,
        start_offset: u64,
        end_offset: u64,
        partition_value: Option<PartitionValue>,
    ) -> Result<LogicalPlan>;
}

#[async_trait]
impl TopicLogicalPlanExt for Topic {
    async fn logical_plan(
        &self,
        ctx: &SessionContext,
        start_offset: u64,
        end_offset: u64,
        partition_value: Option<PartitionValue>,
    ) -> Result<LogicalPlan> {
        let offset_expr = col("__offset__").between(lit(start_offset), lit(end_offset));

        let filter = if let Some(field) = self.partition_field() {
            let Some(value) = partition_value else {
                return Err(TopicLogicalPlanError::MissingPartitionValue {
                    field: field.name().to_string(),
                });
            };

            let partition_expr = col(field.name()).eq(value.into_lit());
            and(offset_expr, partition_expr)
        } else {
            offset_expr
        };

        let sort = col("__offset__").sort(true, false);

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
    use wings_resources::{NamespaceName, TenantName, TopicName, TopicOptions};
    use wings_schema::{DataType, Field as WingsField, SchemaBuilder};

    use super::*;

    fn create_test_topic_without_partition() -> Topic {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let schema = SchemaBuilder::new(vec![
            WingsField::new("id", 1, DataType::Int32, false),
            WingsField::new("name", 2, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new(schema);
        Topic::new(topic_name, options)
    }

    fn create_test_topic_with_partition() -> Topic {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("partitioned-topic", namespace_name).unwrap();
        let schema = SchemaBuilder::new(vec![
            WingsField::new("region_id", 0, DataType::Int64, false),
            WingsField::new("id", 1, DataType::Int32, false),
            WingsField::new("name", 2, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        Topic::new(topic_name, options)
    }

    async fn create_session_with_table(topic: &Topic) -> SessionContext {
        let ctx = SessionContext::new();

        // Create an Arrow schema that includes the topic's columns plus the __offset__ column
        let arrow_schema = topic.arrow_schema();
        let mut fields = arrow_schema.fields().to_vec();
        fields.push(Arc::new(ArrowField::new(
            "__offset__",
            ArrowDataType::UInt64,
            true,
        )));
        let full_schema = Arc::new(ArrowSchema::new(fields));

        // Create empty record batch to initialize the table
        let batch = RecordBatch::new_empty(full_schema.clone());
        let table = MemTable::try_new(full_schema, vec![vec![batch]]).unwrap();

        // Register the table with the topic's ID
        ctx.register_table(topic.name.id(), Arc::new(table))
            .expect("register table");

        ctx
    }

    #[tokio::test]
    async fn test_logical_plan_non_partitioned_topic() {
        // Arrange
        let topic = create_test_topic_without_partition();
        let ctx = create_session_with_table(&topic).await;
        let start_offset = 0u64;
        let end_offset = 100u64;

        // Act
        let plan = topic
            .logical_plan(&ctx, start_offset, end_offset, None)
            .await
            .expect("logical_plan should succeed");

        // Assert
        insta::assert_snapshot!(plan.display_indent(), @r"
        Sort: test-topic.__offset__ ASC NULLS LAST
          Filter: test-topic.__offset__ BETWEEN UInt64(0) AND UInt64(100)
            TableScan: test-topic
        ");
    }

    #[tokio::test]
    async fn test_logical_plan_partitioned_topic_with_value() {
        // Arrange
        let topic = create_test_topic_with_partition();
        let ctx = create_session_with_table(&topic).await;
        let start_offset = 10u64;
        let end_offset = 50u64;
        let partition_value = Some(PartitionValue::Int64(42));

        // Act
        let plan = topic
            .logical_plan(&ctx, start_offset, end_offset, partition_value)
            .await
            .expect("logical_plan should succeed");

        // Assert
        insta::assert_snapshot!(plan.display_indent(), @r"
        Sort: partitioned-topic.__offset__ ASC NULLS LAST
          Filter: partitioned-topic.__offset__ BETWEEN UInt64(10) AND UInt64(50) AND partitioned-topic.region_id = Int64(42)
            TableScan: partitioned-topic
        ");
    }

    #[tokio::test]
    async fn test_logical_plan_partitioned_topic_missing_value() {
        // Arrange
        let topic = create_test_topic_with_partition();
        let ctx = create_session_with_table(&topic).await;
        let start_offset = 10u64;
        let end_offset = 50u64;

        // Act
        let result = topic
            .logical_plan(&ctx, start_offset, end_offset, None)
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
        let topic = create_test_topic_with_partition();
        let ctx = create_session_with_table(&topic).await;

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
            let result = topic
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
        // Arrange - create a topic but don't register a table
        let topic = create_test_topic_without_partition();
        let ctx = SessionContext::new(); // Empty context without the table

        // Act
        let result = topic.logical_plan(&ctx, 0u64, 100u64, None).await;

        // Assert - should return DataFusion error for missing table
        insta::assert_debug_snapshot!(result.unwrap_err(), @r#"
        DataFusion {
            source: Plan(
                "No table named 'test-topic'",
            ),
        }
        "#);
    }
}
