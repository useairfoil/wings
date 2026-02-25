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

            let partition_expr = col(field.name()).eq(value.to_lit());
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
