use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::{ExecutionPlan, empty::EmptyExec, union::UnionExec},
    prelude::Expr,
};
use futures::TryStreamExt;
use tokio::pin;

use crate::{
    datafusion_helpers::apply_projection,
    system_tables::{exec::TopicPartitionValueDiscoveryExec, helpers::find_topic_name_in_filters},
};
use wings_metadata_core::{
    admin::{Admin, NamespaceName},
    offset_registry::OffsetRegistry,
};

use super::exec::paginated_topic_stream;

/// System table for discovering partition values for topics.
pub struct TopicPartitionValueSystemTable {
    admin: Arc<dyn Admin>,
    offset_registry: Arc<dyn OffsetRegistry>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicPartitionValueSystemTable {
    pub fn new(
        admin: Arc<dyn Admin>,
        offset_registry: Arc<dyn OffsetRegistry>,
        namespace: NamespaceName,
    ) -> Self {
        Self {
            admin,
            offset_registry,
            namespace,
            schema: TopicPartitionValueDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TopicPartitionValueSystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let topics_filter = find_topic_name_in_filters(&filters);
        let batch_size = state.config().batch_size();

        let stream = {
            let admin = self.admin.clone();
            let namespace = self.namespace.clone();

            paginated_topic_stream(admin, namespace, batch_size, topics_filter)
        };
        pin!(stream);

        // TODO: we accumulate all topics into a vector to use `UnionExec`.
        // This obviously won't work if we have too many topics.
        let mut topic_exec: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        while let Some(page) = stream.try_next().await.map_err(DataFusionError::from)? {
            for topic in page {
                let exec =
                    TopicPartitionValueDiscoveryExec::new(self.offset_registry.clone(), topic);
                topic_exec.push(Arc::new(exec));
            }
        }

        let union_exec: Arc<dyn ExecutionPlan> = match topic_exec.as_slice() {
            [] => Arc::new(EmptyExec::new(self.schema.clone())),
            [topic] => topic.clone(),
            _ => Arc::new(UnionExec::new(topic_exec)),
        };
        apply_projection(union_exec, projection)
    }
}

impl std::fmt::Debug for TopicPartitionValueSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicPartitionValueSystemTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
