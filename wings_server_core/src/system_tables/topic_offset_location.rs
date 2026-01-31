use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::arrow::datatypes::SchemaRef,
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use wings_control_plane_core::{cluster_metadata::ClusterMetadata, log_metadata::LogMetadata};
use wings_resources::NamespaceName;

use super::{exec::TopicOffsetLocationDiscoveryExec, helpers::find_topic_name_in_filters};
use crate::{datafusion_helpers::apply_projection, options::SessionConfigExt};

pub struct TopicOffsetLocationSystemTable {
    cluster_meta: Arc<dyn ClusterMetadata>,
    log_meta: Arc<dyn LogMetadata>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicOffsetLocationSystemTable {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        log_meta: Arc<dyn LogMetadata>,
        namespace: NamespaceName,
    ) -> Self {
        Self {
            cluster_meta,
            log_meta,
            namespace,
            schema: TopicOffsetLocationDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TopicOffsetLocationSystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
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
        let topics_filter = find_topic_name_in_filters(filters);

        let fetch_options = state.config().fetch_options().clone();

        let topic_offset_exec = TopicOffsetLocationDiscoveryExec::new(
            self.cluster_meta.clone(),
            self.log_meta.clone(),
            self.namespace.clone(),
            topics_filter,
            fetch_options,
        );

        apply_projection(Arc::new(topic_offset_exec), projection)
    }
}

impl std::fmt::Debug for TopicOffsetLocationSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicOffsetLocationTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
