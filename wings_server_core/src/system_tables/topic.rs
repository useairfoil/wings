use std::{any::Any, sync::Arc, usize};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::{ExecutionPlan, PhysicalExpr, expressions::col, projection::ProjectionExec},
    prelude::Expr,
};
use wings_metadata_core::admin::{Admin, NamespaceName};

use super::{exec::TopicDiscoveryExec, helpers::find_topic_name_in_filters};

pub struct TopicSystemTable {
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicSystemTable {
    pub fn new(admin: Arc<dyn Admin>, namespace: NamespaceName) -> Self {
        Self {
            admin,
            namespace,
            schema: TopicDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TopicSystemTable {
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let topic_filters = find_topic_name_in_filters(&filters);

        let topic_exec =
            TopicDiscoveryExec::new(self.admin.clone(), self.namespace.clone(), topic_filters);
        let topic_exec = Arc::new(topic_exec);

        let Some(projection) = projection else {
            return Ok(topic_exec);
        };

        let base_schema = topic_exec.schema();

        let projected_exprs: Vec<(Arc<dyn PhysicalExpr + 'static>, String)> = projection
            .iter()
            .map(|&i| {
                let field_name = base_schema.field(i).name();
                let col_expr = col(field_name, &base_schema)?;
                Ok::<_, DataFusionError>((col_expr, field_name.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let projection_exec = ProjectionExec::try_new(projected_exprs, topic_exec)?;

        Ok(Arc::new(projection_exec))
    }
}

impl std::fmt::Debug for TopicSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
