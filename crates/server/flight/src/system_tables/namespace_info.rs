use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    catalog::{Session, TableProvider},
    common::arrow::datatypes::SchemaRef,
    datasource::TableType,
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::TableProviderFilterPushDown,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use futures::stream;
use tracing::debug;
use wings_meta_db::ClusterStore;
use wings_resources::{DataLakeConfiguration, NamespaceName, ObjectStoreConfiguration};

use crate::datafusion_helpers::apply_projection;

pub struct NamespaceInfoSystemTable {
    store: ClusterStore,
    name: NamespaceName,
    schema: SchemaRef,
}

struct NamespaceInfoDiscoveryExec {
    store: ClusterStore,
    name: NamespaceName,
    properties: PlanProperties,
}

impl NamespaceInfoSystemTable {
    pub fn new(store: ClusterStore, name: NamespaceName) -> Self {
        Self {
            store,
            name,
            schema: NamespaceInfoDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for NamespaceInfoSystemTable {
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let namespace_exec = NamespaceInfoDiscoveryExec::new(self.store.clone(), self.name.clone());
        let namespace_exec = Arc::new(namespace_exec);

        apply_projection(namespace_exec, projection)
    }
}

impl NamespaceInfoDiscoveryExec {
    pub fn new(store: ClusterStore, name: NamespaceName) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            store,
            name,
            properties,
        }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("namespace_id", DataType::Utf8, false),
            Field::new("object_store_type", DataType::Utf8, false),
            Field::new("object_store_config", DataType::Utf8, false),
            Field::new("lake_type", DataType::Utf8, false),
            Field::new("lake_config", DataType::Utf8, false),
        ];

        Arc::new(Schema::new(fields))
    }

    fn compute_properties(schema: &SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;

        PlanProperties::new(eq_properties, partitioning, emission_type, boundedness)
    }
}

impl ExecutionPlan for NamespaceInfoDiscoveryExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn schema(&self) -> SchemaRef {
        Self::schema()
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        debug!(namespace = %self.name, "NamespaceInfoSystemTable::execute");

        let schema = self.schema();
        let store = self.store.clone();
        let namespace = self.name.clone();
        let stream_schema = schema.clone();
        let stream = stream::once(async move {
            let namespace = store
                .namespace(namespace)
                .load()
                .await
                .map_err(external_error)?;

            from_namespace(stream_schema, namespace)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn from_namespace(
    schema: SchemaRef,
    namespace: wings_meta_db::StoredNamespace,
) -> Result<RecordBatch, DataFusionError> {
    let object_store = namespace.object_store().clone().into_redacted();
    let lake = namespace.lake().clone();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![namespace.name().id().to_string()])),
        Arc::new(StringArray::from(vec![object_store_type(&object_store)])),
        Arc::new(StringArray::from(vec![to_json(&object_store)?])),
        Arc::new(StringArray::from(vec![lake_type(&lake)])),
        Arc::new(StringArray::from(vec![to_json(&lake)?])),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

fn object_store_type(config: &ObjectStoreConfiguration) -> &'static str {
    match config {
        ObjectStoreConfiguration::Aws(_) => "aws",
        ObjectStoreConfiguration::Azure(_) => "azure",
        ObjectStoreConfiguration::Google(_) => "google",
        ObjectStoreConfiguration::S3Compatible(_) => "s3-compatible",
    }
}

fn lake_type(config: &DataLakeConfiguration) -> &'static str {
    match config {
        DataLakeConfiguration::Parquet(_) => "parquet",
        DataLakeConfiguration::Iceberg(_) => "iceberg",
        DataLakeConfiguration::Delta(_) => "delta",
    }
}

fn to_json(value: &impl serde::Serialize) -> Result<String, DataFusionError> {
    serde_json::to_string(value).map_err(external_error)
}

fn external_error(err: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

impl fmt::Debug for NamespaceInfoSystemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespaceInfoSystemTable")
            .field("namespace", &self.name)
            .finish()
    }
}

impl DisplayAs for NamespaceInfoDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NamespaceInfoDiscoveryExec: namespace=[{}]", self.name)
    }
}

impl fmt::Debug for NamespaceInfoDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespaceInfoDiscoveryExec")
            .field("namespace", &self.name)
            .finish()
    }
}
