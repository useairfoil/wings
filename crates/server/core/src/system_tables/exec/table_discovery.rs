use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    common::arrow::{
        array::{ArrayRef, RecordBatch, StringBuilder, UInt32Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use tracing::debug;
use wings_control_plane_core::cluster_metadata::{ClusterMetadata, stream::PaginatedTableStream};
use wings_resources::{NamespaceName, Table};

use crate::system_tables::helpers::TOPIC_NAME_COLUMN;

pub struct TableDiscoveryExec {
    cluster_meta: Arc<dyn ClusterMetadata>,
    namespace: NamespaceName,
    tables: Option<Vec<String>>,
    properties: PlanProperties,
}

impl TableDiscoveryExec {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        namespace: NamespaceName,
        tables: Option<Vec<String>>,
    ) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            cluster_meta,
            namespace,
            tables,
            properties,
        }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new(TOPIC_NAME_COLUMN, DataType::Utf8, false),
            Field::new("partition_key", DataType::UInt32, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("compaction_freshness_ms", DataType::UInt64, true),
            Field::new("compaction_ttl_ms", DataType::UInt64, true),
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

impl ExecutionPlan for TableDiscoveryExec {
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
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        let batch_size = context.session_config().batch_size();
        debug!(namespace = %self.namespace, "TableDiscoveryExec::execute");

        let schema = self.schema();

        let tables = PaginatedTableStream::new(
            self.cluster_meta.clone(),
            self.namespace.clone(),
            batch_size,
            self.tables.clone(),
        );

        let stream = RecordBatchStreamAdapter::new(
            self.schema(),
            tables.map(move |result| {
                result
                    .map_err(DataFusionError::from)
                    .and_then(|tables| from_tables(schema.clone(), &tables))
            }),
        );

        Ok(Box::pin(stream))
    }
}

impl DisplayAs for TableDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopicDiscovery: namespace=[{}]", self.namespace)
    }
}

impl fmt::Debug for TableDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableDiscoveryExec")
            .field("namespace", &self.namespace)
            .finish()
    }
}

fn from_tables(schema: SchemaRef, tables: &[Table]) -> Result<RecordBatch> {
    let mut tenant_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut namespace_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut table_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut partition_key_arr = UInt32Builder::with_capacity(tables.len());
    let mut description_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut compaction_freshness_ms_arr = UInt64Builder::with_capacity(tables.len());
    let mut compaction_ttl_ms_arr = UInt64Builder::with_capacity(tables.len());

    for table in tables {
        let table_name = table.name.clone();

        table_arr.append_value(table_name.id.clone());

        let namespace_name = table_name.parent();
        namespace_arr.append_value(namespace_name.id.clone());

        let tenant_name = namespace_name.parent();
        tenant_arr.append_value(tenant_name.id.clone());

        if let Some(partition_key) = table.partition_key {
            partition_key_arr.append_value(partition_key as _);
        } else {
            partition_key_arr.append_null();
        }

        if let Some(description) = &table.description {
            description_arr.append_value(description);
        } else {
            description_arr.append_null();
        }

        compaction_freshness_ms_arr.append_value(table.compaction.freshness.as_millis() as u64);

        if let Some(compaction_ttl) = &table.compaction.ttl {
            compaction_ttl_ms_arr.append_value(compaction_ttl.as_millis() as u64);
        } else {
            compaction_ttl_ms_arr.append_null();
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_arr.finish()),
        Arc::new(namespace_arr.finish()),
        Arc::new(table_arr.finish()),
        Arc::new(partition_key_arr.finish()),
        Arc::new(description_arr.finish()),
        Arc::new(compaction_freshness_ms_arr.finish()),
        Arc::new(compaction_ttl_ms_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
