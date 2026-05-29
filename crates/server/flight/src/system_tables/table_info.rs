use std::{any::Any, collections::VecDeque, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray, UInt64Array},
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
use futures::{StreamExt as _, TryStreamExt as _, stream};
use tracing::debug;
use wings_meta_db::ClusterStore;
use wings_resources::{NamespaceName, Table, TableName};

use crate::{datafusion_helpers::apply_projection, system_tables::find_table_name_in_filters};

const LOAD_TABLE_CONCURRENCY: usize = 16;

pub struct TableInfoSystemTable {
    store: ClusterStore,
    name: NamespaceName,
    schema: SchemaRef,
}

struct TableInfoDiscoveryExec {
    store: ClusterStore,
    name: NamespaceName,
    tables: Option<VecDeque<TableName>>,
    properties: PlanProperties,
}

struct TableInfoScanState {
    store: ClusterStore,
    namespace: NamespaceName,
    tables: Option<VecDeque<TableName>>,
    next_page_token: Option<String>,
    done: bool,
    batch_size: usize,
    schema: SchemaRef,
}

impl TableInfoSystemTable {
    pub fn new(store: ClusterStore, name: NamespaceName) -> Self {
        Self {
            store,
            name,
            schema: TableInfoDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TableInfoSystemTable {
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
        let table_filters = find_table_name_in_filters(filters, "table_id");

        let table_exec =
            TableInfoDiscoveryExec::new(self.store.clone(), self.name.clone(), table_filters);
        let table_exec = Arc::new(table_exec);

        apply_projection(table_exec, projection)
    }
}

impl TableInfoDiscoveryExec {
    pub fn new(store: ClusterStore, name: NamespaceName, tables: Option<Vec<String>>) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);
        let tables = tables.map(|tables| table_names_from_filters(&name, tables));

        Self {
            store,
            name,
            tables,
            properties,
        }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("namespace_id", DataType::Utf8, false),
            Field::new("table_id", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("key_field_id", DataType::UInt64, false),
            Field::new("key_field_name", DataType::Utf8, false),
            Field::new("version_field_id", DataType::UInt64, false),
            Field::new("version_field_name", DataType::Utf8, false),
            Field::new("partition_field_id", DataType::UInt64, true),
            Field::new("partition_field_name", DataType::Utf8, true),
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

impl ExecutionPlan for TableInfoDiscoveryExec {
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
        debug!(namespace = %self.name, "TableInfoSystemTable::execute");

        let schema = self.schema();
        let state = TableInfoScanState {
            store: self.store.clone(),
            namespace: self.name.clone(),
            tables: self.tables.clone(),
            next_page_token: None,
            done: false,
            batch_size,
            schema: schema.clone(),
        };

        let stream = stream::try_unfold(state, |mut state| async move {
            loop {
                let table_names = state.next_table_names().await?;
                let Some(table_names) = table_names else {
                    return Ok(None);
                };

                let tables = load_tables(state.store.clone(), table_names).await?;
                if tables.is_empty() {
                    if state.done {
                        return Ok(None);
                    }

                    continue;
                }

                let batch = from_tables(state.schema.clone(), &tables)?;
                return Ok(Some((batch, state)));
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl TableInfoScanState {
    async fn next_table_names(&mut self) -> Result<Option<Vec<TableName>>, DataFusionError> {
        if self.done {
            return Ok(None);
        }

        if let Some(tables) = &mut self.tables {
            let len = tables.len().min(self.batch_size);
            let names = tables.drain(..len).collect::<Vec<_>>();
            self.done = tables.is_empty();

            return Ok(Some(names));
        }

        let page = self
            .store
            .list_tables(
                self.namespace.clone(),
                Some(self.batch_size),
                self.next_page_token.take(),
            )
            .await
            .map_err(external_error)?;

        self.next_page_token = page.next_page_token;
        self.done = self.next_page_token.is_none();

        Ok(Some(page.tables))
    }
}

fn table_names_from_filters(namespace: &NamespaceName, tables: Vec<String>) -> VecDeque<TableName> {
    tables
        .into_iter()
        .filter_map(|table| TableName::new(table, namespace.clone()).ok())
        .collect()
}

async fn load_tables(
    store: ClusterStore,
    table_names: Vec<TableName>,
) -> Result<Vec<Table>, DataFusionError> {
    stream::iter(table_names)
        .map(|name| {
            let store = store.clone();
            async move { store.table(name).try_load().await }
        })
        .buffered(LOAD_TABLE_CONCURRENCY)
        .try_filter_map(|table| async move { Ok(table.map(|table| table.table())) })
        .try_collect()
        .await
        .map_err(external_error)
}

fn from_tables(schema: SchemaRef, tables: &[Table]) -> Result<RecordBatch, DataFusionError> {
    let namespaces = tables
        .iter()
        .map(|table| table.name.parent().id().to_string())
        .collect::<Vec<_>>();
    let names = tables
        .iter()
        .map(|table| table.name.id().to_string())
        .collect::<Vec<_>>();
    let descriptions = tables
        .iter()
        .map(|table| table.description.clone())
        .collect::<Vec<_>>();
    let key_field_ids = tables
        .iter()
        .map(|table| table.key_field_id)
        .collect::<Vec<_>>();
    let key_field_names = tables
        .iter()
        .map(|table| table.key_field().name().to_string())
        .collect::<Vec<_>>();
    let version_field_ids = tables
        .iter()
        .map(|table| table.version_field_id)
        .collect::<Vec<_>>();
    let version_field_names = tables
        .iter()
        .map(|table| table.version_field().name().to_string())
        .collect::<Vec<_>>();
    let partition_field_ids = tables
        .iter()
        .map(|table| table.partition_field_id)
        .collect::<Vec<_>>();
    let partition_field_names = tables
        .iter()
        .map(|table| {
            table
                .partition_field()
                .map(|field| field.name().to_string())
        })
        .collect::<Vec<_>>();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(namespaces)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(descriptions)),
        Arc::new(UInt64Array::from(key_field_ids)),
        Arc::new(StringArray::from(key_field_names)),
        Arc::new(UInt64Array::from(version_field_ids)),
        Arc::new(StringArray::from(version_field_names)),
        Arc::new(UInt64Array::from(partition_field_ids)),
        Arc::new(StringArray::from(partition_field_names)),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

fn external_error(err: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

impl fmt::Debug for TableInfoSystemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInfoSystemTable")
            .field("namespace", &self.name)
            .finish()
    }
}

impl DisplayAs for TableInfoDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TableInfoDiscoveryExec: namespace=[{}]", self.name)
    }
}

impl fmt::Debug for TableInfoDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInfoDiscoveryExec")
            .field("namespace", &self.name)
            .finish()
    }
}
