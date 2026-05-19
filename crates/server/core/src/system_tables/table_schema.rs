use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    common::arrow::{
        array::{ArrayRef, BooleanBuilder, RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    error::DataFusionError,
    prelude::Expr,
};
use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, CollectNamespaceTablesOptions, collect_namespace_tables,
};
use wings_resources::{NamespaceName, Table};

use super::{helpers::TOPIC_NAME_COLUMN, provider::SystemTable};

pub struct TableSchemaTable {
    cluster_meta: Arc<dyn ClusterMetadata>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TableSchemaTable {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>, namespace: NamespaceName) -> Self {
        Self {
            cluster_meta,
            namespace,
            schema: table_schema_schema(),
        }
    }
}

#[async_trait]
impl SystemTable for TableSchemaTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _filters: Vec<Expr>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let tables = collect_namespace_tables(
            &self.cluster_meta,
            &self.namespace,
            CollectNamespaceTablesOptions::default(),
        )
        .await?;

        from_tables(self.schema.clone(), &tables)
    }
}

impl std::fmt::Debug for TableSchemaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableSchemaTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}

fn table_schema_schema() -> SchemaRef {
    let fields = vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("namespace", DataType::Utf8, false),
        Field::new(TOPIC_NAME_COLUMN, DataType::Utf8, false),
        Field::new("field", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("nullable", DataType::Boolean, false),
        Field::new("is_partition_key", DataType::Boolean, false),
    ];

    Arc::new(Schema::new(fields))
}

fn from_tables(schema: SchemaRef, tables: &[Table]) -> Result<RecordBatch, DataFusionError> {
    let mut tenant_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut namespace_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut table_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut field_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut data_type_arr = StringBuilder::with_capacity(tables.len(), 0);
    let mut nullable_arr = BooleanBuilder::with_capacity(tables.len());
    let mut is_partition_key_arr = BooleanBuilder::with_capacity(tables.len());

    for table in tables {
        let partition_key = table.partition_key;
        for field in table.schema().fields_iter() {
            let table_name = table.name.clone();
            table_arr.append_value(table_name.id.clone());
            let namespace_name = table_name.parent();
            namespace_arr.append_value(namespace_name.id.clone());
            let tenant_name = namespace_name.parent();
            tenant_arr.append_value(tenant_name.id.clone());

            field_arr.append_value(&field.name);
            data_type_arr.append_value(field.data_type.to_string());
            nullable_arr.append_value(field.nullable);
            is_partition_key_arr.append_value(Some(field.id) == partition_key);
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_arr.finish()),
        Arc::new(namespace_arr.finish()),
        Arc::new(table_arr.finish()),
        Arc::new(field_arr.finish()),
        Arc::new(data_type_arr.finish()),
        Arc::new(nullable_arr.finish()),
        Arc::new(is_partition_key_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
