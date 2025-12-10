use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    common::arrow::{
        array::{RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    error::DataFusionError,
    prelude::Expr,
};
use wings_control_plane::{cluster_metadata::ClusterMetadata, resources::NamespaceName};

use super::provider::SystemTable;

pub struct NamespaceInfoTable {
    cluster_meta: Arc<dyn ClusterMetadata>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl NamespaceInfoTable {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>, namespace: NamespaceName) -> Self {
        Self {
            cluster_meta,
            namespace,
            schema: namespace_info_schema(),
        }
    }
}

#[async_trait]
impl SystemTable for NamespaceInfoTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _filters: Vec<Expr>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let namespace = self
            .cluster_meta
            .get_namespace(self.namespace.clone())
            .await?;

        let tenant_arr = StringArray::from(vec![self.namespace.parent().id().to_string()]);
        let namespace_arr = StringArray::from(vec![self.namespace.id().to_string()]);
        let flush_size_bytes_arr = UInt64Array::from(vec![namespace.flush_size.as_u64()]);
        let flush_interval_ms_arr =
            UInt64Array::from(vec![namespace.flush_interval.as_millis() as u64]);
        let object_store_arr = StringArray::from(vec![namespace.object_store.to_string()]);
        let data_lake_arr = StringArray::from(vec![namespace.data_lake.to_string()]);

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(tenant_arr),
                Arc::new(namespace_arr),
                Arc::new(flush_size_bytes_arr),
                Arc::new(flush_interval_ms_arr),
                Arc::new(object_store_arr),
                Arc::new(data_lake_arr),
            ],
        )?;

        Ok(batch)
    }
}

impl std::fmt::Debug for NamespaceInfoTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceInfoTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}

fn namespace_info_schema() -> SchemaRef {
    let fields = vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("namespace", DataType::Utf8, false),
        Field::new("flush_size_bytes", DataType::UInt64, false),
        Field::new("flush_interval_ms", DataType::UInt64, false),
        Field::new("object_store", DataType::Utf8, false),
        Field::new("data_lake", DataType::Utf8, true),
    ];

    Arc::new(Schema::new(fields))
}
