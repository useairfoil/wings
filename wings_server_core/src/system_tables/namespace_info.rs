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
use wings_metadata_core::admin::{Admin, NamespaceName};

use super::provider::SystemTable;

pub struct NamespaceInfoTable {
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl NamespaceInfoTable {
    pub fn new(admin: Arc<dyn Admin>, namespace: NamespaceName) -> Self {
        Self {
            admin,
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
        let namespace = self.admin.get_namespace(self.namespace.clone()).await?;

        let tenant_arr = StringArray::from(vec![self.namespace.parent().id().to_string()]);
        let namespace_arr = StringArray::from(vec![self.namespace.id().to_string()]);
        let flush_size_bytes_arr = UInt64Array::from(vec![namespace.flush_size.as_u64()]);
        let flush_interval_ms_arr =
            UInt64Array::from(vec![namespace.flush_interval.as_millis() as u64]);
        let default_object_store_config_arr =
            StringArray::from(vec![namespace.default_object_store_config.to_string()]);
        let frozen_object_store_config_arr = StringArray::from(vec![
            namespace
                .frozen_object_store_config
                .map(|config| config.to_string()),
        ]);

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(tenant_arr),
                Arc::new(namespace_arr),
                Arc::new(flush_size_bytes_arr),
                Arc::new(flush_interval_ms_arr),
                Arc::new(default_object_store_config_arr),
                Arc::new(frozen_object_store_config_arr),
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
        Field::new("default_object_store_config", DataType::Utf8, false),
        Field::new("frozen_object_store_config", DataType::Utf8, true),
    ];

    Arc::new(Schema::new(fields))
}
