mod namespace_info;
mod table_info;
mod table_schema;

use std::{any::Any, collections::HashMap, ops::Deref, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
    logical_expr::{BinaryExpr, Operator},
    prelude::{Expr, col},
    scalar::ScalarValue,
};
use wings_meta_db::ClusterStore;
use wings_resources::NamespaceName;

use crate::system_tables::{
    namespace_info::NamespaceInfoSystemTable, table_info::TableInfoSystemTable,
    table_schema::TableSchemaSystemTable,
};

pub const NAMESPACE_INFO_TABLE_NAME: &str = "namespace_info";
pub const TABLE_TABLE_NAME: &str = "tables";
pub const TABLE_SCHEMA_TABLE_NAME: &str = "table_schemas";

pub struct SystemSchemaProvider {
    name: NamespaceName,
    tables: HashMap<&'static str, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|name| name.to_string()).collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }
}

impl SystemSchemaProvider {
    pub fn new(store: ClusterStore, name: NamespaceName) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();

        let namespace_info = Arc::new(NamespaceInfoSystemTable::new(store.clone(), name.clone()));
        tables.insert(NAMESPACE_INFO_TABLE_NAME, namespace_info);

        let table_info = Arc::new(TableInfoSystemTable::new(store.clone(), name.clone()));
        tables.insert(TABLE_TABLE_NAME, table_info);

        let table_schema = Arc::new(TableSchemaSystemTable::new(store.clone(), name.clone()));
        tables.insert(TABLE_SCHEMA_TABLE_NAME, table_schema);

        Self { name, tables }
    }
}

impl std::fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemSchemaProvider")
            .field("namespace", &self.name)
            .finish()
    }
}

pub fn find_table_name_in_filters(filters: &[Expr], field_name: &str) -> Option<Vec<String>> {
    let filters: Vec<_> = filters
        .iter()
        .flat_map(|filter| find_topic_name_in_filter(filter, field_name))
        .collect();
    if filters.is_empty() {
        None
    } else {
        Some(filters)
    }
}

fn find_topic_name_in_filter(filter: &Expr, field_name: &str) -> Option<String> {
    match filter {
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Eq,
        }) => {
            if left.deref() != &col(field_name) {
                return None;
            }

            match right.deref() {
                Expr::Literal(
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)),
                    _,
                ) => Some(s.to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}
