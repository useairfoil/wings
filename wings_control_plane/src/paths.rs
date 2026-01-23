use crate::resources::{NamespaceName, TopicName};

pub fn format_folio_path(namespace: &NamespaceName, folio_id: &str) -> String {
    format!("{}/folio/{}.folio", namespace, folio_id)
}

pub fn format_parquet_data_path(topic: &TopicName, file_id: &str) -> String {
    format!("{}/data/{}.parquet", topic, file_id)
}
