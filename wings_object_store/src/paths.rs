use wings_resources::{NamespaceName, PartitionValue, TopicName};

pub fn format_folio_path(namespace: &NamespaceName, folio_id: &str) -> String {
    format!("{}/folio/{}.folio", namespace, folio_id)
}

pub fn format_parquet_data_path(topic: &TopicName, file_id: &str) -> String {
    format!("{}/data/{}.parquet", topic, file_id)
}

pub fn format_partitioned_parquet_data_path(
    topic: &TopicName,
    field_name: &str,
    partition: &Option<PartitionValue>,
    file_id: &str,
) -> String {
    let partition_value = match partition {
        None => "null".to_string(),
        Some(partition) => partition.to_string(),
    };

    format!(
        "{}/data/{}={}/{}.parquet",
        topic, field_name, partition_value, file_id
    )
}
