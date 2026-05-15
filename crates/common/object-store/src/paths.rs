use percent_encoding::utf8_percent_encode;
use wings_resources::{NamespaceName, PartitionValue, TableName};

pub fn format_folio_path(namespace: &NamespaceName, folio_id: &str) -> String {
    format!("{}/folio/{}.folio", namespace, folio_id)
}

pub fn format_parquet_data_path(table: &TableName, file_id: &str) -> String {
    format!("{}/data/{}.parquet", table, file_id)
}

pub fn format_partitioned_parquet_data_path(
    table: &TableName,
    field_name: &str,
    partition: &Option<PartitionValue>,
    file_id: &str,
) -> String {
    use percent_encoding::NON_ALPHANUMERIC;

    let partition_value = match partition {
        None => "null".to_string(),
        Some(partition) => {
            utf8_percent_encode(&partition.to_string(), NON_ALPHANUMERIC).to_string()
        }
    };

    format!(
        "{}/data/{}={}/{}.parquet",
        table, field_name, partition_value, file_id
    )
}
