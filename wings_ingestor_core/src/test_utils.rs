use std::{sync::Arc, time::SystemTime};

use bytesize::ByteSize;
use datafusion::common::arrow::{
    array::{Int32Array, UInt64Array},
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use wings_resources::{
    DataLakeName, Namespace, NamespaceName, NamespaceOptions, NamespaceRef, ObjectStoreName,
    PartitionValue, TenantName, Topic, TopicName, TopicOptions, TopicRef,
};
use wings_schema::{DataType as WingsDataType, Field as WingsField, SchemaBuilder};

use crate::request::WriteBatchRequest;

/// Generate a test batch with the specified number of rows.
/// The batch contains:
/// - `id`: row index (0, 1, 2, ...)
/// - `batch_size`: constant value equal to num_rows
pub fn generate_test_batch(num_rows: usize) -> RecordBatch {
    generate_test_batch_with_schema(0, num_rows, schema())
}

pub fn generate_test_batch_with_schema(
    start_index: i32,
    num_rows: usize,
    schema: Arc<Schema>,
) -> RecordBatch {
    let ids: Vec<i32> = (start_index..(start_index + num_rows as i32)).collect();
    let batch_sizes: Vec<u64> = vec![num_rows as u64; num_rows];

    let id_array = Arc::new(Int32Array::from(ids));
    let batch_size_array = Arc::new(UInt64Array::from(batch_sizes));

    RecordBatch::try_new(schema, vec![id_array, batch_size_array])
        .expect("Failed to create test batch")
}

pub fn fields() -> Fields {
    vec![
        Field::new("id", DataType::Int32, false),
        Field::new("batch_size", DataType::UInt64, false),
    ]
    .into()
}

/// Returns the schema for test data.
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(fields()))
}

pub fn test_tenant_name() -> TenantName {
    TenantName::new_unchecked("test-tenant")
}

pub fn test_namespace_name() -> NamespaceName {
    NamespaceName::new_unchecked("test-namespace", test_tenant_name())
}

pub fn test_topic_name() -> TopicName {
    TopicName::new_unchecked("test-topic", test_namespace_name())
}

pub fn test_namespace() -> NamespaceRef {
    test_namespace_with_flush_size(ByteSize::mb(8))
}

pub fn test_namespace_with_flush_size(flush_size: ByteSize) -> NamespaceRef {
    let tenant_name = test_tenant_name();
    let object_store_name =
        ObjectStoreName::new_unchecked("test-object-store", tenant_name.clone());
    let data_lake_name = DataLakeName::new_unchecked("test-data-lake", tenant_name);

    Arc::new(Namespace::new(
        test_namespace_name(),
        NamespaceOptions::new(object_store_name, data_lake_name).with_flush_size(flush_size),
    ))
}

pub fn test_topic() -> TopicRef {
    let schema = SchemaBuilder::new(vec![
        WingsField::new("id", 1, WingsDataType::Int32, false),
        WingsField::new("batch_size", 2, WingsDataType::UInt64, false),
    ])
    .build()
    .expect("failed to create test topic schema");

    Arc::new(Topic::new(test_topic_name(), TopicOptions::new(schema)))
}

pub fn generate_write_request(
    records: RecordBatch,
    timestamp: Option<SystemTime>,
) -> WriteBatchRequest {
    generate_write_request_for(test_topic(), None, records, timestamp)
}

pub fn generate_write_request_for(
    topic: TopicRef,
    partition: Option<PartitionValue>,
    records: RecordBatch,
    timestamp: Option<SystemTime>,
) -> WriteBatchRequest {
    WriteBatchRequest {
        topic,
        partition,
        records,
        timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_test_batch() {
        let batch = generate_test_batch(5);
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);

        let id_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.values(), &[0, 1, 2, 3, 4]);

        let batch_size_column = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(batch_size_column.values(), &[5, 5, 5, 5, 5]);
    }

    #[test]
    fn test_schema() {
        let schema = schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "batch_size");
        assert_eq!(schema.field(1).data_type(), &DataType::UInt64);
    }
}
