//! RecordBatch generators.
use std::{ops::RangeInclusive, sync::Arc, time::Duration};

use clap::ValueEnum;
use datafusion::common::arrow::{
    array::{Date32Array, Int32Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use tpchgen::generators::{
    CustomerGenerator, CustomerGeneratorIterator, OrderGenerator, OrderGeneratorIterator,
};
use wings_client::WriteRequest;
use wings_resources::{CompactionConfiguration, PartitionValue, TopicOptions};
use wings_schema::{DataType, Field, Schema, SchemaBuilder, schema_without_partition_field};

use crate::conversions::{string_array_from_display_iter, to_arrow_date32};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug, Hash)]
pub enum TopicType {
    /// Orders.
    Order,
    /// Customers.
    Customer,
}

pub struct RequestGenerator {
    _batch_size_range: RangeInclusive<u64>,
    inner: Box<dyn RecordBatchGenerator + Send + Sync + 'static>,
}

pub trait RecordBatchGenerator {
    fn new_batch(&mut self, size: usize) -> WriteRequest;
}

impl RequestGenerator {
    pub fn new(topic: TopicType, batch_size_range: RangeInclusive<u64>, partitions: u64) -> Self {
        let inner = topic.generator(partitions);

        Self {
            _batch_size_range: batch_size_range,
            inner,
        }
    }

    pub fn create_request(&mut self) -> WriteRequest {
        // random batch size
        let batch_size = 420;
        self.inner.new_batch(batch_size)
    }
}

pub struct OrderRecordBatchGenerator {
    schema: Schema,
    order_generator_iter: OrderGeneratorIterator<'static>,
}

pub struct CustomerRecordBatchGenerator {
    nationkey: i64,
    partitions: i64,
    schema: Schema,
    customer_generator_iter: CustomerGeneratorIterator<'static>,
}

impl OrderRecordBatchGenerator {
    fn new(_partitions: u64) -> Self {
        let generator = OrderGenerator::new(1.0, 1, 1);

        let schema = SchemaBuilder::new(Self::fields())
            .build()
            .expect("valid order schema");

        Self {
            schema,
            order_generator_iter: generator.iter(),
        }
    }

    fn fields() -> Vec<Field> {
        vec![
            Field::new("o_orderkey", 0, DataType::Int64, false),
            Field::new("o_custkey", 1, DataType::Int64, false),
            Field::new("o_orderstatus", 2, DataType::Utf8, false),
            Field::new("o_orderdate", 3, DataType::Date32, false),
            Field::new("o_orderpriority", 4, DataType::Utf8, false),
            Field::new("o_clerk", 5, DataType::Utf8, false),
            Field::new("o_shippriority", 6, DataType::Int32, false),
            Field::new("o_comment", 7, DataType::Utf8, false),
        ]
    }

    fn partition_key() -> Option<u64> {
        None
    }

    fn topic_name() -> &'static str {
        "orders"
    }
}

impl CustomerRecordBatchGenerator {
    fn new(partitions: u64) -> Self {
        let generator = CustomerGenerator::new(1.0, 1, 1);

        let schema = SchemaBuilder::new(Self::fields())
            .build()
            .expect("valid customer schema");
        let schema = schema_without_partition_field(&schema, Self::partition_key());

        Self {
            nationkey: 1,
            schema,
            partitions: partitions as _,
            customer_generator_iter: generator.iter(),
        }
    }

    fn fields() -> Vec<Field> {
        vec![
            Field::new("c_custkey", 1, DataType::Int64, false),
            Field::new("c_name", 2, DataType::Utf8, false),
            Field::new("c_address", 3, DataType::Utf8, false),
            Field::new("c_nationkey", 4, DataType::Int64, false),
            Field::new("c_phone", 5, DataType::Utf8, false),
            Field::new("c_mktsegment", 6, DataType::Utf8, false),
            Field::new("c_comment", 7, DataType::Utf8, false),
        ]
    }

    fn partition_key() -> Option<u64> {
        Some(4) // c_nationkey
    }

    fn topic_name() -> &'static str {
        "customers"
    }
}

impl RecordBatchGenerator for OrderRecordBatchGenerator {
    fn new_batch(&mut self, batch_size: usize) -> WriteRequest {
        let rows: Vec<_> = self
            .order_generator_iter
            .by_ref()
            .take(batch_size)
            .collect();

        let batch = if rows.is_empty() {
            RecordBatch::new_empty(self.schema.arrow_schema().into())
        } else {
            let o_orderkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_orderkey));
            let o_custkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_custkey));
            let o_orderstatus =
                string_array_from_display_iter(rows.iter().map(|r| r.o_orderstatus));
            let o_orderdate = Date32Array::from_iter_values(
                rows.iter().map(|r| r.o_orderdate).map(to_arrow_date32),
            );
            let o_orderpriority =
                StringArray::from_iter_values(rows.iter().map(|r| r.o_orderpriority));
            let o_clerk = string_array_from_display_iter(rows.iter().map(|r| r.o_clerk));
            let o_shippriority =
                Int32Array::from_iter_values(rows.iter().map(|r| r.o_shippriority));
            let o_comment = StringArray::from_iter_values(rows.iter().map(|r| r.o_comment));

            RecordBatch::try_new(
                self.schema.arrow_schema().into(),
                vec![
                    Arc::new(o_orderkey),
                    Arc::new(o_custkey),
                    Arc::new(o_orderstatus),
                    Arc::new(o_orderdate),
                    Arc::new(o_orderpriority),
                    Arc::new(o_clerk),
                    Arc::new(o_shippriority),
                    Arc::new(o_comment),
                ],
            )
            .unwrap()
        };

        WriteRequest {
            data: batch,
            partition_value: None,
            timestamp: None,
        }
    }
}

impl RecordBatchGenerator for CustomerRecordBatchGenerator {
    fn new_batch(&mut self, batch_size: usize) -> WriteRequest {
        let nationkey = self.nationkey;

        let rows: Vec<_> = self
            .customer_generator_iter
            .by_ref()
            .take(batch_size)
            .collect();

        let batch = if rows.is_empty() {
            RecordBatch::new_empty(self.schema.arrow_schema().into())
        } else {
            let c_custkey = Int64Array::from_iter_values(rows.iter().map(|r| r.c_custkey));
            let c_name = string_array_from_display_iter(rows.iter().map(|r| r.c_name));
            let c_address =
                string_array_from_display_iter(rows.iter().map(|r| r.c_address.clone()));
            let c_phone = string_array_from_display_iter(rows.iter().map(|r| r.c_phone.clone()));
            let c_mktsegment = string_array_from_display_iter(rows.iter().map(|r| r.c_mktsegment));
            let c_comment = string_array_from_display_iter(rows.iter().map(|r| r.c_comment));

            RecordBatch::try_new(
                self.schema.arrow_schema().into(),
                vec![
                    Arc::new(c_custkey),
                    Arc::new(c_name),
                    Arc::new(c_address),
                    Arc::new(c_phone),
                    Arc::new(c_mktsegment),
                    Arc::new(c_comment),
                ],
            )
            .unwrap()
        };

        self.nationkey += 1;
        if self.nationkey > self.partitions || self.nationkey < 0 {
            self.nationkey = 1;
        }

        WriteRequest {
            data: batch,
            partition_value: PartitionValue::Int64(nationkey).into(),
            timestamp: None,
        }
    }
}

impl TopicType {
    pub fn topic_name(&self) -> &str {
        match self {
            TopicType::Order => OrderRecordBatchGenerator::topic_name(),
            TopicType::Customer => CustomerRecordBatchGenerator::topic_name(),
        }
    }

    pub fn topic_options(&self) -> TopicOptions {
        match self {
            TopicType::Order => TopicOptions {
                schema: SchemaBuilder::new(OrderRecordBatchGenerator::fields())
                    .build()
                    .expect("valid order schema"),
                partition_key: OrderRecordBatchGenerator::partition_key(),
                description: "TPC-H orders table".to_string().into(),
                compaction: CompactionConfiguration {
                    freshness: Duration::from_mins(1),
                    ..Default::default()
                },
            },
            TopicType::Customer => TopicOptions {
                schema: SchemaBuilder::new(CustomerRecordBatchGenerator::fields())
                    .build()
                    .expect("valid customer schema"),
                partition_key: CustomerRecordBatchGenerator::partition_key(),
                description: "TPC-H customers table".to_string().into(),
                compaction: CompactionConfiguration {
                    freshness: Duration::from_mins(1),
                    ..Default::default()
                },
            },
        }
    }

    pub fn generator(
        &self,
        partitions: u64,
    ) -> Box<dyn RecordBatchGenerator + Send + Sync + 'static> {
        match self {
            TopicType::Order => Box::new(OrderRecordBatchGenerator::new(partitions)),
            TopicType::Customer => Box::new(CustomerRecordBatchGenerator::new(partitions)),
        }
    }
}
