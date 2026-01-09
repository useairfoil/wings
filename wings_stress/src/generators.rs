//! RecordBatch generators.
use std::{ops::RangeInclusive, sync::Arc, time::SystemTime};

use clap::ValueEnum;
use datafusion::common::arrow::{
    array::{Date32Array, Int32Array, Int64Array, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use tpchgen::generators::{OrderGenerator, OrderGeneratorIterator};
use wings_client::WriteRequest;
use wings_control_plane::{
    resources::{PartitionValue, TopicOptions},
    schema::{Field, Schema},
};

use crate::conversions::{
    decimal128_array_from_iter, string_array_from_display_iter, to_arrow_date32,
};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug, Hash)]
pub enum TopicType {
    /// A small topic with just a few columns.
    Order,
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
    customer_id: i64,
    partitions: i64,
    schema: Schema,
    order_generator_iter: OrderGeneratorIterator<'static>,
}

impl OrderRecordBatchGenerator {
    pub const SCHEMA_ID: u64 = 0;
    const PARTITION_KEY: u64 = 1;

    fn new(partitions: u64) -> Self {
        let generator = OrderGenerator::new(1.0, 1, 1);
        let fields_without_partition_key = Self::fields()
            .into_iter()
            .filter(|field| field.id != Self::PARTITION_KEY)
            .collect::<Vec<_>>();

        let schema = Schema::new(Self::SCHEMA_ID, fields_without_partition_key);

        Self {
            customer_id: 1,
            schema,
            partitions: partitions as _,
            order_generator_iter: generator.iter(),
        }
    }

    fn fields() -> Vec<Field> {
        vec![
            Field::new("o_orderkey", 0, DataType::Int64, false),
            Field::new("o_custkey", Self::PARTITION_KEY, DataType::Int64, false),
            Field::new("o_custkey_check", 2, DataType::Int64, false),
            Field::new("o_orderstatus", 3, DataType::Utf8, false),
            Field::new("o_totalprice", 4, DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", 5, DataType::Date32, false),
            Field::new("o_orderpriority", 6, DataType::Utf8, false),
            Field::new("o_clerk", 7, DataType::Utf8, false),
            Field::new("o_shippriority", 8, DataType::Int32, false),
            Field::new("o_comment", 9, DataType::Utf8, false),
        ]
    }

    fn partition_key() -> Option<u64> {
        Some(Self::PARTITION_KEY)
    }

    fn topic_name() -> &'static str {
        "orders"
    }
}

impl RecordBatchGenerator for OrderRecordBatchGenerator {
    fn new_batch(&mut self, batch_size: usize) -> WriteRequest {
        let customer_id = self.customer_id;

        // For now, generate invalid timestamps every 13th record
        let timestamp = if customer_id % 13 == 0 {
            Some(SystemTime::UNIX_EPOCH)
        } else {
            None
        };

        let partition_value = PartitionValue::Int64(customer_id);

        let rows: Vec<_> = self
            .order_generator_iter
            .by_ref()
            .take(batch_size)
            .collect();

        let batch = if rows.is_empty() {
            RecordBatch::new_empty(self.schema.arrow_schema().into())
        } else {
            let o_orderkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_orderkey));
            let o_custkey_check =
                Int64Array::from_iter_values(std::iter::repeat_n(customer_id, batch_size));
            let o_orderstatus =
                string_array_from_display_iter(rows.iter().map(|r| r.o_orderstatus));
            let o_totalprice = decimal128_array_from_iter(rows.iter().map(|r| r.o_totalprice));
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
                    Arc::new(o_custkey_check),
                    Arc::new(o_orderstatus),
                    Arc::new(o_totalprice),
                    Arc::new(o_orderdate),
                    Arc::new(o_orderpriority),
                    Arc::new(o_clerk),
                    Arc::new(o_shippriority),
                    Arc::new(o_comment),
                ],
            )
            .unwrap()
        };

        self.customer_id += 1;
        if self.customer_id > self.partitions || self.customer_id < 0 {
            self.customer_id = 1;
        }

        WriteRequest {
            data: batch,
            partition_value: Some(partition_value),
            timestamp,
        }
    }
}

impl TopicType {
    pub fn topic_name(&self) -> &str {
        match self {
            TopicType::Order => OrderRecordBatchGenerator::topic_name(),
        }
    }

    pub fn topic_options(&self) -> TopicOptions {
        match self {
            TopicType::Order => TopicOptions {
                schema: Schema::new(
                    OrderRecordBatchGenerator::SCHEMA_ID,
                    OrderRecordBatchGenerator::fields(),
                ),
                partition_key: OrderRecordBatchGenerator::partition_key(),
                description: "TPC-H orders table".to_string().into(),
                compaction: Default::default(),
            },
        }
    }

    pub fn generator(
        &self,
        partitions: u64,
    ) -> Box<dyn RecordBatchGenerator + Send + Sync + 'static> {
        match self {
            TopicType::Order => Box::new(OrderRecordBatchGenerator::new(partitions)),
        }
    }
}
