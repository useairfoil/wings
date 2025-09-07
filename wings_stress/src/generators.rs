//! RecordBatch generators.
use std::ops::RangeInclusive;

use clap::ValueEnum;
use datafusion::common::arrow::datatypes::{DataType, Field, Fields};
use serde_json::{Value, json};
use tpchgen::generators::{OrderGenerator, OrderGeneratorIterator};
use wings_metadata_core::{admin::TopicOptions, partition::PartitionValue};
use wings_push_client::{HttpPushClient, http::PushRequestBuilder};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum TopicType {
    /// A small topic with just a few columns.
    Order,
}

pub struct RequestGenerator {
    _batch_size_range: RangeInclusive<u64>,
    inner: Vec<Box<dyn RecordBatchGenerator>>,
}

pub trait RecordBatchGenerator {
    fn new_batch(&mut self, size: usize) -> (PartitionValue, Vec<Value>);
    fn topic(&self) -> String;
}

impl RequestGenerator {
    pub fn new(
        topics: Vec<TopicType>,
        batch_size_range: RangeInclusive<u64>,
        partitions: u64,
    ) -> Self {
        let inner = topics
            .into_iter()
            .map(|topic| topic.generator(partitions))
            .collect();

        Self {
            _batch_size_range: batch_size_range,
            inner,
        }
    }

    pub fn create_request(&mut self, client: &HttpPushClient) -> PushRequestBuilder {
        let batch_size = 42;

        let mut req = client.push();
        for topic_gen in self.inner.iter_mut() {
            let (partition, batch) = topic_gen.new_batch(batch_size as usize);
            req = req
                .topic(topic_gen.topic())
                .partitioned(partition, batch, None);
        }

        req
    }
}

pub struct OrderRecordBatchGenerator {
    customer_id: i64,
    partitions: i64,
    order_generator_iter: OrderGeneratorIterator<'static>,
}

impl OrderRecordBatchGenerator {
    fn new(partitions: u64) -> Self {
        let generator = OrderGenerator::new(1.0, 1, 1);
        Self {
            customer_id: 1,
            partitions: partitions as _,
            order_generator_iter: generator.iter(),
        }
    }

    fn fields() -> Fields {
        Fields::from(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8View, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8View, false),
            Field::new("o_clerk", DataType::Utf8View, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8View, false),
        ])
    }

    fn partition_key() -> Option<usize> {
        Some(1)
    }

    fn topic_name() -> &'static str {
        "orders"
    }
}

impl RecordBatchGenerator for OrderRecordBatchGenerator {
    fn topic(&self) -> String {
        Self::topic_name().to_string()
    }

    fn new_batch(&mut self, size: usize) -> (PartitionValue, Vec<Value>) {
        let customer_id = self.customer_id;

        let batch: Vec<_> = self
            .order_generator_iter
            .by_ref()
            .take(size)
            .map(|row| {
                json!({
                    "o_orderkey": row.o_orderkey,
                    "o_orderstatus": row.o_orderstatus.to_string(),
                    "o_totalprice": row.o_totalprice.to_string(),
                    "o_orderdate": row.o_orderdate.to_string(),
                    "o_orderpriority": row.o_orderpriority.to_string(),
                    "o_clerk": row.o_clerk.to_string(),
                    "o_shippriority": row.o_shippriority,
                    "o_comment": row.o_comment.to_string()
                })
            })
            .collect();

        let partition_value = PartitionValue::Int64(customer_id);

        self.customer_id += 1;
        if self.customer_id > self.partitions || self.customer_id < 0 {
            self.customer_id = 1;
        }

        (partition_value, batch)
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
                fields: OrderRecordBatchGenerator::fields(),
                partition_key: OrderRecordBatchGenerator::partition_key(),
            },
        }
    }

    pub fn generator(&self, partitions: u64) -> Box<dyn RecordBatchGenerator> {
        match self {
            TopicType::Order => Box::new(OrderRecordBatchGenerator::new(partitions)),
        }
    }
}
