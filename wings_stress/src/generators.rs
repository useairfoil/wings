//! RecordBatch generators.
use std::{ops::RangeInclusive, sync::Arc};

use clap::ValueEnum;
use datafusion::common::arrow::{
    array::{Date32Array, Int32Array, Int64Array, StringViewArray},
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use tpchgen::generators::{OrderGenerator, OrderGeneratorIterator};
use wings_client::WriteRequest;
use wings_control_plane::resources::{PartitionValue, TopicOptions};

use crate::conversions::{
    decimal128_array_from_iter, string_view_array_from_display_iter, to_arrow_date32,
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
    fn new_batch(&mut self, size: usize) -> (Option<PartitionValue>, RecordBatch);
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
        let batch_size = 42;
        let (partition_value, data) = self.inner.new_batch(batch_size);
        WriteRequest {
            data,
            partition_value,
            timestamp: None,
        }
    }
}

pub struct OrderRecordBatchGenerator {
    customer_id: i64,
    partitions: i64,
    schema: Arc<Schema>,
    order_generator_iter: OrderGeneratorIterator<'static>,
}

impl OrderRecordBatchGenerator {
    fn new(partitions: u64) -> Self {
        let generator = OrderGenerator::new(1.0, 1, 1);
        let schema = Arc::new(Schema::new(Self::fields()));
        Self {
            customer_id: 1,
            schema,
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
        // Some(1)
        None
    }

    fn topic_name() -> &'static str {
        "orders"
    }
}

impl RecordBatchGenerator for OrderRecordBatchGenerator {
    fn new_batch(&mut self, batch_size: usize) -> (Option<PartitionValue>, RecordBatch) {
        let customer_id = self.customer_id;

        let _partition_value = PartitionValue::Int64(customer_id);

        let rows: Vec<_> = self
            .order_generator_iter
            .by_ref()
            .take(batch_size)
            .collect();

        let batch = if rows.is_empty() {
            RecordBatch::new_empty(self.schema.clone())
        } else {
            let o_orderkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_orderkey));
            let o_custkey = Int64Array::from_iter_values(rows.iter().map(|r| r.o_custkey));
            let o_orderstatus =
                string_view_array_from_display_iter(rows.iter().map(|r| r.o_orderstatus));
            let o_totalprice = decimal128_array_from_iter(rows.iter().map(|r| r.o_totalprice));
            let o_orderdate = Date32Array::from_iter_values(
                rows.iter().map(|r| r.o_orderdate).map(to_arrow_date32),
            );
            let o_orderpriority =
                StringViewArray::from_iter_values(rows.iter().map(|r| r.o_orderpriority));
            let o_clerk = string_view_array_from_display_iter(rows.iter().map(|r| r.o_clerk));
            let o_shippriority =
                Int32Array::from_iter_values(rows.iter().map(|r| r.o_shippriority));
            let o_comment = StringViewArray::from_iter_values(rows.iter().map(|r| r.o_comment));

            RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(o_orderkey),
                    Arc::new(o_custkey),
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

        // (Some(partition_value), batch)
        (None, batch)
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

    pub fn generator(
        &self,
        partitions: u64,
    ) -> Box<dyn RecordBatchGenerator + Send + Sync + 'static> {
        match self {
            TopicType::Order => Box::new(OrderRecordBatchGenerator::new(partitions)),
        }
    }
}
