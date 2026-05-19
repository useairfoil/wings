//! Helpers to access table metadata as streams.

use std::{
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project::pin_project;
use wings_resources::{PartitionValue, TableName};

use super::{GetTableLocationOptions, TableLocation, TableMetadata, PartitionMetadata, Result};
use crate::table_metadata::{GetTableLocationRequest, ListPartitionsRequest};

pub trait PartitionMetadataPageStream:
    Stream<Item = Result<(TableName, Vec<PartitionMetadata>)>>
{
}
pub type SendablePartitionMetadataPageStream = Pin<Box<dyn PartitionMetadataPageStream + Send>>;

impl<T> PartitionMetadataPageStream for T where
    T: Stream<Item = Result<(TableName, Vec<PartitionMetadata>)>>
{
}

pub trait TableLocationStream:
    Stream<Item = Result<(TableName, Option<PartitionValue>, TableLocation)>>
{
}
pub type SendableTableLocationStream = Pin<Box<dyn TableLocationStream + Send>>;

impl<T> TableLocationStream for T where
    T: Stream<Item = Result<(TableName, Option<PartitionValue>, TableLocation)>>
{
}

/// A stream yielding partition metadata pages.
#[pin_project]
pub struct PaginatedPartitionMetadataStream {
    #[pin]
    inner: SendablePartitionMetadataPageStream,
}

/// A stream yielding table location pages.
#[pin_project]
pub struct PaginatedTableLocationStream {
    #[pin]
    inner: SendableTableLocationStream,
}

impl PaginatedPartitionMetadataStream {
    pub fn new(
        table_metadata: Arc<dyn TableMetadata>,
        table_name: TableName,
        page_size: usize,
    ) -> Self {
        let inner = gen_partition_metadata_stream(table_metadata, table_name, page_size);
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl PaginatedTableLocationStream {
    pub fn new(
        table_metadata: Arc<dyn TableMetadata>,
        table_name: TableName,
        partition_value: Option<PartitionValue>,
        options: GetTableLocationOptions,
    ) -> Self {
        let inner = gen_table_location_stream(table_metadata, table_name, partition_value, options);
        Self {
            inner: Box::pin(inner),
        }
    }

    pub fn new_in_seqnum_range(
        table_metadata: Arc<dyn TableMetadata>,
        table_name: TableName,
        partition_value: Option<PartitionValue>,
        seqnum_range: RangeInclusive<u64>,
        options: GetTableLocationOptions,
    ) -> Self {
        let inner = gen_table_location_stream_in_range(
            table_metadata,
            table_name,
            partition_value,
            seqnum_range,
            options,
        );
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for PaginatedPartitionMetadataStream {
    type Item = Result<(TableName, Vec<PartitionMetadata>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl Stream for PaginatedTableLocationStream {
    type Item = Result<(TableName, Option<PartitionValue>, TableLocation)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pub fn gen_partition_metadata_stream(
    table_metadata: Arc<dyn TableMetadata>,
    table_name: TableName,
    page_size: usize,
) -> impl PartitionMetadataPageStream {
    async_stream::stream! {
        let mut page_token = None;
        loop {
            let response = table_metadata
                .list_partitions(ListPartitionsRequest {
                    table_name: table_name.clone(),
                    page_size: page_size.into(),
                    page_token,
                })
                .await?;

            page_token = response.next_page_token;

            yield Ok((table_name.clone(), response.partitions));

            if page_token.is_none() {
                break;
            }
        }
    }
}

pub fn gen_table_location_stream(
    table_metadata: Arc<dyn TableMetadata>,
    table_name: TableName,
    partition_value: Option<PartitionValue>,
    options: GetTableLocationOptions,
) -> impl TableLocationStream {
    gen_table_location_stream_in_range(
        table_metadata,
        table_name,
        partition_value,
        0..=u64::MAX,
        options,
    )
}

pub fn gen_table_location_stream_in_range(
    table_metadata: Arc<dyn TableMetadata>,
    table_name: TableName,
    partition_value: Option<PartitionValue>,
    seqnum_range: RangeInclusive<u64>,
    options: GetTableLocationOptions,
) -> impl TableLocationStream {
    async_stream::stream! {
        let mut current_seqnum = *seqnum_range.start();
        'outer: loop {
            if current_seqnum > *seqnum_range.end() {
                break;
            }

            let request = GetTableLocationRequest {
                table_name: table_name.clone(),
                partition_value: partition_value.clone(),
                seqnum: current_seqnum,
                options: options.clone(),
            };

            let response = table_metadata
                .get_table_location(request)
                .await?;

            if response.is_empty() {
                break;
            }

            for location in response {
                let Some(end_seqnum) = location.end_seqnum() else {
                    break 'outer;
                };

                current_seqnum = end_seqnum.seqnum + 1;

                yield Ok((table_name.clone(), partition_value.clone(), location));
            }
        }
    }
}
