//! Helpers to access log metadata as streams.

use std::{
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project::pin_project;

use crate::{
    log_metadata::{GetLogLocationRequest, ListPartitionsRequest, LogLocationRequest},
    resources::{PartitionValue, TopicName},
};

use super::{LogLocation, LogMetadata, PartitionMetadata, Result};

pub trait PartitionMetadataPageStream:
    Stream<Item = Result<(TopicName, Vec<PartitionMetadata>)>>
{
}
pub type SendablePartitionMetadataPageStream = Pin<Box<dyn PartitionMetadataPageStream + Send>>;

impl<T> PartitionMetadataPageStream for T where
    T: Stream<Item = Result<(TopicName, Vec<PartitionMetadata>)>>
{
}

pub trait LogLocationStream:
    Stream<Item = Result<(TopicName, Option<PartitionValue>, LogLocation)>>
{
}
pub type SendableLogLocationStream = Pin<Box<dyn LogLocationStream + Send>>;

impl<T> LogLocationStream for T where
    T: Stream<Item = Result<(TopicName, Option<PartitionValue>, LogLocation)>>
{
}

/// A stream yielding partition metadata pages.
#[pin_project]
pub struct PaginatedPartitionMetadataStream {
    #[pin]
    inner: SendablePartitionMetadataPageStream,
}

/// A stream yielding log location pages.
#[pin_project]
pub struct PaginatedLogLocationStream {
    #[pin]
    inner: SendableLogLocationStream,
}

impl PaginatedPartitionMetadataStream {
    pub fn new(
        log_metadata: Arc<dyn LogMetadata>,
        topic_name: TopicName,
        page_size: usize,
    ) -> Self {
        let inner = gen_partition_metadata_stream(log_metadata, topic_name, page_size);
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl PaginatedLogLocationStream {
    pub fn new(
        log_metadata: Arc<dyn LogMetadata>,
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
    ) -> Self {
        let inner = gen_log_location_stream(log_metadata, topic_name, partition_value);
        Self {
            inner: Box::pin(inner),
        }
    }

    pub fn new_in_offset_range(
        log_metadata: Arc<dyn LogMetadata>,
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
        offset_range: RangeInclusive<u64>,
    ) -> Self {
        let inner = gen_log_location_stream_in_range(
            log_metadata,
            topic_name,
            partition_value,
            offset_range,
        );
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for PaginatedPartitionMetadataStream {
    type Item = Result<(TopicName, Vec<PartitionMetadata>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl Stream for PaginatedLogLocationStream {
    type Item = Result<(TopicName, Option<PartitionValue>, LogLocation)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pub fn gen_partition_metadata_stream(
    log_metadata: Arc<dyn LogMetadata>,
    topic_name: TopicName,
    page_size: usize,
) -> impl PartitionMetadataPageStream {
    async_stream::stream! {
        let mut page_token = None;
        loop {
            let response = log_metadata
                .list_partitions(ListPartitionsRequest {
                    topic_name: topic_name.clone(),
                    page_size: page_size.into(),
                    page_token,
                })
                .await?;

            page_token = response.next_page_token;

            yield Ok((topic_name.clone(), response.partitions));

            if page_token.is_none() {
                break;
            }
        }
    }
}

pub fn gen_log_location_stream(
    log_metadata: Arc<dyn LogMetadata>,
    topic_name: TopicName,
    partition_value: Option<PartitionValue>,
) -> impl LogLocationStream {
    gen_log_location_stream_in_range(log_metadata, topic_name, partition_value, 0..=u64::MAX)
}

pub fn gen_log_location_stream_in_range(
    log_metadata: Arc<dyn LogMetadata>,
    topic_name: TopicName,
    partition_value: Option<PartitionValue>,
    offset_range: RangeInclusive<u64>,
) -> impl LogLocationStream {
    async_stream::stream! {
        let mut current_offset = *offset_range.start();
        loop {
            if current_offset > *offset_range.end() {
                break;
            }

            let request = GetLogLocationRequest {
                topic_name: topic_name.clone(),
                partition_value: partition_value.clone(),
                location: LogLocationRequest::Offset(current_offset),
                deadline: None,
            };

            let response = log_metadata
                .get_log_location(request)
                .await?;

            let Some(location) = response else {
                break;
            };

            let Some(end_offset) = location.end_offset() else {
                break;
            };

            current_offset = end_offset.offset + 1;

            yield Ok((topic_name.clone(), partition_value.clone(), location));
        }
    }
}
