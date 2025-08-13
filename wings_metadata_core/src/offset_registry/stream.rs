use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};

use futures::Stream;
use pin_project::pin_project;

use crate::{
    admin::TopicName, offset_registry::ListTopicPartitionStatesRequest, partition::PartitionValue,
};

use super::{OffsetLocation, OffsetRegistry, OffsetRegistryError, PartitionValueState};

pub trait PartitionValuePageStream:
    Stream<Item = Result<(TopicName, Vec<PartitionValueState>), OffsetRegistryError>>
{
}
pub type SendablePartitionStatePageStream = Pin<Box<dyn PartitionValuePageStream + Send>>;

impl<T> PartitionValuePageStream for T where
    T: Stream<Item = Result<(TopicName, Vec<PartitionValueState>), OffsetRegistryError>>
{
}

pub trait OffsetLocationStream:
    Stream<Item = Result<(TopicName, Option<PartitionValue>, OffsetLocation), OffsetRegistryError>>
{
}
pub type SendableOffsetLocationStream = Pin<Box<dyn OffsetLocationStream + Send>>;

impl<T> OffsetLocationStream for T where
    T: Stream<
        Item = Result<(TopicName, Option<PartitionValue>, OffsetLocation), OffsetRegistryError>,
    >
{
}

#[pin_project]
pub struct PaginatedPartitionStateStream {
    #[pin]
    inner: SendablePartitionStatePageStream,
}

#[pin_project]
pub struct PaginatedOffsetLocationStream {
    #[pin]
    inner: SendableOffsetLocationStream,
}

impl PaginatedPartitionStateStream {
    pub fn new(
        offset_registry: Arc<dyn OffsetRegistry>,
        topic_name: TopicName,
        page_size: usize,
    ) -> Self {
        let inner = gen_partition_value_stream(offset_registry, topic_name, page_size);
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl PaginatedOffsetLocationStream {
    pub fn new(
        offset_registry: Arc<dyn OffsetRegistry>,
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
    ) -> Self {
        let inner = gen_offset_location_stream(offset_registry, topic_name, partition_value);
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for PaginatedPartitionStateStream {
    type Item = Result<(TopicName, Vec<PartitionValueState>), OffsetRegistryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl Stream for PaginatedOffsetLocationStream {
    type Item = Result<(TopicName, Option<PartitionValue>, OffsetLocation), OffsetRegistryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pub fn gen_partition_value_stream(
    offset_registry: Arc<dyn OffsetRegistry>,
    topic_name: TopicName,
    page_size: usize,
) -> impl PartitionValuePageStream {
    async_stream::stream! {
        let mut page_token = None;
        loop {
            let response = offset_registry
                .list_topic_partition_states(ListTopicPartitionStatesRequest {
                    topic_name: topic_name.clone(),
                    page_size: page_size.into(),
                    page_token,
                })
                .await?;

            page_token = response.next_page_token;
            yield Ok((topic_name.clone(), response.states));

            if page_token.is_none() {
                break;
            }
        }
    }
}

pub fn gen_offset_location_stream(
    offset_registry: Arc<dyn OffsetRegistry>,
    topic_name: TopicName,
    partition_value: Option<PartitionValue>,
) -> impl OffsetLocationStream {
    async_stream::stream! {
        let mut current_offset = 0;
        loop {
            let response = offset_registry
                .offset_location(
                    topic_name.clone(),
                    partition_value.clone(),
                    current_offset,
                    SystemTime::now(),
                )
                .await?;

            let Some(location) = response else {
                break;
            };

            current_offset = location.end_offset() + 1;

            yield Ok((topic_name.clone(), partition_value.clone(), location));
        }
    }
}
