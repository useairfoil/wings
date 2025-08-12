use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project::pin_project;

use crate::{
    admin::TopicName, offset_registry::ListTopicPartitionValuesRequest, partition::PartitionValue,
};

use super::{OffsetRegistry, OffsetRegistryError};

pub trait PartitionValuePageStream:
    Stream<Item = Result<(TopicName, Vec<PartitionValue>), OffsetRegistryError>>
{
}
pub type SendablePartitionValuePageStream = Pin<Box<dyn PartitionValuePageStream + Send>>;

impl<T> PartitionValuePageStream for T where
    T: Stream<Item = Result<(TopicName, Vec<PartitionValue>), OffsetRegistryError>>
{
}

#[pin_project]
pub struct PaginatedPartitionValueStream {
    #[pin]
    inner: SendablePartitionValuePageStream,
}

impl PaginatedPartitionValueStream {
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

impl Stream for PaginatedPartitionValueStream {
    type Item = Result<(TopicName, Vec<PartitionValue>), OffsetRegistryError>;

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
                .list_topic_partition_values(ListTopicPartitionValuesRequest {
                    topic_name: topic_name.clone(),
                    page_size: page_size.into(),
                    page_token,
                })
                .await?;

            page_token = response.next_page_token;
            yield Ok((topic_name.clone(), response.values));

            if page_token.is_none() {
                break;
            }
        }
    }
}
