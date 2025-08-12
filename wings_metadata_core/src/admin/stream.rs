use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project::pin_project;
use snafu::ResultExt;

use crate::admin::{ListTopicsRequest, TopicName, error::InvalidResourceNameSnafu};

use super::{Admin, AdminError, NamespaceName, Topic};

pub trait TopicPageStream: Stream<Item = Result<Vec<Topic>, AdminError>> {}
pub type SendableTopicPageStream = Pin<Box<dyn TopicPageStream + Send>>;

impl<T> TopicPageStream for T where T: Stream<Item = Result<Vec<Topic>, AdminError>> {}

#[pin_project]
pub struct PaginatedTopicStream {
    #[pin]
    inner: SendableTopicPageStream,
}

impl PaginatedTopicStream {
    pub fn new(
        admin: Arc<dyn Admin>,
        namespace: NamespaceName,
        page_size: usize,
        filter: Option<Vec<String>>,
    ) -> Self {
        if let Some(topics_filter) = filter {
            PaginatedTopicStream::new_with_filter(admin, namespace, page_size, topics_filter)
        } else {
            PaginatedTopicStream::new_unfiltered(admin, namespace, page_size)
        }
    }

    pub fn new_unfiltered(
        admin: Arc<dyn Admin>,
        namespace: NamespaceName,
        page_size: usize,
    ) -> Self {
        let inner = gen_paginated_topic_stream(admin, namespace, page_size);
        Self {
            inner: Box::pin(inner),
        }
    }

    pub fn new_with_filter(
        admin: Arc<dyn Admin>,
        namespace: NamespaceName,
        page_size: usize,
        filter: Vec<String>,
    ) -> Self {
        let inner = gen_paginated_topic_stream_with_filter(admin, namespace, page_size, filter);
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for PaginatedTopicStream {
    type Item = Result<Vec<Topic>, AdminError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

fn gen_paginated_topic_stream(
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    page_size: usize,
) -> impl TopicPageStream {
    async_stream::stream! {
        let mut page_token = None;
        loop {
            let response = admin
                .list_topics(ListTopicsRequest {
                    parent: namespace.clone(),
                    page_size: page_size.into(),
                    page_token: page_token.clone(),
                })
                .await?;
            page_token = response.next_page_token;
            yield Ok(response.topics);

            if page_token.is_none() {
                break;
            }
        }
    }
}

fn gen_paginated_topic_stream_with_filter(
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    _page_size: usize,
    topics_filter: Vec<String>,
) -> impl TopicPageStream {
    async_stream::stream! {
        for topic_id in topics_filter {
            let topic_name = TopicName::new(topic_id, namespace.clone())
                .context(InvalidResourceNameSnafu { resource: "topic" })?;

            match admin.get_topic(topic_name).await {
                Ok(topic) => {
                    yield Ok(vec![topic]);
                }
                Err(err) => {
                    if !err.is_not_found() {
                        yield Err(err);
                    }
                }
            }
        }
    }
}
