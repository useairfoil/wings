//! This module provides helpers to consume cluster metadata as streams.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project::pin_project;
use wings_resources::{NamespaceName, Table, TableName};

use super::{ClusterMetadata, ClusterMetadataError};
use crate::cluster_metadata::ListTablesRequest;

pub trait TablePageStream: Stream<Item = Result<Vec<Table>, ClusterMetadataError>> {}
pub type SendableTablePageStream = Pin<Box<dyn TablePageStream + Send>>;

impl<T> TablePageStream for T where T: Stream<Item = Result<Vec<Table>, ClusterMetadataError>> {}

#[pin_project]
pub struct PaginatedTableStream {
    #[pin]
    inner: SendableTablePageStream,
}

impl PaginatedTableStream {
    pub fn new(
        cluster_metadata: Arc<dyn ClusterMetadata>,
        namespace: NamespaceName,
        page_size: usize,
        filter: Option<Vec<String>>,
    ) -> Self {
        if let Some(tables_filter) = filter {
            PaginatedTableStream::new_with_filter(
                cluster_metadata,
                namespace,
                page_size,
                tables_filter,
            )
        } else {
            PaginatedTableStream::new_unfiltered(cluster_metadata, namespace, page_size)
        }
    }

    pub fn new_unfiltered(
        cluster_metadata: Arc<dyn ClusterMetadata>,
        namespace: NamespaceName,
        page_size: usize,
    ) -> Self {
        let inner = gen_paginated_table_stream(cluster_metadata, namespace, page_size);
        Self {
            inner: Box::pin(inner),
        }
    }

    pub fn new_with_filter(
        cluster_metadata: Arc<dyn ClusterMetadata>,
        namespace: NamespaceName,
        page_size: usize,
        filter: Vec<String>,
    ) -> Self {
        let inner =
            gen_paginated_table_stream_with_filter(cluster_metadata, namespace, page_size, filter);
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for PaginatedTableStream {
    type Item = Result<Vec<Table>, ClusterMetadataError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

fn gen_paginated_table_stream(
    cluster_metadata: Arc<dyn ClusterMetadata>,
    namespace: NamespaceName,
    page_size: usize,
) -> impl TablePageStream {
    async_stream::stream! {
        let mut page_token = None;
        loop {
            let response = cluster_metadata
                .list_tables(ListTablesRequest {
                    parent: namespace.clone(),
                    page_size: page_size.into(),
                    page_token: page_token.clone(),
                })
                .await?;
            page_token = response.next_page_token;
            yield Ok(response.tables);

            if page_token.is_none() {
                break;
            }
        }
    }
}

fn gen_paginated_table_stream_with_filter(
    cluster_metadata: Arc<dyn ClusterMetadata>,
    namespace: NamespaceName,
    _page_size: usize,
    tables_filter: Vec<String>,
) -> impl TablePageStream {
    async_stream::stream! {
        for table_id in tables_filter {
            let table_name = TableName::new(table_id, namespace.clone())
                .map_err(|err| ClusterMetadataError::InvalidResourceName { resource: "table".to_string(), message: err.to_string() })?;

            match cluster_metadata.get_table(table_name, Default::default()).await {
                Ok(table) => {
                    yield Ok(vec![table]);
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
