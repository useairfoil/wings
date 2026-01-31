use std::{collections::HashMap, time::SystemTime};

use arrow::compute::concat_batches;
use arrow_json::ReaderBuilder;
use arrow_schema::SchemaRef;
use clap::Parser;
use datafusion::common::arrow::record_batch::RecordBatch;
use futures::{TryStreamExt, stream::FuturesOrdered};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use wings_client::WriteRequest;
use wings_control_plane_core::{
    cluster_metadata::{ClusterMetadata, TopicView, tonic::ClusterMetadataClient},
    log_metadata::CommittedBatch,
};
use wings_resources::{NamespaceName, PartitionValue, TopicName};

use crate::{
    error::{
        ArrowSnafu, CliError, ClientSnafu, ClusterMetadataSnafu, InvalidResourceNameSnafu,
        InvalidTimestampFormatSnafu, IoSnafu, PartitionValueParseSnafu, Result,
    },
    remote::RemoteArgs,
};

/// Push messages to Wings topics
#[derive(Parser)]
pub struct PushArgs {
    /// Namespace name
    namespace: String,

    /// Batches to push in the format: <topic_id> [<partition_value>] <payload>
    ///
    /// - topic_id: required, used to construct the topic name by joining with namespace
    /// - partition_value: optional, value for the partition key
    /// - payload: required, JSON payload or @file_path for file containing JSON messages
    batches: Vec<String>,

    /// Assign this timestamp to the batches.
    #[arg(long)]
    timestamp: Option<String>,
    #[clap(flatten)]
    remote: RemoteArgs,
}

impl PushArgs {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let cluster_meta = self.remote.cluster_metadata_client().await?;
        let client = self.remote.wings_client().await?;

        let namespace_name =
            NamespaceName::parse(&self.namespace).context(InvalidResourceNameSnafu {
                resource: "namespace",
            })?;

        let timestamp = self
            .timestamp
            .as_ref()
            .map(|ts| chrono::DateTime::parse_from_rfc3339(ts))
            .transpose()
            .context(InvalidTimestampFormatSnafu {})?
            .map(SystemTime::from);

        let requests_by_topic = self
            .parse_batches_to_request(namespace_name, timestamp, &cluster_meta)
            .await?;

        for (topic_name, requests) in requests_by_topic.into_iter() {
            println!("Pushing data to topic {}", topic_name);
            let topic_client = client
                .push_client(topic_name)
                .await
                .context(ClientSnafu {})?;
            let mut futures = FuturesOrdered::new();

            for request in requests {
                let response_fut = topic_client.push(request).await.context(ClientSnafu {})?;
                futures.push_back(response_fut.wait_for_response());
            }

            while let Some(response) = futures.try_next().await.context(ClientSnafu {})? {
                match response {
                    CommittedBatch::Accepted(info) => println!("Accepted: {:?}", info),
                    CommittedBatch::Rejected(info) => println!("Rejected: {:?}", info),
                }
            }
        }

        Ok(())
    }

    async fn parse_batches_to_request(
        &self,
        namespace_name: NamespaceName,
        timestamp: Option<SystemTime>,
        cluster_meta: &ClusterMetadataClient<Channel>,
    ) -> Result<HashMap<TopicName, Vec<WriteRequest>>> {
        let mut i = 0;

        let mut requests = HashMap::<TopicName, Vec<WriteRequest>>::new();

        while i < self.batches.len() {
            let remaining = self.batches.len() - i;

            if remaining < 2 {
                return Err(CliError::InvalidArgument {
                    name: "batch",
                    message: "Each batch requires at least topic_id and payload".to_string(),
                });
            }

            let topic_id = &self.batches[i];
            let topic_name = TopicName::new(topic_id, namespace_name.clone())
                .context(InvalidResourceNameSnafu { resource: "topic" })?;

            let topic = cluster_meta
                .get_topic(topic_name, TopicView::Basic)
                .await
                .context(ClusterMetadataSnafu {
                    operation: "get_topic",
                })?;

            let partition_field = topic.partition_field();

            let next_arg = &self.batches[i + 1];

            // Check what's the next argument based on whether the topic has a partition column
            let (partition_value, payload_index) = if let Some(partition_column) = partition_field {
                if remaining >= 3 {
                    // Next arg is partition value, followed by payload
                    let partition_value =
                        PartitionValue::parse_with_datatype(&partition_column.data_type, next_arg)
                            .context(PartitionValueParseSnafu {})?;
                    (Some(partition_value), i + 2)
                } else {
                    return Err(CliError::InvalidArgument {
                        name: "batch",
                        message: "Missing payload after partition value".to_string(),
                    });
                }
            } else {
                (None, i + 1)
            };

            if payload_index >= self.batches.len() {
                return Err(CliError::InvalidArgument {
                    name: "batch",
                    message: "Missing payload".to_string(),
                });
            }

            let payload_str = &self.batches[payload_index];
            let data =
                self.parse_payload(payload_str, topic.arrow_schema_without_partition_field())?;

            let topic = TopicName::new(topic_id, namespace_name.clone())
                .context(InvalidResourceNameSnafu { resource: "topic" })?;

            requests.entry(topic).or_default().push(WriteRequest {
                partition_value,
                timestamp,
                data,
            });

            i = payload_index + 1;
        }

        Ok(requests)
    }

    fn parse_payload(&self, payload_str: &str, schema: SchemaRef) -> Result<RecordBatch> {
        let cursor = if let Some(file_path) = payload_str.strip_prefix('@') {
            // File path mode
            let content = std::fs::read_to_string(file_path)
                .context(IoSnafu {})?
                .into_bytes();
            std::io::Cursor::new(content)
        } else {
            // Direct JSON mode
            let content = payload_str.to_string().into_bytes();
            std::io::Cursor::new(content)
        };

        let reader = ReaderBuilder::new(schema.clone())
            .build(cursor)
            .context(ArrowSnafu {})?;
        let batches = reader
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .context(ArrowSnafu {})?;
        concat_batches(&schema, batches.as_slice()).context(ArrowSnafu {})
    }
}
