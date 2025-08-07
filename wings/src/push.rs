use clap::Parser;
use serde_json::Value;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use wings_ingestor_http::types::BatchResponse;
use wings_metadata_core::admin::{Admin, NamespaceName, RemoteAdminService, TopicName};

use crate::{
    error::{
        AdminSnafu, CliError, InvalidResourceNameSnafu, IoSnafu, JsonParseSnafu, PushClientSnafu,
        Result,
    },
    helpers::convert_partition_value,
    http_client::{HttpPushClient, PushRequestBuilder},
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

    /// HTTP ingestor address.
    ///
    /// The address where the Wings HTTP ingestor is running. Should match
    /// the address used in the 'wings dev' command.
    #[arg(long, default_value = "http://127.0.0.1:7780")]
    http_address: String,
    #[clap(flatten)]
    remote: RemoteArgs,
}

impl PushArgs {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let admin = self.remote.admin_client().await?;

        let namespace_name =
            NamespaceName::parse(&self.namespace).context(InvalidResourceNameSnafu {
                resource: "namespace",
            })?;

        let client = HttpPushClient::new(&self.http_address, namespace_name.clone());

        let request = self
            .parse_batches_to_request(namespace_name, &admin, &client)
            .await?;

        let response = request.send().await.context(PushClientSnafu {})?;

        for batch in response.batches {
            match batch {
                BatchResponse::Success {
                    start_offset,
                    end_offset,
                } => {
                    println!("SUCCESS {start_offset} - {end_offset}");
                }
                BatchResponse::Error { message } => {
                    println!("ERROR: {message}");
                }
            }
        }

        Ok(())
    }

    async fn parse_batches_to_request(
        &self,
        namespace_name: NamespaceName,
        admin: &RemoteAdminService<Channel>,
        client: &HttpPushClient,
    ) -> Result<PushRequestBuilder> {
        let mut i = 0;

        let mut request = client.push();
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

            let topic = admin.get_topic(topic_name).await.context(AdminSnafu {
                operation: "get_topic",
            })?;

            let partition_column = topic.partition_column();

            let next_arg = &self.batches[i + 1];

            // Check what's the next argument based on whether the topic has a partition column
            let (partition_value, payload_index) = if let Some(partition_column) = partition_column
            {
                if remaining >= 3 {
                    // Next arg is partition value, followed by payload
                    let partition_value =
                        convert_partition_value(next_arg, partition_column.data_type())?;
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
            let messages = self.parse_payload(payload_str)?;

            let topic_request = request.topic(topic_id.clone());
            request = if let Some(partition_value) = partition_value {
                topic_request.partitioned(partition_value, messages)
            } else {
                topic_request.unpartitioned(messages)
            };

            i = payload_index + 1;
        }

        Ok(request)
    }

    fn parse_payload(&self, payload_str: &str) -> Result<Vec<Value>> {
        if let Some(file_path) = payload_str.strip_prefix('@') {
            // File path mode
            let content = std::fs::read_to_string(file_path).context(IoSnafu {})?;

            let mut messages = Vec::new();
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let json: Value = serde_json::from_str(line).context(JsonParseSnafu {})?;

                messages.push(json);
            }

            Ok(messages)
        } else {
            // Direct JSON mode
            let json: Value = serde_json::from_str(payload_str).context(JsonParseSnafu {})?;

            Ok(vec![json])
        }
    }
}
