use arrow::util::pretty::print_batches;
use clap::Parser;
use error_stack::{ResultExt, report};
use tokio_util::sync::CancellationToken;
use wings_ingestor_http::push::parse_json_to_arrow;
use wings_metadata_core::admin::{Admin, NamespaceName, Topic, TopicName};
use wings_server_http::fetch::{FetchRequest, FetchResponse, TopicRequest, TopicResponse};

use crate::{
    error::{CliError, CliResult},
    helpers::convert_partition_value,
    remote::RemoteArgs,
};

#[derive(Parser)]
pub struct FetchArgs {
    /// The name of the topic
    #[arg(long)]
    topic: String,

    /// The partition value if the topic is partitioned
    #[arg(long)]
    partition_value: Option<String>,

    /// The offset to start fetching from
    #[arg(long)]
    offset: u64,

    /// The maximum time to wait for data to be available (milliseconds)
    #[arg(long)]
    timeout: Option<u64>,

    /// Maximum number of messages to fetch
    #[arg(long)]
    max_messages: Option<usize>,

    /// Minimum number of messages to fetch
    #[arg(long)]
    min_messages: Option<usize>,

    /// The namespace to fetch from
    #[arg(long, default_value = "tenant/default/namespace/default")]
    namespace: String,

    /// HTTP server address
    #[arg(long, default_value = "http://127.0.0.1:7780")]
    http_address: String,
    #[clap(flatten)]
    remote: RemoteArgs,
}

impl FetchArgs {
    pub async fn run(self, _ct: CancellationToken) -> CliResult<()> {
        let admin = self.remote.admin_client().await?;

        let namespace_name = NamespaceName::parse(&self.namespace).change_context(
            CliError::InvalidConfiguration {
                message: "invalid namespace name".to_string(),
            },
        )?;

        let topic_name = TopicName::new(&self.topic, namespace_name.clone()).change_context(
            CliError::InvalidConfiguration {
                message: "invalid topic name".to_string(),
            },
        )?;

        let topic =
            admin
                .get_topic(topic_name.clone())
                .await
                .change_context(CliError::AdminApi {
                    message: "failed to get topic".to_string(),
                })?;

        let client = reqwest::Client::new();

        let partition_value = match (self.partition_value.clone(), topic.partition_column()) {
            (None, None) => None,
            (Some(partition_value), Some(partition_column)) => Some(
                convert_partition_value(&partition_value, partition_column.data_type())
                    .change_context(CliError::InvalidArguments)?,
            ),
            (None, Some(_)) => {
                return Err(report!(CliError::InvalidArguments))
                    .attach_printable("missing required partition value");
            }
            (Some(_), None) => {
                return Err(report!(CliError::InvalidArguments))
                    .attach_printable("unexpected partition value");
            }
        };

        let request = FetchRequest {
            namespace: namespace_name.to_string(),
            timeout_ms: self.timeout,
            min_messages: self.min_messages,
            max_messages: self.max_messages,
            topics: vec![TopicRequest {
                topic: topic_name.id().to_string(),
                partition_value,
                offset: self.offset,
            }],
        };

        let response: FetchResponse = client
            .post(format!("{}/v1/fetch", self.http_address))
            .json(&request)
            .send()
            .await
            .change_context(CliError::Server {
                message: "HTTP request failed".to_string(),
            })?
            .json()
            .await
            .change_context(CliError::Server {
                message: "failed to parse response".to_string(),
            })?;

        self.display_response(topic, &response)?;

        Ok(())
    }

    fn display_response(&self, topic: Topic, response: &FetchResponse) -> CliResult<()> {
        let response = response
            .topics
            .first()
            .ok_or(CliError::Remote)
            .attach_printable("missing response")?;

        match response {
            TopicResponse::Success(success) => {
                let partition_str = success
                    .partition_value
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "None".to_string());

                println!(
                    "Topic: {}, Partition: {}, Start: {}, End: {}",
                    success.topic, partition_str, success.start_offset, success.end_offset
                );

                if success.messages.is_empty() {
                    return Ok(());
                }

                let record_batch =
                    parse_json_to_arrow(topic.schema_without_partition_column(), &success.messages)
                        .change_context(CliError::IoError)
                        .attach_printable("failed to parse response")?;

                print_batches(&[record_batch])
                    .change_context(CliError::IoError)
                    .attach_printable("failed to print messages")?;
            }
            TopicResponse::Error(error) => {
                let partition_str = error
                    .partition_value
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "None".to_string());

                eprintln!(
                    "Topic: {}, Partition: {}, Error: {}",
                    error.topic, partition_str, error.message
                );
            }
        }

        Ok(())
    }
}
