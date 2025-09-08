use clap::Parser;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_control_plane::{
    admin::{Admin, NamespaceName, TopicName},
    partition::PartitionValue,
};
use wings_server_http::types::{FetchRequest, TopicRequest};

use crate::{
    error::{AdminSnafu, InvalidResourceNameSnafu, PartitionValueParseSnafu, Result},
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
    #[arg(long, default_value = "tenants/default/namespaces/default")]
    namespace: String,

    /// HTTP server address
    #[arg(long, default_value = "http://127.0.0.1:7780")]
    http_address: String,
    #[clap(flatten)]
    remote: RemoteArgs,
}

impl FetchArgs {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let admin = self.remote.admin_client().await?;

        let namespace_name =
            NamespaceName::parse(&self.namespace).context(InvalidResourceNameSnafu {
                resource: "namespace",
            })?;

        let topic_name = TopicName::new(&self.topic, namespace_name.clone())
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let topic = admin
            .get_topic(topic_name.clone())
            .await
            .context(AdminSnafu {
                operation: "get_topic",
            })?;

        let _client = reqwest::Client::new();

        let partition_value = PartitionValue::parse_with_datatype_option(
            topic.partition_column_data_type(),
            self.partition_value.as_ref().map(String::as_str),
        )
        .context(PartitionValueParseSnafu {})?;

        let _request = FetchRequest {
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

        /*
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
        */

        Ok(())
    }
}
