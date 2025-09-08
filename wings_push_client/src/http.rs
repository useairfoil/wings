//! HTTP client for pushing messages to Wings.

use std::time::SystemTime;

use reqwest::StatusCode;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use wings_ingestor_http::types::{Batch, ErrorResponse, PushRequest, PushResponse};
use wings_control_plane::{admin::NamespaceName, partition::PartitionValue};

/// A client for pushing messages to Wings over HTTP.
#[derive(Debug, Clone)]
pub struct HttpPushClient {
    client: reqwest::Client,
    base_url: String,
    namespace: NamespaceName,
}

#[derive(Debug, Snafu)]
pub enum HttpPushClientError {
    #[snafu(display("Request error"))]
    Request { source: reqwest::Error },
    #[snafu(display("Response error: status={status}, message={message}"))]
    Response { status: StatusCode, message: String },
}

pub type Result<T, E = HttpPushClientError> = std::result::Result<T, E>;

impl HttpPushClient {
    /// Create a new HTTP push client.
    pub fn new(base_url: impl Into<String>, namespace: NamespaceName) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            namespace,
        }
    }

    /// Start building a push request for the given topic.
    pub fn push(&self) -> PushRequestBuilder {
        PushRequestBuilder::new(
            self.client.clone(),
            self.base_url.clone(),
            self.namespace.clone(),
        )
    }
}

/// Builder for constructing push requests.
#[derive(Debug)]
pub struct PushRequestBuilder {
    client: reqwest::Client,
    base_url: String,
    namespace: NamespaceName,
    batches: Vec<Batch>,
}

#[derive(Debug)]
pub struct TopicRequestBuilder {
    topic: String,
    push: PushRequestBuilder,
}

impl PushRequestBuilder {
    fn new(client: reqwest::Client, base_url: String, namespace: NamespaceName) -> Self {
        Self {
            client,
            base_url,
            namespace,
            batches: Vec::new(),
        }
    }

    pub fn topic(self, topic: String) -> TopicRequestBuilder {
        TopicRequestBuilder { push: self, topic }
    }

    /// Send the push request to the server.
    pub async fn send(self) -> Result<PushResponse> {
        let request = PushRequest {
            namespace: self.namespace.to_string(),
            batches: self.batches,
        };

        let url = format!("{}/v1/push", self.base_url);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context(RequestSnafu {})?;

        if response.status().is_success() {
            return response
                .json::<PushResponse>()
                .await
                .context(RequestSnafu {});
        }

        let status = response.status();
        let body = response
            .json::<ErrorResponse>()
            .await
            .context(RequestSnafu {})?;

        Err(HttpPushClientError::Response {
            status,
            message: body.message,
        })
    }

    fn add_batch(&mut self, batch: Batch) {
        self.batches.push(batch);
    }
}

impl TopicRequestBuilder {
    pub fn partitioned(
        mut self,
        partition_value: PartitionValue,
        data: Vec<Value>,
        timestamp: Option<SystemTime>,
    ) -> PushRequestBuilder {
        let timestamp = timestamp.map(|ts| {
            ts.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        });
        let batch = Batch {
            topic: self.topic,
            partition: Some(partition_value),
            data,
            timestamp,
        };
        self.push.add_batch(batch);
        self.push
    }

    pub fn unpartitioned(
        mut self,
        data: Vec<Value>,
        timestamp: Option<SystemTime>,
    ) -> PushRequestBuilder {
        let timestamp = timestamp.map(|ts| {
            ts.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        });
        let batch = Batch {
            topic: self.topic,
            partition: None,
            data,
            timestamp,
        };
        self.push.add_batch(batch);
        self.push
    }
}
