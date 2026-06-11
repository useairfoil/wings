use std::{future::Future, sync::Arc, time::Duration};

use backon::{BackoffBuilder, ExponentialBuilder};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use tokio::sync::RwLock;
use tonic::{Code, Response, Status, transport::Channel};
use tracing::{info, warn};
use wings_common::clock::SystemClock;
use wings_observability::{Histogram, KeyValue};

use crate::{ManifestStore, pb};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("manifest store error: {0}")]
    ManifestStore(#[from] crate::manifest_store::Error),
    #[error("invalid broker url: {0}")]
    InvalidBrokerUrl(#[from] http::uri::InvalidUri),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("request timed out: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("broker request failed: {0}")]
    Status(#[from] tonic::Status),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct QueueClientOptions {
    backoff: ExponentialBuilder,
    timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct QueueClient {
    clock: Arc<dyn SystemClock>,
    store: ManifestStore,
    client: Arc<RwLock<pb::broker_client::BrokerClient<Channel>>>,
    metrics: QueueClientMetrics,
    timeout: Duration,
    backoff: ExponentialBuilder,
}

#[derive(Debug, Clone)]
struct QueueClientMetrics {
    request_duration: Histogram<f64>,
}

impl QueueClient {
    /// Initializes a new QueueClient.
    ///
    /// This method errors if the broker connection fails.
    pub async fn init(
        clock: Arc<dyn SystemClock>,
        store: Arc<dyn ObjectStore>,
        options: QueueClientOptions,
    ) -> Result<Self> {
        let store = ManifestStore::new(store);
        let header = store.load_header_only().await?;

        info!(?header, "queue manifest header loaded");

        let channel = Channel::from_shared(header.broker_url())?.connect().await?;

        let client = pb::broker_client::BrokerClient::new(channel);

        Ok(Self {
            clock,
            store,
            client: RwLock::new(client).into(),
            metrics: QueueClientMetrics::default(),
            timeout: options.timeout,
            backoff: options.backoff,
        })
    }

    /// Reloads the queue client.
    pub async fn reload(&self) -> Result<()> {
        let header = self.store.load_header_only().await?;

        let channel = Channel::from_shared(header.broker_url())?.connect().await?;

        let client = pb::broker_client::BrokerClient::new(channel);

        let mut guard = self.client.write().await;
        *guard = client;

        info!(?header, "queue manifest header reloaded");

        Ok(())
    }

    pub async fn schedule_task(
        &self,
        request: pb::ScheduleTaskRequest,
    ) -> Result<pb::ScheduleTaskResponse> {
        self.request("schedule_task", request, |mut client, request| async move {
            client.schedule_task(request).await
        })
        .await
    }

    pub async fn request_tasks(
        &self,
        request: pb::RequestTasksRequest,
    ) -> Result<pb::RequestTasksResponse> {
        self.request("request_tasks", request, |mut client, request| async move {
            client.request_tasks(request).await
        })
        .await
    }

    pub async fn acknowledge_task(
        &self,
        request: pb::AcknowledgeTaskRequest,
    ) -> Result<pb::AcknowledgeTaskResponse> {
        self.request(
            "acknowledge_task",
            request,
            |mut client, request| async move { client.acknowledge_task(request).await },
        )
        .await
    }

    pub async fn heartbeat(&self, request: pb::HeartbeatRequest) -> Result<pb::HeartbeatResponse> {
        self.request("heartbeat", request, |mut client, request| async move {
            client.heartbeat(request).await
        })
        .await
    }

    async fn request<Req, Res, Fut, F>(
        &self,
        rpc: &'static str,
        request: Req,
        mut call: F,
    ) -> Result<Res>
    where
        Req: Clone,
        F: FnMut(pb::broker_client::BrokerClient<Channel>, Req) -> Fut,
        Fut: Future<Output = std::result::Result<Response<Res>, Status>>,
    {
        let started_at = self.clock.now();
        let result = self.retry_request(request, &mut call).await;
        self.record_request_duration(rpc, started_at);

        result
    }

    async fn retry_request<Req, Res, Fut, F>(&self, request: Req, call: &mut F) -> Result<Res>
    where
        Req: Clone,
        F: FnMut(pb::broker_client::BrokerClient<Channel>, Req) -> Fut,
        Fut: Future<Output = std::result::Result<Response<Res>, Status>>,
    {
        let mut backoff = self.backoff.build();

        loop {
            match self.request_once(request.clone(), call).await {
                Ok(response) => return Ok(response),
                Err(error) => {
                    if error.reloads_broker() {
                        match self.reload().await {
                            Ok(()) => {}
                            Err(reload_error) => {
                                let Some(delay) = backoff.next() else {
                                    return Err(reload_error);
                                };

                                warn!(
                                    ?error,
                                    ?reload_error,
                                    ?delay,
                                    "queue request failed; broker reload failed; retrying"
                                );
                                self.clock.sleep(delay).await;
                                continue;
                            }
                        }
                    }

                    let Some(delay) = backoff.next() else {
                        return Err(error);
                    };

                    warn!(?error, ?delay, "queue request failed; retrying");
                    self.clock.sleep(delay).await;
                }
            }
        }
    }

    async fn request_once<Req, Res, Fut, F>(&self, request: Req, call: &mut F) -> Result<Res>
    where
        F: FnMut(pb::broker_client::BrokerClient<Channel>, Req) -> Fut,
        Fut: Future<Output = std::result::Result<Response<Res>, Status>>,
    {
        let client = self.client.read().await.clone();
        let response = tokio::time::timeout(self.timeout, call(client, request))
            .await??
            .into_inner();

        Ok(response)
    }

    fn record_request_duration(&self, rpc: &'static str, started_at: DateTime<Utc>) {
        let elapsed = self.clock.now() - started_at;
        let seconds = elapsed.num_milliseconds().max(0) as f64 / 1_000.0;

        self.metrics
            .request_duration
            .record(seconds, &[KeyValue::new("rpc", rpc)]);
    }
}

impl Error {
    fn reloads_broker(&self) -> bool {
        match self {
            Self::Timeout(_) => true,
            Self::Status(status) => {
                matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
            }
            Self::ManifestStore(_) | Self::InvalidBrokerUrl(_) | Self::Transport(_) => false,
        }
    }
}

impl Default for QueueClientOptions {
    fn default() -> Self {
        Self {
            backoff: ExponentialBuilder::default(),
            timeout: Duration::from_secs(5),
        }
    }
}

impl Default for QueueClientMetrics {
    fn default() -> Self {
        let meter = wings_observability::meter("queue");
        Self {
            request_duration: meter
                .f64_histogram("client.request.duration")
                .with_description("Duration of requests to the broker (schedule, update, request)")
                .with_unit("s")
                .with_boundaries(vec![
                    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
                ])
                .build(),
        }
    }
}
