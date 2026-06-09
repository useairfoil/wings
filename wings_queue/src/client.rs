use std::{sync::Arc, time::Duration};

use backon::ExponentialBuilder;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use tonic::transport::Channel;
use wings_common::clock::SystemClock;
use wings_observability::Histogram;

use crate::pb;

#[derive(Debug, thiserror::Error)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct QueueClientOptions {
    backoff: ExponentialBuilder,
    timeout: Duration,
}

#[derive(Debug)]
pub struct QueueClient {
    clock: Arc<dyn SystemClock>,
    store: Arc<dyn ObjectStore>,
    client: pb::broker_client::BrokerClient<Channel>,
    timeout: Duration,
    backoff: ExponentialBuilder,
}

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
        todo!()
        // Ok(Self {
        //     clock,
        //     store,
        //     timeout: options.timeout,
        //     backoff: options.backoff,
        // })
    }

    /// Reloads the queue client.
    pub async fn reload(&self) {}
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
