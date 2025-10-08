use arrow_flight::flight_service_client::FlightServiceClient;
use error::ClusterMetadataSnafu;
use fetch::FetchClient;
use snafu::ResultExt;
use tonic::transport::Channel;
use wings_control_plane::cluster_metadata::ClusterMetadata;
use wings_control_plane::cluster_metadata::tonic::ClusterMetadataClient;
use wings_control_plane::resources::TopicName;

mod encode;
mod error;
mod fetch;
mod metadata;
mod push;

pub use self::error::{ClientError, Result};
pub use self::push::{PushClient, WriteRequest, WriteResponse};

/// A high-level client to interact with Wings' data plane.
#[derive(Debug, Clone)]
pub struct WingsClient {
    /// Arrow fligt client.
    flight: FlightServiceClient<Channel>,
    /// Cluster metadata client. Used to fetch the topic's schema.
    cluster_meta: ClusterMetadataClient<Channel>,
}

impl WingsClient {
    /// Creates a client with the provided tonic [`Channel`].
    pub fn new(channel: Channel) -> Self {
        let flight = FlightServiceClient::new(channel.clone());
        let cluster_meta = ClusterMetadataClient::new(channel);

        Self {
            flight,
            cluster_meta,
        }
    }

    /// Returns a client to fetch data from the specified topic.
    pub async fn fetch_client(&self, topic_name: TopicName) -> Result<FetchClient> {
        let topic = self
            .cluster_meta
            .get_topic(topic_name.clone())
            .await
            .context(ClusterMetadataSnafu {})?;

        Ok(FetchClient::new(self, topic))
    }

    /// Returns a client to push data to the specified topic.
    pub async fn push_client(&self, topic_name: TopicName) -> Result<PushClient> {
        let topic = self
            .cluster_meta
            .get_topic(topic_name.clone())
            .await
            .context(ClusterMetadataSnafu {})?;

        PushClient::new(self, topic).await
    }
}
