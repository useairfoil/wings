use arrow_flight::flight_service_client::FlightServiceClient;
use error::ClusterMetadataSnafu;
use snafu::ResultExt;
use tonic::transport::Channel;
use wings_control_plane::cluster_metadata::ClusterMetadata;
use wings_control_plane::cluster_metadata::tonic::ClusterMetadataClient;
use wings_control_plane::resources::TopicName;

mod client;
mod encode;
mod error;

pub use self::client::{TopicClient, WriteError, WriteRequest, WriteResponse};
pub use self::error::{ClientError, Result};

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

    /// Returns a client to push data to the specified topic.
    pub async fn topic(&self, topic_name: TopicName) -> Result<TopicClient> {
        let topic = self
            .cluster_meta
            .get_topic(topic_name.clone())
            .await
            .context(ClusterMetadataSnafu {})?;

        TopicClient::new(&self, topic).await
    }
}
