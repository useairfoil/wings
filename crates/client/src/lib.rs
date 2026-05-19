use arrow_flight::flight_service_client::FlightServiceClient;
use error::ClusterMetadataSnafu;
use fetch::FetchClient;
use snafu::ResultExt;
use tonic::transport::Channel;
use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, TableView, tonic::ClusterMetadataClient,
};
use wings_resources::TableName;

mod encode;
mod error;
mod fetch;
mod metadata;
mod push;

pub use self::{
    error::{ClientError, Result},
    push::{PushClient, WriteRequest, WriteResponse},
};

/// A high-level client to interact with Wings' data plane.
#[derive(Debug, Clone)]
pub struct WingsClient {
    /// Arrow fligt client.
    flight: FlightServiceClient<Channel>,
    /// Cluster metadata client. Used to fetch the table's schema.
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

    /// Returns a client to fetch data from the specified table.
    pub async fn fetch_client(&self, table_name: TableName) -> Result<FetchClient> {
        let table = self
            .cluster_meta
            .get_table(table_name.clone(), TableView::Basic)
            .await
            .context(ClusterMetadataSnafu {})?;

        Ok(FetchClient::new(self, table))
    }

    /// Returns a client to push data to the specified table.
    pub async fn push_client(&self, table_name: TableName) -> Result<PushClient> {
        let table = self
            .cluster_meta
            .get_table(table_name.clone(), TableView::Basic)
            .await
            .context(ClusterMetadataSnafu {})?;

        PushClient::new(self, table).await
    }
}
