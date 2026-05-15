use arrow_flight::{
    flight_service_client::FlightServiceClient, sql::client::FlightSqlServiceClient,
};
use clap::Args;
use snafu::ResultExt;
use tonic::transport::Channel;
use wings_client::WingsClient;
use wings_control_plane_core::cluster_metadata::tonic::ClusterMetadataClient;

use crate::error::{ConnectionSnafu, InvalidRemoteUrlSnafu, Result};

/// Arguments for configuring the remote server connection.
#[derive(Args, Debug, Clone)]
pub struct RemoteArgs {
    /// The address of the remote Wings admin server
    #[arg(long, default_value = "http://localhost:7777")]
    pub remote_address: String,
}

impl RemoteArgs {
    /// Create a new gRPC client for the admin service.
    pub async fn cluster_metadata_client(&self) -> Result<ClusterMetadataClient<Channel>> {
        let channel = self.channel().await?;
        Ok(ClusterMetadataClient::new(channel))
    }

    pub async fn flight_client(&self) -> Result<FlightServiceClient<Channel>> {
        let channel = self.channel().await?;
        Ok(FlightServiceClient::new(channel))
    }

    pub async fn flight_sql_client(
        &self,
        namespace: &str,
    ) -> Result<FlightSqlServiceClient<Channel>> {
        let inner = self.flight_client().await?;
        let mut client = FlightSqlServiceClient::new_from_inner(inner);
        client.set_header("x-wings-namespace", namespace);
        Ok(client)
    }

    pub async fn wings_client(&self) -> Result<WingsClient> {
        let channel = self.channel().await?;
        Ok(WingsClient::new(channel))
    }

    async fn channel(&self) -> Result<Channel> {
        let channel = Channel::from_shared(self.remote_address.clone())
            .context(InvalidRemoteUrlSnafu {})?
            .connect()
            .await
            .context(ConnectionSnafu {})?;

        Ok(channel)
    }
}
