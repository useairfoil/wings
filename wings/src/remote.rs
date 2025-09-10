use clap::Args;
use snafu::ResultExt;
use tonic::transport::Channel;
use wings_control_plane::{
    cluster_metadata::tonic::ClusterMetadataClient, log_metadata::tonic::LogMetadataClient,
};

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

    pub async fn log_metadata_client(&self) -> Result<LogMetadataClient<Channel>> {
        let channel = self.channel().await?;
        Ok(LogMetadataClient::new(channel))
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
