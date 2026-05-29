use arrow_flight::{
    flight_service_client::FlightServiceClient, sql::client::FlightSqlServiceClient,
};
use clap::Args;
use snafu::ResultExt;
use tonic::transport::Channel;

use crate::error::{ConnectionSnafu, InvalidRemoteUrlSnafu, Result};

/// Arguments for configuring the remote server connection.
#[derive(Args, Debug, Clone)]
pub struct RemoteArgs {
    /// The Wings Flight server address
    #[arg(long, default_value = "http://localhost:7253")]
    pub remote_address: String,
}

impl RemoteArgs {
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

    async fn channel(&self) -> Result<Channel> {
        let channel = Channel::from_shared(self.remote_address.clone())
            .context(InvalidRemoteUrlSnafu {})?
            .connect()
            .await
            .context(ConnectionSnafu {})?;

        Ok(channel)
    }
}
