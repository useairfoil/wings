use clap::Args;
use tokio::net::TcpListener;

#[derive(Debug, Args)]
pub struct ServerArgs {
    #[arg(
        long = "server.address",
        default_value = "127.0.0.1:0",
        env = "WINGS_SERVER_ADDRESS"
    )]
    pub server_address: String,
}

impl ServerArgs {
    pub async fn bind_listener(&self) -> std::io::Result<TcpListener> {
        TcpListener::bind(&self.server_address).await
    }
}
