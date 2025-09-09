mod client;
mod proto;
mod server;

pub mod pb {
    tonic::include_proto!("wings.v1.log_metadata");
}

pub use self::client::LogMetadataClient;
pub use self::server::LogMetadataServer;
