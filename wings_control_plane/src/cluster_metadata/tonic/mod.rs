mod client;
mod proto;
mod server;

pub mod pb {
    tonic::include_proto!("wings.v1.cluster_metadata");
}

pub use self::client::ClusterMetadataClient;
pub use self::server::ClusterMetadataServer;
