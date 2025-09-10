mod client;
mod proto;
mod server;

pub mod pb {
    tonic::include_proto!("wings.v1.cluster_metadata");
}

pub use self::client::ClusterMetadataClient;
pub use self::server::ClusterMetadataServer;

const DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("wings_v1_cluster_metadata");

pub fn file_descriptor_set() -> &'static [u8] {
    DESCRIPTOR_SET
}
