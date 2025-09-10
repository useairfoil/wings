mod client;
mod proto;
mod server;

pub mod pb {
    tonic::include_proto!("wings.v1.log_metadata");
}

pub use self::client::LogMetadataClient;
pub use self::server::LogMetadataServer;

const DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("wings_v1_log_metadata");

pub fn file_descriptor_set() -> &'static [u8] {
    DESCRIPTOR_SET
}
