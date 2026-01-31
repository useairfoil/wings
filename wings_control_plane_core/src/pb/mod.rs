mod cluster_metadata;
mod log_metadata;
mod schema;

tonic::include_proto!("wings.v1.log_metadata");
tonic::include_proto!("wings.v1.cluster_metadata");
tonic::include_proto!("wings.schema");

const LOG_METADATA_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("wings_v1_log_metadata");

pub fn log_metadata_file_descriptor_set() -> &'static [u8] {
    LOG_METADATA_DESCRIPTOR_SET
}

const CLUSTER_METADATA_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("wings_v1_cluster_metadata");

pub fn cluster_metadata_file_descriptor_set() -> &'static [u8] {
    CLUSTER_METADATA_DESCRIPTOR_SET
}
