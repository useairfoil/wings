//! Protobuf definitions for the wings gRPC server and client.

pub mod pb {
    tonic::include_proto!("wings.v1");
}

const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("wings_grpc_common");

pub fn file_descriptor_set() -> &'static [u8] {
    FILE_DESCRIPTOR_SET
}
