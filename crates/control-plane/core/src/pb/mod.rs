mod cluster_metadata;
mod error;
mod table_metadata;
pub mod schema;

pub use self::error::WireError;

tonic::include_proto!("wings.v1.table_metadata");
tonic::include_proto!("wings.v1.cluster_metadata");
tonic::include_proto!("wings.schema");

const TABLE_METADATA_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("wings_v1_table_metadata");

pub fn table_metadata_file_descriptor_set() -> &'static [u8] {
    TABLE_METADATA_DESCRIPTOR_SET
}

const CLUSTER_METADATA_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("wings_v1_cluster_metadata");

pub fn cluster_metadata_file_descriptor_set() -> &'static [u8] {
    CLUSTER_METADATA_DESCRIPTOR_SET
}

#[derive(prost::Message)]
pub struct CommittedBatches {
    #[prost(message, repeated, tag = "1")]
    pub batches: Vec<self::CommittedBatch>,
}

impl CommittedBatches {
    pub fn new(batches: Vec<crate::table_metadata::CommittedBatch>) -> Self {
        let batches = batches.into_iter().map(Into::into).collect();
        Self { batches }
    }
}
