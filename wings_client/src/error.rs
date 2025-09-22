use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ClientError {
    #[snafu(display("Tonic gRPC error"))]
    Tonic { source: tonic::Status },
    #[snafu(display("Arrow error"))]
    Arrow { source: arrow::error::ArrowError },
    #[snafu(display("Cluster metadata request error"))]
    ClusterMetadata {
        source: wings_control_plane::cluster_metadata::ClusterMetadataError,
    },
}

pub type Result<T, E = ClientError> = std::result::Result<T, E>;

impl From<tonic::Status> for ClientError {
    fn from(source: tonic::Status) -> Self {
        Self::Tonic { source }
    }
}
