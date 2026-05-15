use arrow_flight::error::FlightError;
use snafu::Snafu;
use wings_control_plane_core::cluster_metadata::ClusterMetadataError;
use wings_flight::{TicketDecodeError, TicketEncodeError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ClientError {
    #[snafu(display("Tonic gRPC error"))]
    Tonic {
        #[snafu(source(from(tonic::Status, Box::new)))]
        source: Box<tonic::Status>,
    },
    #[snafu(display("Arrow error"))]
    Arrow { source: arrow::error::ArrowError },
    #[snafu(display("Cluster metadata request error"))]
    ClusterMetadata { source: ClusterMetadataError },
    #[snafu(display("Ticket decode error"))]
    TicketDecode { source: TicketDecodeError },
    #[snafu(display("Ticket encode error"))]
    TicketEncode { source: TicketEncodeError },
    #[snafu(display("Missing partition value. Please set it with `with_partition`"))]
    MissingPartitionValue,
    #[snafu(display("Flight error"))]
    FlightError { source: FlightError },
    #[snafu(display("Stream closed"))]
    StreamClosed,
    #[snafu(display("Timeout"))]
    Timeout,
    #[snafu(display("Unexpected batch ID: expected {expected}, actual {actual}"))]
    UnexpectedBatchId { expected: u32, actual: u32 },
}

pub type Result<T, E = ClientError> = std::result::Result<T, E>;

impl From<tonic::Status> for ClientError {
    fn from(source: tonic::Status) -> Self {
        Self::Tonic {
            source: source.into(),
        }
    }
}
