use arrow_flight::error::FlightError;
use snafu::Snafu;
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
    ClusterMetadata {
        source: wings_control_plane::cluster_metadata::ClusterMetadataError,
    },
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
    #[snafu(display("Unexpected request ID: expected {expected}, actual {actual}"))]
    UnexpectedRequestId { expected: u64, actual: u64 },
}

pub type Result<T, E = ClientError> = std::result::Result<T, E>;

impl From<tonic::Status> for ClientError {
    fn from(source: tonic::Status) -> Self {
        Self::Tonic {
            source: source.into(),
        }
    }
}
