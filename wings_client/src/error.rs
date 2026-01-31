use arrow_flight::error::FlightError;
use snafu::Snafu;
use wings_control_plane_core::cluster_metadata::ClusterMetadataError;
use wings_flight::{TicketDecodeError, TicketEncodeError};
use wings_observability::ErrorKind;

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

impl ClientError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::MissingPartitionValue => ErrorKind::Validation,
            Self::UnexpectedRequestId { .. } => ErrorKind::Internal,
            Self::ClusterMetadata { source } => source.kind(),
            _ => ErrorKind::Temporary,
        }
    }
}
