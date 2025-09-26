use std::time::SystemTime;

use arrow_flight::{
    Ticket,
    sql::{ProstMessageExt, TicketStatementQuery},
};
use prost::{DecodeError, Message, bytes::Bytes};
use prost_types::TimestampError;
use snafu::{ResultExt, Snafu};
use wings_control_plane::{
    log_metadata::{CommittedBatch, LogMetadataError},
    resources::PartitionValue,
};

use crate::error::FlightServerError;

#[derive(Clone, prost::Message)]
pub struct StatementQueryTicket {
    #[prost(string, tag = "1")]
    pub query: String,
}

#[derive(Clone, Debug)]
pub struct IngestionRequestMetadata {
    pub request_id: u64,
    pub partition_value: Option<PartitionValue>,
    pub timestamp: Option<SystemTime>,
}

#[derive(Clone, Debug)]
pub struct IngestionResponseMetadata {
    pub request_id: u64,
    pub result: CommittedBatch,
}

#[derive(Debug, Snafu)]
pub enum TicketDecodeError {
    #[snafu(display("Failed to decode prost message"))]
    Prost { source: DecodeError },
    #[snafu(display("Failed to decode partition value"))]
    PartitionValue { source: LogMetadataError },
    #[snafu(display("Failed to decode committed batch"))]
    CommittedBatch { source: LogMetadataError },
    #[snafu(display("Failed to decode timestamp"))]
    Timestamp { source: TimestampError },
    #[snafu(display("Missing message field: {}", field))]
    MissingField { field: String },
}

pub mod pb {
    use wings_control_plane::log_metadata::tonic::pb;

    #[derive(Clone, prost::Message)]
    pub struct IngestionRequestMetadata {
        #[prost(uint64, tag = "1")]
        pub request_id: u64,
        #[prost(message, tag = "2")]
        pub partition_value: Option<pb::PartitionValue>,
        #[prost(message, tag = "3")]
        pub timestamp: Option<prost_types::Timestamp>,
    }

    #[derive(Clone, prost::Message)]
    pub struct IngestionResponseMetadata {
        #[prost(uint64, tag = "1")]
        pub request_id: u64,
        #[prost(message, tag = "2")]
        pub result: Option<pb::CommittedBatch>,
    }
}

impl StatementQueryTicket {
    const TYPE_URL: &str = "type.googleapis.com/wings.v1.StatementQueryTicket";

    pub fn new(query: String) -> Self {
        StatementQueryTicket { query }
    }

    pub fn try_decode(ticket: Bytes) -> Result<Self, FlightServerError> {
        let any = prost_types::Any::decode(ticket)
            .map_err(|_| FlightServerError::invalid_ticket("failed to decode"))?;

        if any.type_url != Self::TYPE_URL {
            return Err(FlightServerError::invalid_ticket("invalid type URL"));
        }

        let ticket = StatementQueryTicket::decode(any.value.as_ref())
            .map_err(|_| FlightServerError::invalid_ticket("failed to decode"))?;

        Ok(ticket)
    }

    pub fn into_ticket(self) -> Ticket {
        let any = prost_types::Any {
            type_url: Self::TYPE_URL.to_string(),
            value: self.encode_to_vec(),
        };

        let statement = TicketStatementQuery {
            statement_handle: any.encode_to_vec().into(),
        };

        Ticket {
            ticket: statement.as_any().encode_to_vec().into(),
        }
    }
}

impl IngestionRequestMetadata {
    pub fn new(
        request_id: u64,
        partition_value: Option<PartitionValue>,
        timestamp: Option<SystemTime>,
    ) -> Self {
        Self {
            request_id,
            partition_value,
            timestamp,
        }
    }

    pub fn try_decode(ticket: Bytes) -> Result<Self, TicketDecodeError> {
        let proto = pb::IngestionRequestMetadata::decode(ticket).context(ProstSnafu {})?;
        let partition_value = proto
            .partition_value
            .map(PartitionValue::try_from)
            .transpose()
            .context(PartitionValueSnafu {})?;
        let timestamp = proto
            .timestamp
            .map(SystemTime::try_from)
            .transpose()
            .context(TimestampSnafu {})?;
        Ok(Self::new(proto.request_id, partition_value, timestamp))
    }

    pub fn encode(self) -> Bytes {
        let proto = pb::IngestionRequestMetadata {
            request_id: self.request_id,
            partition_value: self.partition_value.as_ref().map(Into::into),
            timestamp: self.timestamp.map(Into::into),
        };

        proto.encode_to_vec().into()
    }
}

impl IngestionResponseMetadata {
    pub fn new(request_id: u64, result: CommittedBatch) -> Self {
        Self { request_id, result }
    }

    pub fn try_decode_partial(
        ticket: Bytes,
    ) -> Result<pb::IngestionResponseMetadata, TicketDecodeError> {
        let proto = pb::IngestionResponseMetadata::decode(ticket).context(ProstSnafu {})?;
        Ok(proto)
    }

    pub fn try_decode(ticket: Bytes) -> Result<Self, TicketDecodeError> {
        let proto = pb::IngestionResponseMetadata::decode(ticket).context(ProstSnafu {})?;
        let result = proto
            .result
            .ok_or_else(|| {
                MissingFieldSnafu {
                    field: "result".to_string(),
                }
                .build()
            })?
            .try_into()
            .context(CommittedBatchSnafu {})?;

        Ok(Self::new(proto.request_id, result))
    }

    pub fn encode(self) -> Bytes {
        let proto = pb::IngestionResponseMetadata {
            request_id: self.request_id,
            result: Some(self.result.into()),
        };

        proto.encode_to_vec().into()
    }
}
