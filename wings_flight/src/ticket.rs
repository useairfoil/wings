use std::time::{Duration, SystemTime};

use arrow_flight::{
    Ticket,
    sql::{ProstMessageExt, TicketStatementQuery},
};
use prost::{DecodeError, Message, bytes::Bytes};
use prost_types::{DurationError, TimestampError};
use snafu::{ResultExt, Snafu};
use wings_control_plane::{
    log_metadata::{CommittedBatch, LogMetadataError},
    resources::{PartitionValue, ResourceError, TopicName},
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

#[derive(Clone, Debug)]
pub struct FetchTicket {
    pub topic_name: TopicName,
    pub partition_value: Option<PartitionValue>,
    pub offset: u64,
    pub timeout: Duration,
    pub min_batch_size: Option<usize>,
    pub max_batch_size: Option<usize>,
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
    #[snafu(display("Failed to decode duration"))]
    Duration { source: DurationError },
    #[snafu(display("Missing message field: {}", field))]
    MissingField { field: String },
    #[snafu(display("Failed to decode {resource} name"))]
    ResourceName {
        source: ResourceError,
        resource: &'static str,
    },
}

#[derive(Debug, Snafu)]
pub enum TicketEncodeError {
    #[snafu(display("Failed to encode duration"))]
    EncodeDuration { source: DurationError },
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

    #[derive(Clone, prost::Message)]
    pub struct FetchTicket {
        #[prost(string, tag = "1")]
        pub topic_name: String,
        #[prost(message, tag = "2")]
        pub partition_value: Option<pb::PartitionValue>,
        #[prost(uint64, tag = "3")]
        pub offset: u64,
        #[prost(message, tag = "4")]
        pub timeout: Option<prost_types::Duration>,
        #[prost(uint32, optional, tag = "5")]
        pub min_batch_size: Option<u32>,
        #[prost(uint32, optional, tag = "6")]
        pub max_batch_size: Option<u32>,
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

    pub fn try_decode_schema_message(ticket: Bytes) -> Result<u64, TicketDecodeError> {
        let proto = pb::IngestionResponseMetadata::decode(ticket).context(ProstSnafu {})?;
        Ok(proto.request_id)
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

impl FetchTicket {
    const TYPE_URL: &str = "type.googleapis.com/wings.v1.FetchTicket";
    const DEFAULT_TIMEOUT: Duration = Duration::from_millis(100);

    pub fn new(topic_name: TopicName) -> Self {
        Self {
            topic_name,
            partition_value: None,
            offset: 0,
            timeout: Self::DEFAULT_TIMEOUT,
            min_batch_size: None,
            max_batch_size: None,
        }
    }

    pub fn with_partition_value(mut self, partition_value: Option<PartitionValue>) -> Self {
        self.partition_value = partition_value;
        self
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_min_batch_size(mut self, min_batch_size: usize) -> Self {
        self.min_batch_size = Some(min_batch_size);
        self
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = Some(max_batch_size);
        self
    }

    pub fn try_decode_any(any: arrow_flight::sql::Any) -> Result<Self, FlightServerError> {
        if any.type_url != Self::TYPE_URL {
            return Err(FlightServerError::invalid_ticket("invalid type URL"));
        }

        let ticket = Self::try_decode(&any.value)
            .map_err(|_| FlightServerError::invalid_ticket("failed to decode"))?;

        Ok(ticket)
    }

    pub fn try_decode(ticket: &[u8]) -> Result<Self, TicketDecodeError> {
        let proto = pb::FetchTicket::decode(ticket).context(ProstSnafu {})?;

        let topic_name =
            TopicName::parse(&proto.topic_name).context(ResourceNameSnafu { resource: "topic" })?;

        let partition_value = proto
            .partition_value
            .map(PartitionValue::try_from)
            .transpose()
            .context(PartitionValueSnafu {})?;

        let timeout = proto
            .timeout
            .map(Duration::try_from)
            .transpose()
            .context(DurationSnafu {})?
            .unwrap_or(Self::DEFAULT_TIMEOUT);

        Ok(Self {
            topic_name,
            partition_value,
            offset: proto.offset,
            timeout,
            min_batch_size: proto.min_batch_size.map(|v| v as usize),
            max_batch_size: proto.max_batch_size.map(|v| v as usize),
        })
    }

    pub fn into_ticket(self) -> Result<Ticket, TicketEncodeError> {
        let timeout: prost_types::Duration =
            self.timeout.try_into().context(EncodeDurationSnafu {})?;

        let proto = pb::FetchTicket {
            topic_name: self.topic_name.to_string(),
            partition_value: self.partition_value.as_ref().map(Into::into),
            offset: self.offset,
            timeout: timeout.into(),
            min_batch_size: self.min_batch_size.map(|v| v as u32),
            max_batch_size: self.max_batch_size.map(|v| v as u32),
        };

        let any = prost_types::Any {
            type_url: Self::TYPE_URL.to_string(),
            value: proto.encode_to_vec(),
        };

        Ok(Ticket {
            ticket: any.encode_to_vec().into(),
        })
    }
}
