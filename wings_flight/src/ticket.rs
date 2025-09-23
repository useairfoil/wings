use std::time::SystemTime;

use arrow_flight::{
    Ticket,
    sql::{ProstMessageExt, TicketStatementQuery},
};
use prost::{Message, bytes::Bytes};
use wings_control_plane::{
    log_metadata::{CommittedBatch, tonic::pb},
    resources::{PartitionValue, TopicName},
};

use crate::error::FlightServerError;

#[derive(Clone, prost::Message)]
pub struct StatementQueryTicket {
    #[prost(string, tag = "1")]
    pub query: String,
}

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
            value: self.encode_to_vec().into(),
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
            partition_value: partition_value.as_ref().map(Into::into),
            timestamp: timestamp.map(Into::into),
        }
    }

    pub fn try_decode(ticket: Bytes) -> Result<Self, FlightServerError> {
        let decoded = Self::decode(ticket).unwrap();
        Ok(decoded)
    }

    pub fn encode(self) -> Bytes {
        self.encode_to_vec().into()
    }
}

impl IngestionResponseMetadata {
    pub fn new(request_id: u64, result: CommittedBatch) -> Self {
        Self {
            request_id,
            result: Some(result.into()),
        }
    }

    pub fn try_decode(ticket: Bytes) -> Result<Self, FlightServerError> {
        let decoded = Self::decode(ticket).unwrap();
        Ok(decoded)
    }

    pub fn encode(self) -> Bytes {
        self.encode_to_vec().into()
    }
}
