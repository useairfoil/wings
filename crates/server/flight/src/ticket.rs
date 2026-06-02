use arrow_flight::{
    Ticket,
    sql::{ProstMessageExt, TicketStatementQuery},
};
use bytes::Bytes;
use prost::Message;
use wings_resources::{PartitionValue, pb};

use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum IngestionOperation {
    Unspecified = 0,
    Upsert = 1,
    Delete = 2,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct IngestionRequestMetadata {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(enumeration = "IngestionOperation", tag = "2")]
    pub operation: i32,
    #[prost(message, optional, tag = "3")]
    pub partition_value: Option<pb::PartitionValue>,
}

impl IngestionRequestMetadata {
    pub fn try_decode(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        Self::decode(bytes)
    }

    pub fn checked_operation(&self) -> Result<IngestionOperation, String> {
        let operation = IngestionOperation::try_from(self.operation)
            .map_err(|_| format!("unknown ingestion operation: {}", self.operation))?;

        if operation == IngestionOperation::Unspecified {
            return Err("missing ingestion operation".to_string());
        }

        Ok(operation)
    }

    pub fn partition_value(&self) -> Result<Option<PartitionValue>, pb::WireError> {
        self.partition_value
            .as_ref()
            .map(PartitionValue::try_from)
            .transpose()
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct IngestionResponseMetadata {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(bool, tag = "2")]
    pub accepted: bool,
    #[prost(string, tag = "3")]
    pub message: String,
}

impl IngestionResponseMetadata {
    pub fn accepted(request_id: u64) -> Self {
        Self {
            request_id,
            accepted: true,
            message: String::new(),
        }
    }

    pub fn rejected(request_id: u64, message: impl Into<String>) -> Self {
        Self {
            request_id,
            accepted: false,
            message: message.into(),
        }
    }
}

#[derive(Clone, prost::Message)]
pub struct StatementQueryTicket {
    #[prost(string, tag = "1")]
    pub query: String,
}

impl StatementQueryTicket {
    const TYPE_URL: &str = "type.googleapis.com/wings.StatementQueryTicket";

    pub fn new(query: String) -> Self {
        StatementQueryTicket { query }
    }

    pub fn try_decode(ticket: Bytes) -> Result<Self, Error> {
        let any = prost_types::Any::decode(ticket)
            .map_err(|_| Error::invalid_ticket("failed to decode"))?;

        if any.type_url != Self::TYPE_URL {
            return Err(Error::invalid_ticket("invalid type URL"));
        }

        let ticket = StatementQueryTicket::decode(any.value.as_ref())
            .map_err(|_| Error::invalid_ticket("failed to decode"))?;

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
