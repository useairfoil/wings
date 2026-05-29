use arrow_flight::{
    Ticket,
    sql::{ProstMessageExt, TicketStatementQuery},
};
use bytes::Bytes;
use prost::Message;

use crate::error::Error;

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
