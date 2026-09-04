use std::{pin::Pin, time::Duration};

use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, PutResult, Ticket,
    flight_service_server::{FlightService as ArrowFlightService, FlightServiceServer},
    sql::{
        Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetTables, CommandStatementQuery,
        SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::{
    Request, Response, Status,
    codegen::tokio_stream::{StreamExt, wrappers::ReceiverStream},
};
use wings_meta_store::catalog::{CatalogName, CatalogStore, TableIdent};

use crate::catalog::catalog_store_error_to_status;

/// Maximum number of data messages awaiting acknowledgment at the same time.
const MAX_IN_FLIGHT_ACKS: usize = 16;

/// Maximum delay before acknowledging a data message, in milliseconds.
///
/// The delay is uniform in `[0, MAX_ACK_DELAY_MS)` and is used to introduce
/// non-ordering in the acknowledgment responses.
const MAX_ACK_DELAY_MS: u64 = 100;

#[derive(Debug)]
pub struct FlightService {
    catalog_store: CatalogStore,
}

impl FlightService {
    pub fn new(catalog_store: CatalogStore) -> Self {
        Self { catalog_store }
    }

    pub fn into_service(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightService {
    type FlightService = FlightService;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        _command: Any,
    ) -> Result<Response<<Self as ArrowFlightService>::DoPutStream>, Status> {
        let mut stream = Box::pin(request.into_inner());

        // The request stream must start with a schema message.
        let Some(message) = stream.next().await else {
            return Err(Status::invalid_argument(
                "missing schema message: do_put stream is empty",
            ));
        };
        let message = message?;
        let metadata = parse_schema_message_metadata(&message)?;

        let catalog = self
            .catalog_store
            .get(metadata.catalog_name.clone())
            .await
            .map_err(catalog_store_error_to_status)?
            .ok_or_else(|| {
                Status::not_found(format!("catalog not found: {}", metadata.catalog_name))
            })?;
        catalog
            .try_load_table(metadata.table_ident.clone())
            .await
            .map_err(catalog_store_error_to_status)?
            .ok_or_else(|| {
                Status::not_found(format!("table not found: {}", metadata.table_ident))
            })?;

        // Acknowledge data messages out of order.
        let (tx, rx) = mpsc::channel::<Result<PutResult, Status>>(MAX_IN_FLIGHT_ACKS);
        tokio::spawn(ack_data_messages(stream, tx));

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// App metadata of the schema message of a `do_put` ingestion request.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct SchemaMessageMetadata {
    catalog_name: CatalogName,
    table_ident: TableIdent,
}

/// App metadata of a data message of a `do_put` ingestion request.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct DataMessageMetadata {
    message_id: u64,
}

/// Parses the app metadata of the schema message into [`SchemaMessageMetadata`].
fn parse_schema_message_metadata(message: &FlightData) -> Result<SchemaMessageMetadata, Status> {
    serde_json::from_slice(&message.app_metadata).map_err(|source| {
        Status::invalid_argument(format!("invalid schema message app metadata: {source}"))
    })
}

/// Reads data messages from the request stream and acknowledges them.
///
/// Each data message is acknowledged by a separate task, after waiting a
/// random amount of time, to introduce non-ordering in the responses.
async fn ack_data_messages(
    stream: Pin<Box<PeekableFlightDataStream>>,
    tx: mpsc::Sender<Result<PutResult, Status>>,
) {
    let mut stream = stream;
    while let Some(message) = stream.next().await {
        let message = match message {
            Ok(message) => message,
            Err(status) => {
                let _ = tx.send(Err(status)).await;
                return;
            }
        };

        tokio::spawn(ack_data_message(message, tx.clone()));
    }
}

/// Waits a random amount of time, then acknowledges a data message with its
/// `message_id` in the app metadata.
async fn ack_data_message(message: FlightData, tx: mpsc::Sender<Result<PutResult, Status>>) {
    if let Err(source) = serde_json::from_slice::<DataMessageMetadata>(&message.app_metadata) {
        let _ = tx
            .send(Err(Status::invalid_argument(format!(
                "invalid data message app metadata: {source}"
            ))))
            .await;
        return;
    }

    let delay = rand::random_range(0..MAX_ACK_DELAY_MS);
    tokio::time::sleep(Duration::from_millis(delay)).await;

    let _ = tx
        .send(Ok(PutResult {
            app_metadata: message.app_metadata,
        }))
        .await;
}

#[cfg(test)]
mod tests {
    use tonic::Code;
    use wings_meta_store::catalog::NamespaceIdent;

    use super::*;

    fn table_ident() -> TableIdent {
        TableIdent::new(
            NamespaceIdent::from_strs(["sales", "2024"]).unwrap(),
            "orders".to_string(),
        )
    }

    #[test]
    fn schema_message_metadata_roundtrip() {
        let metadata = SchemaMessageMetadata {
            catalog_name: CatalogName::new("test-catalog").unwrap(),
            table_ident: table_ident(),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        insta::assert_snapshot!(
            json,
            @r#"{"catalog_name":"catalogs/test-catalog","table_ident":{"namespace":["sales","2024"],"name":"orders"}}"#
        );

        let deserialized: SchemaMessageMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, metadata);
    }

    #[test]
    fn data_message_metadata_roundtrip() {
        let metadata = DataMessageMetadata { message_id: 42 };

        let json = serde_json::to_string(&metadata).unwrap();
        insta::assert_snapshot!(json, @r#"{"message_id":42}"#);

        let deserialized: DataMessageMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, metadata);
    }

    #[test]
    fn schema_message_metadata_invalid() {
        let message = FlightData::default();
        let error = parse_schema_message_metadata(&message).unwrap_err();

        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            error
                .message()
                .starts_with("invalid schema message app metadata: ")
        );
    }
}
