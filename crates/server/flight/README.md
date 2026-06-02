# Wings Flight Server

## Ingestion Protocol

Ingestion is implemented by sending a `do_put` request to the server. Clients can ingest data for multiple tables belonging to the same namespace within the same request.

**Namespace header**: the namespace to ingest data into is specified with the `x-wings-namespace` header.

Each ingestion request starts with a Flight `Schema` message. The table id is required and is encoded as the only element in the schema message's `FlightDescriptor.path` and is joined with the namespace name to form the table name. The ingestion request metadata is protobuf-encoded in the schema message's `app_metadata` field.

For every `IngestionRequestMetadata`, the server returns a `PutResult` whose `app_metadata` field contains a protobuf-encoded `IngestionResponseMetadata`. The response is sent only after the server has finished receiving the record batches for that table, which happens when the next schema message arrives or when the input stream is closed. Request ids are not required to be monotonically increasing and/or unique. If `accepted = false`, the `message` field contains a human-readable error message.

Malformed metadata, malformed descriptors, and Arrow stream decoding errors are fatal and stop the stream with a gRPC status error. Validation failures are not fatal: the server returns `accepted = false` at the table-section boundary, skips the following record batches, and resumes validation at the next schema message.

For upserts, the Arrow schema must match the table schema without the partition column. For deletes, the Arrow schema must contain exactly the table key field and version field. Partitioned tables require a partition value with the partition field's type. Unpartitioned tables reject partition values.

```proto
enum IngestionOperation {
    INGESTION_OPERATION_UNSPECIFIED = 0;
    INGESTION_OPERATION_UPSERT = 1;
    INGESTION_OPERATION_DELETE = 2;
}

message PartitionValue {
  oneof value {
    google.protobuf.Empty null_value = 1;

    int32 int8_value = 2;
    int32 int16_value = 3;
    int32 int32_value = 4;
    int64 int64_value = 5;

    uint32 uint8_value = 6;
    uint32 uint16_value = 7;
    uint32 uint32_value = 8;
    uint64 uint64_value = 9;

    string string_value = 10;
    bytes bytes_value = 11;
    bool bool_value = 12;
  }
}

message IngestionRequestMetadata {
    uint64 request_id = 1;
    IngestionOperation operation = 2;
    optional PartitionValue partition_value = 3;
}

message IngestionResponseMetadata {
    uint64 request_id = 1;
    bool accepted = 2;
    string message = 3;
}
```

### Flow diagram

```txt
Client                             Server
  |                                  |
  |-id=0-------- Schema ------------>|     # Schema has FlightDescriptor.path = [table_name]
  |                                  |
  |------------- Batch ------------->|     # The client sends batches of data
  |------------- Batch ------------->|
  |                                  |
  |-id=1-------- Schema ------------>|     # The client starts ingestion for a different table
  |                                  |
  |<---------- Accepted --------id=0-|     # Response for id=0 is sent after id=1 starts
  |                                  |
  |------------- Batch ------------->|
  |                                  |
  |-id=2-------- Schema ------------>|  
  |------------- Batch ------------->|
  |                                  |
  |------------- Close ------------->|     # Response for id=2 is sent after the input stream closes
  |                                  |
  |<---------- Accepted --------id=2-|     # Responses are not guaranteed to be in order
  |<---------- Accepted --------id=1-| 
  |                                  |
```
