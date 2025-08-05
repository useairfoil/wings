# Fetch method

**Revision**: 1
**Created**: 2025-07-28
**Authors**: Francesco Ceccon
**Status**: Draft

## Overview

We are implementing a pull-based method to fetch messages for a specific topic and partition.
Clients continuously call this method to consume messages in the queue.

Completion of this work makes Wings a full-fledged message queue: clients can push and pull messages from it.

**Goals**

 - Create a module to fetch messages for a specific topic and partition. This module uses long polling to fetch messages when it reaches the end of the log.
 - Implement an HTTP endpoint to fetch messages. This method inherits the long polling behavior from the module.
 - Extend the CLI to provide a `fetch` command.

**Non-goals**

 - A push-based stream is not part of this work.

## Background and context

A message queue needs both a method to push messages into it, and a method to pull messages from it.
At the moment, Wings is a black hole where messages go in but never come out. This spec changes this.

We expect to implement Kafka's protocol in the future, so the semantics of the fetch method should be compatible with it.
The API described in this document is inspired by the [Kafka `Fetch Request (Version 17)`](https://kafka.apache.org/protocol) method.
Most notably:

 - Long polling with the timeout specified in the request.
 - Minimum and maximum size of the response specified in the request.
 - Multiple topics and partitions in the same request.

We make the following changes:

 - Minimum and maximum size is in number of messages. This simplifies the implementation.
 - Topics and partitions must belong to the same namespace.

## Proposal

### Core module

We are going to implement a `wings_server_core` module to provide access to the data.
This module is then used by the concrete `wings_server_http` implementation to serve data over HTTP. In the future, it will be used by the Arrow Flight implementation too.

The module handles the complexity of fetching data:

 - Follow the deadline specified by the caller.
 - Follow the minimum and maximum size specified in the request, across multiple topics and partitions.
 - Fetching data happens concurrently.
 - Requests to the metadata server are coalesced.

**Functional requirements**

The `wings_server_core` module must expose a method to fetch data for a specific topic, partition, offset, and computation constraints.

 - `topic: TopicName`: this is the global topic name.
 - `partition: Option<PartitionValue>`: this is the partition value, if any. This component must validate that the topic and partition value are compatible.
 - `offset: u64`: the offset of the first record to fetch.
 - computation constraints: this object is shared among many (possibly concurrent) calls to fetch. It provides a deadline for the request, and tracks how many messages have been fetched so far (together with the minimum and maximum number of messages).
 - `ct: CancellationToken`: this is a cancellation token that can be used to cancel the request.

This method must return the following data:

 - `topic: TopicRef`: the topic.
 - `partition: Option<PartitionValue>`: the partition value, if any.
 - `schema: SchemaRef`: the schema of the messages returned.
 - `start_offset: u64`: the offset of the first record returned.
 - `end_offset: u64`: the offset of the last record returned.
 - `batch: RecordBatch`: the messages, as Arrow RecordBatch.

It is the job of the caller to then convert and return the data to the user.

The fetch method's complexity comes from the shared computation constraints. These constraints are as follows:

 - `deadline: Instant` (default: now() + 500ms, must be greater than 1ms): the component must stop when the deadline is reached.
 - `min_messages: usize` (default: 1, min: 1, max: 100_000, must be smaller than `max_messages`): if the fetch method reaches the end-of-log offset and collected less data than this, it should wait for more data to be available before returning (but should still follow the specified deadline). If the method collected more data than this, it should return immediately.
 - `max_messages: usize` (default: 10_000, min: 1, max: 100_000, must be greater than `min_messages`): if the fetch method collects at least this many messages, it should return immediately.

Notice that the interaction between `deadline` and `min_messages` is crucial for implementing long polling. Once the client reaches the head of the log, it will always wait for more data to be available before returning. Setting the deadline to a reasonable value (500ms) ensures that the client doesn't flood the server with requests that return no data.

**Non-functional requirements**

Since this is an initial implementation, we prioritize simplicity and scalability over performance.

 - Avoid shared state unless it's used to reduce IO.
 - Limit the number of concurrent requests in all components.

### HTTP server

We should implement the HTTP endpoint in the `wings_server_http` module.

POST `/v1/fetch` request:

 - `namespace: String`: the Wings namespace.
 - `timeout_ms: u64`: the request timeout in milliseconds.
 - `min_messages: usize`: the minimum number of messages to fetch.
 - `max_messages: usize`: the maximum number of messages to fetch.
 - `topics: Vec<TopicRequest>`: the topic-specific requests.

Where `TopicRequest` is a struct that contains the following fields:

 - `topic: String`: the topic id. Together with the namespace, this forms the `TopicName`.
 - `partition_value: Option<PartitionValue>`: the partition value if the topic is partitioned.
 - `offset: u64`: the offset to start fetching from.

A successful response object contains the following fields:

 - `topics: Vec<TopicResponse>`: the topic-specific responses.

Where `TopicResponse` is an enum: one case for success (`TopicResponseSuccess`) and one for error (`TopicResponseError`). We use the tag `_tag` to distinguish between the two cases.

The `TopicResponseSuccess` (tag: `success`) struct that contains the following fields:

 - `topic: String`: the full topic name.
 - `partition_value: Option<PartitionValue>`: the partition value if the topic is partitioned.
 - `start_offset: u64`: the offset of the first record returned.
 - `end_offset: u64`: the offset of the last record returned.
 - `messages: Vec<serde_json::Value>`: the fetched messages, serialized as JSON objects.

The `TopicResponseError` (tag: `error`) struct that contains the following fields:

 - `topic: String`: the full topic name.
 - `partition_value: Option<PartitionValue>`: the partition value if the topic is partitioned.
 - `message: String`: the error message.

If any validation error occurs, the response will be an error response with a single `message` field.

**Functional requirements**

The HTTP handler must:

 - validate the namespace name and topic name.
 - delegate the validation of the partition value to the core module.
 - fetch all topics and partitions in parallel.

### CLI

We are going to add the `fetch` command to the CLI. This command is useful not only for users, but also for end-to-end testing.

This command takes the following arguments:

 - `topic` (required): the name of the topic.
 - `partition value` (optional, default: `None`): the partition value if the topic is partitioned.
 - `offset` (required): the offset to start fetching from.
 - `timeout` (optional): the maximum time to wait for data to be available.
 - `max_messages` (optional): change the request `max_messages` parameter to this.
 - `min_messages` (optional): change the request `min_messages` parameter to this.

The data returned by the API should be formatted as a table.

Notice that since we request only data for one specific topic/partition, the result always contains a single topic.

In case of success, print the following:

```text
Topic: <topic>, Partition: <partition>, Start: <start_offset>, End: <end_offset>

<messages_as_table />
```

Where `<messages_as_table />` is a table created by parsing the messages returned by the API into an arrow `RecordBatch` and printing it. Use `arrow_json` to parse the messages, using the schema from the topic definition.

In case of error, print the topic, partition value and error message with the following format:

```text
Topic: <topic>, Partition: <partition>, Error: <error_message>
```


## Design and architecture

### Shared computation constraints

The `FetchState` struct tracks how many messages have been collected across many topic and partition fetches.
For convenience, it also tracks the minimum and maximum number of  messages requested by the user and the deadline.

When the fetch service collects data for a topic/partition, it increases the number of messages in the shared state. Since data can be collected by tasks on different threads, it must be an atomic value.

### Fetcher service

The `Fetcher` struct is responsible for implementing the logic to fetch data as described in the "Core module" section above.

This struct holds all the clients for the external services necessary to fetch data:

 - `Arc<dyn Admin>`: fetch the metadata for the topic's namespace. The namespace is needed to create an object store client using the `ObjectStoreFactory`.
 - `Arc<dyn OffsetRegistry>`: interact with the offset registry to get the location of the specific offset.
 - `Arc<dyn ObjectStoreFactory>`: create a client to read with the object store.

 The `fetch` method  collects data until one of the stopping conditions outlined in the "Core module" section above is met.

### Offset registry updates

The current `OffsetRegistry` trait (defined in `wings_metadata_core/src/offset_registry/mod.rs`) already provides a method to retrieve the location for a given topic, partition and location.

At the moment, if the requested offset is _after_ the most recent offset, the function returns a `OffsetRegistryError::OffsetNotFound`.

We need to make the following changes:

 - The `offset_location` should return `OffsetRegistryResult<Option<OffsetLocation>>`. The value of `None` is returned if the requested offset is _after_ the most recent offset.
 - The function must accept a `deadline: Instant` parameter. If the specified offset is not present yet, the implementation must listen for offset changes until the offset is ingested, or the deadline is reached (whatever comes first).

After this change, we should update the `InMemoryOffsetRegitry` to handle this behaviour. This implementation simply registers a group of "waiters" using the concurrent hashmap in the `dashmap` crate and `tokio::sync::Notify`.
When a topic/partition offset is updated, all registered waiters are removed and notified.

Finally, we should update the gRPC service protocol to transmit the specified deadline using the `Timestamp` well-known type.

### Remote service request deduplication

All "get" requests must be deduplicated:

 - `RemoteOffsetRegistryService`
    + `offset_location`
 - `RemoteAdminService`
    + `get_tenant`
    + `get_namespace`
    + `get_topic`

We should implement a `Deduplicate<K, V>` struct that is used to implement deduplication.

The struct only provides a `fetch` method with the following signature (NOTE: the name `fetch` in this module is unrelated to the `fetch` described in the other sections):

```rust
impl<K, V> Deduplicate<K, V> {
    pub async fn fetch<F, FU>(&self, key: K, fetch: F) -> Fetch<K, V, F>
    where
        V: Clone,
        F: FnOnce(K) -> FU,
        FU: Future<Output = V> + Send + Sync + 'static;
}
```

Where `Fetch` is an enum that represents the possible outcomes of the fetch operation:

 - `Wait(oneshot::Receiver<V>)`: another request is already in progress for the same key. This contains a oneshot receiver that will receive the value when it is available.
 - `Miss(JoinHandle<V>)`: the requested key is not present in struct. This contains the join handle of the Tokio's task spawned to do the work.

The `fetch` implementation works as follows:

 - track waiters in the `Deduplicate` dashmap.
 - on `fetch`, check whether waiters exist for the provided key or not.
 - if it exists, create a oneshot channel, push the receiver to the waiters list and return `Wait`.
 - if not, insert an empty list of waiters and spawn a new task to fetch the value. When the task completes, remove the waiters from the `Deduplicate` dashmap and send the value to all receivers before returning the value.

`Fetch` implements `Future` returning the value `V`. Notice that `JoinHandle` is fallible, in that case it's fine to panic to unwrap the error (use `expect` and an helpful error message).

### HTTP Server

The HTTP server validates the request:

 - delegates parameters validation to the `FetchState` struct constructor.
 - validates that the specified topics exist.

After that, it starts one task per partition to fetch the data. Once the tasks are completed, it builds the response and returns it.

## Implementation plan

The first step is to create the HTTP server and CLI. Once we have this, we can start running the server and call it with the CLI.

- [x] Stub the HTTP server request and response types, and the HTTP handler. For now, just return a server error response.
- [x] Implement the `fetch` command in the CLI. Use the types from the HTTP server for the request and response.

After that, we can start implementing the core module.

 - [x] Create the `Fetcher` struct in the `wings_server_core` crate.
 - [x] Define the `fetch` method, but don't implement it yet.
 - [x] Pass the `Fetcher` object to the HTTP server.
 - [x] Update the HTTP server to call `fetch` in parallel.
    + Make sure to convert all HTTP types into the core fetcher types. The return values must be converted back to the HTTP types.
    + Make sure to pass the shared state to all the `fetch` calls.

After this, we can implement `Fetcher` in the core module.

 - [ ] Validate that the topic and partition value are compatible. Use the topic's partition column schema to validate this.
 - [ ] Start accumulating messages (records) in a buffer, starting from the provided offset.
 - [ ] Stop one of the stopping conditions is met. Return the values in the buffer together with any metadata needed to construct the response.

Then update the in memory offset registry service to implement long polling for the `offset_location` method.

 - [ ] Update `InMemoryOffsetRegistry::offset_location` to support long polling.
 - [ ] Update the `OffsetRegistry` gRPC service to include a deadline parameter in the request.
 - [ ] Update the `RemoteOffsetRegistry` client to forward the provided deadline to the remote server.

Finally, it's time to reduce the number of remote calls by implementing request deduplication.

 - [ ] Implement the `Deduplicate` struct.
 - [ ] Add `Deduplicate` to the `RemoteOffsetRegistryService` client.
 - [ ] Add `Deduplicate` to the `RemoteAdminService` client.

## Testing strategy

> Describe how to implement unit tests for the components described in the
> design and architecture section.
> Describe what type of integration tests to implement.

For now, DO NOT WRITE ANY TESTS.
