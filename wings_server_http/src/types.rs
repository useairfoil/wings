//! Request and response types for the HTTP server fetch endpoint.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use wings_control_plane::partition::PartitionValue;

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    pub namespace: String,
    pub timeout_ms: Option<u64>,
    pub min_messages: Option<usize>,
    pub max_messages: Option<usize>,
    pub topics: Vec<TopicRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicRequest {
    pub topic: String,
    pub partition_value: Option<PartitionValue>,
    pub offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchResponse {
    pub topics: Vec<TopicResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "_tag")]
pub enum TopicResponse {
    #[serde(rename = "success")]
    Success(TopicResponseSuccess),
    #[serde(rename = "error")]
    Error(TopicResponseError),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicResponseSuccess {
    pub topic: String,
    pub partition_value: Option<PartitionValue>,
    pub start_offset: u64,
    pub end_offset: u64,
    pub messages: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicResponseError {
    pub topic: String,
    pub partition_value: Option<PartitionValue>,
    pub message: String,
}

/// Response payload for errors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorResponse {
    pub message: String,
}

impl From<TopicResponseError> for TopicResponse {
    fn from(error: TopicResponseError) -> Self {
        TopicResponse::Error(error)
    }
}

impl From<TopicResponseSuccess> for TopicResponse {
    fn from(success: TopicResponseSuccess) -> Self {
        TopicResponse::Success(success)
    }
}
