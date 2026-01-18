use std::{collections::BTreeMap, ops::Bound, time::SystemTime};

use crate::log_metadata::{
    CommitPageRequest, CommitPageResponse, GetLogLocationOptions, LogLocation, error::Result,
    memory::partition::GetLogLocationResult,
};

use super::partition::{PartitionKey, PartitionLogState};

#[derive(Default, Debug, Clone)]
pub struct TopicLogState {
    /// Maps partition keys to their offset tracking
    partitions: BTreeMap<PartitionKey, PartitionLogState>,
}

impl TopicLogState {
    pub fn partition_range(
        &self,
        start: Bound<&PartitionKey>,
        end: Bound<&PartitionKey>,
    ) -> impl Iterator<Item = (&PartitionKey, &PartitionLogState)> {
        self.partitions.range((start, end))
    }

    pub fn commit_page(
        &mut self,
        partition_key: PartitionKey,
        page: &CommitPageRequest,
        file_ref: String,
        now_ts: SystemTime,
    ) -> Result<CommitPageResponse> {
        let partition_state = self
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionLogState::new(partition_key));

        partition_state.commit_page(page, file_ref, now_ts)
    }

    pub fn get_log_location(
        &self,
        partition_key: &PartitionKey,
        offset: u64,
        options: &GetLogLocationOptions,
        locations: &mut Vec<LogLocation>,
    ) -> Result<GetLogLocationResult> {
        let Some(partition_state) = self.partitions.get(partition_key) else {
            return Ok(GetLogLocationResult::Done);
        };

        partition_state.get_log_location(offset, options, locations)
    }
}
