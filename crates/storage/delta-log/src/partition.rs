use wings_resources::{PartitionValue, TableRef};

use crate::DeltaLog;

/// A collection of delta logs for a specific partition of a table.
#[derive(Debug, Clone)]
pub struct PartitionDeltaLog {
    pub table: TableRef,
    pub partition_value: Option<PartitionValue>,
    pub logs: Vec<DeltaLog>,
}

impl PartitionDeltaLog {
    pub fn new(table: TableRef, partition_value: Option<PartitionValue>) -> Self {
        Self {
            table,
            partition_value,
            logs: Vec::new(),
        }
    }

    pub fn append(&mut self, log: DeltaLog) {
        self.logs.push(log);
    }
}
