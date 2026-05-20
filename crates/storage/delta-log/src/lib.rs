mod partition;
pub mod test_util;

use datafusion::arrow::array::RecordBatch;

#[derive(Debug, Clone)]
pub enum DeltaLog {
    Update(DeltaUpdate),
    Delete(DeltaDelete),
}

#[derive(Debug, Clone)]
pub struct DeltaUpdate {
    pub records: RecordBatch,
}

#[derive(Debug, Clone)]
pub struct DeltaDelete {
    pub records: RecordBatch,
}

impl DeltaLog {
    pub fn records(&self) -> &RecordBatch {
        match self {
            DeltaLog::Update(update) => &update.records,
            DeltaLog::Delete(delete) => &delete.records,
        }
    }
}
