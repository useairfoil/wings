//! Helper functions to validate timestamps in requests.
use std::{cmp::Ordering, time::SystemTime};

use super::{CommitBatchRequest, LogOffset};

#[derive(Debug, PartialEq, Eq)]
pub enum ValidateRequestResult {
    Reject {
        reason: &'static str,
    },
    Accept {
        start_offset: u64,
        end_offset: u64,
        timestamp: Option<SystemTime>,
        next_offset: LogOffset,
    },
}

/// Compare the two batches by their timestamps.
/// We expect all `None` timestamps to come after `Some(timestamp)`.
pub fn compare_batch_request_timestamps(
    a: &CommitBatchRequest,
    b: &CommitBatchRequest,
) -> Ordering {
    compare_timestamps(&a.timestamp, &b.timestamp)
}

/// Compare the two batches by their timestamps.
/// We expect all `None` timestamps to come after `Some(timestamp)`.
pub fn compare_timestamps(a: &Option<SystemTime>, b: &Option<SystemTime>) -> Ordering {
    match (a.as_ref(), b.as_ref()) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => a.cmp(b),
    }
}

pub fn validate_timestamp_in_request(
    ts: &LogOffset,
    request: &CommitBatchRequest,
) -> ValidateRequestResult {
    if request.num_rows == 0 {
        return ValidateRequestResult::Reject {
            reason: "EMPTY_BATCH",
        };
    };

    let num_rows = request.num_rows as u64;
    let Some(timestamp) = request.timestamp else {
        let next_offset = LogOffset {
            offset: ts.offset + num_rows,
            timestamp: ts.timestamp,
        };

        return ValidateRequestResult::Accept {
            start_offset: ts.offset,
            end_offset: ts.offset + num_rows - 1,
            timestamp: None,
            next_offset,
        };
    };

    if timestamp < ts.timestamp {
        return ValidateRequestResult::Reject {
            reason: "INVALID_TIMESTAMP",
        };
    };

    let next_offset = LogOffset {
        offset: ts.offset + num_rows,
        timestamp,
    };

    ValidateRequestResult::Accept {
        start_offset: ts.offset,
        end_offset: ts.offset + num_rows - 1,
        timestamp: timestamp.into(),
        next_offset,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::*;

    #[test]
    fn test_compare_batch_timestamps() {
        let batch1 = CommitBatchRequest::new(1);
        let batch2 = {
            let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
            CommitBatchRequest::new_with_timestamp(2, ts)
        };
        let batch3 = {
            let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(120);
            CommitBatchRequest::new_with_timestamp(2, ts)
        };

        assert_eq!(
            compare_batch_request_timestamps(&batch1, &batch2),
            Ordering::Greater
        );

        assert_eq!(
            compare_batch_request_timestamps(&batch2, &batch3),
            Ordering::Less
        );
        assert_eq!(
            compare_batch_request_timestamps(&batch1, &batch3),
            Ordering::Greater
        );

        assert_eq!(
            compare_batch_request_timestamps(&batch1, &batch1),
            Ordering::Equal
        );
        assert_eq!(
            compare_batch_request_timestamps(&batch3, &batch3),
            Ordering::Equal
        );
    }

    #[test]
    fn test_log_offset_previous() {
        let offset = LogOffset {
            offset: 0,
            timestamp: SystemTime::UNIX_EPOCH,
        };

        let previous_offset = offset.previous();

        assert_eq!(previous_offset.offset, 0);
        assert_eq!(previous_offset.timestamp, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn test_log_offset_validate() {
        let offset = LogOffset {
            offset: 123,
            timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(120),
        };

        // No messages in the batch.
        let req = CommitBatchRequest::new_with_timestamp(
            0,
            SystemTime::UNIX_EPOCH + Duration::from_secs(240),
        );
        assert_eq!(
            validate_timestamp_in_request(&offset, &req),
            ValidateRequestResult::Reject {
                reason: "EMPTY_BATCH",
            }
        );

        // Timestamp before the current offset.
        let req = CommitBatchRequest::new_with_timestamp(10, SystemTime::UNIX_EPOCH);
        assert_eq!(
            validate_timestamp_in_request(&offset, &req),
            ValidateRequestResult::Reject {
                reason: "INVALID_TIMESTAMP",
            }
        );

        // Same timestamp is fine.
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(120);
        let req = CommitBatchRequest::new_with_timestamp(10, ts);
        assert_eq!(
            validate_timestamp_in_request(&offset, &req),
            ValidateRequestResult::Accept {
                start_offset: 123,
                end_offset: 132,
                timestamp: Some(ts),
                next_offset: LogOffset {
                    offset: 133,
                    timestamp: ts
                }
            }
        );

        // Larger timestamp is also fine
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(121);
        let req = CommitBatchRequest::new_with_timestamp(10, ts);
        assert_eq!(
            validate_timestamp_in_request(&offset, &req),
            ValidateRequestResult::Accept {
                start_offset: 123,
                end_offset: 132,
                timestamp: Some(ts),
                next_offset: LogOffset {
                    offset: 133,
                    timestamp: ts
                }
            }
        );

        // Timestamp is larger than the current offset
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(122);
        let req = CommitBatchRequest::new_with_timestamp(10, ts);
        assert_eq!(
            validate_timestamp_in_request(&offset, &req),
            ValidateRequestResult::Accept {
                start_offset: 123,
                end_offset: 132,
                timestamp: Some(ts),
                next_offset: LogOffset {
                    offset: 133,
                    timestamp: ts
                }
            }
        );

        // None leaves the timestamp as is.
        let req = CommitBatchRequest::new(10);
        assert_eq!(
            validate_timestamp_in_request(&offset, &req),
            ValidateRequestResult::Accept {
                start_offset: 123,
                end_offset: 132,
                timestamp: None,
                next_offset: LogOffset {
                    offset: 133,
                    timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(120)
                }
            }
        );

        // Just in case test this too.
        let req = CommitBatchRequest::new(1);
        assert_eq!(
            validate_timestamp_in_request(&offset, &req),
            ValidateRequestResult::Accept {
                start_offset: 123,
                end_offset: 123,
                timestamp: None,
                next_offset: LogOffset {
                    offset: 124,
                    timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(120)
                }
            }
        );
    }
}
