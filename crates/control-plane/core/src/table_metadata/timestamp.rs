//! Helper functions to validate timestamps in requests.
use std::{cmp::Ordering, time::SystemTime};

use super::{CommitBatchRequest, SeqNum};

#[derive(Debug, PartialEq, Eq)]
pub enum ValidateRequestResult {
    Reject {
        reason: &'static str,
    },
    Accept {
        start_seqnum: u64,
        end_seqnum: u64,
        timestamp: Option<SystemTime>,
        next_seqnum: SeqNum,
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
    ts: &SeqNum,
    request: &CommitBatchRequest,
) -> ValidateRequestResult {
    if request.num_rows == 0 {
        return ValidateRequestResult::Reject {
            reason: "EMPTY_BATCH",
        };
    };

    let num_rows = request.num_rows as u64;
    let Some(timestamp) = request.timestamp else {
        let next_seqnum = SeqNum {
            seqnum: ts.seqnum + num_rows,
            timestamp: ts.timestamp,
        };

        return ValidateRequestResult::Accept {
            start_seqnum: ts.seqnum,
            end_seqnum: ts.seqnum + num_rows - 1,
            timestamp: None,
            next_seqnum,
        };
    };

    if timestamp < ts.timestamp {
        return ValidateRequestResult::Reject {
            reason: "INVALID_TIMESTAMP",
        };
    };

    let next_seqnum = SeqNum {
        seqnum: ts.seqnum + num_rows,
        timestamp,
    };

    ValidateRequestResult::Accept {
        start_seqnum: ts.seqnum,
        end_seqnum: ts.seqnum + num_rows - 1,
        timestamp: timestamp.into(),
        next_seqnum,
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
    fn test_row_seqnum_previous() {
        let seqnum = SeqNum {
            seqnum: 0,
            timestamp: SystemTime::UNIX_EPOCH,
        };

        let previous_seqnum = seqnum.previous();

        assert_eq!(previous_seqnum.seqnum, 0);
        assert_eq!(previous_seqnum.timestamp, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn test_row_seqnum_validate() {
        let seqnum = SeqNum {
            seqnum: 123,
            timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(120),
        };

        // No messages in the batch.
        let req = CommitBatchRequest::new_with_timestamp(
            0,
            SystemTime::UNIX_EPOCH + Duration::from_secs(240),
        );
        assert_eq!(
            validate_timestamp_in_request(&seqnum, &req),
            ValidateRequestResult::Reject {
                reason: "EMPTY_BATCH",
            }
        );

        // Timestamp before the current seqnum.
        let req = CommitBatchRequest::new_with_timestamp(10, SystemTime::UNIX_EPOCH);
        assert_eq!(
            validate_timestamp_in_request(&seqnum, &req),
            ValidateRequestResult::Reject {
                reason: "INVALID_TIMESTAMP",
            }
        );

        // Same timestamp is fine.
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(120);
        let req = CommitBatchRequest::new_with_timestamp(10, ts);
        assert_eq!(
            validate_timestamp_in_request(&seqnum, &req),
            ValidateRequestResult::Accept {
                start_seqnum: 123,
                end_seqnum: 132,
                timestamp: Some(ts),
                next_seqnum: SeqNum {
                    seqnum: 133,
                    timestamp: ts
                }
            }
        );

        // Larger timestamp is also fine
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(121);
        let req = CommitBatchRequest::new_with_timestamp(10, ts);
        assert_eq!(
            validate_timestamp_in_request(&seqnum, &req),
            ValidateRequestResult::Accept {
                start_seqnum: 123,
                end_seqnum: 132,
                timestamp: Some(ts),
                next_seqnum: SeqNum {
                    seqnum: 133,
                    timestamp: ts
                }
            }
        );

        // Timestamp is larger than the current seqnum
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(122);
        let req = CommitBatchRequest::new_with_timestamp(10, ts);
        assert_eq!(
            validate_timestamp_in_request(&seqnum, &req),
            ValidateRequestResult::Accept {
                start_seqnum: 123,
                end_seqnum: 132,
                timestamp: Some(ts),
                next_seqnum: SeqNum {
                    seqnum: 133,
                    timestamp: ts
                }
            }
        );

        // None leaves the timestamp as is.
        let req = CommitBatchRequest::new(10);
        assert_eq!(
            validate_timestamp_in_request(&seqnum, &req),
            ValidateRequestResult::Accept {
                start_seqnum: 123,
                end_seqnum: 132,
                timestamp: None,
                next_seqnum: SeqNum {
                    seqnum: 133,
                    timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(120)
                }
            }
        );

        // Just in case test this too.
        let req = CommitBatchRequest::new(1);
        assert_eq!(
            validate_timestamp_in_request(&seqnum, &req),
            ValidateRequestResult::Accept {
                start_seqnum: 123,
                end_seqnum: 123,
                timestamp: None,
                next_seqnum: SeqNum {
                    seqnum: 124,
                    timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(120)
                }
            }
        );
    }
}
