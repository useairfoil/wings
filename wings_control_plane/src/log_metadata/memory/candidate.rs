//! Candidate task queue for managing task polling instructions.
//!
//! This module provides a queue for managing candidate tasks that instruct
//! external callers which service to poll for tasks next. Tasks can be queued
//! for immediate availability or delayed until a specific time.

use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;
use tokio_util::time::delay_queue::DelayQueue;

use crate::resources::{PartitionValue, TopicName};

/// A candidate task that instructs which service to poll for tasks next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateTask {
    /// The topic may have a task ready.
    Topic(TopicName),
    /// The topic's partition may have a task ready.
    Partition(TopicName, Option<PartitionValue>),
}

/// A queue for managing candidate tasks with timing control.
///
/// This queue allows tasks to be queued for immediate availability or delayed
/// until a specific time. It uses tokio's `DelayQueue` internally for timing
/// management and a `VecDeque` for available tasks.
#[derive(Debug)]
pub struct CandidateTaskQueue {
    /// Delay queue for managing when tasks become available.
    delay_queue: DelayQueue<CandidateTask>,
    /// Queue of available tasks ready to be returned.
    available_queue: VecDeque<CandidateTask>,
}

impl CandidateTaskQueue {
    /// Create a new candidate task queue.
    pub fn new() -> Self {
        Self {
            delay_queue: DelayQueue::new(),
            available_queue: VecDeque::new(),
        }
    }

    /// Queue a candidate task for later availability.
    ///
    /// # Arguments
    /// * `candidate` - The candidate task to queue
    /// * `duration` - How long to wait before the task becomes available
    pub fn queue(&mut self, candidate: CandidateTask, duration: Duration) {
        let deadline = Instant::now() + duration;
        self.queue_at(candidate, deadline);
    }

    /// Queue a candidate task for availability at a specific instant.
    ///
    /// # Arguments
    /// * `candidate` - The candidate task to queue
    /// * `instant` - When the task should become available
    pub fn queue_at(&mut self, candidate: CandidateTask, instant: Instant) {
        self.delay_queue.insert_at(candidate, instant);
    }

    /// Queue a candidate task for immediate availability.
    ///
    /// # Arguments
    /// * `candidate` - The candidate task to queue
    pub fn queue_immediate(&mut self, candidate: CandidateTask) {
        self.available_queue.push_back(candidate);
    }

    /// Return the next available candidate task, if any.
    ///
    /// This function first checks for any new tasks becoming available in the delay
    /// queue (using the `peek` method in a loop). Tasks that are available are then
    /// removed from the delay queue and put in the available vec dequeue. Then the
    /// first task in the available vec dequeue is popped and returned to the caller.
    ///
    /// This function is NOT async and NOT blocking.
    pub fn next_candidate(&mut self) -> Option<CandidateTask> {
        // Check for expired tasks in the delay queue
        let now = Instant::now();
        while let Some(key) = self.delay_queue.peek() {
            if self.delay_queue.deadline(&key) > now {
                break;
            }

            let expired = self.delay_queue.remove(&key);
            self.available_queue.push_back(expired.into_inner());
        }

        // Return the next available task
        self.available_queue.pop_front()
    }
}

impl Default for CandidateTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_topic(name: &str) -> TopicName {
        // For tests, we'll use a default namespace
        use crate::resources::NamespaceName;
        let namespace =
            NamespaceName::parse("tenants/default/namespaces/default").expect("valid namespace");
        TopicName::new(name.to_string(), namespace).expect("valid topic name")
    }

    fn create_test_partition(value: &str) -> PartitionValue {
        PartitionValue::String(value.to_string())
    }

    #[tokio::test(start_paused = true)]
    async fn test_queue_immediate() {
        let mut queue = CandidateTaskQueue::new();
        let topic = create_test_topic("test-topic");
        let candidate = CandidateTask::Topic(topic.clone());

        queue.queue_immediate(candidate.clone());

        let result = queue.next_candidate();
        assert_eq!(result, Some(candidate));

        // Should return None when queue is empty
        let result = queue.next_candidate();
        assert_eq!(result, None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_queue_with_delay() {
        let mut queue = CandidateTaskQueue::new();
        let topic = create_test_topic("test-topic");
        let candidate = CandidateTask::Topic(topic.clone());

        // Queue with 5 second delay
        queue.queue(candidate.clone(), Duration::from_secs(5));

        // Should not be available immediately
        let result = queue.next_candidate();
        assert_eq!(result, None);

        // Advance time by 5 seconds
        tokio::time::advance(Duration::from_secs(5)).await;

        // Now should be available
        let result = queue.next_candidate();
        assert_eq!(result, Some(candidate));
    }

    #[tokio::test(start_paused = true)]
    async fn test_queue_at_specific_instant() {
        let mut queue = CandidateTaskQueue::new();
        let topic = create_test_topic("test-topic");
        let candidate = CandidateTask::Topic(topic.clone());

        let target_time = Instant::now() + Duration::from_secs(10);
        queue.queue_at(candidate.clone(), target_time);

        // Should not be available before target time
        let result = queue.next_candidate();
        assert_eq!(result, None);

        // Advance time by 9 seconds (still not available)
        tokio::time::advance(Duration::from_secs(9)).await;
        let result = queue.next_candidate();
        assert_eq!(result, None);

        // Advance time by 1 more second (total 10)
        tokio::time::advance(Duration::from_secs(1)).await;

        // Now should be available
        let result = queue.next_candidate();
        assert_eq!(result, Some(candidate));
    }

    #[tokio::test(start_paused = true)]
    async fn test_mixed_queue_operations() {
        let mut queue = CandidateTaskQueue::new();
        let topic1 = create_test_topic("topic-1");
        let topic2 = create_test_topic("topic-2");
        let partition_value = create_test_partition("partition-1");

        let immediate_candidate = CandidateTask::Topic(topic1.clone());
        let delayed_candidate = CandidateTask::Topic(topic2.clone());
        let partition_candidate =
            CandidateTask::Partition(topic1.clone(), Some(partition_value.clone()));

        // Queue immediate task
        queue.queue_immediate(immediate_candidate.clone());

        // Queue delayed task (3 seconds)
        queue.queue(delayed_candidate.clone(), Duration::from_secs(3));

        // Queue partition task (1 second)
        queue.queue(partition_candidate.clone(), Duration::from_secs(1));

        // Should get immediate task first
        let result = queue.next_candidate();
        assert_eq!(result, Some(immediate_candidate));

        // Should get partition task after 1 second
        tokio::time::advance(Duration::from_secs(1)).await;
        let result = queue.next_candidate();
        assert_eq!(result, Some(partition_candidate));

        // Should get delayed task after 2 more seconds (total 3)
        tokio::time::advance(Duration::from_secs(2)).await;
        let result = queue.next_candidate();
        assert_eq!(result, Some(delayed_candidate));

        // Should be empty now
        let result = queue.next_candidate();
        assert_eq!(result, None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_multiple_candidates_same_time() {
        let mut queue = CandidateTaskQueue::new();
        let topic1 = create_test_topic("topic-1");
        let topic2 = create_test_topic("topic-2");

        let candidate1 = CandidateTask::Topic(topic1.clone());
        let candidate2 = CandidateTask::Topic(topic2.clone());

        // Queue both for the same time
        queue.queue(candidate1.clone(), Duration::from_secs(5));
        queue.queue(candidate2.clone(), Duration::from_secs(5));

        // Advance time
        tokio::time::advance(Duration::from_secs(5)).await;

        // Should get both in any order
        let result1 = queue.next_candidate().unwrap();
        let result2 = queue.next_candidate().unwrap();

        assert!(result1 != result2);
        assert!(result1 == candidate1 || result1 == candidate2);
        assert!(result2 == candidate1 || result2 == candidate2);

        // Should be empty
        let result = queue.next_candidate();
        assert_eq!(result, None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_empty_queue() {
        let mut queue = CandidateTaskQueue::new();

        let result = queue.next_candidate();
        assert_eq!(result, None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_partition_candidates() {
        let mut queue = CandidateTaskQueue::new();
        let topic = create_test_topic("test-topic");
        let partition_value = create_test_partition("partition-1");

        let partition_candidate =
            CandidateTask::Partition(topic.clone(), Some(partition_value.clone()));
        let topic_candidate = CandidateTask::Topic(topic.clone());

        // Queue both types
        queue.queue_immediate(partition_candidate.clone());
        queue.queue_immediate(topic_candidate.clone());

        // Should get them in order
        let result1 = queue.next_candidate();
        let result2 = queue.next_candidate();

        assert_eq!(result1, Some(partition_candidate));
        assert_eq!(result2, Some(topic_candidate));
    }
}
