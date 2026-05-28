//! This module contains utility methods and structs for handling time.
//!
//! This code comes directly from slatedb.
//!
//! Copyright 2024 Chris Riccomini
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

#[cfg(feature = "test-util")]
use std::sync::atomic::{AtomicI64, Ordering};
use std::{future::Future, pin::Pin, time::Duration};

use time::OffsetDateTime;

pub trait Clock: std::fmt::Debug + Send + Sync {
    /// Returns the current time.
    fn now(&self) -> time::OffsetDateTime;

    /// Advances the clock by the specified duration.
    #[cfg(feature = "test-util")]
    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Sleeps for the specified duration
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// A system clock implementation that uses tokio::time::Instant to measure time duration.
/// Utc::now() is used to track the initial timestamp (ms since Unix epoch). This DateTime
/// is used to convert the tokio::time::Instant to a DateTime when now() is called.
///
/// Note that, because we're using tokio::time::Instant, manipulating tokio's clock with
/// tokio::time::pause(), tokio::time::advance(), and so on will affect the
/// DefaultSystemClock's time as well.
#[derive(Debug, Clone, Copy)]
pub struct DefaultClock {
    initial_ts: OffsetDateTime,
    initial_instant: tokio::time::Instant,
}

impl DefaultClock {
    pub fn new() -> Self {
        Self {
            initial_ts: time::OffsetDateTime::now_utc(),
            initial_instant: tokio::time::Instant::now(),
        }
    }
}

impl Default for DefaultClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for DefaultClock {
    fn now(&self) -> time::OffsetDateTime {
        let elapsed = tokio::time::Instant::now().duration_since(self.initial_instant);
        self.initial_ts + elapsed
    }

    #[cfg(feature = "test-util")]
    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::advance(duration))
    }

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// A mock system clock implementation that uses an atomic i64 to track time.
/// The clock always starts at 0 (the Unix epoch). Time only advances when the
/// `advance` method is called.
#[derive(Debug)]
#[cfg(feature = "test-util")]
pub struct MockClock {
    /// The current timestamp in milliseconds since the Unix epoch.
    /// Can be negative to represent a time before the epoch.
    current_ts: AtomicI64,
}

#[cfg(feature = "test-util")]
impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "test-util")]
impl MockClock {
    pub fn new() -> Self {
        Self::with_time(0)
    }

    /// Creates a new mock system clock with the specified timestamp
    pub fn with_time(ts_millis: i64) -> Self {
        Self {
            current_ts: AtomicI64::new(ts_millis),
        }
    }

    /// Sets the current timestamp of the mock system clock
    pub fn set(&self, ts_millis: i64) {
        self.current_ts.store(ts_millis, Ordering::SeqCst);
    }
}

#[cfg(feature = "test-util")]
impl Clock for MockClock {
    #[allow(clippy::panic)]
    fn now(&self) -> OffsetDateTime {
        let current_ts = self.current_ts.load(Ordering::SeqCst);
        OffsetDateTime::from_unix_timestamp_nanos(current_ts as i128 * 1_000_000i128)
            .unwrap_or_else(|_| panic!("invalid timestamp: {}", current_ts))
    }

    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.current_ts
            .fetch_add(duration.as_millis() as i64, Ordering::SeqCst);
        Box::pin(async move {
            // An empty async block always returns Poll::Ready(()) because nothing inside
            // the block can yield control to other tasks. Calling advance() in a tight loop
            // would prevent other tasks from running in this case. Yielding control to other
            // tasks explicitly so we avoid this issue.
            tokio::task::yield_now().await;
        })
    }

    /// Sleeps for the specified duration. Note that sleep() does not advance the clock.
    /// Another thread or task must call advance() to advance the clock to unblock the sleep.
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let end_time = self.current_ts.load(Ordering::SeqCst) + duration.as_millis() as i64;
        Box::pin(async move {
            #[allow(clippy::while_immutable_condition)]
            while self.current_ts.load(Ordering::SeqCst) < end_time {
                tokio::task::yield_now().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "test-util")]
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_clock_default() {
        let clock = MockClock::default();
        insta::assert_debug_snapshot!(clock.now(), @"1970-01-01 0:00:00.0 +00:00:00");
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_set_now() {
        let clock = std::sync::Arc::new(MockClock::new());

        // Test positive timestamp
        let positive_ts = 1625097600000i64; // 2021-07-01T00:00:00Z in milliseconds
        clock.set(positive_ts);

        insta::assert_debug_snapshot!(clock.now(), @"2021-07-01 0:00:00.0 +00:00:00");

        // Test negative timestamp (before Unix epoch)
        let negative_ts = -1625097600000; // Before Unix epoch
        clock.set(negative_ts);

        insta::assert_debug_snapshot!(clock.now(), @"1918-07-04 0:00:00.0 +00:00:00");
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_advance() {
        let clock = std::sync::Arc::new(MockClock::new());
        let initial_ts = 1000;

        // Set initial time
        clock.clone().set(initial_ts);

        // Advance by 500ms
        let duration = Duration::from_millis(500);
        clock.clone().advance(duration).await;

        // Check that time advanced correctly
        insta::assert_debug_snapshot!(clock.now(), @"1970-01-01 0:00:01.5 +00:00:00");
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_sleep() {
        let clock = std::sync::Arc::new(MockClock::new());
        let initial_ts = 2000;

        // Set initial time
        clock.clone().set(initial_ts);

        // Start sleep for 1000ms
        let sleep_duration = Duration::from_millis(1000);
        let sleep_handle1 = clock.sleep(sleep_duration);
        let sleep_handle2 = clock.sleep(sleep_duration);
        let sleep_handle3 = clock.sleep(sleep_duration);

        // Verify sleep doesn't complete immediately
        assert!(
            timeout(Duration::from_millis(10), sleep_handle1)
                .await
                .is_err(),
            "Sleep should not complete until time advances"
        );

        // Advance clock by 500ms (not enough to complete sleep)
        clock.set(initial_ts + 500);
        assert!(
            timeout(Duration::from_millis(10), sleep_handle2)
                .await
                .is_err(),
            "Sleep should not complete when time has advanced by less than sleep duration"
        );

        // Advance clock by enough to complete sleep
        clock.set(initial_ts + 1000);
        assert!(
            timeout(Duration::from_millis(100), sleep_handle3)
                .await
                .is_ok(),
            "Sleep should complete when time has advanced by at least sleep duration"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_clock_now() {
        let clock = std::sync::Arc::new(DefaultClock::new());

        // Record initial time
        let initial_now = clock.now();

        // Sleep a bit
        let sleep_duration = Duration::from_millis(100);
        clock.clone().sleep(sleep_duration).await;

        // Check that time advances
        let new_now = clock.clone().now();
        assert_eq!(
            new_now,
            initial_now + sleep_duration,
            "DefaultClock now() should advance with time"
        );
    }

    #[tokio::test(start_paused = true)]
    #[cfg(feature = "test-util")]
    async fn test_default_clock_advance() {
        let clock = std::sync::Arc::new(DefaultClock::new());
        let start = clock.now();
        let duration = Duration::from_millis(500);
        clock.clone().advance(duration).await;

        // Check that time advanced correctly
        assert_eq!(
            start + duration,
            clock.now(),
            "DefaultClock should advance time by the specified duration"
        );
    }
}
