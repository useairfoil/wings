mod clock;

#[cfg(feature = "test-util")]
pub use self::clock::MockClock;
pub use self::clock::{Clock, DefaultClock};
