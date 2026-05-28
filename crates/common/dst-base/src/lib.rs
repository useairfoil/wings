mod clock;

pub use self::clock::{Clock, DefaultClock};
#[cfg(feature = "test-util")]
pub use self::clock::MockClock;
