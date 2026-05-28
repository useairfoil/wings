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
use rand::{Rng, RngExt};
use ulid::Ulid;
use uuid::Uuid;

use crate::Clock;

/// Trait for generating UUIDs and ULIDs from a random number generator.
pub trait IdGenerator {
    fn gen_uuid(&mut self) -> Uuid;
    fn gen_ulid(&mut self, clock: &dyn Clock) -> Ulid;
}

impl<R: Rng> IdGenerator for R {
    /// Generates a random UUID using the provided RNG.
    fn gen_uuid(&mut self) -> Uuid {
        let mut bytes = [0u8; 16];
        self.fill_bytes(&mut bytes);
        // set version = 4
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        // set variant = RFC4122
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    /// Generates a random ULID using the provided RNG. The clock is used to generate
    /// the timestamp component of the ULID.
    fn gen_ulid(&mut self, clock: &dyn Clock) -> Ulid {
        let now = u64::try_from(clock.now().unix_timestamp_nanos() / 1_000_000)
            .expect("timestamp outside u64 range in gen_ulid");
        let random_bytes = self.random::<u128>();
        Ulid::from_parts(now, random_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "test-util")]
    use crate::MockClock;
    use crate::ThreadRng;

    #[test]
    fn gen_uuid_sets_version_and_variant_bits() {
        let thread_rng = ThreadRng::new(42);
        let mut rng = thread_rng.rng();

        let uuid = rng.gen_uuid();

        insta::assert_snapshot!(uuid.to_string(), @"233c1def-caf6-4ae8-b149-a5a5b203a354");
        let bytes = uuid.as_bytes();
        assert_eq!(bytes[6] >> 4, 4, "UUID version should be 4");
        assert_eq!(bytes[8] >> 6, 2, "UUID variant should be RFC 4122");
    }

    #[test]
    #[cfg(feature = "test-util")]
    fn gen_ulid_uses_clock_timestamp_and_rng_randomness() {
        let clock = MockClock::with_time(1_625_097_600_000);
        let thread_rng = ThreadRng::new(42);
        let mut rng = thread_rng.rng();

        let ulid = rng.gen_ulid(&clock);

        insta::assert_snapshot!(ulid.to_string(), @"01F9FNTZ0094RYH2QPSBQHTF13");
        assert_eq!(ulid.timestamp_ms(), 1_625_097_600_000);
    }

    #[test]
    #[cfg(feature = "test-util")]
    #[should_panic(expected = "timestamp outside u64 range in gen_ulid")]
    fn gen_ulid_panics_for_negative_timestamps() {
        let clock = MockClock::with_time(-1_000);
        let thread_rng = ThreadRng::new(42);
        let mut rng = thread_rng.rng();

        let _ = rng.gen_ulid(&clock);
    }
}
