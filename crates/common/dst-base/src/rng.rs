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
use std::{
    cell::RefCell,
    sync::atomic::{AtomicU64, Ordering},
};

use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use thread_local::ThreadLocal;

#[derive(Debug)]
pub struct ThreadRng {
    seed_counter: AtomicU64,
    thread_rng: ThreadLocal<RefCell<Xoroshiro128PlusPlus>>,
}

impl ThreadRng {
    pub fn new(seed: u64) -> Self {
        Self {
            seed_counter: AtomicU64::new(seed),
            thread_rng: ThreadLocal::new(),
        }
    }

    pub fn rng(&self) -> std::cell::RefMut<'_, impl Rng> {
        self.thread_rng
            .get_or(|| {
                let seed = self.seed_counter.fetch_add(1, Ordering::Relaxed);
                RefCell::new(Xoroshiro128PlusPlus::seed_from_u64(seed))
            })
            .borrow_mut()
    }
}

impl Default for ThreadRng {
    fn default() -> Self {
        Self::new(rand::random())
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::BorrowMut, collections::HashSet, sync::Arc, thread};

    use super::*;

    #[test]
    fn test_deterministic_behavior() {
        // Same seed should produce same sequence of random numbers
        let rng1 = ThreadRng::new(42);
        let rng2 = ThreadRng::new(42);

        let mut values1 = Vec::new();
        let mut values2 = Vec::new();

        // Generate some random numbers
        for _ in 0..10 {
            values1.push(rng1.rng().next_u64());
            values2.push(rng2.rng().next_u64());
        }

        println!("values1: {:?}", values1);
        println!("values2: {:?}", values2);

        assert_eq!(values1, values2);
    }

    #[test]
    fn test_different_seeds_produce_different_sequences() {
        let rng1 = ThreadRng::new(42);
        let rng2 = ThreadRng::new(43);

        let mut values1 = Vec::new();
        let mut values2 = Vec::new();

        // Generate some random numbers
        for _ in 0..10 {
            values1.push(rng1.rng().next_u64());
            values2.push(rng2.rng().next_u64());
        }

        println!("values1: {:?}", values1);
        println!("values2: {:?}", values2);

        // Very small chance this would randomly fail, but practically zero
        assert_ne!(values1, values2);
    }

    #[test]
    fn test_thread_rngs_differ_across_threads() {
        // Create a ThreadRng instance with a known seed
        let rand = Arc::new(ThreadRng::new(100));

        // Number of threads to spawn
        let n_threads = 4;

        // Collect first random number from each thread
        let thread_handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let rand = Arc::clone(&rand);

                thread::spawn(move || {
                    // Get the pointer to this thread's RNG and return it as a
                    // usize. This is used to verify that each thread has a
                    // different RNG.
                    let ptr = &mut *(rand.rng().borrow_mut()) as *mut _;
                    ptr as usize
                })
            })
            .collect();

        // Collect results from all threads
        let results: Vec<usize> = thread_handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        // Create a HashSet to check for duplicates
        let unique_values: HashSet<_> = results.iter().cloned().collect();

        // Each thread should get a different RNG with a different seed,
        // so we should have n_threads unique values
        assert_eq!(
            unique_values.len(),
            n_threads,
            "Expected {} unique random values from threads, got {}. \
                   This indicates thread RNGs are not different across threads.",
            n_threads,
            unique_values.len()
        );
    }
}
