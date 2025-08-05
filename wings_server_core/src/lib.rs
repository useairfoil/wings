//! Core server functionality for Wings message queue.
//!
//! This crate provides the core functionality for fetching messages from Wings,
//! including the `Fetcher` struct and related types.

pub mod error;
pub mod fetcher;

pub use error::*;
pub use fetcher::*;
