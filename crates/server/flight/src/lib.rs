//! Apache Arrow Flight server support for Wings.

mod datafusion_helpers;
mod error;
pub mod pb;
mod query;
mod service;
mod system_tables;
mod ticket;

pub use crate::{error::Error, service::ClusterFlightService};
