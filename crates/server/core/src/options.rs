use std::time::Duration;

use datafusion::{common::extensions_options, config::ConfigExtension, prelude::SessionConfig};
use wings_control_plane_core::log_metadata::GetLogLocationOptions;

extensions_options! {
    pub struct FetchOptions {
        /// The minimum number of rows to fetch
        min_rows: usize, default = 1
        /// The maximum number of rows to fetch
        max_rows: usize, default = 10_000
        /// Wait this long to fetch at least `min_rows` rows
        timeout_ms: u64, default = 250
    }
}

impl ConfigExtension for FetchOptions {
    const PREFIX: &'static str = "wings";
}

impl FetchOptions {
    pub fn get_log_location_options(&self) -> GetLogLocationOptions {
        let deadline = Duration::from_millis(self.timeout_ms);
        GetLogLocationOptions {
            deadline,
            min_rows: self.min_rows,
            max_rows: self.max_rows,
        }
    }
}

pub trait SessionConfigExt {
    fn fetch_options(&self) -> &FetchOptions;
}

impl SessionConfigExt for SessionConfig {
    fn fetch_options(&self) -> &FetchOptions {
        self.options()
            .extensions
            .get::<FetchOptions>()
            .expect("FetchOptions not registered with SessionConfig")
    }
}
