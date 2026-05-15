use wings_observability::Counter;

#[derive(Clone)]
pub struct FlightServerMetrics {
    pub fetch_rows: Counter<u64>,
    pub fetch_bytes: Counter<u64>,
}

impl Default for FlightServerMetrics {
    fn default() -> Self {
        let meter = wings_observability::meter("flight");
        Self {
            fetch_rows: meter
                .u64_counter("server.fetch.rows")
                .with_unit("{row}")
                .with_description("number of rows fetched by clients")
                .build(),
            fetch_bytes: meter
                .u64_counter("server.fetch.bytes")
                .with_unit("By")
                .with_description("data (in bytes) fetched by clients")
                .build(),
        }
    }
}
