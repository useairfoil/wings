use wings_observability::Counter;

pub struct IngestionMetrics {
    pub written_bytes: Counter<u64>,
    pub written_rows: Counter<u64>,
}

impl Default for IngestionMetrics {
    fn default() -> Self {
        let meter = wings_observability::meter("ingestion");
        Self {
            written_bytes: meter
                .u64_counter("server.push.bytes")
                .with_unit("By")
                .with_description("bytes written to topics")
                .build(),
            written_rows: meter
                .u64_counter("server.push.rows")
                .with_unit("{row}")
                .with_description("number of rows written to topics")
                .build(),
        }
    }
}
