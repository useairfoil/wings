use wings_observability::Counter;

pub struct IngestorMetrics {
    pub written_rows: Counter<u64>,
    pub written_bytes: Counter<u64>,
}

impl Default for IngestorMetrics {
    fn default() -> Self {
        let meter = wings_observability::meter("ingestion");
        Self {
            written_rows: meter
                .u64_counter("write.rows")
                .with_unit("{row}")
                .with_description("number of rows written")
                .build(),
            written_bytes: meter
                .u64_counter("write.bytes")
                .with_unit("By")
                .with_description("number of bytes written")
                .build(),
        }
    }
}
