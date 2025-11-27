use wings_observability::UpDownCounter;

#[derive(Debug)]
pub struct ClusterMetadataMetrics {
    pub tenants_count: UpDownCounter<i64>,
    pub namespaces_count: UpDownCounter<i64>,
    pub topics_count: UpDownCounter<i64>,
    pub object_store_credentials_count: UpDownCounter<i64>,
}

impl Default for ClusterMetadataMetrics {
    fn default() -> Self {
        let meter = wings_observability::meter("metadata");

        Self {
            tenants_count: meter
                .i64_up_down_counter("metadata.tenants.count")
                .with_description("the number of tenants")
                .build(),
            namespaces_count: meter
                .i64_up_down_counter("metadata.namespaces.count")
                .with_description("the number of namespaces")
                .build(),
            topics_count: meter
                .i64_up_down_counter("metadata.topics.count")
                .with_description("the number of topics")
                .build(),
            object_store_credentials_count: meter
                .i64_up_down_counter("metadata.object_store_credentials.count")
                .with_description("the number of object store credentials")
                .build(),
        }
    }
}
