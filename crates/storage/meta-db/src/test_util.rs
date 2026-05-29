use std::sync::Arc;

use object_store::ObjectStore;
use wings_dst_base::{MockClock, ThreadRng};
use wings_resources::{
    DataLakeConfiguration, NamespaceOptions, ObjectStoreConfiguration, ParquetConfiguration,
    S3CompatibleConfiguration,
};
use wings_secret_manager::memory::MemorySecretManager;

use crate::ClusterStore;

pub fn new_test_cluster_store(object_store: Arc<dyn ObjectStore>) -> ClusterStore {
    ClusterStore {
        object_store,
        secret_manager: Arc::new(MemorySecretManager::new()),
        clock: Arc::new(MockClock::with_time(1_700_000_000_000)),
        rng: Arc::new(ThreadRng::new(42)),
    }
}

pub fn new_test_namespace_options() -> NamespaceOptions {
    NamespaceOptions {
        object_store: ObjectStoreConfiguration::S3Compatible(S3CompatibleConfiguration {
            bucket_name: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            access_key_id: "access-key-id".to_string(),
            secret_access_key: "secret-access-key".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            region: Some("test-region".to_string()),
            allow_http: true,
        }),
        lake: DataLakeConfiguration::Parquet(ParquetConfiguration::default()),
    }
}
