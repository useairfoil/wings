#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, Int32Array, StringArray},
    record_batch::RecordBatch,
};
use bytesize::ByteSize;
use futures::TryStreamExt;
use mockall::mock;
use object_store::ObjectStore;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use wings_control_plane_core::{
    cluster_metadata::ClusterMetadata,
    log_metadata::{
        CommitBatchRequest, CommittedBatch, CompleteTaskRequest, CompleteTaskResponse,
        GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation,
        LogMetadata, RequestTaskRequest, RequestTaskResponse,
    },
};
use wings_control_plane_sql::SqlControlPlane;
use wings_ingestor_core::{Ingestor, IngestorClient, Result, WriteBatchRequest};
use wings_object_store::{InMemoryFactory, ObjectStoreFactory};
use wings_resources::{
    AwsConfiguration, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName,
    NamespaceOptions, ObjectStoreConfiguration, ObjectStoreName, TenantName, Topic, TopicName,
    TopicOptions,
};
use wings_schema::{DataType, Field, Schema, SchemaBuilder};

mock! {
    pub LogMetadataService {}

    #[async_trait::async_trait]
    impl LogMetadata for LogMetadataService {
        async fn commit(
            &self,
            namespace: NamespaceName,
            batches: Vec<CommitBatchRequest>,
        ) -> wings_control_plane_core::log_metadata::Result<Vec<CommittedBatch>>;

        async fn get_log_location(
            &self,
            request: GetLogLocationRequest,
        ) -> wings_control_plane_core::log_metadata::Result<Vec<LogLocation>>;

        async fn list_partitions(
            &self,
            request: ListPartitionsRequest,
        ) -> wings_control_plane_core::log_metadata::Result<ListPartitionsResponse>;

        async fn request_task(
            &self,
            request: RequestTaskRequest,
        ) -> wings_control_plane_core::log_metadata::Result<RequestTaskResponse>;

        async fn complete_task(
            &self,
            request: CompleteTaskRequest,
        ) -> wings_control_plane_core::log_metadata::Result<CompleteTaskResponse>;
    }
}

pub struct TestIngestor {
    task: JoinHandle<()>,
    ct_guard: DropGuard,
    pub client: IngestorClient,
    pub cluster_meta: Arc<dyn ClusterMetadata>,
    object_store_factory: Arc<InMemoryFactory>,
}

impl TestIngestor {
    pub async fn start(log_meta: MockLogMetadataService) -> Self {
        let control_plane = Arc::new(SqlControlPlane::new_in_memory().await);
        let cluster_meta: Arc<dyn ClusterMetadata> = control_plane.clone();
        let log_meta: Arc<dyn LogMetadata> = Arc::new(log_meta);
        let object_store_factory = Arc::new(InMemoryFactory::new());
        let ingestor = Ingestor::new(
            object_store_factory.clone() as Arc<dyn ObjectStoreFactory>,
            log_meta,
        );
        let client = ingestor.client();
        let ct = CancellationToken::new();
        let ct_guard = ct.clone().drop_guard();
        let task = tokio::spawn(async move {
            ingestor.run(ct).await.expect("ingestor run");
        });

        Self {
            task,
            ct_guard,
            client,
            cluster_meta,
            object_store_factory,
        }
    }

    pub async fn shutdown(self) {
        drop(self.ct_guard);
        self.task.await.expect("ingestor terminated");
    }

    pub async fn ingest(
        &self,
        namespace: Arc<Namespace>,
        requests: Vec<WriteBatchRequest>,
    ) -> Result<Vec<wings_control_plane_core::log_metadata::CommittedBatch>> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.client
                .ingest(namespace, futures::stream::iter(requests)),
        )
        .await
        .expect("ingestion timed out")
    }

    pub async fn assert_folio_written(&self, namespace: &Namespace) {
        let store = self
            .object_store_factory
            .get(namespace.object_store.clone());
        let objects = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list objects");

        assert_eq!(objects.len(), 1);

        let data = store
            .get(&objects[0].location)
            .await
            .expect("get folio")
            .bytes()
            .await
            .expect("read folio");

        assert!(!data.is_empty());
    }
}

pub async fn initialize_test_namespace(cluster_meta: &Arc<dyn ClusterMetadata>) -> Arc<Namespace> {
    let tenant_name = TenantName::new_unchecked("test");
    cluster_meta
        .create_tenant(tenant_name.clone())
        .await
        .expect("create_tenant");

    let object_store_name = ObjectStoreName::new_unchecked("test-cred", tenant_name.clone());
    let aws_config = AwsConfiguration {
        bucket_name: "test".to_string(),
        access_key_id: Default::default(),
        secret_access_key: Default::default(),
        prefix: None,
        region: None,
    };
    cluster_meta
        .create_object_store(
            object_store_name.clone(),
            ObjectStoreConfiguration::Aws(aws_config),
        )
        .await
        .expect("create_object_store");

    let data_lake_name = DataLakeName::new_unchecked("test-data-lake", tenant_name.clone());
    cluster_meta
        .create_data_lake(
            data_lake_name.clone(),
            DataLakeConfiguration::Parquet(Default::default()),
        )
        .await
        .expect("create_data_lake");

    let namespace_name = NamespaceName::new_unchecked("test-ns", tenant_name);
    let namespace = cluster_meta
        .create_namespace(
            namespace_name,
            NamespaceOptions::new(object_store_name, data_lake_name)
                .with_flush_size(ByteSize::mb(8))
                .with_flush_interval(Duration::from_millis(10)),
        )
        .await
        .expect("create_namespace");

    namespace.into()
}

pub async fn initialize_test_topic(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Topic> {
    let topic_name = TopicName::new_unchecked("people", namespace.clone());
    cluster_meta
        .create_topic(topic_name, TopicOptions::new(schema_without_partition()))
        .await
        .expect("create_topic")
        .into()
}

pub async fn initialize_test_partitioned_topic(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Topic> {
    let topic_name = TopicName::new_unchecked("people_by_region", namespace.clone());
    cluster_meta
        .create_topic(
            topic_name,
            TopicOptions::new_with_partition_key(schema_with_partition(), Some(0)),
        )
        .await
        .expect("create_topic")
        .into()
}

pub fn people_records(topic: &Topic, people: &[(i32, &str, i32)]) -> RecordBatch {
    let ids = people.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let names = people.iter().map(|(_, name, _)| *name).collect::<Vec<_>>();
    let ages = people.iter().map(|(_, _, age)| *age).collect::<Vec<_>>();

    RecordBatch::try_new(
        topic.arrow_schema_without_partition_field(),
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int32Array::from(ages)) as ArrayRef,
        ],
    )
    .expect("create people record batch")
}

fn schema_without_partition() -> Schema {
    SchemaBuilder::new(vec![
        Field::new("id", 1, DataType::Int32, false),
        Field::new("name", 2, DataType::Utf8, false),
        Field::new("age", 3, DataType::Int32, false),
    ])
    .build()
    .expect("schema without partition")
}

fn schema_with_partition() -> Schema {
    SchemaBuilder::new(vec![
        Field::new("region_id", 0, DataType::Int64, false),
        Field::new("id", 1, DataType::Int32, false),
        Field::new("name", 2, DataType::Utf8, false),
        Field::new("age", 3, DataType::Int32, false),
    ])
    .build()
    .expect("schema with partition")
}

pub fn assert_accepted_batch(
    batch: &wings_control_plane_core::log_metadata::CommittedBatch,
    start_offset: u64,
    end_offset: u64,
) {
    match batch {
        wings_control_plane_core::log_metadata::CommittedBatch::Accepted(info) => {
            assert_eq!(info.start_offset, start_offset);
            assert_eq!(info.end_offset, end_offset);
        }
        other => panic!("expected accepted batch, got {other:?}"),
    }
}
