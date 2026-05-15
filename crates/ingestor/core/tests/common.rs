#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, Int32Array, StringArray},
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use bytesize::ByteSize;
use futures::{Stream, StreamExt, TryStreamExt};
use mockall::{automock, mock};
use object_store::ObjectStore;
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use wings_control_plane_core::{
    cluster_metadata::ClusterMetadata,
    table_metadata::{
        CommitBatchRequest, CommittedBatch, CompleteTaskRequest, CompleteTaskResponse,
        GetTableLocationRequest, ListPartitionsRequest, ListPartitionsResponse, TableLocation,
        TableMetadata, RequestTaskRequest, RequestTaskResponse,
    },
};
use wings_control_plane_sql::SqlControlPlane;
use wings_ingestor_core::{IngestionRequest, Ingestor, IngestorClient, Result, WriteBatchRequest};
use wings_object_store::{InMemoryFactory, ObjectStoreFactory};
use wings_resources::{
    AwsConfiguration, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName,
    NamespaceOptions, ObjectStoreConfiguration, ObjectStoreName, TenantName, Table, TableName,
    TableOptions,
};
use wings_schema::{DataType, Field, Schema, SchemaBuilder};

mock! {
    pub TableMetadataService {}

    #[async_trait::async_trait]
    impl TableMetadata for TableMetadataService {
        async fn commit(
            &self,
            namespace: NamespaceName,
            batches: Vec<CommitBatchRequest>,
        ) -> wings_control_plane_core::table_metadata::Result<Vec<CommittedBatch>>;

        async fn get_table_location(
            &self,
            request: GetTableLocationRequest,
        ) -> wings_control_plane_core::table_metadata::Result<Vec<TableLocation>>;

        async fn list_partitions(
            &self,
            request: ListPartitionsRequest,
        ) -> wings_control_plane_core::table_metadata::Result<ListPartitionsResponse>;

        async fn request_task(
            &self,
            request: RequestTaskRequest,
        ) -> wings_control_plane_core::table_metadata::Result<RequestTaskResponse>;

        async fn complete_task(
            &self,
            request: CompleteTaskRequest,
        ) -> wings_control_plane_core::table_metadata::Result<CompleteTaskResponse>;
    }
}

#[automock]
pub trait ObjectStorePutOpts {
    fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> impl Future<Output = object_store::Result<object_store::PutResult>> + Send;
}

impl std::fmt::Display for MockObjectStorePutOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockObjectStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for MockObjectStorePutOpts {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        ObjectStorePutOpts::put_opts(self, location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &object_store::path::Path,
        _opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(object_store::Error::NotImplemented)
    }

    async fn get_opts(
        &self,
        _location: &object_store::path::Path,
        _options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        Err(object_store::Error::NotImplemented)
    }

    async fn delete(&self, _location: &object_store::path::Path) -> object_store::Result<()> {
        Ok(())
    }

    fn list(
        &self,
        _prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        futures::stream::empty().boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        Ok(object_store::ListResult {
            objects: vec![],
            common_prefixes: vec![],
        })
    }

    async fn copy(
        &self,
        _from: &object_store::path::Path,
        _to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        Ok(())
    }

    async fn copy_if_not_exists(
        &self,
        _from: &object_store::path::Path,
        _to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        Ok(())
    }
}

pub struct MockObjectStoreFactory {
    store: Arc<dyn object_store::ObjectStore>,
}

impl MockObjectStoreFactory {
    pub fn new(store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for MockObjectStoreFactory {
    async fn create_object_store(
        &self,
        _object_store_name: ObjectStoreName,
    ) -> std::result::Result<Arc<dyn object_store::ObjectStore>, object_store::Error> {
        Ok(self.store.clone())
    }
}

#[derive(Debug, Serialize)]
pub struct ObjectMeta {
    pub path: String,
    pub size: u64,
}

pub struct TestIngestor {
    task: JoinHandle<()>,
    ct_guard: DropGuard,
    pub client: IngestorClient,
    pub cluster_meta: Arc<dyn ClusterMetadata>,
    object_store_factory: Arc<dyn ObjectStoreFactory>,
}

impl TestIngestor {
    pub async fn start(table_metadata: MockTableMetadataService) -> Self {
        let control_plane = Arc::new(SqlControlPlane::new_in_memory().await);
        let cluster_meta: Arc<dyn ClusterMetadata> = control_plane.clone();
        let table_metadata: Arc<dyn TableMetadata> = Arc::new(table_metadata);
        let object_store_factory: Arc<dyn ObjectStoreFactory> = Arc::new(InMemoryFactory::new());
        let ingestor = Ingestor::new(object_store_factory.clone(), table_metadata);
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

    pub async fn start_with_factory(
        table_metadata: MockTableMetadataService,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
    ) -> Self {
        let control_plane = Arc::new(SqlControlPlane::new_in_memory().await);
        let cluster_meta: Arc<dyn ClusterMetadata> = control_plane.clone();
        let table_metadata: Arc<dyn TableMetadata> = Arc::new(table_metadata);
        let ingestor = Ingestor::new(object_store_factory, table_metadata);
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
            object_store_factory: Arc::new(InMemoryFactory::new()),
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
    ) -> Result<Vec<CommittedBatch>> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.client.ingest(
                namespace,
                futures::stream::iter(requests.into_iter().map(IngestionRequest::from)),
            ),
        )
        .await
        .expect("ingestion timed out")
    }

    pub async fn ingest_stream(
        &self,
        namespace: Arc<Namespace>,
        requests: impl Stream<Item = WriteBatchRequest>,
    ) -> Result<Vec<CommittedBatch>> {
        self.client
            .ingest(namespace, requests.map(IngestionRequest::from))
            .await
    }

    pub async fn list_files(&self, namespace: &Namespace) -> Vec<ObjectMeta> {
        let store = self
            .object_store_factory
            .create_object_store(namespace.object_store.clone())
            .await
            .expect("create object store");
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list objects")
            .into_iter()
            .map(Into::into)
            .collect()
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

pub async fn initialize_test_table(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Table> {
    let table_name = TableName::new_unchecked("people", namespace.clone());
    cluster_meta
        .create_table(table_name, TableOptions::new(schema_without_partition()))
        .await
        .expect("create_table")
        .into()
}

pub async fn initialize_test_partitioned_table(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Table> {
    let table_name = TableName::new_unchecked("people_by_region", namespace.clone());
    cluster_meta
        .create_table(
            table_name,
            TableOptions::new_with_partition_key(schema_with_partition(), Some(0)),
        )
        .await
        .expect("create_table")
        .into()
}

pub fn people_records(table: &Table, people: &[(i32, &str, i32)]) -> RecordBatch {
    let ids = people.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let names = people.iter().map(|(_, name, _)| *name).collect::<Vec<_>>();
    let ages = people.iter().map(|(_, _, age)| *age).collect::<Vec<_>>();

    RecordBatch::try_new(
        table.arrow_schema_without_partition_field(),
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

pub fn mismatched_records(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "other",
        ArrowDataType::Int32,
        false,
    )]));
    let values = Arc::new(Int32Array::from((1..=num_rows as i32).collect::<Vec<_>>()));

    RecordBatch::try_new(schema, vec![values as ArrayRef]).expect("create mismatched batch")
}

pub fn sort_by_batch_id(responses: &mut [CommittedBatch]) {
    responses.sort_by_key(CommittedBatch::batch_id);
}

impl From<object_store::ObjectMeta> for ObjectMeta {
    fn from(m: object_store::ObjectMeta) -> Self {
        Self {
            path: m.location.to_string(),
            size: m.size,
        }
    }
}
