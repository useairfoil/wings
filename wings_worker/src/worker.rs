use std::{sync::Arc, time::Duration};

use futures::TryStreamExt;
use object_store::{PutMode, PutOptions, PutPayload, path::Path};
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use wings_control_plane::{
    cluster_metadata::cache::{NamespaceCache, TopicCache},
    log_metadata::{
        CompactionTask, CompleteTaskRequest, LogMetadata, RequestTaskRequest, RequestTaskResponse,
        Task, TaskMetadata,
    },
    paths::format_parquet_path,
};
use wings_object_store::ObjectStoreFactory;
use wings_server_core::query::{NamespaceProviderFactory, TopicTableProvider};

use crate::error::{
    ClusterMetadataSnafu, DataFusionSnafu, LogMetadataSnafu, ObjectStoreSnafu, ParquetPathSnafu,
    ParquetSnafu, Result,
};

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct Worker {
    topic_cache: TopicCache,
    namespace_cache: NamespaceCache,
    log_meta: Arc<dyn LogMetadata>,
    object_store_factory: Arc<dyn ObjectStoreFactory>,
    namespace_provider_factory: NamespaceProviderFactory,
}

impl Worker {
    pub fn new(
        topic_cache: TopicCache,
        namespace_cache: NamespaceCache,
        log_meta: Arc<dyn LogMetadata>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        namespace_provider_factory: NamespaceProviderFactory,
    ) -> Self {
        Self {
            topic_cache,
            namespace_cache,
            log_meta,
            object_store_factory,
            namespace_provider_factory,
        }
    }

    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        'outer: loop {
            let RequestTaskResponse { task } = self
                .log_meta
                .request_task(RequestTaskRequest::default())
                .await
                .context(LogMetadataSnafu {
                    operation: "request_task",
                })?;

            let Some(task) = task else {
                continue;
            };

            loop {
                // Keep retrying until the task is executed successfully, since tasks are not supposed to fail.
                match self.execute_task(&task, ct.clone()).await {
                    Ok(_) => {
                        // The task finished because it was cancelled.
                        if ct.is_cancelled() {
                            return Ok(());
                        }

                        // Get the next task.
                        continue 'outer;
                    }
                    Err(err) => {
                        warn!(err = ?err, "Task execution failed. Retrying.");
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        }
    }

    async fn execute_task(&self, task: &Task, ct: CancellationToken) -> Result<()> {
        match task {
            Task::Compaction { metadata, task } => {
                self.execute_compaction_task(metadata, task, ct).await
            }
        }
    }

    async fn execute_compaction_task(
        &self,
        metadata: &TaskMetadata,
        task: &CompactionTask,
        ct: CancellationToken,
    ) -> Result<()> {
        let namespace_name = task.topic_name.parent().clone();

        let topic_ref = match self.topic_cache.get(task.topic_name.clone()).await {
            Ok(topic_ref) => topic_ref,
            Err(err) => {
                if err.is_not_found() {
                    warn!(
                        topic = %task.topic_name,
                        "received compaction task for non-existent topic"
                    );
                    return Ok(());
                }
                return Err(err).context(ClusterMetadataSnafu {
                    operation: "get_topic",
                });
            }
        };

        let namespace_ref = self
            .namespace_cache
            .get(namespace_name.clone())
            .await
            .context(ClusterMetadataSnafu {
                operation: "get_namespace",
            })?;

        let table_schema = TopicTableProvider::output_schema(topic_ref.schema());

        let provider = self
            .namespace_provider_factory
            .create_provider(namespace_name)
            .await
            .context(DataFusionSnafu {})?;

        let ctx = provider
            .new_session_context()
            .await
            .context(DataFusionSnafu {})?;

        // TODO: rewrite all of this to build the plan programatically
        let partition_query = if let Some(field) = topic_ref.partition_field() {
            format!(
                "AND {} = {}",
                field.name(),
                task.partition_value
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_default()
            )
        } else {
            String::new()
        };

        let query = format!(
            "SELECT * FROM {} WHERE __offset__ BETWEEN {} AND {} {} ORDER BY __offset__ ASC",
            task.topic_name.id(),
            task.start_offset,
            task.end_offset,
            partition_query
        );

        println!("Compaction query: {}", query);

        let df = ctx.sql(&query).await.context(DataFusionSnafu {})?;

        let mut stream = df.execute_stream().await.context(DataFusionSnafu {})?;

        let mut writer = {
            let partition_value = task.partition_value.as_ref().map(|v| v.to_string());
            let kv_metadata = vec![
                KeyValue::new("WINGS:topic-name".to_string(), task.topic_name.to_string()),
                KeyValue::new("WINGS:partition-value".to_string(), partition_value),
            ];

            let write_properties = WriterProperties::builder()
                .set_key_value_metadata(kv_metadata.into())
                .build();

            let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
            ArrowWriter::try_new(buffer, table_schema.clone(), write_properties.into())
                .context(ParquetSnafu {})?
        };

        while let Some(batch) = stream.try_next().await.context(DataFusionSnafu {})? {
            if ct.is_cancelled() {
                return Ok(());
            }

            writer.write(&batch).context(ParquetSnafu {})?;
        }

        if ct.is_cancelled() {
            return Ok(());
        }

        let _parquet_metadata = writer.finish().context(ParquetSnafu {})?;
        let output_bytes = writer.into_inner().context(ParquetSnafu {})?;

        let object_store = self
            .object_store_factory
            .create_object_store(namespace_ref.object_store.clone())
            .await
            .context(ObjectStoreSnafu {})?;

        let file_ref: Path = format_parquet_path(&namespace_ref.name)
            .with_offset_range(task.start_offset, task.end_offset)
            .with_partition(topic_ref.partition_field(), task.partition_value.as_ref())
            .build()
            .context(ParquetPathSnafu {})?
            .into();

        debug!(
            task_id = metadata.task_id,
            file_ref = %file_ref,
            "Uploading parquet file to storage"
        );

        object_store
            .put_opts(
                &file_ref,
                PutPayload::from_bytes(output_bytes.into()),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .context(ObjectStoreSnafu {})?;

        // Later: update datalake catalog with this data.
        // Notice that multiple partitions may be compacted at the same time
        // so we need to be careful updating the catalog concurrently.

        // TODO: we should include the compacted range and file reference in the complete task request.
        self.log_meta
            .complete_task(CompleteTaskRequest::new_completed(metadata.task_id.clone()))
            .await
            .context(LogMetadataSnafu {
                operation: "complete_task",
            })?;

        info!(
            task_id = metadata.task_id,
            file_ref = %file_ref,
            "Compaction task completed"
        );

        Ok(())
    }
}
