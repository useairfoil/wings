use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use object_store::{PutMode, PutOptions, PutPayload};
use wings_control_plane::{
    log_metadata::{CommitBatchRequest, CommitPageRequest},
    object_store::ObjectStoreFactory,
    paths::format_folio_path,
    resources::{NamespaceName, NamespaceRef},
};

use crate::{
    WriteBatchError,
    batcher::NamespaceFolio,
    error::Result,
    write::{ReplyWithWriteBatchError, WithReplyChannel},
};

#[derive(Debug)]
pub struct UploadedNamespaceFolioMetadata {
    /// The namespace.
    pub namespace: NamespaceRef,
    /// The filename with the namespace folio.
    pub file_ref: String,
    /// The pages of the folio.
    pub pages: Vec<CommitPageRequest<WithReplyChannel<CommitBatchRequest>>>,
}

/// Trait for generating unique IDs for folios.
pub trait FolioIdGenerator: Send + Sync + 'static {
    fn generate_id(&self) -> String;
}

/// An object to upload folios to the object store.
#[derive(Clone)]
pub struct FolioUploader {
    pub id_generator: Arc<dyn FolioIdGenerator>,
    pub object_store_factory: Arc<dyn ObjectStoreFactory>,
}

/// Generates unique IDs using the ULID algorithm.
#[derive(Debug, Clone)]
pub struct UlidFolioIdGenerator;

impl FolioUploader {
    pub fn new(
        id_generator: Arc<dyn FolioIdGenerator>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
    ) -> Self {
        FolioUploader {
            id_generator,
            object_store_factory,
        }
    }

    pub fn new_ulid(object_store_factory: Arc<dyn ObjectStoreFactory>) -> Self {
        FolioUploader::new(Arc::new(UlidFolioIdGenerator), object_store_factory)
    }

    pub fn new_folio_id(&self, namespace: &NamespaceName) -> String {
        let folio_id = self.id_generator.generate_id();
        format_folio_path(namespace, &folio_id)
    }

    pub async fn upload_folio(
        &self,
        folio: NamespaceFolio,
    ) -> Result<UploadedNamespaceFolioMetadata, ReplyWithWriteBatchError> {
        // TODO: we probably want to add some metadata at the end of the file to make them
        // inspectable.
        let estimated_file_size = folio.pages.iter().fold(0, |acc, p| acc + p.data.len());

        let mut content = BytesMut::with_capacity(estimated_file_size);
        let mut page_metadata = Vec::with_capacity(folio.pages.len());

        for page in folio.pages {
            let offset_bytes = content.len() as _;
            let batch_size_bytes = page.data.len() as _;
            content.extend_from_slice(&page.data);
            let num_messages = page
                .batches
                .iter()
                .fold(0, |acc, b| acc + b.data.num_messages);
            page_metadata.push(CommitPageRequest {
                topic_name: page.topic_name,
                partition_value: page.partition_value,
                num_messages,
                offset_bytes,
                batch_size_bytes,
                batches: page.batches,
            });
        }

        let file_ref = self.new_folio_id(&folio.namespace.name);

        match self
            .upload_to_namespace(folio.namespace.clone(), file_ref.clone(), content.freeze())
            .await
        {
            Ok(_) => Ok(UploadedNamespaceFolioMetadata {
                namespace: folio.namespace,
                file_ref,
                pages: page_metadata,
            }),
            Err(err) => {
                let replies = page_metadata
                    .into_iter()
                    .flat_map(|page| page.batches.into_iter().map(|batch| batch.reply))
                    .collect();

                ReplyWithWriteBatchError::new_fanout(err, replies).into()
            }
        }
    }

    async fn upload_to_namespace(
        &self,
        namespace: NamespaceRef,
        file_ref: String,
        data: Bytes,
    ) -> Result<(), WriteBatchError> {
        let object_store_name = &namespace.object_store;

        let object_store = self
            .object_store_factory
            .create_object_store(object_store_name.clone())
            .await
            .map_err(|err| WriteBatchError::ObjectStore {
                message: "failed to create object store client".to_string(),
                source: Arc::new(err),
            })?;

        object_store
            .put_opts(
                &file_ref.into(),
                PutPayload::from_bytes(data),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| WriteBatchError::ObjectStore {
                message: "failed to upload folio".to_string(),
                source: Arc::new(err),
            })?;

        Ok(())
    }
}

impl FolioIdGenerator for UlidFolioIdGenerator {
    fn generate_id(&self) -> String {
        ulid::Ulid::new().to_string()
    }
}
