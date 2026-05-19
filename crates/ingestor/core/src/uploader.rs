use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use object_store::{PutMode, PutOptions, PutPayload};
use snafu::ResultExt;
use wings_object_store::{ObjectStoreFactory, paths::format_folio_path};
use wings_resources::{NamespaceName, NamespaceRef};

use crate::{
    error::{ObjectStoreSnafu, Result},
    namespace_writer::NamespaceFolio,
    response::{FolioPageMetadata, WriteBatchResponse},
};

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

    pub async fn upload_folio(&self, folio: NamespaceFolio) {
        // TODO: we probably want to add some metadata at the end of the file to make them
        // inspectable.
        let estimated_file_size = folio.pages.iter().fold(0, |acc, p| acc + p.data.len());

        let mut content = BytesMut::with_capacity(estimated_file_size);

        let file_ref = self.new_folio_id(&folio.namespace.name);

        let mut replies = Vec::with_capacity(folio.pages.len());

        for page in folio.pages {
            let offset_bytes = content.len() as _;
            let page_size_bytes = page.data.len() as _;
            content.extend_from_slice(&page.data);

            let folio_page_meta = FolioPageMetadata {
                file_ref: file_ref.clone(),
                offset_bytes,
                size_bytes: page_size_bytes,
            };

            for reply in page.replies.into_iter() {
                let response = WriteBatchResponse {
                    batch_id: reply.data.batch_id,
                    table_name: page.table_name.clone(),
                    partition_value: page.partition_value.clone(),
                    folio: folio_page_meta.clone(),
                    num_rows: reply.data.num_rows as _,
                    seqnum: reply.data.offset_rows as _,
                    timestamp: reply.data.timestamp,
                };
                replies.push((reply.reply, response));
            }
        }

        match self
            .upload_to_namespace(folio.namespace.clone(), file_ref.clone(), content.freeze())
            .await
        {
            Ok(_) => {
                for (reply, response) in replies {
                    let _ = reply.send(Ok(response));
                }
            }
            Err(err) => {
                for (reply, _) in replies {
                    let _ = reply.send(Err(err.clone()));
                }
            }
        }
    }

    async fn upload_to_namespace(
        &self,
        namespace: NamespaceRef,
        file_ref: String,
        data: Bytes,
    ) -> Result<()> {
        let object_store_name = &namespace.object_store;

        let object_store = self
            .object_store_factory
            .create_object_store(object_store_name.clone())
            .await
            .context(ObjectStoreSnafu {
                message: "failed to create object store client".to_string(),
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
            .context(ObjectStoreSnafu {
                message: "failed to upload folio".to_string(),
            })?;

        Ok(())
    }
}

impl FolioIdGenerator for UlidFolioIdGenerator {
    fn generate_id(&self) -> String {
        ulid::Ulid::new().to_string()
    }
}
