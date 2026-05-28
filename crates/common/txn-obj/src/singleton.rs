use std::sync::Arc;

use async_trait::async_trait;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path,
};

use crate::{ObjectCodec, TransactionalObjectError, TransactionalStorageProtocol};

/// Implements `TransactionStorageProtocol<T>` on object storage.
///
/// ## File layout and naming
/// - We rely on `put_if_not_exists` to enforce CAS at the storage layer. If a file with
///   the same id already exists, the write fails with `ObjectVersionExists`.
pub struct ObjectStoreSingletonStorageProtocol<T> {
    object_store: Arc<dyn ObjectStore>,
    filename: Path,
    codec: Box<dyn ObjectCodec<T>>,
}

impl<T> ObjectStoreSingletonStorageProtocol<T> {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        filename: Path,
        codec: Box<dyn ObjectCodec<T>>,
    ) -> Self {
        Self {
            object_store,
            filename,
            codec,
        }
    }
}

#[async_trait]
impl<T: Send + Sync> TransactionalStorageProtocol<T, UpdateVersion>
    for ObjectStoreSingletonStorageProtocol<T>
{
    async fn write(
        &self,
        current_id: Option<UpdateVersion>,
        new_value: &T,
    ) -> Result<UpdateVersion, TransactionalObjectError> {
        let mode = match current_id {
            Some(id) => PutMode::Update(id),
            None => PutMode::Create,
        };

        let result = self
            .object_store
            .put_opts(
                &self.filename,
                PutPayload::from_bytes(self.codec.encode(new_value)),
                PutOptions::from(mode),
            )
            .await
            .map_err(|err| {
                if let ::object_store::Error::AlreadyExists { path: _, source: _ } = err {
                    TransactionalObjectError::ObjectVersionExists
                } else {
                    TransactionalObjectError::ObjectStoreError(err)
                }
            })?;

        Ok(UpdateVersion::from(result))
    }

    async fn try_read_latest(
        &self,
    ) -> Result<Option<(UpdateVersion, T)>, TransactionalObjectError> {
        match self.object_store.get(&self.filename).await {
            Ok(result) => {
                let id = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };

                let bytes = result
                    .bytes()
                    .await
                    .map_err(TransactionalObjectError::ObjectStoreError)?;
                let value = self
                    .codec
                    .decode(&bytes)
                    .map_err(TransactionalObjectError::CallbackError)?;
                Ok(Some((id, value)))
            }
            Err(::object_store::Error::NotFound { path: _, source: _ }) => Ok(None),
            Err(err) => Err(TransactionalObjectError::ObjectStoreError(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{UpdateVersion, memory::InMemory};

    use crate::{
        ObjectCodec, SimpleTransactionalObject, TransactionalObject, TransactionalObjectError,
        TransactionalStorageProtocol, singleton::ObjectStoreSingletonStorageProtocol,
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) struct TestVal {
        pub(crate) epoch: u64,
        pub(crate) payload: u64,
    }

    pub(crate) struct TestValCodec;

    pub(crate) struct FailingDecodeCodec;

    impl ObjectCodec<TestVal> for TestValCodec {
        fn encode(&self, value: &TestVal) -> Bytes {
            // simple "epoch:payload" encoding
            Bytes::from(format!("{}:{}", value.epoch, value.payload))
        }

        fn decode(
            &self,
            bytes: &Bytes,
        ) -> Result<TestVal, Box<dyn std::error::Error + Send + Sync>> {
            let s = std::str::from_utf8(bytes).unwrap();
            let mut parts = s.split(':');
            let epoch = parts.next().unwrap().parse().unwrap();
            let payload = parts.next().unwrap().parse().unwrap();
            Ok(TestVal { epoch, payload })
        }
    }

    impl ObjectCodec<TestVal> for FailingDecodeCodec {
        fn encode(&self, value: &TestVal) -> Bytes {
            TestValCodec.encode(value)
        }

        fn decode(
            &self,
            _bytes: &Bytes,
        ) -> Result<TestVal, Box<dyn std::error::Error + Send + Sync>> {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad payload").into())
        }
    }

    pub fn new_store() -> Arc<ObjectStoreSingletonStorageProtocol<TestVal>> {
        let inner: Arc<_> = InMemory::new().into();
        Arc::new(ObjectStoreSingletonStorageProtocol::new(
            inner,
            "manifest.json".into(),
            Box::new(TestValCodec),
        ))
    }

    #[tokio::test]
    async fn try_read_latest_returns_none_when_object_does_not_exist() {
        let store = new_store();

        let value = store.try_read_latest().await.unwrap();

        assert!(value.is_none());
    }

    #[tokio::test]
    async fn write_then_read_latest_round_trips_value_and_version() {
        let store = new_store();
        let value = TestVal {
            epoch: 1,
            payload: 42,
        };

        let id = store.write(None, &value).await.unwrap();
        let (read_id, read_value) = store.try_read_latest().await.unwrap().unwrap();

        assert_eq!(read_value, value);
        assert_eq!(read_id.e_tag, id.e_tag);
        assert_eq!(read_id.version, id.version);
    }

    #[tokio::test]
    async fn write_with_current_version_updates_object() {
        let store = new_store();
        let first_value = TestVal {
            epoch: 1,
            payload: 42,
        };
        let second_value = TestVal {
            epoch: 2,
            payload: 84,
        };

        let first_id = store.write(None, &first_value).await.unwrap();
        let second_id = store.write(Some(first_id), &second_value).await.unwrap();
        let (read_id, read_value) = store.try_read_latest().await.unwrap().unwrap();

        assert_eq!(read_value, second_value);
        assert_eq!(read_id.e_tag, second_id.e_tag);
        assert_eq!(read_id.version, second_id.version);
    }

    #[tokio::test]
    async fn write_create_fails_when_object_already_exists() {
        let store = new_store();
        let first_value = TestVal {
            epoch: 1,
            payload: 42,
        };
        let second_value = TestVal {
            epoch: 2,
            payload: 84,
        };

        store.write(None, &first_value).await.unwrap();
        let err = store.write(None, &second_value).await.unwrap_err();

        assert!(matches!(err, TransactionalObjectError::ObjectVersionExists));
    }

    #[tokio::test]
    async fn try_read_latest_returns_callback_error_when_decode_fails() {
        let inner: Arc<_> = InMemory::new().into();
        let store = ObjectStoreSingletonStorageProtocol::new(
            inner,
            "manifest.json".into(),
            Box::new(FailingDecodeCodec),
        );
        let value = TestVal {
            epoch: 1,
            payload: 42,
        };

        store.write(None, &value).await.unwrap();
        let err = store.try_read_latest().await.unwrap_err();

        assert!(matches!(err, TransactionalObjectError::CallbackError(..)));
    }

    #[tokio::test]
    async fn test_init_write_and_read_latest() {
        let store = new_store();
        let mut sr = SimpleTransactionalObject::<TestVal, UpdateVersion>::init(
            Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<TestVal, UpdateVersion>>,
            TestVal {
                epoch: 0,
                payload: 1,
            },
        )
        .await
        .unwrap();
        insta::assert_compact_debug_snapshot!(sr.id(), @r#"UpdateVersion { e_tag: Some("0"), version: None }"#);
        insta::assert_compact_debug_snapshot!(sr.object(), @"TestVal { epoch: 0, payload: 1 }");

        // update to next id
        let mut dirty = sr.prepare_dirty().unwrap();
        dirty.value = TestVal {
            epoch: 0,
            payload: 2,
        };
        sr.update(dirty).await.unwrap();
        insta::assert_compact_debug_snapshot!(sr.id(), @r#"UpdateVersion { e_tag: Some("1"), version: None }"#);
        insta::assert_compact_debug_snapshot!(sr.object(), @"TestVal { epoch: 0, payload: 2 }");

        // try_read_latest matches stored
        let (latest_id, latest_obj) = store.try_read_latest().await.unwrap().unwrap();
        insta::assert_compact_debug_snapshot!(latest_id, @r#"UpdateVersion { e_tag: Some("1"), version: None }"#);
        insta::assert_compact_debug_snapshot!(latest_obj, @"TestVal { epoch: 0, payload: 2 }");
    }
}
