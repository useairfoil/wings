use std::sync::Arc;

use dashmap::DashMap;
use object_store::{Error as ObjectStoreError, ObjectStore, memory::InMemory};
use wings_resources::ObjectStoreName;

use crate::ObjectStoreFactory;

/// Factory for creating shared in-memory object stores.
///
/// Each object store name maps to one [`InMemory`] instance. Repeated calls to
/// create the same object store return handles to the same backing store.
#[derive(Debug, Default)]
pub struct InMemoryFactory {
    stores: DashMap<ObjectStoreName, Arc<InMemory>>,
}

impl InMemoryFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, object_store_name: ObjectStoreName) -> Arc<InMemory> {
        self.get_or_create(object_store_name)
    }

    fn get_or_create(&self, object_store_name: ObjectStoreName) -> Arc<InMemory> {
        self.stores
            .entry(object_store_name)
            .or_insert_with(|| Arc::new(InMemory::new()))
            .clone()
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for InMemoryFactory {
    async fn create_object_store(
        &self,
        object_store_name: ObjectStoreName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        Ok(self.get_or_create(object_store_name))
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStore, PutPayload, path::Path};
    use wings_resources::{ObjectStoreName, TenantName};

    use super::*;

    #[tokio::test]
    async fn returns_same_store_for_same_name() {
        let factory = InMemoryFactory::new();
        let object_store_name = object_store_name("test-store");

        let store = factory
            .create_object_store(object_store_name.clone())
            .await
            .expect("create object store");
        store
            .put(&Path::from("test-file"), PutPayload::from("test"))
            .await
            .expect("put object");

        let store = factory.get(object_store_name);
        let data = store
            .get(&Path::from("test-file"))
            .await
            .expect("get object")
            .bytes()
            .await
            .expect("read object");

        assert_eq!(data.as_ref(), b"test");
    }

    #[tokio::test]
    async fn isolates_different_names() {
        let factory = InMemoryFactory::new();
        let first_name = object_store_name("first-store");
        let second_name = object_store_name("second-store");

        let first = factory
            .create_object_store(first_name.clone())
            .await
            .expect("create first store");
        first
            .put(&Path::from("test-file"), PutPayload::from("test"))
            .await
            .expect("put object");

        let second = factory.get(second_name);
        let result = second.get(&Path::from("test-file")).await;

        assert!(result.is_err());
    }

    fn object_store_name(id: &str) -> ObjectStoreName {
        let tenant_name = TenantName::new_unchecked("test-tenant");
        ObjectStoreName::new_unchecked(id, tenant_name)
    }
}
