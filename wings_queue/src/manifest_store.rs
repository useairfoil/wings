use std::sync::Arc;

use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutPayload, UpdateVersion,
    path::Path,
};

use crate::{Header, Manifest};

pub const QUEUE_LOCATION: &str = "queue.bin";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("manifest write conflict")]
    Conflict,
    #[error("object store error: {0}")]
    ObjecStore(#[from] object_store::Error),
    #[error("encoding error: {0}")]
    Wire(#[from] binrw::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ManifestStore {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

pub struct ManifestWriter {
    store: Arc<dyn ObjectStore>,
    path: Path,
    version: UpdateVersion,
}

impl ManifestStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::new_with_path(store, Path::from(QUEUE_LOCATION))
    }

    pub fn new_with_path(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }

    /// Loads the header from the queue file.
    ///
    /// This is used by clients (workers, pusher) to discover the broker's location.
    pub async fn load_header_only(&self) -> Result<Header> {
        let range = GetRange::Bounded(0..Header::SIZE as _);

        let response = self
            .store
            .get_opts(
                &self.path,
                GetOptions {
                    range: Some(range),
                    ..Default::default()
                },
            )
            .await?;

        let bytes = response.bytes().await?;
        let footer = Header::from_bytes(bytes.as_ref())?;

        Ok(footer)
    }

    pub async fn load_or_init(&self, header: Header) -> Result<(Manifest, ManifestWriter)> {
        loop {
            match self.do_load_or_init(&header).await {
                Ok(manifest) => return Ok(manifest),
                Err(Error::Conflict) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    async fn do_load_or_init(&self, header: &Header) -> Result<(Manifest, ManifestWriter)> {
        let response = match self.store.get(&self.path).await {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => {
                return self.init_new(&header).await;
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        let version = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };

        let bytes = response.bytes().await?;
        let mut manifest = Manifest::from_bytes(bytes.as_ref())?;

        let mut header = header.clone();
        header.epoch = manifest.header.epoch + 1;
        manifest.header = header;

        let put_mode = PutMode::Update(version);

        let version = do_put(&self.store, &self.path, &manifest, put_mode).await?;

        let writer = ManifestWriter {
            store: Arc::clone(&self.store),
            path: self.path.clone(),
            version,
        };

        Ok((manifest, writer))
    }

    async fn init_new(&self, header: &Header) -> Result<(Manifest, ManifestWriter)> {
        let manifest = Manifest {
            header: header.clone(),
            data: Vec::new().into(),
        };

        let put_mode = PutMode::Create;

        let version = do_put(&self.store, &self.path, &manifest, put_mode).await?;

        let writer = ManifestWriter {
            store: Arc::clone(&self.store),
            path: self.path.clone(),
            version,
        };

        Ok((manifest, writer))
    }
}

impl ManifestWriter {
    pub async fn update(&mut self, manifest: &Manifest) -> Result<()> {
        let put_mode = PutMode::Update(self.version.clone());

        let version = do_put(&self.store, &self.path, &manifest, put_mode).await?;

        self.version = version;

        Ok(())
    }
}

async fn do_put(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    manifest: &Manifest,
    put_mode: PutMode,
) -> Result<UpdateVersion> {
    let data = manifest.to_bytes()?;
    let payload = PutPayload::from(data);
    match store.put_opts(path, payload, put_mode.into()).await {
        Ok(response) => {
            let version = UpdateVersion {
                e_tag: response.e_tag.clone(),
                version: response.version.clone(),
            };

            Ok(version)
        }
        Err(
            object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. },
        ) => Err(Error::Conflict),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use object_store::{ObjectStore, memory::InMemory};

    use super::*;
    use crate::manifest::CompressionType;

    fn new_manifest_store() -> ManifestStore {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        ManifestStore::new(Arc::clone(&store))
    }

    fn new_header(port: u16, epoch: u64) -> Header {
        Header {
            address: Ipv4Addr::new(127, 0, 0, 1),
            port,
            epoch,
            compression: CompressionType::None,
        }
    }

    #[tokio::test]
    async fn load_or_init_creates_empty_manifest() {
        let manifest_store = new_manifest_store();
        let header = new_header(8080, 0);

        let (manifest, _stored) = manifest_store
            .load_or_init(header.clone())
            .await
            .expect("manifest should initialize");

        insta::assert_debug_snapshot!(manifest, @"
        Manifest {
            header: Header {
                address: 127.0.0.1,
                port: 8080,
                epoch: 0,
                compression: None,
            },
            data: TaskData {
                entries: [],
            },
        }
        ");

        let header_only = manifest_store
            .load_header_only()
            .await
            .expect("header should load");

        insta::assert_debug_snapshot!(header_only, @"
        Header {
            address: 127.0.0.1,
            port: 8080,
            epoch: 0,
            compression: None,
        }
        ");
    }

    #[tokio::test]
    async fn load_or_init_reloads_manifest_and_bumps_epoch() {
        let manifest_store = new_manifest_store();
        let first_header = new_header(8080, 41);

        let (_manifest, mut writer) = manifest_store
            .load_or_init(first_header.clone())
            .await
            .expect("manifest should initialize");

        writer
            .update(&Manifest {
                header: first_header,
                data: vec![].into(),
            })
            .await
            .expect("manifest update should succeed");

        let new_broker_header = new_header(9090, 0);
        let (reloaded, _writer) = manifest_store
            .load_or_init(new_broker_header.clone())
            .await
            .expect("manifest should reload");

        insta::assert_debug_snapshot!(reloaded, @"
        Manifest {
            header: Header {
                address: 127.0.0.1,
                port: 9090,
                epoch: 42,
                compression: None,
            },
            data: TaskData {
                entries: [],
            },
        }
        ");
    }

    #[tokio::test]
    async fn stored_manifest_update_persists_manifest_and_refreshes_version() {
        let manifest_store = new_manifest_store();
        let (manifest, mut writer) = manifest_store
            .load_or_init(new_header(8080, 0))
            .await
            .expect("manifest should initialize");

        writer
            .update(&manifest)
            .await
            .expect("first update should succeed");

        writer
            .update(&manifest)
            .await
            .expect("second update should use refreshed version");
    }

    #[tokio::test]
    async fn stale_stored_manifest_update_returns_conflict() {
        let manifest_store = new_manifest_store();
        let (_manifest, mut slow_writer) = manifest_store
            .load_or_init(new_header(8080, 0))
            .await
            .expect("manifest should initialize");

        let fast_broker_header = new_header(9090, 0);
        let (_manifest, _fast_writer) = manifest_store
            .load_or_init(fast_broker_header.clone())
            .await
            .expect("fast broker should reload and update manifest");

        let err = slow_writer
            .update(&Manifest {
                header: new_header(8080, 0),
                data: Vec::new().into(),
            })
            .await
            .expect_err("stale update should conflict");

        insta::assert_compact_debug_snapshot!(err, @"Conflict");

        let header = manifest_store
            .load_header_only()
            .await
            .expect("header should load");

        insta::assert_debug_snapshot!(header, @"
        Header {
            address: 127.0.0.1,
            port: 9090,
            epoch: 1,
            compression: None,
        }
        ");
    }
}
