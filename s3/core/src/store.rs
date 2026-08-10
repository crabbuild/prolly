use std::sync::Arc;

use prolly::{AsyncStore, BatchOp};

use crate::{
    codec::sha256, DeleteOutcome, Error, ErrorCode, GetRequest, ImmutablePut, ObjectPath,
    ObjectPlane, PhysicalVersion, Result,
};

/// Prolly node store backed by immutable objects in an [`ObjectPlane`].
pub struct ProllyObjectStore<P> {
    plane: Arc<P>,
    repository_prefix: String,
    protection: Option<Arc<dyn crate::ProtectionSink>>,
}

impl<P> Clone for ProllyObjectStore<P> {
    fn clone(&self) -> Self {
        Self {
            plane: self.plane.clone(),
            repository_prefix: self.repository_prefix.clone(),
            protection: self.protection.clone(),
        }
    }
}

impl<P> ProllyObjectStore<P> {
    pub fn new(plane: Arc<P>, repository_prefix: impl Into<String>) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            protection: None,
        }
    }

    pub fn with_protection_sink(mut self, sink: Arc<dyn crate::ProtectionSink>) -> Self {
        self.protection = Some(sink);
        self
    }

    fn path_for_key(&self, key: &[u8]) -> Result<ObjectPath> {
        if key.len() != 32 {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                format!("Prolly node key has {} bytes, expected 32", key.len()),
            ));
        }
        let encoded = hex::encode(key);
        ObjectPath::new(format!(
            "{}/nodes/sha256/{}/{}/{}",
            self.repository_prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}

impl<P: ObjectPlane> AsyncStore for ProllyObjectStore<P> {
    type Error = Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let path = self.path_for_key(key)?;
        let Some(object) = self
            .plane
            .get(GetRequest {
                path,
                range: None,
                physical_version: None,
            })
            .await?
        else {
            return Ok(None);
        };
        if sha256(&object.bytes).as_slice() != key {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "stored Prolly node does not match its CID",
            ));
        }
        Ok(Some(object.bytes))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if sha256(value).as_slice() != key {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "attempted Prolly node write under the wrong CID",
            ));
        }
        let path = self.path_for_key(key)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                bytes: value.to_vec(),
                expected_sha256: sha256(value),
            })
            .await?;
        if let Some(sink) = &self.protection {
            sink.protect(path).await?;
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let path = self.path_for_key(key)?;
        let Some(metadata) = self.plane.head(&path).await? else {
            return Ok(());
        };
        let version = metadata
            .token
            .version_id
            .clone()
            .map(|version_id| PhysicalVersion::Versioned { version_id })
            .unwrap_or_else(|| PhysicalVersion::Unversioned {
                token: Some(metadata.token),
            });
        match self.plane.delete_exact(&path, version).await? {
            DeleteOutcome::Deleted | DeleteOutcome::NotFound => Ok(()),
            DeleteOutcome::TokenMismatch => Err(Error::new(
                ErrorCode::RefConflict,
                "node changed during exact delete",
            )),
        }
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<()> {
        for operation in ops {
            match operation {
                BatchOp::Upsert { key, value } => self.put(key, value).await?,
                BatchOp::Delete { key } => self.delete(key).await?,
            }
        }
        Ok(())
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<()> {
        for (key, value) in entries {
            self.put(key, value).await?;
        }
        Ok(())
    }

    fn read_parallelism(&self) -> usize {
        16
    }
}
