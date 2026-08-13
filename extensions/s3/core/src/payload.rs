use std::{path::PathBuf, sync::Arc};

use crate::{
    codec::sha256, GetRequest, ImmutableFilePut, ImmutablePut, ObjectPath, ObjectPlane,
    PayloadBinding, PhysicalVersion, RepositoryId, Result,
};

#[derive(Clone)]
pub struct ImmutablePayloadStore<P: ObjectPlane> {
    plane: Arc<P>,
    prefix: String,
    repository: RepositoryId,
}

impl<P: ObjectPlane> ImmutablePayloadStore<P> {
    pub fn new(plane: Arc<P>, prefix: impl Into<String>, repository: RepositoryId) -> Self {
        Self {
            plane,
            prefix: prefix.into(),
            repository,
        }
    }

    pub async fn put(&self, bytes: Vec<u8>) -> Result<PayloadBinding> {
        let checksum_sha256 = sha256(&bytes);
        let path = self.path(checksum_sha256)?;
        let outcome = self
            .plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                expected_sha256: checksum_sha256,
                bytes,
            })
            .await?;
        let metadata = match outcome {
            crate::ImmutablePutOutcome::Created(metadata)
            | crate::ImmutablePutOutcome::AlreadyPresent(metadata) => metadata,
        };
        let binding = PayloadBinding {
            path,
            provider_version_id: metadata.token.version_id,
            provider_etag: metadata.token.etag,
            checksum_sha256,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub async fn put_file(
        &self,
        body_path: PathBuf,
        size: u64,
        checksum_sha256: [u8; 32],
    ) -> Result<PayloadBinding> {
        let path = self.path(checksum_sha256)?;
        let outcome = self
            .plane
            .put_immutable_file(ImmutableFilePut {
                path: path.clone(),
                body_path,
                size,
                expected_sha256: checksum_sha256,
            })
            .await?;
        let metadata = match outcome {
            crate::ImmutablePutOutcome::Created(metadata)
            | crate::ImmutablePutOutcome::AlreadyPresent(metadata) => metadata,
        };
        if metadata.len != size || metadata.sha256 != checksum_sha256 {
            return Err(crate::Error::new(
                crate::ErrorCode::ChecksumMismatch,
                "immutable payload metadata does not match the staged file",
            ));
        }
        let binding = PayloadBinding {
            path,
            provider_version_id: metadata.token.version_id,
            provider_etag: metadata.token.etag,
            checksum_sha256,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub async fn get(&self, binding: &PayloadBinding) -> Result<Vec<u8>> {
        binding.validate()?;
        let expected_path = self.path(binding.checksum_sha256)?;
        if binding.path != expected_path {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "payload binding path does not match its content checksum",
            ));
        }
        let physical_version =
            binding
                .provider_version_id
                .as_ref()
                .map(|version_id| PhysicalVersion::Versioned {
                    version_id: version_id.clone(),
                });
        let stored = self
            .plane
            .get(GetRequest {
                path: binding.path.clone(),
                range: None,
                physical_version,
            })
            .await?
            .ok_or_else(|| {
                crate::Error::new(crate::ErrorCode::MissingClosure, "payload is missing")
            })?;
        if sha256(&stored.bytes) != binding.checksum_sha256 {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "immutable payload checksum mismatch",
            ));
        }
        Ok(stored.bytes)
    }

    pub fn path(&self, checksum_sha256: [u8; 32]) -> Result<ObjectPath> {
        let encoded = hex::encode(checksum_sha256);
        ObjectPath::new(format!(
            "{}/payloads/{}/sha256/{}/{}/{}",
            self.prefix,
            hex::encode(self.repository.as_bytes()),
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}
