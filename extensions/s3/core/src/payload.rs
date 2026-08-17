use std::{path::PathBuf, sync::Arc};

use crate::{
    codec::sha256, GetRequest, ImmutableFilePut, ImmutablePut, ImmutablePutOutcome,
    ImmutableTransfer, ObjectPath, ObjectPlane, PayloadBinding, PhysicalVersion, RepositoryId,
    Result,
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

    /// Transfer one complete immutable payload through the provider boundary.
    /// The repository never observes transfer parts or reconstructs the body.
    pub async fn transfer_from<Q: ObjectPlane>(
        &self,
        source: &ImmutablePayloadStore<Q>,
        binding: &PayloadBinding,
        size: u64,
    ) -> Result<PayloadBinding> {
        binding.validate()?;
        if binding.path != source.expected_path(binding)? {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "transfer source binding path does not match its content checksum",
            ));
        }
        let path = self.path(binding.checksum_sha256)?;
        let source_physical_version =
            binding
                .provider_version_id
                .as_ref()
                .map(|version_id| PhysicalVersion::Versioned {
                    version_id: version_id.clone(),
                });
        let outcome = self
            .plane
            .transfer_immutable_from(
                source.plane.as_ref(),
                ImmutableTransfer {
                    source_path: binding.path.clone(),
                    source_physical_version,
                    destination_path: path.clone(),
                    size,
                    expected_sha256: binding.checksum_sha256,
                },
            )
            .await?;
        let metadata = match outcome {
            ImmutablePutOutcome::Created(metadata)
            | ImmutablePutOutcome::AlreadyPresent(metadata) => metadata,
        };
        if metadata.len != size || metadata.sha256 != binding.checksum_sha256 {
            return Err(crate::Error::new(
                crate::ErrorCode::ChecksumMismatch,
                "transferred object does not match its whole-object identity",
            ));
        }
        let transferred = PayloadBinding {
            path,
            provider_version_id: metadata.token.version_id,
            provider_etag: metadata.token.etag,
            checksum_sha256: binding.checksum_sha256,
        };
        transferred.validate()?;
        Ok(transferred)
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

    pub fn expected_path(&self, binding: &PayloadBinding) -> Result<ObjectPath> {
        self.path(binding.checksum_sha256)
    }
}
