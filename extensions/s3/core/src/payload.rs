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
            pack_checksum_sha256: None,
            pack_range: None,
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
            pack_checksum_sha256: None,
            pack_range: None,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub async fn get(&self, binding: &PayloadBinding) -> Result<Vec<u8>> {
        binding.validate()?;
        let expected_path = if binding.is_packed() {
            self.pack_path(binding.physical_checksum_sha256())?
        } else {
            self.path(binding.checksum_sha256)?
        };
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
                range: binding.pack_range.map(|(start, end)| start..=end),
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

    pub async fn put_pack(&self, objects: Vec<([u8; 32], Vec<u8>)>) -> Result<Vec<PayloadBinding>> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let total = objects.iter().try_fold(0_usize, |total, (_, bytes)| {
            total.checked_add(bytes.len()).ok_or_else(|| {
                crate::Error::new(
                    crate::ErrorCode::EntityTooLarge,
                    "payload pack size overflow",
                )
            })
        })?;
        let mut packed = Vec::with_capacity(total);
        let mut ranges = Vec::with_capacity(objects.len());
        let mut extents = std::collections::BTreeMap::new();
        for (logical_checksum, bytes) in objects {
            if sha256(&bytes) != logical_checksum {
                return Err(crate::Error::new(
                    crate::ErrorCode::ChecksumMismatch,
                    "payload pack input does not match its logical checksum",
                ));
            }
            if let Some((start, end)) = extents.get(&logical_checksum).copied() {
                ranges.push((logical_checksum, start, end));
                continue;
            }
            let start = u64::try_from(packed.len()).map_err(|_| {
                crate::Error::new(
                    crate::ErrorCode::EntityTooLarge,
                    "payload pack offset overflow",
                )
            })?;
            packed.extend_from_slice(&bytes);
            let end = u64::try_from(packed.len() - 1).map_err(|_| {
                crate::Error::new(
                    crate::ErrorCode::EntityTooLarge,
                    "payload pack extent overflow",
                )
            })?;
            extents.insert(logical_checksum, (start, end));
            ranges.push((logical_checksum, start, end));
        }
        let pack_checksum = sha256(&packed);
        let path = self.pack_path(pack_checksum)?;
        let outcome = self
            .plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                expected_sha256: pack_checksum,
                bytes: packed,
            })
            .await?;
        let metadata = match outcome {
            crate::ImmutablePutOutcome::Created(metadata)
            | crate::ImmutablePutOutcome::AlreadyPresent(metadata) => metadata,
        };
        ranges
            .into_iter()
            .map(|(logical_checksum, start, end)| {
                let binding = PayloadBinding {
                    path: path.clone(),
                    provider_version_id: metadata.token.version_id.clone(),
                    provider_etag: metadata.token.etag.clone(),
                    checksum_sha256: logical_checksum,
                    pack_checksum_sha256: Some(pack_checksum),
                    pack_range: Some((start, end)),
                };
                binding.validate()?;
                Ok(binding)
            })
            .collect()
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

    pub fn pack_path(&self, checksum_sha256: [u8; 32]) -> Result<ObjectPath> {
        let encoded = hex::encode(checksum_sha256);
        ObjectPath::new(format!(
            "{}/payload-packs/{}/sha256/{}/{}/{}",
            self.prefix,
            hex::encode(self.repository.as_bytes()),
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }

    pub fn expected_path(&self, binding: &PayloadBinding) -> Result<ObjectPath> {
        if binding.is_packed() {
            self.pack_path(binding.physical_checksum_sha256())
        } else {
            self.path(binding.checksum_sha256)
        }
    }
}
