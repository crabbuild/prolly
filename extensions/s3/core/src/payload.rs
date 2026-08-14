use std::{path::PathBuf, sync::Arc};

use futures_util::{stream, StreamExt};

use crate::{
    codec::sha256, decode_canonical, encode_canonical, ChunkManifest, ChunkManifestBinding,
    GetRequest, ImmutableFilePut, ImmutablePut, ObjectPath, ObjectPlane, PayloadBinding,
    PayloadChunk, PhysicalVersion, RepositoryId, Result,
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
            chunk_manifest: None,
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
            chunk_manifest: None,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub async fn get(&self, binding: &PayloadBinding) -> Result<Vec<u8>> {
        binding.validate()?;
        let expected_path = if binding.is_chunked() {
            self.manifest_path(binding.physical_checksum_sha256())?
        } else if binding.is_packed() {
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
        if stored.metadata.sha256 != binding.physical_checksum_sha256()
            || stored.metadata.token.etag != binding.provider_etag
            || stored.metadata.token.version_id != binding.provider_version_id
        {
            return Err(crate::Error::new(
                crate::ErrorCode::ChecksumMismatch,
                "immutable payload metadata does not match its binding",
            ));
        }
        if binding.is_chunked() {
            return self.get_chunked(binding, stored.bytes).await;
        }
        if sha256(&stored.bytes) != binding.checksum_sha256 {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "immutable payload checksum mismatch",
            ));
        }
        Ok(stored.bytes)
    }

    pub async fn put_chunk(&self, bytes: Vec<u8>) -> Result<PayloadChunk> {
        let size = u64::try_from(bytes.len()).map_err(|_| {
            crate::Error::new(
                crate::ErrorCode::EntityTooLarge,
                "payload chunk exceeds u64",
            )
        })?;
        let binding = self.put(bytes).await?;
        Ok(PayloadChunk {
            path: binding.path,
            provider_version_id: binding.provider_version_id,
            provider_etag: binding.provider_etag,
            size,
            checksum_sha256: binding.checksum_sha256,
        })
    }

    pub async fn put_chunk_manifest(
        &self,
        logical_size: u64,
        logical_checksum_sha256: [u8; 32],
        chunks: Vec<PayloadChunk>,
    ) -> Result<PayloadBinding> {
        let manifest = ChunkManifest {
            format_version: ChunkManifest::FORMAT_VERSION,
            logical_size,
            logical_checksum_sha256,
            chunks,
        };
        manifest.validate()?;
        let bytes = encode_canonical(&manifest)?;
        let manifest_checksum = sha256(&bytes);
        let path = self.manifest_path(manifest_checksum)?;
        let outcome = self
            .plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                expected_sha256: manifest_checksum,
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
            checksum_sha256: logical_checksum_sha256,
            pack_checksum_sha256: None,
            pack_range: None,
            chunk_manifest: Some(ChunkManifestBinding {
                checksum_sha256: manifest_checksum,
                chunk_count: u32::try_from(manifest.chunks.len()).map_err(|_| {
                    crate::Error::new(
                        crate::ErrorCode::EntityTooLarge,
                        "chunk manifest count exceeds u32",
                    )
                })?,
            }),
        };
        binding.validate()?;
        Ok(binding)
    }

    async fn get_chunked(
        &self,
        binding: &PayloadBinding,
        manifest_bytes: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let descriptor = binding.chunk_manifest.as_ref().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorCode::CorruptCommit,
                "chunk descriptor is missing",
            )
        })?;
        if sha256(&manifest_bytes) != descriptor.checksum_sha256 {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "chunk manifest checksum mismatch",
            ));
        }
        let manifest: ChunkManifest = decode_canonical(&manifest_bytes)?;
        manifest.validate()?;
        if manifest.logical_checksum_sha256 != binding.checksum_sha256
            || manifest.chunks.len() != descriptor.chunk_count as usize
        {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "chunk manifest disagrees with its binding",
            ));
        }
        let chunks = stream::iter(manifest.chunks.into_iter().map(|chunk| async move {
            let physical_version =
                chunk
                    .provider_version_id
                    .as_ref()
                    .map(|version_id| PhysicalVersion::Versioned {
                        version_id: version_id.clone(),
                    });
            let stored = self
                .plane
                .get(GetRequest {
                    path: chunk.path.clone(),
                    range: None,
                    physical_version,
                })
                .await?
                .ok_or_else(|| {
                    crate::Error::new(crate::ErrorCode::MissingClosure, "payload chunk is missing")
                })?;
            if stored.bytes.len() as u64 != chunk.size
                || sha256(&stored.bytes) != chunk.checksum_sha256
                || stored.metadata.token.etag != chunk.provider_etag
                || stored.metadata.token.version_id != chunk.provider_version_id
            {
                return Err(crate::Error::new(
                    crate::ErrorCode::CorruptContent,
                    "payload chunk does not match its manifest",
                ));
            }
            Ok::<_, crate::Error>(stored.bytes)
        }))
        .buffered(16)
        .collect::<Vec<_>>()
        .await;
        let capacity = usize::try_from(manifest.logical_size).map_err(|_| {
            crate::Error::new(
                crate::ErrorCode::EntityTooLarge,
                "logical payload exceeds usize",
            )
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        for chunk in chunks {
            bytes.extend_from_slice(&chunk?);
        }
        if bytes.len() as u64 != manifest.logical_size || sha256(&bytes) != binding.checksum_sha256
        {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "chunked logical payload checksum mismatch",
            ));
        }
        Ok(bytes)
    }

    pub async fn get_chunked_range(
        &self,
        binding: &PayloadBinding,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<Vec<u8>> {
        binding.validate()?;
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
                crate::Error::new(
                    crate::ErrorCode::MissingClosure,
                    "chunk manifest is missing",
                )
            })?;
        let descriptor = binding.chunk_manifest.as_ref().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorCode::CorruptCommit,
                "chunk descriptor is missing",
            )
        })?;
        if sha256(&stored.bytes) != descriptor.checksum_sha256 {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "chunk manifest checksum mismatch",
            ));
        }
        let manifest: ChunkManifest = decode_canonical(&stored.bytes)?;
        manifest.validate()?;
        let mut offset = 0_u64;
        let mut selected = Vec::new();
        for chunk in manifest.chunks {
            let end = offset.checked_add(chunk.size - 1).ok_or_else(|| {
                crate::Error::new(crate::ErrorCode::EntityTooLarge, "chunk offset overflow")
            })?;
            if end >= *range.start() && offset <= *range.end() {
                let local_start = range.start().saturating_sub(offset);
                let local_end = (*range.end()).min(end) - offset;
                selected.push((chunk, local_start..=local_end));
            }
            offset = end + 1;
        }
        let parts = stream::iter(selected.into_iter().map(|(chunk, local_range)| async move {
            let physical_version =
                chunk
                    .provider_version_id
                    .as_ref()
                    .map(|version_id| PhysicalVersion::Versioned {
                        version_id: version_id.clone(),
                    });
            let stored = self
                .plane
                .get(GetRequest {
                    path: chunk.path.clone(),
                    range: Some(local_range),
                    physical_version,
                })
                .await?
                .ok_or_else(|| {
                    crate::Error::new(crate::ErrorCode::MissingClosure, "payload chunk is missing")
                })?;
            if stored.metadata.sha256 != chunk.checksum_sha256
                || stored.metadata.token.etag != chunk.provider_etag
                || stored.metadata.token.version_id != chunk.provider_version_id
            {
                return Err(crate::Error::new(
                    crate::ErrorCode::CorruptContent,
                    "payload chunk range does not match its manifest",
                ));
            }
            Ok::<_, crate::Error>(stored.bytes)
        }))
        .buffered(16)
        .collect::<Vec<_>>()
        .await;
        let mut bytes = Vec::new();
        for part in parts {
            bytes.extend_from_slice(&part?);
        }
        Ok(bytes)
    }

    pub(crate) async fn load_chunk_manifest(
        &self,
        binding: &PayloadBinding,
    ) -> Result<ChunkManifest> {
        if !binding.is_chunked()
            || binding.path != self.manifest_path(binding.physical_checksum_sha256())?
        {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "chunk manifest binding path is invalid",
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
                crate::Error::new(
                    crate::ErrorCode::MissingClosure,
                    "chunk manifest is missing",
                )
            })?;
        let descriptor = binding.chunk_manifest.as_ref().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorCode::CorruptCommit,
                "chunk descriptor is missing",
            )
        })?;
        if sha256(&stored.bytes) != descriptor.checksum_sha256
            || stored.metadata.token.etag != binding.provider_etag
            || stored.metadata.token.version_id != binding.provider_version_id
        {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "chunk manifest does not match its binding",
            ));
        }
        let manifest: ChunkManifest = decode_canonical(&stored.bytes)?;
        manifest.validate()?;
        if manifest.logical_checksum_sha256 != binding.checksum_sha256
            || manifest.chunks.len() != descriptor.chunk_count as usize
        {
            return Err(crate::Error::new(
                crate::ErrorCode::CorruptContent,
                "chunk manifest disagrees with its descriptor",
            ));
        }
        Ok(manifest)
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
                    chunk_manifest: None,
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

    pub fn manifest_path(&self, checksum_sha256: [u8; 32]) -> Result<ObjectPath> {
        let encoded = hex::encode(checksum_sha256);
        ObjectPath::new(format!(
            "{}/payload-manifests/{}/sha256/{}/{}/{}",
            self.prefix,
            hex::encode(self.repository.as_bytes()),
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }

    pub fn expected_path(&self, binding: &PayloadBinding) -> Result<ObjectPath> {
        if binding.is_chunked() {
            self.manifest_path(binding.physical_checksum_sha256())
        } else if binding.is_packed() {
            self.pack_path(binding.physical_checksum_sha256())
        } else {
            self.path(binding.checksum_sha256)
        }
    }
}
