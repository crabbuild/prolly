use std::{collections::BTreeSet, convert::Infallible, sync::Arc};

use bytes::Bytes;
use futures_util::{stream::BoxStream, Stream, StreamExt};
use md5::{Digest as _, Md5};
use prolly::{AsyncProlly, AsyncSortedBatchBuilder, Cid, Config, RuntimeConfig, Tree, TreeFormat};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::{
    codec::sha256, decode_canonical, derive_content_manifest_id, encode_canonical,
    tree_format_digest, Checksums, ContentChunkRef, ContentLayoutV1, ContentManifestRef,
    ContentManifestV1, ContentRef, Error, ErrorCode, GetRequest, ImmutablePut, ObjectPath,
    ObjectPlane, ProllyObjectStore, Result, TreeRootV1,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredContent {
    pub reference: ContentRef,
    pub size: u64,
    pub logical_etag: String,
    pub checksums: Checksums,
}

pub struct ContentStore<P: ObjectPlane> {
    plane: Arc<P>,
    repository_prefix: String,
    chunk_bytes: usize,
    format: TreeFormat,
    engine: AsyncProlly<ProllyObjectStore<P>>,
    protection: Option<Arc<dyn crate::ProtectionSink>>,
}

impl<P: ObjectPlane> Clone for ContentStore<P> {
    fn clone(&self) -> Self {
        let mut cloned = Self::new(
            self.plane.clone(),
            self.repository_prefix.clone(),
            self.chunk_bytes,
            self.format.clone(),
        );
        cloned.protection = self.protection.clone();
        if let Some(sink) = &cloned.protection {
            cloned.engine = AsyncProlly::new(
                ProllyObjectStore::new(cloned.plane.clone(), cloned.repository_prefix.clone())
                    .with_protection_sink(sink.clone()),
                Config {
                    format: cloned.format.clone(),
                    runtime: RuntimeConfig::default(),
                },
            );
        }
        cloned
    }
}

impl<P: ObjectPlane> ContentStore<P> {
    pub fn new(
        plane: Arc<P>,
        repository_prefix: impl Into<String>,
        chunk_bytes: usize,
        format: TreeFormat,
    ) -> Self {
        let repository_prefix = repository_prefix.into();
        let config = Config {
            format: format.clone(),
            runtime: RuntimeConfig::default(),
        };
        let engine = AsyncProlly::new(
            ProllyObjectStore::new(plane.clone(), repository_prefix.clone()),
            config,
        );
        Self {
            plane,
            repository_prefix,
            chunk_bytes,
            format,
            engine,
            protection: None,
        }
    }

    pub fn with_protection_sink(mut self, sink: Arc<dyn crate::ProtectionSink>) -> Self {
        self.engine = AsyncProlly::new(
            ProllyObjectStore::new(self.plane.clone(), self.repository_prefix.clone())
                .with_protection_sink(sink.clone()),
            Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        self.protection = Some(sink);
        self
    }

    pub(crate) async fn retained_paths(
        &self,
        reference: &ContentRef,
    ) -> Result<BTreeSet<ObjectPath>> {
        let mut paths = BTreeSet::new();
        let ContentRef::Chunks(reference) = reference else {
            return Ok(paths);
        };
        let manifest_path = self.manifest_path(*reference)?;
        let object = self
            .plane
            .get(GetRequest {
                path: manifest_path.clone(),
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "content manifest is missing"))?;
        if derive_content_manifest_id(&object.bytes) != *reference {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "content manifest ID mismatch",
            ));
        }
        paths.insert(manifest_path);
        let manifest: ContentManifestV1 = decode_canonical(&object.bytes)?;
        let tree = self.tree_from_root(&manifest.chunk_index)?;
        let reachable = self
            .engine
            .mark_reachable(std::slice::from_ref(&tree))
            .await?;
        for cid in reachable.cids() {
            paths.insert(node_path(&self.repository_prefix, cid)?);
        }
        let mut iter = self.engine.range(&tree, &[], None).await?;
        while let Some(entry) = iter.next().await {
            let (_, value) = entry?;
            let chunk: ContentChunkRef = decode_canonical(&value)?;
            paths.insert(self.chunk_path(&chunk.cid)?);
        }
        Ok(paths)
    }

    fn node_store(&self) -> ProllyObjectStore<P> {
        let store = ProllyObjectStore::new(self.plane.clone(), self.repository_prefix.clone());
        match &self.protection {
            Some(sink) => store.with_protection_sink(sink.clone()),
            None => store,
        }
    }

    async fn protect(&self, path: ObjectPath) -> Result<()> {
        if let Some(sink) = &self.protection {
            sink.protect(path).await?;
        }
        Ok(())
    }

    pub async fn write_bytes(&self, bytes: Vec<u8>) -> Result<StoredContent> {
        self.write_stream(
            futures_util::stream::once(async move { Ok::<_, Infallible>(bytes) }),
            u64::MAX,
        )
        .await
    }

    /// Consume a body exactly once and retain at most one canonical chunk.
    pub async fn write_stream<S, B, E>(&self, stream: S, max_bytes: u64) -> Result<StoredContent>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        futures_util::pin_mut!(stream);
        let mut md5 = Md5::new();
        let mut sha = Sha256::new();
        let mut index = AsyncSortedBatchBuilder::new(
            self.node_store(),
            Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let mut buffer = Vec::with_capacity(self.chunk_bytes);
        let mut offset = 0u64;
        let mut chunk_count = 0u64;
        while let Some(item) = stream.next().await {
            let item = item.map_err(|error| {
                Error::new(
                    ErrorCode::IncompleteBody,
                    format!("body stream failed: {error}"),
                )
            })?;
            let mut bytes = item.as_ref();
            while !bytes.is_empty() {
                let take = (self.chunk_bytes - buffer.len()).min(bytes.len());
                buffer.extend_from_slice(&bytes[..take]);
                bytes = &bytes[take..];
                if buffer.len() == self.chunk_bytes {
                    self.persist_stream_chunk(&mut index, offset, &buffer)
                        .await?;
                    md5.update(&buffer);
                    sha.update(&buffer);
                    offset = checked_content_len(offset, buffer.len(), max_bytes)?;
                    chunk_count += 1;
                    buffer.clear();
                }
            }
        }
        if !buffer.is_empty() {
            self.persist_stream_chunk(&mut index, offset, &buffer)
                .await?;
            md5.update(&buffer);
            sha.update(&buffer);
            offset = checked_content_len(offset, buffer.len(), max_bytes)?;
            chunk_count += 1;
        }

        let md5_bytes: [u8; 16] = md5.finalize().into();
        let sha256_bytes: [u8; 32] = sha.finalize().into();
        let checksums = Checksums {
            md5: Some(md5_bytes),
            sha256: Some(sha256_bytes),
            ..Checksums::default()
        };
        let logical_etag = format!("\"{}\"", hex::encode(md5_bytes));
        if offset == 0 {
            return Ok(StoredContent {
                reference: ContentRef::Empty,
                size: 0,
                logical_etag,
                checksums,
            });
        }

        let index = index.build().await?;
        let manifest = ContentManifestV1 {
            total_len: offset,
            chunk_count,
            layout: ContentLayoutV1::CanonicalFixed,
            chunk_index: TreeRootV1::from_tree(&index)?,
        };
        let encoded = encode_canonical(&manifest)?;
        let reference = derive_content_manifest_id(&encoded);
        let manifest_path = self.manifest_path(reference)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: manifest_path.clone(),
                expected_sha256: sha256(&encoded),
                bytes: encoded,
            })
            .await?;
        self.protect(manifest_path).await?;

        Ok(StoredContent {
            reference: ContentRef::Chunks(reference),
            size: offset,
            logical_etag,
            checksums,
        })
    }

    async fn persist_stream_chunk(
        &self,
        index: &mut AsyncSortedBatchBuilder<ProllyObjectStore<P>>,
        offset: u64,
        chunk: &[u8],
    ) -> Result<()> {
        let cid = Cid::from_bytes(chunk);
        let chunk_path = self.chunk_path(&cid)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: chunk_path.clone(),
                bytes: chunk.to_vec(),
                expected_sha256: sha256(chunk),
            })
            .await?;
        self.protect(chunk_path).await?;
        index
            .add(
                offset.to_be_bytes().to_vec(),
                encode_canonical(&ContentChunkRef {
                    cid,
                    len: u32::try_from(chunk.len()).map_err(|_| {
                        Error::new(ErrorCode::EntityTooLarge, "chunk exceeds u32 length")
                    })?,
                })?,
            )
            .await?;
        Ok(())
    }

    /// Compose uploaded part manifests without rereading payload chunks.
    pub async fn compose(&self, parts: &[StoredContent]) -> Result<StoredContent> {
        if parts.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "multipart completion has no parts",
            ));
        }
        let mut composite_md5 = Md5::new();
        let mut index = AsyncSortedBatchBuilder::new(
            self.node_store(),
            Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let mut total_len = 0u64;
        let mut chunk_count = 0u64;
        for part in parts {
            let md5 = part.checksums.md5.ok_or_else(|| {
                Error::new(ErrorCode::CorruptContent, "multipart part has no MD5")
            })?;
            composite_md5.update(md5);
            if part.size == 0 {
                continue;
            }
            let ContentRef::Chunks(reference) = part.reference else {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "nonempty part has no manifest",
                ));
            };
            let object = self
                .plane
                .get(GetRequest {
                    path: self.manifest_path(reference)?,
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "missing part manifest"))?;
            if derive_content_manifest_id(&object.bytes) != reference {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "part manifest ID mismatch",
                ));
            }
            let manifest: ContentManifestV1 = decode_canonical(&object.bytes)?;
            if manifest.total_len != part.size {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "part size disagrees with manifest",
                ));
            }
            let tree = self.tree_from_root(&manifest.chunk_index)?;
            let mut iter = self.engine.range(&tree, &[], None).await?;
            let mut part_offset = 0u64;
            while let Some(entry) = iter.next().await {
                let (encoded_offset, value) = entry?;
                if encoded_offset.len() != 8 {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "invalid part chunk offset",
                    ));
                }
                let offset = u64::from_be_bytes(encoded_offset.try_into().expect("length checked"));
                if offset != part_offset {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "part chunks are not contiguous",
                    ));
                }
                let chunk: ContentChunkRef = decode_canonical(&value)?;
                index.add(total_len.to_be_bytes().to_vec(), value).await?;
                part_offset = part_offset
                    .checked_add(chunk.len as u64)
                    .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "part length overflow"))?;
                total_len = total_len.checked_add(chunk.len as u64).ok_or_else(|| {
                    Error::new(ErrorCode::EntityTooLarge, "composed length overflow")
                })?;
                chunk_count += 1;
            }
            if part_offset != part.size {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "part chunk totals disagree",
                ));
            }
        }
        let digest: [u8; 16] = composite_md5.finalize().into();
        let logical_etag = format!("\"{}-{}\"", hex::encode(digest), parts.len());
        let mut checksums = Checksums::default();
        checksums
            .algorithm_values
            .insert("multipart-composite-md5".to_string(), digest.to_vec());
        if total_len == 0 {
            return Ok(StoredContent {
                reference: ContentRef::Empty,
                size: 0,
                logical_etag,
                checksums,
            });
        }
        let index = index.build().await?;
        let manifest = ContentManifestV1 {
            total_len,
            chunk_count,
            layout: ContentLayoutV1::Composed,
            chunk_index: TreeRootV1::from_tree(&index)?,
        };
        let encoded = encode_canonical(&manifest)?;
        let reference = derive_content_manifest_id(&encoded);
        let manifest_path = self.manifest_path(reference)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: manifest_path.clone(),
                expected_sha256: sha256(&encoded),
                bytes: encoded,
            })
            .await?;
        self.protect(manifest_path).await?;
        Ok(StoredContent {
            reference: ContentRef::Chunks(reference),
            size: total_len,
            logical_etag,
            checksums,
        })
    }

    /// Produce verified chunks lazily, suitable for a backpressured SDK body.
    pub fn read_stream(
        &self,
        reference: ContentRef,
        range: Option<(u64, u64)>,
    ) -> BoxStream<'static, Result<Bytes>> {
        let this = self.clone();
        Box::pin(async_stream::try_stream! {
            let ContentRef::Chunks(reference) = reference else { return };
            let object = this.plane.get(GetRequest {
                path: this.manifest_path(reference)?,
                range: None,
                physical_version: None,
            }).await?.ok_or_else(|| Error::new(ErrorCode::MissingClosure, "missing content manifest"))?;
            if derive_content_manifest_id(&object.bytes) != reference {
                Err(Error::new(ErrorCode::CorruptContent, "content manifest ID mismatch"))?;
            }
            let manifest: ContentManifestV1 = decode_canonical(&object.bytes)?;
            if let Some((start, end)) = range {
                if start > end || end >= manifest.total_len {
                    Err(Error::new(ErrorCode::InvalidRange, "unsatisfiable content range"))?;
                }
            }
            let tree = this.tree_from_root(&manifest.chunk_index)?;
            let mut iter = this.engine.range(&tree, &[], None).await?;
            let mut expected_offset = 0u64;
            let mut chunks = 0u64;
            while let Some(entry) = iter.next().await {
                let (offset, encoded) = entry?;
                if offset.len() != 8 {
                    Err(Error::new(ErrorCode::CorruptContent, "invalid chunk-index offset"))?;
                }
                let offset = u64::from_be_bytes(offset.try_into().expect("length checked"));
                if offset != expected_offset {
                    Err(Error::new(ErrorCode::CorruptContent, "chunk index is not contiguous"))?;
                }
                let chunk_ref: ContentChunkRef = decode_canonical(&encoded)?;
                if chunk_ref.len == 0 || chunk_ref.len as usize > this.chunk_bytes {
                    Err(Error::new(ErrorCode::CorruptContent, "invalid chunk length"))?;
                }
                let chunk_start = expected_offset;
                let chunk_end = expected_offset.checked_add(chunk_ref.len as u64)
                    .and_then(|value| value.checked_sub(1))
                    .ok_or_else(|| Error::new(ErrorCode::CorruptContent, "content overflow"))?;
                expected_offset = chunk_end + 1;
                chunks += 1;
                if let Some((start, end)) = range {
                    if chunk_end < start { continue; }
                    if chunk_start > end { break; }
                }
                let chunk = this.plane.get(GetRequest {
                    path: this.chunk_path(&chunk_ref.cid)?,
                    range: None,
                    physical_version: None,
                }).await?.ok_or_else(|| Error::new(ErrorCode::MissingClosure, "missing content chunk"))?;
                if Cid::from_bytes(&chunk.bytes) != chunk_ref.cid
                    || chunk.bytes.len() != chunk_ref.len as usize
                {
                    Err(Error::new(ErrorCode::CorruptContent, "content chunk failed CID/length verification"))?;
                }
                if let Some((start, end)) = range {
                    let local_start = start.saturating_sub(chunk_start) as usize;
                    let local_end = (end.min(chunk_end) - chunk_start) as usize;
                    yield Bytes::copy_from_slice(&chunk.bytes[local_start..=local_end]);
                } else {
                    yield Bytes::from(chunk.bytes);
                }
            }
            if range.is_none() && (expected_offset != manifest.total_len || chunks != manifest.chunk_count) {
                Err(Error::new(
                    ErrorCode::CorruptContent,
                    "content manifest totals do not match its chunk index",
                ))?;
            }
        })
    }

    pub async fn read_all(&self, reference: &ContentRef) -> Result<Vec<u8>> {
        let ContentRef::Chunks(reference) = reference else {
            return Ok(Vec::new());
        };
        let path = self.manifest_path(*reference)?;
        let object = self
            .plane
            .get(GetRequest {
                path,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "missing content manifest"))?;
        if derive_content_manifest_id(&object.bytes) != *reference {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "content manifest ID mismatch",
            ));
        }
        let manifest: ContentManifestV1 = decode_canonical(&object.bytes)?;
        let tree = self.tree_from_root(&manifest.chunk_index)?;
        let mut iter = self.engine.range(&tree, &[], None).await?;
        let mut result = Vec::with_capacity(
            usize::try_from(manifest.total_len)
                .map_err(|_| Error::new(ErrorCode::EntityTooLarge, "object exceeds memory"))?,
        );
        let mut expected_offset = 0u64;
        let mut chunks = 0u64;
        while let Some(entry) = iter.next().await {
            let (offset, encoded) = entry?;
            if offset.len() != 8 {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "invalid chunk-index offset",
                ));
            }
            let offset = u64::from_be_bytes(offset.try_into().expect("length checked"));
            if offset != expected_offset {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "chunk index is not contiguous",
                ));
            }
            let chunk_ref: ContentChunkRef = decode_canonical(&encoded)?;
            if chunk_ref.len == 0 || chunk_ref.len as usize > self.chunk_bytes {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "invalid chunk length",
                ));
            }
            let chunk = self
                .plane
                .get(GetRequest {
                    path: self.chunk_path(&chunk_ref.cid)?,
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "missing content chunk"))?;
            if Cid::from_bytes(&chunk.bytes) != chunk_ref.cid
                || chunk.bytes.len() != chunk_ref.len as usize
            {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "content chunk failed CID/length verification",
                ));
            }
            result.extend_from_slice(&chunk.bytes);
            expected_offset = expected_offset
                .checked_add(chunk_ref.len as u64)
                .ok_or_else(|| Error::new(ErrorCode::CorruptContent, "content overflow"))?;
            chunks += 1;
        }
        if expected_offset != manifest.total_len || chunks != manifest.chunk_count {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "content manifest totals do not match its chunk index",
            ));
        }
        Ok(result)
    }

    fn tree_from_root(&self, root: &TreeRootV1) -> Result<Tree> {
        if root.format_digest != tree_format_digest(&self.format)? {
            return Err(Error::new(
                ErrorCode::UnsupportedRepositoryFormat,
                "content index format digest mismatch",
            ));
        }
        Ok(Tree {
            root: root.root.clone(),
            config: Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        })
    }

    fn chunk_path(&self, cid: &Cid) -> Result<ObjectPath> {
        let encoded = hex::encode(cid.as_bytes());
        ObjectPath::new(format!(
            "{}/chunks/sha256/{}/{}/{}",
            self.repository_prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }

    fn manifest_path(&self, reference: ContentManifestRef) -> Result<ObjectPath> {
        let encoded = hex::encode(reference.as_bytes());
        ObjectPath::new(format!(
            "{}/content-manifests/sha256/{}/{}/{}",
            self.repository_prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}

fn node_path(prefix: &str, cid: &Cid) -> Result<ObjectPath> {
    let encoded = hex::encode(cid.as_bytes());
    ObjectPath::new(format!(
        "{prefix}/nodes/sha256/{}/{}/{}",
        &encoded[..2],
        &encoded[2..4],
        encoded
    ))
}

fn checked_content_len(current: u64, added: usize, max: u64) -> Result<u64> {
    let next = current
        .checked_add(added as u64)
        .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "content length overflow"))?;
    if next > max {
        return Err(Error::new(
            ErrorCode::EntityTooLarge,
            "object exceeds repository limit",
        ));
    }
    Ok(next)
}
