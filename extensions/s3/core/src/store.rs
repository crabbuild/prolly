use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use prolly::{AsyncStore, BatchOp, Cid};

use crate::{
    codec::sha256, DeleteOutcome, Error, ErrorCode, GetRequest, ImmutablePut, ListRequest,
    NodeIndexEntryV1, NodePackAttachmentKindV1, NodePackAttachmentV1, NodePackEntryV1, NodePackId,
    NodePackRefV1, NodePackV1, ObjectPath, ObjectPlane, PhysicalVersion, Result, TreeFormatDigest,
};

#[derive(Clone)]
struct PackedNodeLocation {
    pack: NodePackId,
    absolute_offset: u64,
    len: u32,
    sha256: [u8; 32],
}

#[derive(Default)]
struct PackedNodeState {
    pending: RwLock<BTreeMap<Cid, Vec<u8>>>,
    locations: RwLock<BTreeMap<Cid, PackedNodeLocation>>,
    packs: RwLock<BTreeMap<NodePackId, Arc<NodePackV1>>>,
    indexed_packs: RwLock<BTreeSet<NodePackId>>,
}

/// Prolly node store backed by immutable objects in an [`ObjectPlane`].
pub struct ProllyObjectStore<P> {
    plane: Arc<P>,
    repository_prefix: String,
    protection: Option<Arc<dyn crate::ProtectionSink>>,
    packed: Option<Arc<PackedNodeState>>,
}

impl<P> Clone for ProllyObjectStore<P> {
    fn clone(&self) -> Self {
        Self {
            plane: self.plane.clone(),
            repository_prefix: self.repository_prefix.clone(),
            protection: self.protection.clone(),
            packed: self.packed.clone(),
        }
    }
}

impl<P> ProllyObjectStore<P> {
    pub fn new(plane: Arc<P>, repository_prefix: impl Into<String>) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            protection: None,
            packed: None,
        }
    }

    pub fn new_packed(plane: Arc<P>, repository_prefix: impl Into<String>) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            protection: None,
            packed: Some(Arc::new(PackedNodeState::default())),
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

    fn node_pack_path(&self, id: NodePackId) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/node-packs/sha256/{}/{}/{}.pack",
            self.repository_prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}

impl<P: ObjectPlane> ProllyObjectStore<P> {
    pub fn is_packed(&self) -> bool {
        self.packed.is_some()
    }

    pub fn export_node_index(&self) -> Result<Vec<NodeIndexEntryV1>> {
        let Some(state) = &self.packed else {
            return Ok(Vec::new());
        };
        let locations = state
            .locations
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?;
        Ok(locations
            .iter()
            .map(|(cid, location)| NodeIndexEntryV1 {
                cid: cid.clone(),
                pack: location.pack,
                absolute_offset: location.absolute_offset,
                len: location.len,
                sha256: location.sha256,
            })
            .collect())
    }

    pub fn import_node_index(&self, entries: &[NodeIndexEntryV1]) -> Result<()> {
        let Some(state) = &self.packed else {
            if entries.is_empty() {
                return Ok(());
            }
            return Err(Error::new(
                ErrorCode::InternalInvariant,
                "cannot import a packed-node index into the direct node store",
            ));
        };
        let mut locations = state
            .locations
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?;
        for entry in entries {
            if entry.len == 0 || entry.cid.as_bytes() != entry.sha256 {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "node-index checkpoint entry is invalid",
                ));
            }
            locations.insert(
                entry.cid.clone(),
                PackedNodeLocation {
                    pack: entry.pack,
                    absolute_offset: entry.absolute_offset,
                    len: entry.len,
                    sha256: entry.sha256,
                },
            );
        }
        Ok(())
    }

    pub async fn rebuild_node_index(&self) -> Result<()> {
        if self.packed.is_some() {
            self.scan_node_packs().await?;
        }
        Ok(())
    }

    pub async fn flush_node_pack(
        &self,
        format_digest: TreeFormatDigest,
        attachments: Vec<(NodePackAttachmentKindV1, Vec<u8>)>,
    ) -> Result<Option<NodePackRefV1>> {
        let Some(state) = &self.packed else {
            return Ok(None);
        };
        let pending = state
            .pending
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .clone();
        if pending.is_empty() && attachments.is_empty() {
            return Ok(None);
        }
        let mut payload = Vec::new();
        let mut entries = Vec::with_capacity(pending.len());
        for (cid, bytes) in &pending {
            let offset = payload.len() as u64;
            payload.extend_from_slice(bytes);
            entries.push(NodePackEntryV1 {
                cid: cid.clone(),
                offset,
                len: u32::try_from(bytes.len()).map_err(|_| {
                    Error::new(ErrorCode::InvalidLimit, "packed node exceeds u32 length")
                })?,
                sha256: sha256(bytes),
            });
        }
        let mut packed_attachments = Vec::with_capacity(attachments.len());
        for (kind, bytes) in attachments {
            let offset = payload.len() as u64;
            payload.extend_from_slice(&bytes);
            packed_attachments.push(NodePackAttachmentV1 {
                kind,
                digest: sha256(&bytes),
                offset,
                len: u32::try_from(bytes.len()).map_err(|_| {
                    Error::new(
                        ErrorCode::InvalidLimit,
                        "node-pack attachment exceeds u32 length",
                    )
                })?,
            });
        }
        let pack = NodePackV1 {
            format_digest,
            entries,
            attachments: packed_attachments,
            payload,
        };
        pack.validate()?;
        let reference = pack.reference()?;
        let encoded = pack.encode_object()?;
        let payload_offset = NodePackV1::object_payload_offset(&encoded[..12])?;
        self.plane
            .put_immutable(ImmutablePut {
                path: self.node_pack_path(reference.id)?,
                expected_sha256: sha256(&encoded),
                bytes: encoded,
            })
            .await?;
        {
            let mut locations = state.locations.write().map_err(|_| {
                Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
            })?;
            for entry in &pack.entries {
                locations.insert(
                    entry.cid.clone(),
                    PackedNodeLocation {
                        pack: reference.id,
                        absolute_offset: payload_offset + entry.offset,
                        len: entry.len,
                        sha256: entry.sha256,
                    },
                );
            }
        }
        state
            .packs
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .insert(reference.id, Arc::new(pack));
        state
            .indexed_packs
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .insert(reference.id);
        let mut live_pending = state
            .pending
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?;
        for (cid, bytes) in pending {
            if live_pending.get(&cid) == Some(&bytes) {
                live_pending.remove(&cid);
            }
        }
        Ok(Some(reference))
    }

    async fn get_packed(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() != 32 {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                format!("Prolly node key has {} bytes, expected 32", key.len()),
            ));
        }
        let cid = Cid(key.try_into().expect("length checked"));
        let state = self.packed.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "packed node state is absent")
        })?;
        if let Some(bytes) = state
            .pending
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .get(&cid)
            .cloned()
        {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.cached_packed_node(&cid)? {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.ranged_packed_node(&cid).await? {
            return Ok(Some(bytes));
        }
        self.scan_node_packs().await?;
        if let Some(bytes) = self.cached_packed_node(&cid)? {
            return Ok(Some(bytes));
        }
        self.ranged_packed_node(&cid).await
    }

    fn cached_packed_node(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let state = self.packed.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "packed node state is absent")
        })?;
        let location = state
            .locations
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .get(cid)
            .cloned();
        let Some(location) = location else {
            return Ok(None);
        };
        let packs = state
            .packs
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?;
        let Some(pack) = packs.get(&location.pack) else {
            return Ok(None);
        };
        Ok(pack.node(cid)?.map(ToOwned::to_owned))
    }

    async fn ranged_packed_node(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let state = self.packed.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "packed node state is absent")
        })?;
        let location = state
            .locations
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .get(cid)
            .cloned();
        let Some(location) = location else {
            return Ok(None);
        };
        if location.len == 0 {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "packed node has zero length",
            ));
        }
        let end = location
            .absolute_offset
            .checked_add(u64::from(location.len) - 1)
            .ok_or_else(|| Error::new(ErrorCode::CorruptNode, "packed-node range overflow"))?;
        let object = self
            .plane
            .get(GetRequest {
                path: self.node_pack_path(location.pack)?,
                range: Some(location.absolute_offset..=end),
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "node pack is missing"))?;
        if object.bytes.len() != location.len as usize
            || sha256(&object.bytes) != location.sha256
            || cid.as_bytes() != location.sha256
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "ranged node-pack read failed CID verification",
            ));
        }
        Ok(Some(object.bytes))
    }

    async fn scan_node_packs(&self) -> Result<()> {
        let state = self.packed.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "packed node state is absent")
        })?;
        let prefix = format!("{}/node-packs/sha256/", self.repository_prefix);
        let mut continuation = None;
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: prefix.clone(),
                    continuation,
                    limit: 1_000,
                    include_versions: false,
                })
                .await?;
            for listed in page.entries {
                let name = listed.path.as_str().rsplit('/').next().unwrap_or_default();
                let encoded = name.strip_suffix(".pack").ok_or_else(|| {
                    Error::new(
                        ErrorCode::CorruptNode,
                        "node-pack path has an invalid suffix",
                    )
                })?;
                let raw = hex::decode(encoded).map_err(|_| {
                    Error::new(ErrorCode::CorruptNode, "node-pack path has an invalid ID")
                })?;
                let id = NodePackId::from_hash(raw.try_into().map_err(|_| {
                    Error::new(ErrorCode::CorruptNode, "node-pack ID has the wrong length")
                })?);
                if state
                    .indexed_packs
                    .read()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
                    })?
                    .contains(&id)
                {
                    continue;
                }
                let prefix = self
                    .plane
                    .get(GetRequest {
                        path: listed.path.clone(),
                        range: Some(0..=11),
                        physical_version: None,
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::new(ErrorCode::MissingClosure, "listed node pack disappeared")
                    })?;
                let payload_offset = NodePackV1::object_payload_offset(&prefix.bytes)?;
                if payload_offset < 13 || payload_offset > listed.metadata.len {
                    return Err(Error::new(
                        ErrorCode::CorruptNode,
                        "node-pack table-of-contents offset is invalid",
                    ));
                }
                let header = self
                    .plane
                    .get(GetRequest {
                        path: listed.path,
                        range: Some(12..=payload_offset - 1),
                        physical_version: None,
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::new(ErrorCode::MissingClosure, "listed node pack disappeared")
                    })?;
                let toc = NodePackV1::decode_toc(&header.bytes)?;
                if payload_offset + toc.payload_len != listed.metadata.len {
                    return Err(Error::new(
                        ErrorCode::CorruptNode,
                        "node-pack object length disagrees with its table of contents",
                    ));
                }
                {
                    let mut locations = state.locations.write().map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
                    })?;
                    for entry in &toc.entries {
                        locations
                            .entry(entry.cid.clone())
                            .or_insert_with(|| PackedNodeLocation {
                                pack: id,
                                absolute_offset: payload_offset + entry.offset,
                                len: entry.len,
                                sha256: entry.sha256,
                            });
                    }
                }
                state
                    .indexed_packs
                    .write()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
                    })?
                    .insert(id);
            }
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(());
            }
        }
    }
}

impl<P: ObjectPlane> AsyncStore for ProllyObjectStore<P> {
    type Error = Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.packed.is_some() {
            return self.get_packed(key).await;
        }
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
        if let Some(state) = &self.packed {
            state
                .pending
                .write()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
                .insert(Cid(key.try_into().expect("length checked")), value.to_vec());
            return Ok(());
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
        if let Some(state) = &self.packed {
            if key.len() != 32 {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    format!("Prolly node key has {} bytes, expected 32", key.len()),
                ));
            }
            state
                .pending
                .write()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
                .remove(&Cid(key.try_into().expect("length checked")));
            return Ok(());
        }
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
