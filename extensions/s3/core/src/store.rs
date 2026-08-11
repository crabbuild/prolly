use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, RwLock},
};

use prolly::{AsyncStore, BatchOp, Cid};

use crate::{
    codec::sha256, CommitId, CommitObjectV1, DeleteOutcome, Error, ErrorCode, GetRequest,
    ImmutablePut, ListRequest, NodeIndexEntryV1, NodePackAttachmentKindV1, NodePackAttachmentV1,
    NodePackEntryV1, NodePackId, NodePackRefV1, NodePackV1, ObjectPath, ObjectPlane,
    PhysicalVersion, Result, TreeFormatDigest,
};

#[derive(Clone)]
struct PackedNodeLocation {
    container: CommitId,
    pack: NodePackId,
    absolute_offset: u64,
    len: u32,
    sha256: [u8; 32],
}

struct PackedNodeState {
    pending: RwLock<BTreeMap<Cid, Vec<u8>>>,
    locations: RwLock<BTreeMap<Cid, PackedNodeLocation>>,
    packs: RwLock<PackedNodeCache>,
    indexed_containers: RwLock<BTreeSet<CommitId>>,
}

struct PackedNodeCache {
    entries: BTreeMap<NodePackId, (Arc<NodePackV1>, usize)>,
    order: VecDeque<NodePackId>,
    bytes: usize,
    max_bytes: usize,
}

impl PackedNodeCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    fn insert(&mut self, id: NodePackId, pack: Arc<NodePackV1>, bytes: usize) {
        if let Some((_, previous)) = self.entries.remove(&id) {
            self.bytes = self.bytes.saturating_sub(previous);
        }
        self.order.retain(|candidate| *candidate != id);
        if bytes > self.max_bytes {
            return;
        }
        self.entries.insert(id, (pack, bytes));
        self.order.push_back(id);
        self.bytes = self.bytes.saturating_add(bytes);
        while self.bytes > self.max_bytes && self.entries.len() > 1 {
            if let Some(evicted) = self.order.pop_front() {
                if let Some((_, removed)) = self.entries.remove(&evicted) {
                    self.bytes = self.bytes.saturating_sub(removed);
                }
            }
        }
    }
}

pub(crate) struct PreparedNodePack {
    reference: NodePackRefV1,
    pack: NodePackV1,
    pending: BTreeMap<Cid, Vec<u8>>,
}

impl PreparedNodePack {
    pub(crate) fn reference(&self) -> NodePackRefV1 {
        self.reference.clone()
    }

    pub(crate) fn pack(&self) -> &NodePackV1 {
        &self.pack
    }
}

/// Prolly node store backed by immutable objects in an [`ObjectPlane`].
pub struct ProllyObjectStore<P> {
    plane: Arc<P>,
    repository_prefix: String,
    packed: Option<Arc<PackedNodeState>>,
}

impl<P> Clone for ProllyObjectStore<P> {
    fn clone(&self) -> Self {
        Self {
            plane: self.plane.clone(),
            repository_prefix: self.repository_prefix.clone(),
            packed: self.packed.clone(),
        }
    }
}

impl<P> ProllyObjectStore<P> {
    pub fn new(plane: Arc<P>, repository_prefix: impl Into<String>) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            packed: None,
        }
    }

    pub fn new_packed(plane: Arc<P>, repository_prefix: impl Into<String>) -> Self {
        Self::new_packed_with_cache_limit(plane, repository_prefix, 64 * 1024 * 1024)
    }

    pub fn new_packed_with_cache_limit(
        plane: Arc<P>,
        repository_prefix: impl Into<String>,
        max_cached_pack_bytes: usize,
    ) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            packed: Some(Arc::new(PackedNodeState {
                pending: RwLock::new(BTreeMap::new()),
                locations: RwLock::new(BTreeMap::new()),
                packs: RwLock::new(PackedNodeCache::new(max_cached_pack_bytes)),
                indexed_containers: RwLock::new(BTreeSet::new()),
            })),
        }
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

    fn commit_path(&self, id: CommitId) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/commits/sha256/{}/{}/{}",
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
                container: location.container,
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
                    container: entry.container,
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
            self.scan_commit_objects().await?;
        }
        Ok(())
    }

    pub(crate) fn prepare_node_pack(
        &self,
        format_digest: TreeFormatDigest,
        attachments: Vec<(NodePackAttachmentKindV1, Vec<u8>)>,
    ) -> Result<Option<PreparedNodePack>> {
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
        Ok(Some(PreparedNodePack {
            reference,
            pack,
            pending,
        }))
    }

    pub(crate) fn commit_node_pack(
        &self,
        container: CommitId,
        prepared: PreparedNodePack,
        payload_offset: u64,
    ) -> Result<()> {
        let Some(state) = &self.packed else {
            return Err(Error::new(
                ErrorCode::InternalInvariant,
                "cannot commit a node pack into an unpacked store",
            ));
        };
        let PreparedNodePack {
            reference,
            pack,
            pending,
        } = prepared;
        {
            let mut locations = state.locations.write().map_err(|_| {
                Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
            })?;
            for entry in &pack.entries {
                locations.insert(
                    entry.cid.clone(),
                    PackedNodeLocation {
                        container,
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
            .insert(
                reference.id,
                Arc::new(pack),
                usize::try_from(reference.object_len).unwrap_or(usize::MAX),
            );
        state
            .indexed_containers
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .insert(container);
        let mut live_pending = state
            .pending
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?;
        for (cid, bytes) in pending {
            if live_pending.get(&cid) == Some(&bytes) {
                live_pending.remove(&cid);
            }
        }
        Ok(())
    }

    pub(crate) fn register_commit_object(
        &self,
        container: CommitId,
        object: &CommitObjectV1,
        encoded: &[u8],
    ) -> Result<()> {
        let Some(pack) = object.node_pack.as_ref() else {
            return Ok(());
        };
        let Some(state) = &self.packed else {
            return Ok(());
        };
        let payload_offset = CommitObjectV1::node_payload_offset(encoded)?.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "commit node-pack payload is absent",
            )
        })?;
        let reference = pack.reference()?;
        {
            let mut locations = state.locations.write().map_err(|_| {
                Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
            })?;
            for entry in &pack.entries {
                locations
                    .entry(entry.cid.clone())
                    .or_insert(PackedNodeLocation {
                        container,
                        pack: reference.id,
                        absolute_offset: payload_offset + entry.offset,
                        len: entry.len,
                        sha256: entry.sha256,
                    });
            }
        }
        state
            .packs
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .insert(
                reference.id,
                Arc::new(pack.clone()),
                usize::try_from(reference.object_len).unwrap_or(usize::MAX),
            );
        state
            .indexed_containers
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .insert(container);
        Ok(())
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
        self.scan_commit_objects().await?;
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
        let Some((pack, _)) = packs.entries.get(&location.pack) else {
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
                path: self.commit_path(location.container)?,
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

    async fn scan_commit_objects(&self) -> Result<()> {
        let state = self.packed.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "packed node state is absent")
        })?;
        let prefix = format!("{}/commits/sha256/", self.repository_prefix);
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
                let encoded = listed.path.as_str().rsplit('/').next().unwrap_or_default();
                let raw = hex::decode(encoded).map_err(|_| {
                    Error::new(ErrorCode::CorruptCommit, "commit path has an invalid ID")
                })?;
                let id = CommitId::from_hash(raw.try_into().map_err(|_| {
                    Error::new(ErrorCode::CorruptCommit, "commit ID has the wrong length")
                })?);
                if state
                    .indexed_containers
                    .read()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
                    })?
                    .contains(&id)
                {
                    continue;
                }
                let stored = self
                    .plane
                    .get(GetRequest {
                        path: listed.path.clone(),
                        range: None,
                        physical_version: None,
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::new(ErrorCode::MissingClosure, "listed commit disappeared")
                    })?;
                if stored.bytes.len() as u64 != listed.metadata.len {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "commit object length disagrees with its listing metadata",
                    ));
                }
                let object = CommitObjectV1::decode_object(&stored.bytes)?;
                if object.commit.id()? != id {
                    return Err(Error::new(ErrorCode::CorruptCommit, "commit ID mismatch"));
                }
                self.register_commit_object(id, &object, &stored.bytes)?;
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
