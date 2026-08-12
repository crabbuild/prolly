use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
};

use futures_util::StreamExt;
use prolly::{AsyncStore, BatchOp, Cid};

use crate::{
    codec::sha256, CommitId, CommitObjectV1, DeleteOutcome, Error, ErrorCode, GetRequest,
    ImmutablePut, ListRequest, NodeCache, NodeCacheKey, NodeIndexEntryV1, NodePackAttachmentKindV1,
    NodePackAttachmentV1, NodePackEntryV1, NodePackId, NodePackRefV1, NodePackV1, ObjectPath,
    ObjectPlane, PhysicalVersion, RepositoryId, Result, TreeFormatDigest,
};

#[derive(Clone)]
struct PackedNodeLocation {
    container: CommitId,
    pack: NodePackId,
    absolute_offset: u64,
    len: u32,
    sha256: [u8; 32],
}

type PackedPendingNodes = Arc<RwLock<BTreeMap<Cid, Vec<u8>>>>;

struct PackedNodeState {
    locations: RwLock<BoundedNodeLocations>,
    packs: RwLock<PackedNodeCache>,
    node_cache: Option<Arc<dyn NodeCache>>,
    cache_namespace: Option<NodeCacheNamespace>,
    fetch_locks: Mutex<BTreeMap<Cid, Weak<tokio::sync::Mutex<()>>>>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_insertions: AtomicU64,
    cache_errors: AtomicU64,
    cache_corruptions: AtomicU64,
    coalesced_waits: AtomicU64,
    ranged_fetches: AtomicU64,
    locator: RwLock<Option<Arc<dyn NodeLocator>>>,
}

struct DirectNodeState {
    node_cache: Arc<dyn NodeCache>,
    cache_namespace: NodeCacheNamespace,
    fetch_locks: Mutex<BTreeMap<Cid, Weak<tokio::sync::Mutex<()>>>>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_insertions: AtomicU64,
    cache_errors: AtomicU64,
    cache_corruptions: AtomicU64,
    coalesced_waits: AtomicU64,
    object_fetches: AtomicU64,
}

struct BoundedNodeLocations {
    entries: BTreeMap<Cid, PackedNodeLocation>,
    order: VecDeque<Cid>,
    capacity: usize,
}

impl BoundedNodeLocations {
    fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get(&self, cid: &Cid) -> Option<&PackedNodeLocation> {
        self.entries.get(cid)
    }

    fn insert(&mut self, cid: Cid, location: PackedNodeLocation) {
        if self.entries.insert(cid.clone(), location).is_none() {
            self.order.push_back(cid);
        }
        while self.entries.len() > self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&Cid, &PackedNodeLocation)> {
        self.entries.iter()
    }
}

#[async_trait::async_trait]
pub(crate) trait NodeLocator: Send + Sync + 'static {
    async fn locate(&self, cid: &Cid) -> Result<Option<NodeIndexEntryV1>>;
}

#[derive(Clone, Copy)]
pub(crate) struct NodeCacheNamespace {
    pub(crate) repository: RepositoryId,
    pub(crate) protocol_version: u32,
    pub(crate) tree_format: TreeFormatDigest,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeCacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub errors: u64,
    pub corruptions: u64,
    pub coalesced_waits: u64,
    pub ranged_fetches: u64,
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
    packed_pending: Option<PackedPendingNodes>,
    direct: Option<Arc<DirectNodeState>>,
}

impl<P> Clone for ProllyObjectStore<P> {
    fn clone(&self) -> Self {
        Self {
            plane: self.plane.clone(),
            repository_prefix: self.repository_prefix.clone(),
            packed: self.packed.clone(),
            packed_pending: self.packed_pending.clone(),
            direct: self.direct.clone(),
        }
    }
}

impl<P> ProllyObjectStore<P> {
    pub fn new(plane: Arc<P>, repository_prefix: impl Into<String>) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            packed: None,
            packed_pending: None,
            direct: None,
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
        Self::new_packed_with_limits(plane, repository_prefix, max_cached_pack_bytes, 65_536)
    }

    pub fn new_packed_with_limits(
        plane: Arc<P>,
        repository_prefix: impl Into<String>,
        max_cached_pack_bytes: usize,
        max_cached_locations: usize,
    ) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            packed: Some(Arc::new(PackedNodeState {
                locations: RwLock::new(BoundedNodeLocations::new(max_cached_locations)),
                packs: RwLock::new(PackedNodeCache::new(max_cached_pack_bytes)),
                node_cache: None,
                cache_namespace: None,
                fetch_locks: Mutex::new(BTreeMap::new()),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                cache_insertions: AtomicU64::new(0),
                cache_errors: AtomicU64::new(0),
                cache_corruptions: AtomicU64::new(0),
                coalesced_waits: AtomicU64::new(0),
                ranged_fetches: AtomicU64::new(0),
                locator: RwLock::new(None),
            })),
            packed_pending: Some(Arc::new(RwLock::new(BTreeMap::new()))),
            direct: None,
        }
    }

    pub fn new_cached_direct(
        plane: Arc<P>,
        repository_prefix: impl Into<String>,
        repository: RepositoryId,
        protocol_version: u32,
        tree_format: TreeFormatDigest,
        node_cache: Arc<dyn NodeCache>,
    ) -> Self {
        Self {
            plane,
            repository_prefix: repository_prefix.into(),
            packed: None,
            packed_pending: None,
            direct: Some(Arc::new(DirectNodeState {
                node_cache,
                cache_namespace: NodeCacheNamespace {
                    repository,
                    protocol_version,
                    tree_format,
                },
                fetch_locks: Mutex::new(BTreeMap::new()),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                cache_insertions: AtomicU64::new(0),
                cache_errors: AtomicU64::new(0),
                cache_corruptions: AtomicU64::new(0),
                coalesced_waits: AtomicU64::new(0),
                object_fetches: AtomicU64::new(0),
            })),
        }
    }

    pub(crate) fn new_packed_with_node_cache(
        plane: Arc<P>,
        repository_prefix: impl Into<String>,
        max_cached_pack_bytes: usize,
        max_cached_locations: usize,
        cache_namespace: NodeCacheNamespace,
        node_cache: Arc<dyn NodeCache>,
    ) -> Self {
        let mut store = Self::new_packed_with_limits(
            plane,
            repository_prefix,
            max_cached_pack_bytes,
            max_cached_locations,
        );
        let state = Arc::get_mut(store.packed.as_mut().expect("packed state was created"))
            .expect("newly created packed state has one owner");
        state.node_cache = Some(node_cache);
        state.cache_namespace = Some(cache_namespace);
        store
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

    /// Share immutable node locations and caches while isolating newly built
    /// nodes to one commit construction session.
    pub(crate) fn isolated_write_session(&self) -> Self {
        let mut session = self.clone();
        if self.packed.is_some() {
            session.packed_pending = Some(Arc::new(RwLock::new(BTreeMap::new())));
        }
        session
    }

    pub fn node_cache_snapshot(&self) -> NodeCacheSnapshot {
        if let Some(state) = &self.packed {
            return NodeCacheSnapshot {
                hits: state.cache_hits.load(Ordering::Relaxed),
                misses: state.cache_misses.load(Ordering::Relaxed),
                insertions: state.cache_insertions.load(Ordering::Relaxed),
                errors: state.cache_errors.load(Ordering::Relaxed),
                corruptions: state.cache_corruptions.load(Ordering::Relaxed),
                coalesced_waits: state.coalesced_waits.load(Ordering::Relaxed),
                ranged_fetches: state.ranged_fetches.load(Ordering::Relaxed),
            };
        }
        if let Some(state) = &self.direct {
            return NodeCacheSnapshot {
                hits: state.cache_hits.load(Ordering::Relaxed),
                misses: state.cache_misses.load(Ordering::Relaxed),
                insertions: state.cache_insertions.load(Ordering::Relaxed),
                errors: state.cache_errors.load(Ordering::Relaxed),
                corruptions: state.cache_corruptions.load(Ordering::Relaxed),
                coalesced_waits: state.coalesced_waits.load(Ordering::Relaxed),
                ranged_fetches: state.object_fetches.load(Ordering::Relaxed),
            };
        }
        NodeCacheSnapshot::default()
    }

    pub(crate) fn set_node_locator(&self, locator: Arc<dyn NodeLocator>) -> Result<()> {
        let state = self.packed.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "cannot attach a node locator to an unpacked store",
            )
        })?;
        *state
            .locator
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))? =
            Some(locator);
        Ok(())
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
            self.scan_commit_objects_for(None).await?;
        }
        Ok(())
    }

    /// Resolve the physical envelope that currently supplies a packed node.
    /// GC uses this after walking reachable CIDs so shared nodes retain at
    /// least one verified container even when that container is not in commit
    /// ancestry.
    pub(crate) async fn resolve_node_location(
        &self,
        cid: &Cid,
    ) -> Result<Option<NodeIndexEntryV1>> {
        let Some(state) = &self.packed else {
            return Ok(None);
        };
        let lookup = || -> Result<Option<PackedNodeLocation>> {
            Ok(state
                .locations
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
                .get(cid)
                .cloned())
        };
        let mut location = lookup()?;
        if location.is_none() {
            let locator = state
                .locator
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
                .clone();
            if let Some(locator) = locator {
                if let Some(entry) = locator.locate(cid).await? {
                    if entry.cid != *cid || entry.len == 0 || entry.cid.as_bytes() != entry.sha256 {
                        return Err(Error::new(
                            ErrorCode::CorruptNode,
                            "resolved node-index entry failed validation",
                        ));
                    }
                    let resolved = PackedNodeLocation {
                        container: entry.container,
                        pack: entry.pack,
                        absolute_offset: entry.absolute_offset,
                        len: entry.len,
                        sha256: entry.sha256,
                    };
                    state
                        .locations
                        .write()
                        .map_err(|_| {
                            Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
                        })?
                        .insert(cid.clone(), resolved.clone());
                    location = Some(resolved);
                }
            }
        }
        Ok(location.map(|location| NodeIndexEntryV1 {
            cid: cid.clone(),
            container: location.container,
            pack: location.pack,
            absolute_offset: location.absolute_offset,
            len: location.len,
            sha256: location.sha256,
        }))
    }

    pub(crate) fn clear_node_locations(&self) -> Result<()> {
        let Some(state) = &self.packed else {
            return Ok(());
        };
        let capacity = state
            .locations
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .capacity;
        *state
            .locations
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))? =
            BoundedNodeLocations::new(capacity);
        Ok(())
    }

    pub(crate) fn prepare_node_pack(
        &self,
        format_digest: TreeFormatDigest,
        attachments: Vec<(NodePackAttachmentKindV1, Vec<u8>)>,
    ) -> Result<Option<PreparedNodePack>> {
        if self.packed.is_none() {
            return Ok(None);
        }
        let mut pending_guard = self
            .packed_pending
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "packed pending-node session is absent",
                )
            })?
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?;
        if pending_guard.is_empty() && attachments.is_empty() {
            return Ok(None);
        }
        // A write session owns its pending set. Drain it into the prepared pack
        // so a long-running replay can safely prepare more than one commit.
        let pending = std::mem::take(&mut *pending_guard);
        drop(pending_guard);
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

    pub(crate) async fn commit_node_pack(
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
        futures_util::stream::iter(pending)
            .for_each_concurrent(Some(16), |(cid, bytes)| async move {
                self.admit_node(cid, bytes).await;
            })
            .await;
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
                Arc::new(pack.clone()),
                usize::try_from(reference.object_len).unwrap_or(usize::MAX),
            );
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
        let pending = self.packed_pending.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "packed pending-node session is absent",
            )
        })?;
        if let Some(bytes) = pending
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .get(&cid)
            .cloned()
        {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.cached_node(&cid).await {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.cached_packed_node(&cid)? {
            self.admit_node(cid.clone(), bytes.clone()).await;
            return Ok(Some(bytes));
        }
        let fetch_lock = {
            let mut locks = state
                .fetch_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&cid).and_then(Weak::upgrade) {
                state.coalesced_waits.fetch_add(1, Ordering::Relaxed);
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(cid.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let _fetch = fetch_lock.lock().await;

        // Another request may have populated any tier while this request was
        // waiting for the CID-scoped fetch lock.
        if let Some(bytes) = pending
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .get(&cid)
            .cloned()
        {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.cached_node(&cid).await {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.cached_packed_node(&cid)? {
            self.admit_node(cid.clone(), bytes.clone()).await;
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.ranged_packed_node(&cid).await? {
            self.admit_node(cid.clone(), bytes.clone()).await;
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.scan_commit_objects_for(Some(&cid)).await? {
            self.admit_node(cid.clone(), bytes.clone()).await;
            return Ok(Some(bytes));
        }
        Ok(None)
    }

    fn node_cache_key(&self, cid: Cid) -> Option<NodeCacheKey> {
        let namespace = self.packed.as_ref()?.cache_namespace?;
        Some(NodeCacheKey {
            repository: namespace.repository,
            protocol_version: namespace.protocol_version,
            tree_format: namespace.tree_format,
            cid,
        })
    }

    async fn cached_node(&self, cid: &Cid) -> Option<Vec<u8>> {
        let state = self.packed.as_ref()?;
        let cache = state.node_cache.as_ref()?;
        let key = self.node_cache_key(cid.clone())?;
        match cache.get(&key).await {
            Ok(Some(bytes)) if sha256(&bytes).as_slice() == cid.as_bytes() => {
                state.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(bytes)
            }
            Ok(Some(_)) => {
                state.cache_corruptions.fetch_add(1, Ordering::Relaxed);
                state.cache_misses.fetch_add(1, Ordering::Relaxed);
                if cache.remove(&key).await.is_err() {
                    state.cache_errors.fetch_add(1, Ordering::Relaxed);
                }
                None
            }
            Ok(None) => {
                state.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                state.cache_errors.fetch_add(1, Ordering::Relaxed);
                state.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    async fn admit_node(&self, cid: Cid, bytes: Vec<u8>) {
        let Some(state) = self.packed.as_ref() else {
            return;
        };
        if sha256(&bytes).as_slice() != cid.as_bytes() {
            state.cache_corruptions.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Some(cache) = state.node_cache.as_ref() else {
            return;
        };
        let Some(key) = self.node_cache_key(cid) else {
            return;
        };
        match cache.insert(key, bytes).await {
            Ok(()) => {
                state.cache_insertions.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                state.cache_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
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
        let mut location = state
            .locations
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
            .get(cid)
            .cloned();
        if location.is_none() {
            let locator = state
                .locator
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned"))?
                .clone();
            if let Some(locator) = locator {
                if let Some(entry) = locator.locate(cid).await? {
                    if entry.cid != *cid || entry.len == 0 || entry.cid.as_bytes() != entry.sha256 {
                        return Err(Error::new(
                            ErrorCode::CorruptNode,
                            "lazy node-index entry failed validation",
                        ));
                    }
                    let resolved = PackedNodeLocation {
                        container: entry.container,
                        pack: entry.pack,
                        absolute_offset: entry.absolute_offset,
                        len: entry.len,
                        sha256: entry.sha256,
                    };
                    state
                        .locations
                        .write()
                        .map_err(|_| {
                            Error::new(ErrorCode::InternalInvariant, "packed-node lock poisoned")
                        })?
                        .insert(cid.clone(), resolved.clone());
                    location = Some(resolved);
                }
            }
        }
        let Some(location) = location else {
            return Ok(None);
        };
        state.ranged_fetches.fetch_add(1, Ordering::Relaxed);
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

    async fn scan_commit_objects_for(&self, target: Option<&Cid>) -> Result<Option<Vec<u8>>> {
        self.packed.as_ref().ok_or_else(|| {
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
                let target_bytes = match (target, object.node_pack.as_ref()) {
                    (Some(target), Some(pack)) => pack.node(target)?.map(ToOwned::to_owned),
                    _ => None,
                };
                self.register_commit_object(id, &object, &stored.bytes)?;
                if target_bytes.is_some() {
                    return Ok(target_bytes);
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(None);
            }
        }
    }

    fn direct_cache_key(&self, cid: Cid) -> Option<NodeCacheKey> {
        let namespace = self.direct.as_ref()?.cache_namespace;
        Some(NodeCacheKey {
            repository: namespace.repository,
            protocol_version: namespace.protocol_version,
            tree_format: namespace.tree_format,
            cid,
        })
    }

    async fn direct_cached_node(&self, cid: &Cid) -> Option<Vec<u8>> {
        let state = self.direct.as_ref()?;
        let key = self.direct_cache_key(cid.clone())?;
        match state.node_cache.get(&key).await {
            Ok(Some(bytes)) if sha256(&bytes).as_slice() == cid.as_bytes() => {
                state.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(bytes)
            }
            Ok(Some(_)) => {
                state.cache_corruptions.fetch_add(1, Ordering::Relaxed);
                state.cache_misses.fetch_add(1, Ordering::Relaxed);
                if state.node_cache.remove(&key).await.is_err() {
                    state.cache_errors.fetch_add(1, Ordering::Relaxed);
                }
                None
            }
            Ok(None) => {
                state.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                state.cache_errors.fetch_add(1, Ordering::Relaxed);
                state.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    async fn admit_direct_node(&self, cid: Cid, bytes: Vec<u8>) {
        let Some(state) = self.direct.as_ref() else {
            return;
        };
        if sha256(&bytes).as_slice() != cid.as_bytes() {
            state.cache_corruptions.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Some(key) = self.direct_cache_key(cid) else {
            return;
        };
        match state.node_cache.insert(key, bytes).await {
            Ok(()) => {
                state.cache_insertions.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                state.cache_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn get_direct(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() != 32 {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                format!("Prolly node key has {} bytes, expected 32", key.len()),
            ));
        }
        let cid = Cid(key.try_into().expect("length checked"));
        if let Some(bytes) = self.direct_cached_node(&cid).await {
            return Ok(Some(bytes));
        }
        let Some(state) = self.direct.as_ref() else {
            return self.get_uncached_direct(key).await;
        };
        let fetch_lock = {
            let mut locks = state
                .fetch_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&cid).and_then(Weak::upgrade) {
                state.coalesced_waits.fetch_add(1, Ordering::Relaxed);
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(cid.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let _fetch = fetch_lock.lock().await;
        if let Some(bytes) = self.direct_cached_node(&cid).await {
            return Ok(Some(bytes));
        }
        state.object_fetches.fetch_add(1, Ordering::Relaxed);
        let bytes = self.get_uncached_direct(key).await?;
        if let Some(bytes) = &bytes {
            self.admit_direct_node(cid, bytes.clone()).await;
        }
        Ok(bytes)
    }

    async fn get_uncached_direct(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
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
}

impl<P: ObjectPlane> AsyncStore for ProllyObjectStore<P> {
    type Error = Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.packed.is_some() {
            return self.get_packed(key).await;
        }
        self.get_direct(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if sha256(value).as_slice() != key {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "attempted Prolly node write under the wrong CID",
            ));
        }
        if self.packed.is_some() {
            self.packed_pending
                .as_ref()
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "packed pending-node session is absent",
                    )
                })?
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
        if self.direct.is_some() {
            self.admit_direct_node(Cid(key.try_into().expect("length checked")), value.to_vec())
                .await;
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        if self.packed.is_some() {
            if key.len() != 32 {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    format!("Prolly node key has {} bytes, expected 32", key.len()),
                ));
            }
            self.packed_pending
                .as_ref()
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "packed pending-node session is absent",
                    )
                })?
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
