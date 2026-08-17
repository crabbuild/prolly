use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
};

use futures_util::{stream, StreamExt};
use prolly::{AsyncStore, BatchOp, Cid};

use crate::{
    codec::sha256, CommitId, CommitObject, DeleteOutcome, Error, ErrorCode, GetRequest,
    ImmutablePut, JournalNodeIndexEntry, NodeCache, NodeCacheKey, NodePack, NodePackAttachment,
    NodePackAttachmentKind, NodePackEntry, NodePackId, NodePackRef, ObjectPath, ObjectPlane,
    PhysicalVersion, RepositoryId, Result, TreeFormatDigest,
};

#[derive(Clone, Copy)]
enum PackedCommitId {
    Native(CommitId),
}

#[derive(Clone)]
struct PackedNodeLocation {
    container: PackedCommitId,
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
    fetched_bytes: AtomicU64,
    avoided_bytes: AtomicU64,
    admission_rejections: AtomicU64,
    node_requests: AtomicU64,
    requested_bytes: AtomicU64,
    prefetch_batches: AtomicU64,
    prefetched_nodes: AtomicU64,
    pinned: Mutex<BTreeSet<Cid>>,
    pinned_bytes: AtomicU64,
    max_tracked_pins: usize,
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
    fetched_bytes: AtomicU64,
    avoided_bytes: AtomicU64,
    admission_rejections: AtomicU64,
    node_requests: AtomicU64,
    requested_bytes: AtomicU64,
    prefetch_batches: AtomicU64,
    prefetched_nodes: AtomicU64,
    pinned: Mutex<BTreeSet<Cid>>,
    pinned_bytes: AtomicU64,
    max_tracked_pins: usize,
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
}

#[async_trait::async_trait]
pub(crate) trait NodeLocator: Send + Sync + 'static {
    async fn locate(&self, cid: &Cid) -> Result<Option<LocatedPackedNode>>;
}

#[derive(Clone)]
pub(crate) struct LocatedPackedNode {
    cid: Cid,
    container: PackedCommitId,
    pack: NodePackId,
    absolute_offset: u64,
    len: u32,
    sha256: [u8; 32],
}

impl From<JournalNodeIndexEntry> for LocatedPackedNode {
    fn from(entry: JournalNodeIndexEntry) -> Self {
        Self {
            cid: entry.cid,
            container: PackedCommitId::Native(entry.container),
            pack: entry.pack,
            absolute_offset: entry.absolute_offset,
            len: entry.len,
            sha256: entry.sha256,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct NodeCacheNamespace {
    pub(crate) repository: RepositoryId,
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
    pub fetched_bytes: u64,
    pub avoided_bytes: u64,
    pub admission_rejections: u64,
    /// Number of immutable node values returned to tree engines.
    pub node_requests: u64,
    /// Canonical node bytes requested by tree engines, independent of tier.
    pub requested_bytes: u64,
    /// Predictive multi-node prefetch batches issued by tree traversal.
    pub prefetch_batches: u64,
    /// Nodes requested by predictive prefetch batches.
    pub prefetched_nodes: u64,
    /// Upper-level nodes retained in the advisory pinned tier.
    pub pinned_nodes: u64,
    pub pinned_bytes: u64,
}

impl NodeCacheSnapshot {
    pub(crate) fn saturating_add(self, other: Self) -> Self {
        Self {
            hits: self.hits.saturating_add(other.hits),
            misses: self.misses.saturating_add(other.misses),
            insertions: self.insertions.saturating_add(other.insertions),
            errors: self.errors.saturating_add(other.errors),
            corruptions: self.corruptions.saturating_add(other.corruptions),
            coalesced_waits: self.coalesced_waits.saturating_add(other.coalesced_waits),
            ranged_fetches: self.ranged_fetches.saturating_add(other.ranged_fetches),
            fetched_bytes: self.fetched_bytes.saturating_add(other.fetched_bytes),
            avoided_bytes: self.avoided_bytes.saturating_add(other.avoided_bytes),
            admission_rejections: self
                .admission_rejections
                .saturating_add(other.admission_rejections),
            node_requests: self.node_requests.saturating_add(other.node_requests),
            requested_bytes: self.requested_bytes.saturating_add(other.requested_bytes),
            prefetch_batches: self.prefetch_batches.saturating_add(other.prefetch_batches),
            prefetched_nodes: self.prefetched_nodes.saturating_add(other.prefetched_nodes),
            pinned_nodes: self.pinned_nodes.saturating_add(other.pinned_nodes),
            pinned_bytes: self.pinned_bytes.saturating_add(other.pinned_bytes),
        }
    }

    /// Saturating interval metrics suitable for request and startup reports.
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            hits: self.hits.saturating_sub(earlier.hits),
            misses: self.misses.saturating_sub(earlier.misses),
            insertions: self.insertions.saturating_sub(earlier.insertions),
            errors: self.errors.saturating_sub(earlier.errors),
            corruptions: self.corruptions.saturating_sub(earlier.corruptions),
            coalesced_waits: self.coalesced_waits.saturating_sub(earlier.coalesced_waits),
            ranged_fetches: self.ranged_fetches.saturating_sub(earlier.ranged_fetches),
            fetched_bytes: self.fetched_bytes.saturating_sub(earlier.fetched_bytes),
            avoided_bytes: self.avoided_bytes.saturating_sub(earlier.avoided_bytes),
            admission_rejections: self
                .admission_rejections
                .saturating_sub(earlier.admission_rejections),
            node_requests: self.node_requests.saturating_sub(earlier.node_requests),
            requested_bytes: self.requested_bytes.saturating_sub(earlier.requested_bytes),
            prefetch_batches: self
                .prefetch_batches
                .saturating_sub(earlier.prefetch_batches),
            prefetched_nodes: self
                .prefetched_nodes
                .saturating_sub(earlier.prefetched_nodes),
            pinned_nodes: self.pinned_nodes.saturating_sub(earlier.pinned_nodes),
            pinned_bytes: self.pinned_bytes.saturating_sub(earlier.pinned_bytes),
        }
    }

    pub fn hit_ratio(self) -> f64 {
        let lookups = self.hits.saturating_add(self.misses);
        if lookups == 0 {
            0.0
        } else {
            self.hits as f64 / lookups as f64
        }
    }

    /// Provider node-body bytes fetched per canonical node byte returned.
    /// Values below one indicate cache reuse. Client adapters combine this
    /// with all provider response bytes for end-to-end metadata amplification.
    pub fn byte_amplification(self) -> f64 {
        if self.requested_bytes == 0 {
            0.0
        } else {
            self.fetched_bytes as f64 / self.requested_bytes as f64
        }
    }
}

struct PackedNodeCache {
    entries: BTreeMap<NodePackId, (Arc<NodePack>, usize)>,
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

    fn insert(&mut self, id: NodePackId, pack: Arc<NodePack>, bytes: usize) {
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
    reference: NodePackRef,
    pack: NodePack,
    pending: BTreeMap<Cid, Vec<u8>>,
}

impl PreparedNodePack {
    pub(crate) fn reference(&self) -> NodePackRef {
        self.reference.clone()
    }

    pub(crate) fn pack(&self) -> &NodePack {
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
    write_direct: bool,
}

impl<P> Clone for ProllyObjectStore<P> {
    fn clone(&self) -> Self {
        Self {
            plane: self.plane.clone(),
            repository_prefix: self.repository_prefix.clone(),
            packed: self.packed.clone(),
            packed_pending: self.packed_pending.clone(),
            direct: self.direct.clone(),
            write_direct: self.write_direct,
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
            write_direct: false,
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
                fetched_bytes: AtomicU64::new(0),
                avoided_bytes: AtomicU64::new(0),
                admission_rejections: AtomicU64::new(0),
                node_requests: AtomicU64::new(0),
                requested_bytes: AtomicU64::new(0),
                prefetch_batches: AtomicU64::new(0),
                prefetched_nodes: AtomicU64::new(0),
                pinned: Mutex::new(BTreeSet::new()),
                pinned_bytes: AtomicU64::new(0),
                max_tracked_pins: max_cached_locations,
                locator: RwLock::new(None),
            })),
            packed_pending: Some(Arc::new(RwLock::new(BTreeMap::new()))),
            direct: None,
            write_direct: false,
        }
    }

    pub fn new_cached_direct(
        plane: Arc<P>,
        repository_prefix: impl Into<String>,
        repository: RepositoryId,
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
                fetched_bytes: AtomicU64::new(0),
                avoided_bytes: AtomicU64::new(0),
                admission_rejections: AtomicU64::new(0),
                node_requests: AtomicU64::new(0),
                requested_bytes: AtomicU64::new(0),
                prefetch_batches: AtomicU64::new(0),
                prefetched_nodes: AtomicU64::new(0),
                pinned: Mutex::new(BTreeSet::new()),
                pinned_bytes: AtomicU64::new(0),
                max_tracked_pins: 65_536,
            })),
            write_direct: true,
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

    pub(crate) fn direct_node_path(&self, cid: &Cid) -> Result<ObjectPath> {
        self.path_for_key(cid.as_bytes())
    }

    fn commit_path(&self, id: PackedCommitId) -> Result<ObjectPath> {
        let PackedCommitId::Native(id) = id;
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

    /// Read through the packed-node locator and cache while writing newly
    /// created immutable nodes to their deterministic direct CID paths. This
    /// is used by restartable repository builders whose intermediate roots
    /// must survive process loss before a final commit envelope exists.
    pub(crate) fn durable_direct_write_session(&self) -> Self {
        let mut session = self.clone();
        session.write_direct = true;
        session
    }

    pub fn node_cache_snapshot(&self) -> NodeCacheSnapshot {
        if let Some(state) = &self.packed {
            let exact_pinned = state
                .node_cache
                .as_ref()
                .and_then(|cache| cache.pinned_usage());
            let (pinned_nodes, pinned_bytes) = exact_pinned.map_or_else(
                || {
                    (
                        state
                            .pinned
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .len() as u64,
                        state.pinned_bytes.load(Ordering::Relaxed),
                    )
                },
                |(nodes, bytes)| (nodes as u64, bytes as u64),
            );
            return NodeCacheSnapshot {
                hits: state.cache_hits.load(Ordering::Relaxed),
                misses: state.cache_misses.load(Ordering::Relaxed),
                insertions: state.cache_insertions.load(Ordering::Relaxed),
                errors: state.cache_errors.load(Ordering::Relaxed),
                corruptions: state.cache_corruptions.load(Ordering::Relaxed),
                coalesced_waits: state.coalesced_waits.load(Ordering::Relaxed),
                ranged_fetches: state.ranged_fetches.load(Ordering::Relaxed),
                fetched_bytes: state.fetched_bytes.load(Ordering::Relaxed),
                avoided_bytes: state.avoided_bytes.load(Ordering::Relaxed),
                admission_rejections: state.admission_rejections.load(Ordering::Relaxed),
                node_requests: state.node_requests.load(Ordering::Relaxed),
                requested_bytes: state.requested_bytes.load(Ordering::Relaxed),
                prefetch_batches: state.prefetch_batches.load(Ordering::Relaxed),
                prefetched_nodes: state.prefetched_nodes.load(Ordering::Relaxed),
                pinned_nodes,
                pinned_bytes,
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
                fetched_bytes: state.fetched_bytes.load(Ordering::Relaxed),
                avoided_bytes: state.avoided_bytes.load(Ordering::Relaxed),
                admission_rejections: state.admission_rejections.load(Ordering::Relaxed),
                node_requests: state.node_requests.load(Ordering::Relaxed),
                requested_bytes: state.requested_bytes.load(Ordering::Relaxed),
                prefetch_batches: state.prefetch_batches.load(Ordering::Relaxed),
                prefetched_nodes: state.prefetched_nodes.load(Ordering::Relaxed),
                pinned_nodes: state
                    .pinned
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len() as u64,
                pinned_bytes: state.pinned_bytes.load(Ordering::Relaxed),
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

    pub(crate) fn prepare_node_pack(
        &self,
        format_digest: TreeFormatDigest,
        attachments: Vec<(NodePackAttachmentKind, Vec<u8>)>,
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
            entries.push(NodePackEntry {
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
            packed_attachments.push(NodePackAttachment {
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
        let pack = NodePack {
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
        node_region_offset: u64,
    ) -> Result<()> {
        self.commit_node_pack_for(
            PackedCommitId::Native(container),
            prepared,
            node_region_offset,
        )
        .await
    }

    async fn commit_node_pack_for(
        &self,
        container: PackedCommitId,
        prepared: PreparedNodePack,
        node_region_offset: u64,
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
                        absolute_offset: node_region_offset + entry.offset,
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

    #[allow(dead_code)] // Retained for offline callers that already hold a complete object.
    pub(crate) fn register_commit_object(
        &self,
        container: CommitId,
        object: &CommitObject,
        encoded: &[u8],
    ) -> Result<()> {
        let node_region_offset = CommitObject::node_region_offset(encoded)?;
        self.register_node_pack(
            PackedCommitId::Native(container),
            object.node_pack.as_ref(),
            node_region_offset,
        )
    }

    #[allow(dead_code)]
    fn register_node_pack(
        &self,
        container: PackedCommitId,
        pack: Option<&NodePack>,
        node_region_offset: Option<u64>,
    ) -> Result<()> {
        let Some(pack) = pack else {
            return Ok(());
        };
        let Some(state) = &self.packed else {
            return Ok(());
        };
        let node_region_offset = node_region_offset.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "commit node-pack node region is absent",
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
                        absolute_offset: node_region_offset + entry.offset,
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
        // Administrative builders (notably resumable merges) may
        // checkpoint immutable state nodes directly at their deterministic CID
        // paths. This point lookup is the scale-safe fallback after the
        // journal-derived packed-node index misses; it never scans a namespace.
        if let Some(bytes) = self.get_uncached_direct(key).await? {
            self.admit_node(cid, bytes.clone()).await;
            return Ok(Some(bytes));
        }
        Ok(None)
    }

    fn node_cache_key(&self, cid: Cid) -> Option<NodeCacheKey> {
        let namespace = self.packed.as_ref()?.cache_namespace?;
        Some(NodeCacheKey {
            repository: namespace.repository,
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
                state
                    .avoided_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
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
        if !cache.admits(&key, bytes.len()) {
            state.admission_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
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
        state
            .fetched_bytes
            .fetch_add(object.bytes.len() as u64, Ordering::Relaxed);
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

    fn direct_cache_key(&self, cid: Cid) -> Option<NodeCacheKey> {
        let namespace = self.direct.as_ref()?.cache_namespace;
        Some(NodeCacheKey {
            repository: namespace.repository,
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
                state
                    .avoided_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
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
        if !state.node_cache.admits(&key, bytes.len()) {
            state.admission_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }
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
            state
                .fetched_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
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

    fn record_returned_node(&self, bytes: usize) {
        let bytes = bytes as u64;
        if let Some(state) = &self.packed {
            state.node_requests.fetch_add(1, Ordering::Relaxed);
            state.requested_bytes.fetch_add(bytes, Ordering::Relaxed);
        } else if let Some(state) = &self.direct {
            state.node_requests.fetch_add(1, Ordering::Relaxed);
            state.requested_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn record_prefetch(&self, nodes: usize) {
        let Some(nodes) = u64::try_from(nodes).ok() else {
            return;
        };
        if let Some(state) = &self.packed {
            state.prefetch_batches.fetch_add(1, Ordering::Relaxed);
            state.prefetched_nodes.fetch_add(nodes, Ordering::Relaxed);
        } else if let Some(state) = &self.direct {
            state.prefetch_batches.fetch_add(1, Ordering::Relaxed);
            state.prefetched_nodes.fetch_add(nodes, Ordering::Relaxed);
        }
    }

    /// Promote a verified root or upper-level node into the cache's advisory
    /// pinned tier. The provider remains authoritative if the tier is lost.
    pub(crate) async fn pin_node(&self, cid: Cid, bytes: Vec<u8>) -> Result<()> {
        if sha256(&bytes).as_slice() != cid.as_bytes() {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "cannot pin a node that fails CID verification",
            ));
        }
        let (cache, key) = if let Some(state) = &self.packed {
            let Some(cache) = state.node_cache.as_ref() else {
                return Ok(());
            };
            let Some(key) = self.node_cache_key(cid.clone()) else {
                return Ok(());
            };
            (cache.clone(), key)
        } else if let Some(state) = &self.direct {
            let Some(key) = self.direct_cache_key(cid.clone()) else {
                return Ok(());
            };
            (state.node_cache.clone(), key)
        } else {
            return Ok(());
        };
        if !cache.admits(&key, bytes.len()) {
            if let Some(state) = &self.packed {
                state.admission_rejections.fetch_add(1, Ordering::Relaxed);
            } else if let Some(state) = &self.direct {
                state.admission_rejections.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        }
        if cache.pin(key, bytes.clone()).await.is_err() {
            if let Some(state) = &self.packed {
                state.cache_errors.fetch_add(1, Ordering::Relaxed);
            } else if let Some(state) = &self.direct {
                state.cache_errors.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        }
        let (pinned, pinned_bytes, max_tracked_pins, exact_usage) =
            if let Some(state) = &self.packed {
                (
                    &state.pinned,
                    &state.pinned_bytes,
                    state.max_tracked_pins,
                    cache.pinned_usage().is_some(),
                )
            } else {
                let state = self.direct.as_ref().expect("direct state selected above");
                (
                    &state.pinned,
                    &state.pinned_bytes,
                    state.max_tracked_pins,
                    false,
                )
            };
        if exact_usage {
            return Ok(());
        }
        let mut pinned = pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !pinned.contains(&cid) && pinned.len() < max_tracked_pins && pinned.insert(cid) {
            pinned_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl<P: ObjectPlane> AsyncStore for ProllyObjectStore<P> {
    type Error = Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let value = if self.packed.is_some() {
            self.get_packed(key).await?
        } else {
            self.get_direct(key).await?
        };
        if let Some(bytes) = &value {
            self.record_returned_node(bytes.len());
        }
        Ok(value)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if sha256(value).as_slice() != key {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "attempted Prolly node write under the wrong CID",
            ));
        }
        if self.packed.is_some() && !self.write_direct {
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
        } else if self.packed.is_some() {
            self.admit_node(Cid(key.try_into().expect("length checked")), value.to_vec())
                .await;
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        if self.packed.is_some() && !self.write_direct {
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

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    async fn batch_get_ordered_unique(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        if keys.len() > 1 {
            self.record_prefetch(keys.len());
        }
        let keys = keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        stream::iter(
            keys.into_iter()
                .map(|key| async move { self.get(&key).await }),
        )
        .buffered(self.read_parallelism())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
    }
}
