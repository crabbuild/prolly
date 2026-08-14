use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use crate::{
    decode_canonical, CommitGraphHead, CompareExchange, CompareExchangeOutcome, DeleteOutcome,
    Error, ErrorCode, JournalDerivedIndexHead, ListRequest, NodeIndexHead, ObjectPath, ObjectPlane,
    OperationIndexHead, PhysicalVersion, RefCatalogHead, RefCatalogShardHead, RefValue, Result,
    StorageToken,
};

pub const DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN: usize = 100;

/// Stable classification for every CAS-updated repository control object.
/// Immutable data and create-once format markers deliberately have no class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MutableControlKind {
    AuthorityLease,
    MaintenanceGate,
    BranchRef,
    TagRef,
    NodeIndexHead,
    RefCatalogHead,
    RefCatalogShardHead,
    CommitGraphHead,
    OperationIndexHead,
    JournalDerivedIndexHead,
    GcCoordinator,
    GcCursor,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ControlVersionCompactionReport {
    pub scanned: usize,
    pub retained: usize,
    pub deleted: usize,
    pub already_missing: usize,
}

/// Deep Module for CAS publication and bounded provider versions of mutable
/// control objects. Its Interface makes exact-version retention automatic for
/// every update while the ObjectPlane adapters hide provider pagination and
/// bulk-delete details.
#[derive(Clone)]
pub struct MutableControlStore<P: ObjectPlane> {
    plane: Arc<P>,
    repository_prefix: String,
    versions_to_retain: usize,
    seen_paths: Arc<Mutex<BTreeSet<ObjectPath>>>,
    observer: Option<Arc<dyn MutableControlObserver>>,
    mutation_barrier: Option<Arc<tokio::sync::RwLock<()>>>,
}

#[async_trait::async_trait]
pub trait MutableControlObserver: Send + Sync {
    async fn before_compare_exchange(
        &self,
        kind: MutableControlKind,
        request: &CompareExchange,
    ) -> Result<()>;

    async fn after_compare_exchange(
        &self,
        _kind: MutableControlKind,
        _request: &CompareExchange,
    ) -> Result<()> {
        Ok(())
    }
}

impl<P: ObjectPlane> MutableControlStore<P> {
    pub fn new(
        plane: Arc<P>,
        repository_prefix: impl Into<String>,
        versions_to_retain: usize,
    ) -> Result<Self> {
        let repository_prefix = repository_prefix.into();
        if repository_prefix.is_empty() || !(2..=10_000).contains(&versions_to_retain) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "mutable-control retention must keep between 2 and 10,000 versions",
            ));
        }
        Ok(Self {
            plane,
            repository_prefix,
            versions_to_retain,
            seen_paths: Arc::new(Mutex::new(BTreeSet::new())),
            observer: None,
            mutation_barrier: None,
        })
    }

    pub fn with_observer(mut self, observer: Arc<dyn MutableControlObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn with_mutation_barrier(mut self, barrier: Arc<tokio::sync::RwLock<()>>) -> Self {
        self.mutation_barrier = Some(barrier);
        self
    }

    pub fn classify(&self, path: &ObjectPath) -> Option<MutableControlKind> {
        classify_mutable_control_path(&self.repository_prefix, path)
    }

    /// Compact before updating. Branch refs use their persisted generation for
    /// amortized cleanup; all other mutable controls compact on every update.
    /// If the expected token is stale, the provider CAS reports the ordinary
    /// conflict and the path remains due for first-update compaction.
    pub async fn compare_exchange(
        &self,
        request: CompareExchange,
    ) -> Result<CompareExchangeOutcome> {
        let _barrier = match &self.mutation_barrier {
            Some(barrier) => Some(barrier.read().await),
            None => None,
        };
        let kind = self.classify(&request.path).ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidRequest,
                "compare_exchange through the mutable-control store requires a registered control path",
            )
        })?;
        if let Some(expected) = request.expected.as_ref() {
            let first_update = self.is_unseen(&request.path)?;
            let ref_interval = (self.versions_to_retain / 2).max(1) as u64;
            let scheduled = match kind {
                MutableControlKind::BranchRef => {
                    let reference: RefValue = decode_canonical(&request.bytes)?;
                    first_update || reference.generation.0.is_multiple_of(ref_interval)
                }
                MutableControlKind::NodeIndexHead => {
                    let head: NodeIndexHead = decode_canonical(&request.bytes)?;
                    first_update || head.generation.is_multiple_of(ref_interval)
                }
                MutableControlKind::RefCatalogHead => {
                    let head: RefCatalogHead = decode_canonical(&request.bytes)?;
                    first_update || head.generation.is_multiple_of(ref_interval)
                }
                MutableControlKind::RefCatalogShardHead => {
                    let head: RefCatalogShardHead = decode_canonical(&request.bytes)?;
                    first_update || head.generation.is_multiple_of(ref_interval)
                }
                MutableControlKind::CommitGraphHead => {
                    let head: CommitGraphHead = decode_canonical(&request.bytes)?;
                    first_update || head.generation.is_multiple_of(ref_interval)
                }
                MutableControlKind::OperationIndexHead => {
                    let head: OperationIndexHead = decode_canonical(&request.bytes)?;
                    first_update || head.generation.is_multiple_of(ref_interval)
                }
                MutableControlKind::JournalDerivedIndexHead => {
                    let head: JournalDerivedIndexHead = decode_canonical(&request.bytes)?;
                    first_update || head.generation.is_multiple_of(ref_interval)
                }
                _ => true,
            };
            if scheduled {
                let target = match kind {
                    MutableControlKind::BranchRef
                    | MutableControlKind::NodeIndexHead
                    | MutableControlKind::RefCatalogHead
                    | MutableControlKind::RefCatalogShardHead
                    | MutableControlKind::CommitGraphHead
                    | MutableControlKind::OperationIndexHead
                    | MutableControlKind::JournalDerivedIndexHead => {
                        (self.versions_to_retain / 2).max(1)
                    }
                    _ => self.versions_to_retain.saturating_sub(1),
                };
                self.compact_if_current(&request.path, expected, target)
                    .await?;
            }
        }
        if let Some(observer) = self.observer.as_ref() {
            observer.before_compare_exchange(kind, &request).await?;
        }
        let path = request.path.clone();
        let outcome = self.plane.compare_exchange(request.clone()).await;
        if let Some(observer) = self.observer.as_ref() {
            observer.after_compare_exchange(kind, &request).await?;
        }
        let outcome = outcome?;
        if matches!(outcome, CompareExchangeOutcome::Applied(_)) {
            self.mark_seen(path)?;
        }
        Ok(outcome)
    }

    pub async fn compact_path(&self, path: &ObjectPath) -> Result<ControlVersionCompactionReport> {
        self.compact_path_with_retention(path, self.versions_to_retain)
            .await
    }

    pub async fn compact_path_with_retention(
        &self,
        path: &ObjectPath,
        versions_to_retain: usize,
    ) -> Result<ControlVersionCompactionReport> {
        if self.classify(path).is_none() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical path is not a registered mutable control object",
            ));
        }
        if versions_to_retain == 0 || versions_to_retain > self.versions_to_retain {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "path-specific control retention must fit the repository control bound",
            ));
        }
        let current =
            self.plane.load_mutable(path).await?.ok_or_else(|| {
                Error::new(ErrorCode::InvalidRevision, "control object is missing")
            })?;
        let report = self
            .compact_if_current(path, &current.metadata.token, versions_to_retain)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::RefConflict,
                    "control object changed while starting physical-version compaction",
                )
            })?;
        self.mark_seen(path.clone())?;
        Ok(report)
    }

    fn mark_seen(&self, path: ObjectPath) -> Result<bool> {
        self.seen_paths
            .lock()
            .map(|mut paths| paths.insert(path))
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "mutable-control seen-path lock poisoned",
                )
            })
    }

    fn is_unseen(&self, path: &ObjectPath) -> Result<bool> {
        self.seen_paths
            .lock()
            .map(|paths| !paths.contains(path))
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "mutable-control seen-path lock poisoned",
                )
            })
    }

    async fn compact_if_current(
        &self,
        path: &ObjectPath,
        protected: &StorageToken,
        retain_limit: usize,
    ) -> Result<Option<ControlVersionCompactionReport>> {
        let Some(protected_version) = protected.version_id.as_deref() else {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "mutable-control compaction requires versioned CAS objects",
            ));
        };
        let current = self.plane.load_mutable(path).await?;
        if current
            .as_ref()
            .is_none_or(|stored| &stored.metadata.token != protected)
        {
            return Ok(None);
        }

        let mut continuation = None;
        let mut versions = Vec::new();
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: path.as_str().to_string(),
                    continuation,
                    limit: 1_000,
                    include_versions: true,
                })
                .await?;
            versions.extend(page.entries.into_iter().filter(|entry| entry.path == *path));
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        if !versions.iter().any(|entry| {
            entry.is_latest && entry.metadata.token.version_id.as_deref() == Some(protected_version)
        }) {
            return Ok(None);
        }
        if self
            .plane
            .load_mutable(path)
            .await?
            .is_none_or(|stored| stored.metadata.token != *protected)
        {
            return Ok(None);
        }

        versions.sort_by(|left, right| {
            right
                .is_latest
                .cmp(&left.is_latest)
                .then_with(|| {
                    right
                        .metadata
                        .last_modified_millis
                        .cmp(&left.metadata.last_modified_millis)
                })
                .then_with(|| {
                    right
                        .metadata
                        .token
                        .version_id
                        .cmp(&left.metadata.token.version_id)
                })
        });
        let scanned = versions.len();
        let mut retained = BTreeSet::from([protected_version.to_string()]);
        for entry in &versions {
            if retained.len() >= retain_limit {
                break;
            }
            if let Some(version_id) = entry.metadata.token.version_id.as_ref() {
                retained.insert(version_id.clone());
            }
        }
        let candidates = versions
            .into_iter()
            .filter_map(|entry| {
                let version_id = entry.metadata.token.version_id?;
                (!retained.contains(&version_id))
                    .then_some((entry.path, PhysicalVersion::Versioned { version_id }))
            })
            .collect::<Vec<_>>();
        let mut report = ControlVersionCompactionReport {
            scanned,
            retained: retained.len(),
            ..ControlVersionCompactionReport::default()
        };
        for batch in candidates.chunks(1_000) {
            for outcome in self.plane.delete_exact_batch(batch.to_vec()).await? {
                match outcome {
                    DeleteOutcome::Deleted => report.deleted += 1,
                    DeleteOutcome::NotFound => report.already_missing += 1,
                    DeleteOutcome::TokenMismatch => {
                        return Err(Error::new(
                            ErrorCode::PreconditionFailed,
                            "exact mutable-control version changed during compaction",
                        ));
                    }
                }
            }
        }
        Ok(Some(report))
    }
}

pub fn classify_mutable_control_path(
    repository_prefix: &str,
    path: &ObjectPath,
) -> Option<MutableControlKind> {
    let relative = path
        .as_str()
        .strip_prefix(repository_prefix)?
        .strip_prefix('/')?;
    let parts = relative.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["authority", "branches" | "system", _, "lease.cbor"] => {
            Some(MutableControlKind::AuthorityLease)
        }
        ["authority", "maintenance", "gate.cbor"] => Some(MutableControlKind::MaintenanceGate),
        ["refs", "heads", _] => Some(MutableControlKind::BranchRef),
        ["refs", "tags", _] => Some(MutableControlKind::TagRef),
        ["node-index", "head.cbor"] => Some(MutableControlKind::NodeIndexHead),
        ["ref-catalog", "head.cbor"] => Some(MutableControlKind::RefCatalogHead),
        ["ref-catalog", "shards", _, "head.cbor"] => Some(MutableControlKind::RefCatalogShardHead),
        ["commit-graph", "head.cbor"] => Some(MutableControlKind::CommitGraphHead),
        ["operation-index", "heads", _] => Some(MutableControlKind::OperationIndexHead),
        ["journal-index", "heads", _] => Some(MutableControlKind::JournalDerivedIndexHead),
        ["gc", "coordinator.cbor"] => Some(MutableControlKind::GcCoordinator),
        ["gc", "epochs", _, "cursor.cbor"] => Some(MutableControlKind::GcCursor),
        _ => None,
    }
}
