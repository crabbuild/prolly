use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::{stream, StreamExt, TryStreamExt};
use prolly::{
    indexed_collection_source_map_id, AsyncBlobStore, AsyncManifestStoreScan,
    BlobReachabilityLimits, BlobRef, Cid, GcTraversalLimits, IndexedCollectionState,
    NamedRootManifest, RemoteBatchOp, RemoteStoreBackend, Tree,
};
use prolly_dynamodb_core::{
    Error as CoreError, GcExecutionState, MaintenanceContext, MaintenanceLeaseId,
    MAX_GC_PLAN_DELETES,
};
use prolly_store_dynamodb::{DynamoDbBlobStore, DYNAMODB_SCAN_PAGE_LIMIT};
use serde::{Deserialize, Serialize};

use crate::{Client, Error, Result};

pub const MAX_GC_BLOB_DELETE_PARALLELISM: usize = 64;

/// Content identity for one exact bounded global GC candidate page.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GcPlanId(pub [u8; 32]);

impl std::fmt::Display for GcPlanId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Independent opaque provider cursors for node and blob candidate scans.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcCursor {
    pub node_cursor: Option<Vec<u8>>,
    pub node_done: bool,
    pub blob_cursor: Option<Vec<u8>>,
    pub blob_done: bool,
}

/// Explicit memory and provider-I/O limits for global reachability planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcPlanLimits {
    pub max_roots: usize,
    pub max_live_nodes: usize,
    pub max_live_node_bytes: usize,
    pub max_scanned_values: usize,
    pub max_live_blobs: usize,
    pub max_live_blob_bytes: u64,
    pub candidate_page_evaluation_limit: usize,
}

impl GcPlanLimits {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        max_roots: usize,
        max_live_nodes: usize,
        max_live_node_bytes: usize,
        max_scanned_values: usize,
        max_live_blobs: usize,
        max_live_blob_bytes: u64,
        candidate_page_evaluation_limit: usize,
    ) -> Self {
        Self {
            max_roots,
            max_live_nodes,
            max_live_node_bytes,
            max_scanned_values,
            max_live_blobs,
            max_live_blob_bytes,
            candidate_page_evaluation_limit,
        }
    }

    fn validate(self) -> Result<Self> {
        if self.max_roots == 0
            || self.max_live_nodes == 0
            || self.max_live_node_bytes == 0
            || self.max_scanned_values == 0
            || self.max_live_blobs == 0
            || self.max_live_blob_bytes == 0
            || !(1..=DYNAMODB_SCAN_PAGE_LIMIT).contains(&self.candidate_page_evaluation_limit)
        {
            return Err(Error::InvalidRequest(format!(
                "GC limits must be nonzero and candidate page evaluation limit must be 1..={DYNAMODB_SCAN_PAGE_LIMIT}"
            )));
        }
        Ok(self)
    }
}

/// Exact unreachable blob selected by a dry-run plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcBlobCandidate {
    pub cid: Cid,
    pub len: u64,
}

/// One bounded, read-only global GC plan page under a stable writer fence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcPlan {
    pub id: GcPlanId,
    pub lease_id: MaintenanceLeaseId,
    pub limits: GcPlanLimits,
    pub roots_digest: Cid,
    pub retained_roots: usize,
    /// Total named and indirectly referenced tree roots protected by the plan.
    pub protected_trees: usize,
    pub live_nodes: usize,
    pub live_node_bytes: usize,
    /// Reachable tree nodes inspected by the blob-reference traversal.
    pub scanned_blob_nodes: usize,
    /// Reachable leaf values inspected by the blob-reference traversal.
    pub scanned_values: usize,
    pub live_blobs: usize,
    pub live_blob_bytes: u64,
    pub examined_node_candidates: usize,
    pub reclaimable_nodes: Vec<Cid>,
    pub examined_blob_candidates: usize,
    pub reclaimable_blobs: Vec<GcBlobCandidate>,
    pub cursor: GcCursor,
    pub next_cursor: Option<GcCursor>,
    pub planned_at_millis: u64,
}

/// Bounded physical deletion policy for one reviewed GC plan page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcApplyOptions {
    pub blob_delete_parallelism: usize,
}

impl Default for GcApplyOptions {
    fn default() -> Self {
        Self {
            blob_delete_parallelism: 8,
        }
    }
}

/// Durable completion evidence for one exact GC plan page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcApplyResult {
    pub plan_id: GcPlanId,
    pub lease_id: MaintenanceLeaseId,
    pub node_deletes: usize,
    pub blob_deletes: usize,
    pub completed_at_millis: u64,
    /// True when the durable plan record existed before this call.
    pub replayed: bool,
}

impl Client {
    /// Build one bounded, read-only global GC candidate page. The exact active
    /// maintenance lease and complete named-root digest are sampled before and
    /// after provider scans; any movement fails closed.
    pub async fn plan_gc(
        &self,
        lease_id: &MaintenanceLeaseId,
        cursor: Option<&GcCursor>,
        limits: GcPlanLimits,
    ) -> Result<GcPlan> {
        let limits = limits.validate()?;
        require_lease(self, lease_id).await?;
        let roots = collect_roots(self, limits.max_roots).await?;
        let roots_digest = digest_roots(&roots)?;
        let protection = collect_retained_trees(
            self,
            &roots,
            limits.max_roots,
            limits.max_live_blobs,
            limits.max_scanned_values,
        )
        .await?;
        let reachable = self
            .core()
            .engine()
            .mark_reachable_with_limits(
                &protection.node_trees,
                GcTraversalLimits::new(limits.max_live_nodes, limits.max_live_node_bytes),
            )
            .await
            .map_err(prolly_error)?;
        let reachable_blobs = self
            .core()
            .engine()
            .mark_reachable_blobs_with_limits(
                &protection.blob_scan_trees,
                BlobReachabilityLimits::new(
                    limits.max_live_nodes,
                    limits.max_scanned_values,
                    limits.max_live_blobs,
                    limits.max_live_blob_bytes,
                ),
            )
            .await
            .map_err(prolly_error)?;
        let reachable_blobs =
            merge_registered_blobs(reachable_blobs, protection.registered_blobs, &limits)?;

        let cursor = cursor.cloned().unwrap_or_default();
        validate_cursor(&cursor)?;
        let (examined_node_candidates, reclaimable_nodes, node_cursor, node_done) =
            if cursor.node_done {
                (0, Vec::new(), None, true)
            } else {
                let page = self
                    .backend()
                    .list_node_cids_page(
                        cursor.node_cursor.as_deref(),
                        limits.candidate_page_evaluation_limit,
                    )
                    .await?;
                let examined = page.cids.len();
                let mut reclaimable = page
                    .cids
                    .into_iter()
                    .filter(|cid| {
                        reachable
                            .live_cids
                            .binary_search_by(|live| live.as_bytes().cmp(cid.as_bytes()))
                            .is_err()
                    })
                    .collect::<Vec<_>>();
                reclaimable.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                let done = page.next_cursor.is_none();
                (examined, reclaimable, page.next_cursor, done)
            };

        let (examined_blob_candidates, reclaimable_blobs, blob_cursor, blob_done) = if cursor
            .blob_done
        {
            (0, Vec::new(), None, true)
        } else {
            let page = DynamoDbBlobStore::new(self.core().engine().store().backend().clone())
                .list_blob_refs_page(
                    cursor.blob_cursor.as_deref(),
                    limits.candidate_page_evaluation_limit,
                )
                .await?;
            let examined = page.references.len();
            let mut reclaimable = page
                .references
                .into_iter()
                .filter(|reference| {
                    reachable_blobs
                        .live_blobs
                        .binary_search_by(|live| live.cid.as_bytes().cmp(reference.cid.as_bytes()))
                        .is_err()
                })
                .map(|reference| GcBlobCandidate {
                    cid: reference.cid,
                    len: reference.len,
                })
                .collect::<Vec<_>>();
            reclaimable.sort_by(|left, right| left.cid.as_bytes().cmp(right.cid.as_bytes()));
            let done = page.next_cursor.is_none();
            (examined, reclaimable, page.next_cursor, done)
        };

        require_lease(self, lease_id).await?;
        let roots_after = collect_roots(self, limits.max_roots).await?;
        if digest_roots(&roots_after)? != roots_digest {
            return Err(Error::Core(CoreError::MaintenancePlanStale(
                "named roots changed during GC planning".into(),
            )));
        }
        let next = GcCursor {
            node_cursor,
            node_done,
            blob_cursor,
            blob_done,
        };
        let next_cursor = (!(next.node_done && next.blob_done)).then_some(next);
        let mut plan = GcPlan {
            id: GcPlanId([0; 32]),
            lease_id: lease_id.clone(),
            limits,
            roots_digest,
            retained_roots: roots.len(),
            protected_trees: protection.node_trees.len(),
            live_nodes: reachable.live_nodes,
            live_node_bytes: reachable.live_bytes,
            scanned_blob_nodes: reachable_blobs.scanned_nodes,
            scanned_values: reachable_blobs.scanned_values,
            live_blobs: reachable_blobs.live_blob_count,
            live_blob_bytes: reachable_blobs.live_blob_bytes,
            examined_node_candidates,
            reclaimable_nodes,
            examined_blob_candidates,
            reclaimable_blobs,
            cursor,
            next_cursor,
            planned_at_millis: now_millis(),
        };
        plan.id = gc_plan_id(&plan)?;
        Ok(plan)
    }

    /// Apply one reviewed GC page. A durable in-progress record is committed
    /// before any physical deletion. Failure leaves the writer fence pinned;
    /// retrying the same canonical plan resumes idempotently.
    pub async fn apply_gc(
        &self,
        plan: &GcPlan,
        context: MaintenanceContext,
        options: GcApplyOptions,
    ) -> Result<GcApplyResult> {
        validate_gc_plan(plan)?;
        if !(1..=MAX_GC_BLOB_DELETE_PARALLELISM).contains(&options.blob_delete_parallelism) {
            return Err(Error::InvalidRequest(format!(
                "GC blob delete parallelism must be 1..={MAX_GC_BLOB_DELETE_PARALLELISM}"
            )));
        }

        let prior = self.core().gc_execution(&plan.id.0).await?;
        if prior.is_none() {
            require_lease(self, &plan.lease_id).await?;
            verify_plan_is_current_and_safe(self, plan).await?;
        }
        let execution = self
            .core()
            .begin_gc_execution(
                plan.id.0,
                &plan.lease_id,
                plan.roots_digest.0,
                plan.reclaimable_nodes.len(),
                plan.reclaimable_blobs.len(),
                context,
            )
            .await?;
        if execution.record.state == GcExecutionState::Complete {
            return gc_apply_result(plan, execution.record.completed_at_millis, true);
        }

        let deleter = DynamoGcDeleter {
            backend: self.backend(),
            blob_store: DynamoDbBlobStore::new(self.backend().clone()),
            engine: self.core().engine(),
        };
        delete_gc_candidates(
            &deleter,
            &plan.reclaimable_nodes,
            &plan.reclaimable_blobs,
            options.blob_delete_parallelism,
        )
        .await?;

        let completed = self
            .core()
            .complete_gc_execution(&plan.id.0, &plan.lease_id)
            .await?;
        gc_apply_result(
            plan,
            completed.record.completed_at_millis,
            execution.replayed,
        )
    }
}

trait GcPhysicalDeleter: Sync {
    async fn delete_nodes(&self, candidates: &[Cid]) -> Result<()>;
    fn invalidate_node_cache(&self);
    async fn delete_blob(&self, candidate: GcBlobCandidate) -> Result<()>;
}

struct DynamoGcDeleter<'a> {
    backend: &'a prolly_store_dynamodb::DynamoDbBackend,
    blob_store: DynamoDbBlobStore,
    engine: &'a prolly::AsyncProlly<prolly_store_dynamodb::DynamoDbStore>,
}

impl GcPhysicalDeleter for DynamoGcDeleter<'_> {
    async fn delete_nodes(&self, candidates: &[Cid]) -> Result<()> {
        let operations = candidates
            .iter()
            .map(|cid| RemoteBatchOp::Delete {
                key: cid.as_bytes(),
            })
            .collect::<Vec<_>>();
        self.backend.batch_nodes(&operations).await?;
        Ok(())
    }

    fn invalidate_node_cache(&self) {
        // This path deletes through the raw provider rather than
        // AsyncProlly::sweep_gc. Clear immediately so a later write cannot
        // reuse a cached, physically absent CID without republishing it.
        self.engine.clear_cache();
    }

    async fn delete_blob(&self, candidate: GcBlobCandidate) -> Result<()> {
        self.blob_store
            .delete_blob(&BlobRef {
                cid: candidate.cid,
                len: candidate.len,
            })
            .await?;
        Ok(())
    }
}

async fn delete_gc_candidates<D: GcPhysicalDeleter>(
    deleter: &D,
    nodes: &[Cid],
    blobs: &[GcBlobCandidate],
    blob_parallelism: usize,
) -> Result<()> {
    for chunk in nodes.chunks(25) {
        deleter.delete_nodes(chunk).await?;
        // Invalidate after every successful chunk. A later node or blob
        // deletion may fail, and even that partial outcome must not leave a
        // swept node readable through this process.
        deleter.invalidate_node_cache();
    }
    stream::iter(blobs.iter().cloned())
        .map(|candidate| deleter.delete_blob(candidate))
        .buffer_unordered(blob_parallelism)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn verify_plan_is_current_and_safe(client: &Client, plan: &GcPlan) -> Result<()> {
    let roots = collect_roots(client, plan.limits.max_roots).await?;
    if digest_roots(&roots)? != plan.roots_digest {
        return Err(Error::Core(CoreError::MaintenancePlanStale(
            "named roots changed after GC planning".into(),
        )));
    }
    let protection = collect_retained_trees(
        client,
        &roots,
        plan.limits.max_roots,
        plan.limits.max_live_blobs,
        plan.limits.max_scanned_values,
    )
    .await?;
    let reachable = client
        .core()
        .engine()
        .mark_reachable_with_limits(
            &protection.node_trees,
            GcTraversalLimits::new(plan.limits.max_live_nodes, plan.limits.max_live_node_bytes),
        )
        .await
        .map_err(prolly_error)?;
    let reachable_blobs = client
        .core()
        .engine()
        .mark_reachable_blobs_with_limits(
            &protection.blob_scan_trees,
            BlobReachabilityLimits::new(
                plan.limits.max_live_nodes,
                plan.limits.max_scanned_values,
                plan.limits.max_live_blobs,
                plan.limits.max_live_blob_bytes,
            ),
        )
        .await
        .map_err(prolly_error)?;
    let reachable_blobs =
        merge_registered_blobs(reachable_blobs, protection.registered_blobs, &plan.limits)?;
    if roots.len() != plan.retained_roots
        || protection.node_trees.len() != plan.protected_trees
        || reachable.live_nodes != plan.live_nodes
        || reachable.live_bytes != plan.live_node_bytes
        || reachable_blobs.scanned_nodes != plan.scanned_blob_nodes
        || reachable_blobs.scanned_values != plan.scanned_values
        || reachable_blobs.live_blob_count != plan.live_blobs
        || reachable_blobs.live_blob_bytes != plan.live_blob_bytes
    {
        return Err(Error::Core(CoreError::MaintenancePlanStale(
            "GC reachability summary changed after planning".into(),
        )));
    }
    if plan.reclaimable_nodes.iter().any(|candidate| {
        reachable
            .live_cids
            .binary_search_by(|live| live.as_bytes().cmp(candidate.as_bytes()))
            .is_ok()
    }) || plan.reclaimable_blobs.iter().any(|candidate| {
        reachable_blobs
            .live_blobs
            .binary_search_by(|live| live.cid.as_bytes().cmp(candidate.cid.as_bytes()))
            .is_ok()
    }) {
        return Err(Error::Core(CoreError::MaintenancePlanStale(
            "GC plan contains content that is currently reachable".into(),
        )));
    }
    Ok(())
}

fn validate_gc_plan(plan: &GcPlan) -> Result<()> {
    plan.limits.validate()?;
    validate_cursor(&plan.cursor)?;
    if let Some(next) = &plan.next_cursor {
        validate_cursor(next)?;
    }
    let deletes = plan
        .reclaimable_nodes
        .len()
        .checked_add(plan.reclaimable_blobs.len())
        .ok_or_else(|| Error::InvalidRequest("GC delete count overflow".into()))?;
    if deletes == 0 || deletes > MAX_GC_PLAN_DELETES {
        return Err(Error::InvalidRequest(format!(
            "GC apply requires 1..={MAX_GC_PLAN_DELETES} reclaimable objects"
        )));
    }
    if plan.reclaimable_nodes.len() > plan.examined_node_candidates
        || plan.reclaimable_blobs.len() > plan.examined_blob_candidates
    {
        return Err(Error::InvalidRequest(
            "GC reclaimable count exceeds examined candidate count".into(),
        ));
    }
    if plan.scanned_blob_nodes > plan.limits.max_live_nodes
        || plan.scanned_values > plan.limits.max_scanned_values
        || plan.protected_trees < plan.retained_roots
        || plan.protected_trees > plan.limits.max_roots
    {
        return Err(Error::InvalidRequest(
            "GC reachability work exceeds the plan limits".into(),
        ));
    }
    if plan
        .reclaimable_nodes
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
        || plan
            .reclaimable_blobs
            .windows(2)
            .any(|pair| pair[0].cid.as_bytes() >= pair[1].cid.as_bytes())
    {
        return Err(Error::InvalidRequest(
            "GC candidates must be strictly ordered and unique".into(),
        ));
    }
    if gc_plan_id(plan)? != plan.id {
        return Err(Error::InvalidRequest(
            "GC plan identity does not match its canonical contents".into(),
        ));
    }
    Ok(())
}

fn gc_apply_result(
    plan: &GcPlan,
    completed_at_millis: Option<u64>,
    replayed: bool,
) -> Result<GcApplyResult> {
    Ok(GcApplyResult {
        plan_id: plan.id.clone(),
        lease_id: plan.lease_id.clone(),
        node_deletes: plan.reclaimable_nodes.len(),
        blob_deletes: plan.reclaimable_blobs.len(),
        completed_at_millis: completed_at_millis.ok_or_else(|| {
            Error::Core(CoreError::CorruptData(
                "completed GC execution has no completion timestamp".into(),
            ))
        })?,
        replayed,
    })
}

async fn require_lease(client: &Client, expected: &MaintenanceLeaseId) -> Result<()> {
    match client.maintenance_lease().await? {
        Some(lease) if lease.id == *expected => Ok(()),
        Some(lease) => Err(Error::Core(CoreError::MaintenanceInProgress {
            lease_id: lease.id,
        })),
        None => Err(Error::InvalidRequest(
            "global GC planning requires an active maintenance lease".into(),
        )),
    }
}

async fn collect_roots(client: &Client, max_roots: usize) -> Result<Vec<NamedRootManifest>> {
    let mut roots = Vec::new();
    let mut after = None;
    loop {
        let remaining = max_roots.saturating_sub(roots.len());
        if remaining == 0 {
            return Err(Error::InvalidRequest(format!(
                "named-root count exceeds GC limit {max_roots}"
            )));
        }
        let page = AsyncManifestStoreScan::list_roots_page(
            self_store(client),
            &[],
            after.as_deref(),
            remaining.min(1_000),
        )
        .await
        .map_err(remote_scan_error)?;
        roots.extend(page.roots);
        match page.next_after {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    roots.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(roots)
}

/// Expand the provider's named roots with every source and derived index tree
/// referenced by an indexed-collection state record. Those snapshot roots are
/// durable pointers stored inside state-tree leaf values, so generic Prolly
/// reachability cannot discover them by following internal-node child CIDs.
struct GcProtection {
    node_trees: Vec<Tree>,
    blob_scan_trees: Vec<Tree>,
    registered_blobs: Vec<BlobRef>,
}

async fn collect_retained_trees(
    client: &Client,
    roots: &[NamedRootManifest],
    max_trees: usize,
    max_blobs: usize,
    max_registry_entries: usize,
) -> Result<GcProtection> {
    let mut node_trees = roots
        .iter()
        .map(|root| root.manifest.to_tree())
        .collect::<Vec<_>>();
    let mut covered_value_trees = BTreeSet::new();
    let mut registered_blobs = BTreeMap::<Cid, BlobRef>::new();
    let mut registry_entries = 0usize;
    for root in roots {
        let root_tree = root.manifest.to_tree();
        let remaining = max_trees.saturating_sub(node_trees.len());
        if let Some(expanded) = client
            .core()
            .expand_snapshot_catalog_root(&root.name, &root_tree, remaining)
            .await?
        {
            node_trees.extend(expanded.protected_trees);
            covered_value_trees.extend(expanded.covered_value_trees);
            continue;
        }
        let remaining_entries = max_registry_entries.saturating_sub(registry_entries);
        if let Some(references) = client
            .core()
            .expand_blob_registry_root(&root.name, &root_tree, remaining_entries)
            .await?
        {
            registry_entries = registry_entries
                .checked_add(references.len())
                .ok_or_else(|| {
                    Error::InvalidRequest("blob registry entry count overflow".into())
                })?;
            if registry_entries > max_registry_entries {
                return Err(Error::InvalidRequest(format!(
                    "blob registry entries exceed GC value limit {max_registry_entries}"
                )));
            }
            for reference in references {
                match registered_blobs.insert(reference.cid.clone(), reference.clone()) {
                    Some(existing) if existing.len != reference.len => {
                        return Err(Error::Core(CoreError::CorruptData(
                            "blob registries disagree on one content length".into(),
                        )));
                    }
                    _ => {}
                }
            }
            if registered_blobs.len() > max_blobs {
                return Err(Error::InvalidRequest(format!(
                    "registered blob count exceeds GC limit {max_blobs}"
                )));
            }
            continue;
        }
        let Some(source_map_id) =
            indexed_collection_source_map_id(&root.name).map_err(prolly_error)?
        else {
            continue;
        };
        let state_tree = root.manifest.to_tree();
        let state = IndexedCollectionState::from_async_tree(client.core().engine(), &state_tree)
            .await
            .map_err(prolly_error)?;
        if state.source_map_id != source_map_id {
            return Err(Error::Core(CoreError::CorruptData(
                "indexed collection root name disagrees with its source-map ID".into(),
            )));
        }
        for snapshot in state.snapshots.values() {
            let added = 1usize
                .checked_add(snapshot.indexes.len())
                .ok_or_else(|| Error::InvalidRequest("GC protected tree count overflow".into()))?;
            if node_trees
                .len()
                .checked_add(added)
                .is_none_or(|count| count > max_trees)
            {
                return Err(Error::InvalidRequest(format!(
                    "protected tree count exceeds GC limit {max_trees}"
                )));
            }
            covered_value_trees.insert(
                prolly::MapVersionId::for_tree(&snapshot.source.tree).map_err(prolly_error)?,
            );
            node_trees.push(snapshot.source.tree.clone());
            for index in &snapshot.indexes {
                covered_value_trees
                    .insert(prolly::MapVersionId::for_tree(&index.tree).map_err(prolly_error)?);
                node_trees.push(index.tree.clone());
            }
        }
    }
    let mut blob_scan_trees = Vec::new();
    for tree in &node_trees {
        let id = prolly::MapVersionId::for_tree(tree).map_err(prolly_error)?;
        if !covered_value_trees.contains(&id) {
            blob_scan_trees.push(tree.clone());
        }
    }
    Ok(GcProtection {
        node_trees,
        blob_scan_trees,
        registered_blobs: registered_blobs.into_values().collect(),
    })
}

fn merge_registered_blobs(
    mut reachable: prolly::BlobGcReachability,
    registered: Vec<BlobRef>,
    limits: &GcPlanLimits,
) -> Result<prolly::BlobGcReachability> {
    let mut blobs = reachable
        .live_blobs
        .into_iter()
        .map(|reference| (reference.cid.clone(), reference))
        .collect::<BTreeMap<_, _>>();
    for reference in registered {
        match blobs.insert(reference.cid.clone(), reference.clone()) {
            Some(existing) if existing.len != reference.len => {
                return Err(Error::Core(CoreError::CorruptData(
                    "blob registry disagrees with tree reachability length".into(),
                )));
            }
            _ => {}
        }
    }
    if blobs.len() > limits.max_live_blobs {
        return Err(Error::InvalidRequest(format!(
            "live blob count exceeds GC limit {}",
            limits.max_live_blobs
        )));
    }
    let bytes = blobs.values().try_fold(0u64, |total, reference| {
        total
            .checked_add(reference.len)
            .ok_or_else(|| Error::InvalidRequest("live blob byte count overflow".into()))
    })?;
    if bytes > limits.max_live_blob_bytes {
        return Err(Error::InvalidRequest(format!(
            "live blob bytes exceed GC limit {}",
            limits.max_live_blob_bytes
        )));
    }
    reachable.live_blobs = blobs.into_values().collect();
    reachable.live_blob_count = reachable.live_blobs.len();
    reachable.live_blob_bytes = bytes;
    Ok(reachable)
}

fn self_store(client: &Client) -> &prolly_store_dynamodb::DynamoDbStore {
    client.core().engine().store()
}

fn remote_scan_error(
    error: prolly::RemoteAdapterError<prolly_store_dynamodb::dynamodb::DynamoDbBackendError>,
) -> Error {
    Error::Core(CoreError::Storage(prolly::Error::Store(Box::new(error))))
}

fn digest_roots(roots: &[NamedRootManifest]) -> Result<Cid> {
    let mut bytes = b"DDB-GlobalGcRoots-v1".to_vec();
    for root in roots {
        let name_len = u64::try_from(root.name.len())
            .map_err(|_| Error::InvalidRequest("root name length overflow".into()))?;
        let manifest = root.manifest.to_bytes().map_err(prolly_error)?;
        let manifest_len = u64::try_from(manifest.len())
            .map_err(|_| Error::InvalidRequest("root manifest length overflow".into()))?;
        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(&root.name);
        bytes.extend_from_slice(&manifest_len.to_be_bytes());
        bytes.extend_from_slice(&manifest);
    }
    Ok(Cid::from_bytes(&bytes))
}

fn validate_cursor(cursor: &GcCursor) -> Result<()> {
    if cursor.node_done && cursor.node_cursor.is_some()
        || cursor.blob_done && cursor.blob_cursor.is_some()
    {
        return Err(Error::InvalidRequest(
            "completed GC cursor component must not retain a provider cursor".into(),
        ));
    }
    Ok(())
}

fn gc_plan_id(plan: &GcPlan) -> Result<GcPlanId> {
    let mut identity = plan.clone();
    identity.id = GcPlanId([0; 32]);
    let encoded = serde_cbor::ser::to_vec_packed(&identity)
        .map_err(|error| Error::InvalidRequest(error.to_string()))?;
    Ok(GcPlanId(Cid::from_bytes(&encoded).0))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn prolly_error(error: prolly::Error) -> Error {
    Error::Core(CoreError::Storage(error))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use super::*;

    struct PartialFailureDeleter {
        nodes: Mutex<BTreeSet<Cid>>,
        blobs: Mutex<BTreeSet<Cid>>,
        fail_once: AtomicBool,
        cache_invalidations: Mutex<usize>,
    }

    impl GcPhysicalDeleter for PartialFailureDeleter {
        async fn delete_nodes(&self, candidates: &[Cid]) -> Result<()> {
            self.nodes
                .lock()
                .unwrap()
                .extend(candidates.iter().cloned());
            Ok(())
        }

        fn invalidate_node_cache(&self) {
            *self.cache_invalidations.lock().unwrap() += 1;
        }

        async fn delete_blob(&self, candidate: GcBlobCandidate) -> Result<()> {
            self.blobs.lock().unwrap().insert(candidate.cid);
            if self.fail_once.swap(false, Ordering::SeqCst) {
                return Err(Error::InvalidRequest("injected partial delete".into()));
            }
            Ok(())
        }
    }

    #[test]
    fn gc_plan_identity_is_frozen_and_covers_cursor_and_candidates() {
        let mut plan = GcPlan {
            id: GcPlanId([0; 32]),
            lease_id: MaintenanceLeaseId([1; 32]),
            limits: GcPlanLimits::new(50, 500, 50_000, 5_000, 50, 5_000, 25),
            roots_digest: Cid([2; 32]),
            retained_roots: 17,
            protected_trees: 21,
            live_nodes: 100,
            live_node_bytes: 10_000,
            scanned_blob_nodes: 90,
            scanned_values: 4_000,
            live_blobs: 3,
            live_blob_bytes: 900,
            examined_node_candidates: 10,
            reclaimable_nodes: vec![Cid([3; 32])],
            examined_blob_candidates: 8,
            reclaimable_blobs: vec![GcBlobCandidate {
                cid: Cid([4; 32]),
                len: 300,
            }],
            cursor: GcCursor::default(),
            next_cursor: Some(GcCursor {
                node_cursor: Some(vec![5, 6]),
                node_done: false,
                blob_cursor: None,
                blob_done: true,
            }),
            planned_at_millis: 1_700_000_700_000,
        };
        plan.id = gc_plan_id(&plan).unwrap();
        validate_gc_plan(&plan).unwrap();
        assert_eq!(
            plan.id.to_string(),
            "e038147825927a6f6af766547f178b54ec433973ac08d3073acb8b16772c9e50"
        );
        let mut tampered = plan.clone();
        tampered.reclaimable_nodes.clear();
        assert_ne!(gc_plan_id(&tampered).unwrap(), plan.id);
        assert!(validate_gc_plan(&tampered).is_err());

        let mut tampered = plan.clone();
        tampered.scanned_blob_nodes += 1;
        assert_ne!(gc_plan_id(&tampered).unwrap(), plan.id);
        assert!(validate_gc_plan(&tampered).is_err());

        let mut tampered = plan.clone();
        tampered.scanned_values += 1;
        assert_ne!(gc_plan_id(&tampered).unwrap(), plan.id);
        assert!(validate_gc_plan(&tampered).is_err());
    }

    #[test]
    fn gc_plan_rejects_reachability_work_above_limits() {
        let mut plan = GcPlan {
            id: GcPlanId([0; 32]),
            lease_id: MaintenanceLeaseId([1; 32]),
            limits: GcPlanLimits::new(50, 500, 50_000, 5_000, 50, 5_000, 25),
            roots_digest: Cid([2; 32]),
            retained_roots: 17,
            protected_trees: 21,
            live_nodes: 100,
            live_node_bytes: 10_000,
            scanned_blob_nodes: 501,
            scanned_values: 4_000,
            live_blobs: 3,
            live_blob_bytes: 900,
            examined_node_candidates: 10,
            reclaimable_nodes: vec![Cid([3; 32])],
            examined_blob_candidates: 8,
            reclaimable_blobs: vec![GcBlobCandidate {
                cid: Cid([4; 32]),
                len: 300,
            }],
            cursor: GcCursor::default(),
            next_cursor: None,
            planned_at_millis: 1_700_000_700_000,
        };
        plan.id = gc_plan_id(&plan).unwrap();
        assert!(validate_gc_plan(&plan).is_err());

        plan.scanned_blob_nodes = 90;
        plan.scanned_values = 5_001;
        plan.id = gc_plan_id(&plan).unwrap();
        assert!(validate_gc_plan(&plan).is_err());
    }

    #[test]
    fn completed_gc_cursor_cannot_retain_a_provider_position() {
        assert!(validate_cursor(&GcCursor {
            node_cursor: Some(vec![1]),
            node_done: true,
            ..GcCursor::default()
        })
        .is_err());
    }

    #[tokio::test]
    async fn physical_gc_retry_is_idempotent_after_partial_deletion() {
        let deleter = PartialFailureDeleter {
            nodes: Mutex::new(BTreeSet::new()),
            blobs: Mutex::new(BTreeSet::new()),
            fail_once: AtomicBool::new(true),
            cache_invalidations: Mutex::new(0),
        };
        let nodes = vec![Cid([1; 32]), Cid([2; 32])];
        let blobs = vec![
            GcBlobCandidate {
                cid: Cid([3; 32]),
                len: 30,
            },
            GcBlobCandidate {
                cid: Cid([4; 32]),
                len: 40,
            },
        ];
        assert!(delete_gc_candidates(&deleter, &nodes, &blobs, 1)
            .await
            .is_err());
        assert_eq!(deleter.nodes.lock().unwrap().len(), 2);
        assert_eq!(deleter.blobs.lock().unwrap().len(), 1);
        assert_eq!(*deleter.cache_invalidations.lock().unwrap(), 1);

        delete_gc_candidates(&deleter, &nodes, &blobs, 1)
            .await
            .unwrap();
        assert_eq!(deleter.nodes.lock().unwrap().len(), 2);
        assert_eq!(deleter.blobs.lock().unwrap().len(), 2);
        assert_eq!(*deleter.cache_invalidations.lock().unwrap(), 2);
    }
}
