use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use futures_util::{stream, StreamExt};
use prolly::{AsyncProlly, Config, Mutation, RuntimeConfig, Tree, TreeFormat};
use serde::{Deserialize, Serialize};

use crate::{
    decode_canonical, encode_canonical, tree_format_digest, BucketCommit, CommitId,
    CompareExchange, CompareExchangeOutcome, DeleteOutcome, Error, ErrorCode, GetRequest,
    ImmutablePut, ImmutablePutOutcome, JournalCommitGraphEntry, JournalDerivedIndexHead,
    JournalIndexRebuildChunk, JournalIndexRebuildChunkId, JournalNodeIndexEntry, ListRequest,
    LoadedRef, MemoryNodeCache, MutableControlStore, NodeCache, NodePackToc, ObjectPath,
    ObjectPlane, OperationId, PhysicalVersion, ProllyObjectStore, PublicationEventId,
    PublicationJournalCursor, RefGeneration, RepositoryId, Result, RetryAdvice, RootManifest,
    ShardedBranchPublisher, StorageToken, DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

pub const DEFAULT_JOURNAL_INDEX_MAX_UNINDEXED_EVENTS: usize = 4_096;
const COMMIT_INDEX_LOAD_CONCURRENCY: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalIndexAdvanceReport {
    pub checkpoint: PublicationEventId,
    pub checkpoint_generation: RefGeneration,
    pub indexed_publications: usize,
    pub indexed_commits: usize,
    pub indexed_nodes: usize,
    pub initialized: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalIndexRebuildPhase {
    Discovering,
    Applying,
    Complete,
}

/// Constant-size process-independent cursor for rebuilding branch-local node
/// and commit-graph indexes from an immutable publication-journal snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalIndexRebuildCursor {
    pub repository: RepositoryId,
    pub branch: String,
    pub job: OperationId,
    pub snapshot: PublicationEventId,
    pub snapshot_generation: RefGeneration,
    pub snapshot_target: CommitId,
    pub scan: Option<PublicationJournalCursor>,
    pub oldest_chunk: Option<JournalIndexRebuildChunkId>,
    pub next_chunk: Option<JournalIndexRebuildChunkId>,
    pub next_chunk_sequence: u64,
    pub node_root: RootManifest,
    pub commit_graph_root: RootManifest,
    pub discovered_publications: u64,
    pub indexed_commits: u64,
    pub indexed_nodes: u64,
    pub baseline_checkpoint: Option<PublicationEventId>,
    pub baseline_generation: Option<u64>,
    pub phase: JournalIndexRebuildPhase,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalIndexRebuildStep {
    pub cursor: JournalIndexRebuildCursor,
    pub discovered_publications: usize,
    pub indexed_publications: usize,
    pub indexed_commits: usize,
    pub indexed_nodes: usize,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JournalIndexRebuildCleanup {
    pub deleted_objects: usize,
    pub complete: bool,
}

struct LoadedHead {
    value: JournalDerivedIndexHead,
    token: StorageToken,
}

/// Branch-local node and commit-graph indexes advanced exclusively from the
/// immutable  publication chain. No S3 namespace listing is used.
pub struct JournalDerivedIndexes<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    prefix: String,
    repository: RepositoryId,
    format: TreeFormat,
    node_engine: AsyncProlly<ProllyObjectStore<P>>,
    graph_engine: AsyncProlly<ProllyObjectStore<P>>,
    max_unindexed_events: usize,
}

impl<P: ObjectPlane> JournalDerivedIndexes<P> {
    pub(crate) fn node_cache_snapshot(&self) -> crate::NodeCacheSnapshot {
        self.node_engine
            .store()
            .node_cache_snapshot()
            .saturating_add(self.graph_engine.store().node_cache_snapshot())
    }

    pub fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        format: TreeFormat,
    ) -> Result<Self> {
        Self::new_with_limits(
            plane,
            prefix,
            repository,
            format,
            Arc::new(MemoryNodeCache::new(64 * 1024 * 1024)),
            DEFAULT_JOURNAL_INDEX_MAX_UNINDEXED_EVENTS,
            DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_limits(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        format: TreeFormat,
        node_cache: Arc<dyn NodeCache>,
        max_unindexed_events: usize,
        control_versions_to_retain: usize,
    ) -> Result<Self> {
        if !(1..=1_000_000).contains(&max_unindexed_events) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "journal-index catch-up bound must be between 1 and 1,000,000 events",
            ));
        }
        let prefix = prefix.into();
        let digest = tree_format_digest(&format)?;
        let config = Config {
            format: format.clone(),
            runtime: RuntimeConfig::default(),
        };
        let node_store = ProllyObjectStore::new_cached_direct(
            plane.clone(),
            format!("{prefix}/journal-index/node-tree"),
            repository,
            digest,
            node_cache.clone(),
        );
        let graph_store = ProllyObjectStore::new_cached_direct(
            plane.clone(),
            format!("{prefix}/journal-index/graph-tree"),
            repository,
            digest,
            node_cache,
        );
        let controls =
            MutableControlStore::new(plane.clone(), prefix.clone(), control_versions_to_retain)?;
        Ok(Self {
            plane,
            controls,
            prefix,
            repository,
            format,
            node_engine: AsyncProlly::new(node_store, config.clone()),
            graph_engine: AsyncProlly::new(graph_store, config),
            max_unindexed_events,
        })
    }

    pub async fn advance(
        &self,
        publisher: &ShardedBranchPublisher<P>,
        branch: &str,
        now_millis: u64,
    ) -> Result<JournalIndexAdvanceReport> {
        crate::repository::validate_branch(branch)?;
        let current = publisher.load(branch).await?;
        if now_millis < current.value.updated_at_millis {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "journal-index clock predates the current branch ref",
            ));
        }
        let loaded = self.load_head(branch).await?;
        if loaded
            .as_ref()
            .is_some_and(|head| now_millis < head.value.updated_at_millis)
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "journal-index clock predates its durable head",
            ));
        }
        if loaded.is_none() && current.value.generation.0 != 0 {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "journal indexes must be initialized at branch creation or rebuilt resumably",
            ));
        }
        if loaded
            .as_ref()
            .is_some_and(|head| head.value.checkpoint == current.value.publication)
        {
            let head = &loaded.as_ref().expect("checked as present").value;
            if head.checkpoint_generation.0 > current.value.generation.0
                || head.target != current.value.target
            {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "journal-index checkpoint does not match the current branch ref",
                ));
            }
            return Ok(JournalIndexAdvanceReport {
                checkpoint: current.value.publication,
                checkpoint_generation: current.value.generation,
                indexed_publications: 0,
                indexed_commits: 0,
                indexed_nodes: 0,
                initialized: false,
            });
        }

        let (mut events, initialized) = if let Some(head) = loaded.as_ref() {
            if head.value.checkpoint_generation.0 >= current.value.generation.0 {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "journal-index checkpoint is not behind the branch head",
                ));
            }
            (
                self.collect_unindexed_events(
                    publisher,
                    branch,
                    head.value.checkpoint,
                    head.value.checkpoint_generation,
                    head.value.target,
                    current.value.publication,
                )
                .await?,
                false,
            )
        } else {
            (
                vec![
                    publisher
                        .load_publication(current.value.publication)
                        .await?,
                ],
                true,
            )
        };
        let indexed_publications = events.len();
        events.reverse();

        let digest = tree_format_digest(&self.format)?;
        let empty_node = self.node_engine.create();
        let empty_graph = self.graph_engine.create();
        let mut node_tree = Tree {
            root: loaded
                .as_ref()
                .and_then(|head| head.value.node_root.root.clone())
                .or(empty_node.root),
            config: Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        };
        let mut graph_tree = Tree {
            root: loaded
                .as_ref()
                .and_then(|head| head.value.commit_graph_root.root.clone())
                .or(empty_graph.root),
            config: Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        };
        let (indexed_commits, indexed_nodes) = self
            .index_events(publisher, &mut node_tree, &mut graph_tree, events)
            .await?;

        let next = JournalDerivedIndexHead {
            repository: self.repository,
            branch: branch.to_string(),
            checkpoint: current.value.publication,
            checkpoint_generation: current.value.generation,
            target: current.value.target,
            node_root: RootManifest {
                root: node_tree.root.clone(),
                format_digest: digest,
            },
            commit_graph_root: RootManifest {
                root: graph_tree.root.clone(),
                format_digest: digest,
            },
            generation: loaded.as_ref().map_or(Ok(0), |head| {
                head.value.generation.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "journal-index head generation overflow",
                    )
                })
            })?,
            indexed_publications: loaded
                .as_ref()
                .map_or(0, |head| head.value.indexed_publications)
                .checked_add(u64::try_from(indexed_publications).map_err(|_| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "publication count exceeds u64",
                    )
                })?)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "journal-index publication counter overflow",
                    )
                })?,
            indexed_commits: loaded
                .as_ref()
                .map_or(0, |head| head.value.indexed_commits)
                .checked_add(u64::try_from(indexed_commits).map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "commit count exceeds u64")
                })?)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "journal-index commit counter overflow",
                    )
                })?,
            updated_at_millis: now_millis,
        };
        next.validate(self.repository, branch, digest)?;
        let bytes = encode_canonical(&next)?;
        let path = self.head_path(branch)?;
        let expected = loaded.map(|head| head.token);
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected,
                bytes: bytes.clone(),
            })
            .await
        {
            Ok(CompareExchangeOutcome::Applied(_)) => {}
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {}
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "journal-derived index head advanced concurrently",
                )
                .retry(RetryAdvice::ReloadHead));
            }
            Err(error) => {
                if self
                    .plane
                    .load_mutable(&path)
                    .await?
                    .is_none_or(|current| current.bytes != bytes)
                {
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("journal-index publication outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation));
                }
            }
        }
        Ok(JournalIndexAdvanceReport {
            checkpoint: next.checkpoint,
            checkpoint_generation: next.checkpoint_generation,
            indexed_publications,
            indexed_commits,
            indexed_nodes,
            initialized,
        })
    }

    /// Initialize a newly created branch by reusing immutable repository-wide
    /// node-location and commit-graph roots from the source branch. The roots
    /// are safe supersets: entries are content-addressed and never encode
    /// branch-local authority.
    pub async fn initialize_branch_from(
        &self,
        source_branch: &str,
        branch: &str,
        reference: &LoadedRef,
        now_millis: u64,
    ) -> Result<()> {
        crate::repository::validate_branch(source_branch)?;
        crate::repository::validate_branch(branch)?;
        let source = self
            .require_branch_covers(source_branch, reference.value.target)
            .await?;
        let digest = tree_format_digest(&self.format)?;
        let next = JournalDerivedIndexHead {
            repository: self.repository,
            branch: branch.to_string(),
            checkpoint: reference.value.publication,
            checkpoint_generation: reference.value.generation,
            target: reference.value.target,
            node_root: source.node_root,
            commit_graph_root: source.commit_graph_root,
            generation: 0,
            indexed_publications: 1,
            indexed_commits: source.indexed_commits,
            updated_at_millis: now_millis,
        };
        next.validate(self.repository, branch, digest)?;
        let bytes = encode_canonical(&next)?;
        let path = self.head_path(branch)?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path,
                expected: None,
                bytes: bytes.clone(),
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(()),
            CompareExchangeOutcome::Conflict(Some(current)) if current.bytes == bytes => Ok(()),
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "new branch derived indexes were initialized concurrently",
            )),
        }
    }

    /// Return the source branch's immutable derived roots only when they cover
    /// `target`. Callers use this preflight before publishing a new branch ref.
    pub async fn require_branch_covers(
        &self,
        source_branch: &str,
        target: CommitId,
    ) -> Result<JournalDerivedIndexHead> {
        crate::repository::validate_branch(source_branch)?;
        let source = self.load_head(source_branch).await?.ok_or_else(|| {
            Error::new(
                ErrorCode::PreconditionFailed,
                "source branch derived indexes are not initialized",
            )
        })?;
        let graph = self.tree_from_root(&source.value.commit_graph_root);
        if self
            .graph_engine
            .get(&graph, target.as_bytes())
            .await?
            .is_none()
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "source branch indexes do not cover the requested branch point",
            ));
        }
        Ok(source.value)
    }

    pub async fn start_rebuild(
        &self,
        publisher: &ShardedBranchPublisher<P>,
        branch: &str,
        job: OperationId,
    ) -> Result<JournalIndexRebuildCursor> {
        crate::repository::validate_branch(branch)?;
        if job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "journal-index rebuild requires a non-nil job ID",
            ));
        }
        let scan = publisher.open_journal(branch).await?;
        let snapshot_generation = scan.next_generation.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "journal-index rebuild snapshot has no generation",
            )
        })?;
        let snapshot_target = scan.next_target.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "journal-index rebuild snapshot has no target",
            )
        })?;
        let baseline = self.load_head(branch).await?;
        let digest = tree_format_digest(&self.format)?;
        Ok(JournalIndexRebuildCursor {
            repository: self.repository,
            branch: branch.to_string(),
            job,
            snapshot: scan.snapshot_head,
            snapshot_generation,
            snapshot_target,
            scan: Some(scan),
            oldest_chunk: None,
            next_chunk: None,
            next_chunk_sequence: 0,
            node_root: RootManifest {
                root: None,
                format_digest: digest,
            },
            commit_graph_root: RootManifest {
                root: None,
                format_digest: digest,
            },
            discovered_publications: 0,
            indexed_commits: 0,
            indexed_nodes: 0,
            baseline_checkpoint: baseline.as_ref().map(|head| head.value.checkpoint),
            baseline_generation: baseline.as_ref().map(|head| head.value.generation),
            phase: JournalIndexRebuildPhase::Discovering,
        })
    }

    pub async fn advance_rebuild(
        &self,
        publisher: &ShardedBranchPublisher<P>,
        cursor: &JournalIndexRebuildCursor,
        max_events: usize,
        now_millis: u64,
    ) -> Result<JournalIndexRebuildStep> {
        self.validate_rebuild_cursor(cursor)?;
        if !(1..=1_000).contains(&max_events) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "journal-index rebuild page must contain 1 to 1,000 events",
            ));
        }
        if cursor.phase == JournalIndexRebuildPhase::Complete {
            return Ok(JournalIndexRebuildStep {
                cursor: cursor.clone(),
                discovered_publications: 0,
                indexed_publications: 0,
                indexed_commits: 0,
                indexed_nodes: 0,
                complete: true,
            });
        }
        if cursor.phase == JournalIndexRebuildPhase::Discovering {
            let scan = cursor.scan.as_ref().ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidContinuationToken,
                    "discovering journal-index rebuild has no scan cursor",
                )
            })?;
            let page = publisher.read_journal_page(scan, max_events).await?;
            if page.entries.is_empty() {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "journal-index rebuild discovery returned an empty page",
                ));
            }
            let chunk = JournalIndexRebuildChunk {
                repository: self.repository,
                branch: cursor.branch.clone(),
                job: cursor.job,
                sequence: cursor.next_chunk_sequence,
                newer: cursor.oldest_chunk,
                events: page.entries.into_iter().map(|entry| entry.event).collect(),
            };
            chunk.validate(self.repository, &cursor.branch)?;
            let chunk_id = self.store_rebuild_chunk(&chunk).await?;
            let discovered = chunk.events.len();
            let mut next = cursor.clone();
            next.scan = page.continuation;
            next.oldest_chunk = Some(chunk_id);
            next.next_chunk_sequence =
                next.next_chunk_sequence.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InvalidLimit,
                        "journal rebuild chunk sequence overflow",
                    )
                })?;
            next.discovered_publications = next
                .discovered_publications
                .checked_add(u64::try_from(discovered).map_err(|_| {
                    Error::new(ErrorCode::InvalidLimit, "journal rebuild count overflow")
                })?)
                .ok_or_else(|| {
                    Error::new(ErrorCode::InvalidLimit, "journal rebuild count overflow")
                })?;
            if next.scan.is_none() {
                next.phase = JournalIndexRebuildPhase::Applying;
                next.next_chunk = next.oldest_chunk;
            }
            return Ok(JournalIndexRebuildStep {
                cursor: next,
                discovered_publications: discovered,
                indexed_publications: 0,
                indexed_commits: 0,
                indexed_nodes: 0,
                complete: false,
            });
        }

        let chunk_id = cursor.next_chunk.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "applying journal-index rebuild has no next chunk",
            )
        })?;
        let chunk = self
            .load_rebuild_chunk(&cursor.branch, cursor.job, chunk_id)
            .await?;
        if chunk.events.len() > max_events {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "journal-index rebuild apply limit is smaller than its persisted discovery chunk",
            ));
        }
        let mut node_tree = self.tree_from_root(&cursor.node_root);
        let mut graph_tree = self.tree_from_root(&cursor.commit_graph_root);
        let (indexed_commits, indexed_nodes) = self
            .index_events(
                publisher,
                &mut node_tree,
                &mut graph_tree,
                chunk.events.iter().rev().cloned().collect(),
            )
            .await?;
        let mut next = cursor.clone();
        next.node_root.root = node_tree.root;
        next.commit_graph_root.root = graph_tree.root;
        next.next_chunk = chunk.newer;
        next.indexed_commits = next
            .indexed_commits
            .checked_add(u64::try_from(indexed_commits).map_err(|_| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "journal rebuild commit count overflow",
                )
            })?)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "journal rebuild commit count overflow",
                )
            })?;
        next.indexed_nodes = next
            .indexed_nodes
            .checked_add(u64::try_from(indexed_nodes).map_err(|_| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "journal rebuild node count overflow",
                )
            })?)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "journal rebuild node count overflow",
                )
            })?;
        if next.next_chunk.is_none() {
            self.publish_rebuild(publisher, &next, now_millis).await?;
            next.phase = JournalIndexRebuildPhase::Complete;
        }
        Ok(JournalIndexRebuildStep {
            cursor: next.clone(),
            discovered_publications: 0,
            indexed_publications: chunk.events.len(),
            indexed_commits,
            indexed_nodes,
            complete: next.phase == JournalIndexRebuildPhase::Complete,
        })
    }

    pub async fn cleanup_rebuild(
        &self,
        cursor: &JournalIndexRebuildCursor,
        limit: usize,
    ) -> Result<JournalIndexRebuildCleanup> {
        self.validate_rebuild_cursor(cursor)?;
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "journal-index rebuild cleanup limit must be 1 to 1,000",
            ));
        }
        let page = self
            .plane
            .list(ListRequest {
                prefix: self.rebuild_chunk_prefix(&cursor.branch, cursor.job),
                continuation: None,
                limit,
                include_versions: false,
            })
            .await?;
        let mut targets = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let token = entry.metadata.token;
            let version = token.version_id.clone().map_or_else(
                || PhysicalVersion::Unversioned {
                    token: Some(token.clone()),
                },
                |version_id| PhysicalVersion::Versioned { version_id },
            );
            targets.push((entry.path, version));
        }
        let deleted_objects = targets.len();
        for outcome in self.plane.delete_exact_batch(targets).await? {
            if matches!(outcome, DeleteOutcome::TokenMismatch) {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "journal-index rebuild state changed during cleanup",
                ));
            }
        }
        Ok(JournalIndexRebuildCleanup {
            deleted_objects,
            complete: page.continuation.is_none(),
        })
    }

    pub async fn node_location(
        &self,
        branch: &str,
        cid: &prolly::Cid,
    ) -> Result<Option<JournalNodeIndexEntry>> {
        let Some(head) = self.load_head(branch).await? else {
            return Ok(None);
        };
        let tree = self.tree_from_root(&head.value.node_root);
        let entry = self
            .node_engine
            .get(&tree, cid.as_bytes())
            .await?
            .map(|encoded| decode_canonical(&encoded))
            .transpose()?;
        if entry
            .as_ref()
            .is_some_and(|entry: &JournalNodeIndexEntry| entry.cid != *cid)
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "journal node-index value does not match its key",
            ));
        }
        Ok(entry)
    }

    pub async fn commit_graph_entry(
        &self,
        branch: &str,
        commit: CommitId,
    ) -> Result<Option<JournalCommitGraphEntry>> {
        let Some(head) = self.load_head(branch).await? else {
            return Ok(None);
        };
        let tree = self.tree_from_root(&head.value.commit_graph_root);
        let entry = self
            .graph_engine
            .get(&tree, commit.as_bytes())
            .await?
            .map(|encoded| decode_canonical(&encoded))
            .transpose()?;
        if entry
            .as_ref()
            .is_some_and(|entry: &JournalCommitGraphEntry| entry.commit != commit)
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "journal commit-graph value does not match its key",
            ));
        }
        Ok(entry)
    }

    pub async fn head(&self, branch: &str) -> Result<Option<JournalDerivedIndexHead>> {
        Ok(self.load_head(branch).await?.map(|head| head.value))
    }

    fn validate_rebuild_cursor(&self, cursor: &JournalIndexRebuildCursor) -> Result<()> {
        crate::repository::validate_branch(&cursor.branch)?;
        let digest = tree_format_digest(&self.format)?;
        let phase_shape = match cursor.phase {
            JournalIndexRebuildPhase::Discovering => {
                cursor.scan.is_some() && cursor.next_chunk.is_none()
            }
            JournalIndexRebuildPhase::Applying => {
                cursor.scan.is_none() && cursor.next_chunk.is_some()
            }
            JournalIndexRebuildPhase::Complete => {
                cursor.scan.is_none() && cursor.next_chunk.is_none()
            }
        };
        if cursor.repository != self.repository
            || cursor.job.is_nil()
            || cursor.node_root.format_digest != digest
            || cursor.commit_graph_root.format_digest != digest
            || cursor.baseline_checkpoint.is_some() != cursor.baseline_generation.is_some()
            || !phase_shape
            || cursor.scan.as_ref().is_some_and(|scan| {
                scan.repository != self.repository
                    || scan.branch != cursor.branch
                    || scan.snapshot_head != cursor.snapshot
            })
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "journal-index rebuild cursor is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }

    async fn store_rebuild_chunk(
        &self,
        chunk: &JournalIndexRebuildChunk,
    ) -> Result<JournalIndexRebuildChunkId> {
        chunk.validate(self.repository, &chunk.branch)?;
        let id = chunk.id()?;
        let bytes = encode_canonical(chunk)?;
        let path = self.rebuild_chunk_path(&chunk.branch, chunk.job, id)?;
        let expected_sha256 = crate::codec::sha256(&bytes);
        match self
            .plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                bytes: bytes.clone(),
                expected_sha256,
            })
            .await
        {
            Ok(ImmutablePutOutcome::Created(_) | ImmutablePutOutcome::AlreadyPresent(_)) => Ok(id),
            Err(original) => match self
                .plane
                .get(GetRequest {
                    path,
                    range: None,
                    physical_version: None,
                })
                .await
            {
                Ok(Some(stored)) if stored.bytes == bytes => Ok(id),
                _ => Err(original),
            },
        }
    }

    pub(crate) async fn load_rebuild_chunk(
        &self,
        branch: &str,
        job: OperationId,
        id: JournalIndexRebuildChunkId,
    ) -> Result<JournalIndexRebuildChunk> {
        let stored = self
            .plane
            .get(GetRequest {
                path: self.rebuild_chunk_path(branch, job, id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "journal-index rebuild chunk is missing",
                )
            })?;
        let chunk: JournalIndexRebuildChunk = decode_canonical(&stored.bytes)?;
        chunk.validate(self.repository, branch)?;
        if chunk.job != job || chunk.id()? != id {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "journal-index rebuild chunk does not match its content address",
            ));
        }
        Ok(chunk)
    }

    async fn publish_rebuild(
        &self,
        publisher: &ShardedBranchPublisher<P>,
        cursor: &JournalIndexRebuildCursor,
        now_millis: u64,
    ) -> Result<()> {
        let snapshot = publisher.load_publication(cursor.snapshot).await?;
        let expected_publications =
            cursor.snapshot_generation.0.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "journal-index rebuild publication count overflow",
                )
            })?;
        if snapshot.repository != self.repository
            || snapshot.branch != cursor.branch
            || snapshot.generation != cursor.snapshot_generation
            || snapshot.new_target != cursor.snapshot_target
            || cursor.discovered_publications != expected_publications
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "journal-index rebuild cursor does not cover its complete snapshot",
            ));
        }
        let loaded = self.load_head(&cursor.branch).await?;
        let baseline_matches = match (
            loaded.as_ref(),
            cursor.baseline_checkpoint,
            cursor.baseline_generation,
        ) {
            (None, None, None) => true,
            (Some(head), Some(checkpoint), Some(generation)) => {
                head.value.checkpoint == checkpoint && head.value.generation == generation
            }
            _ => false,
        };
        if !baseline_matches {
            return Err(Error::new(
                ErrorCode::RefConflict,
                "journal index changed while its resumable rebuild was running",
            )
            .retry(RetryAdvice::ReloadHead));
        }
        let generation = loaded.as_ref().map_or(Ok(0), |head| {
            head.value.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "journal-index rebuild head generation overflow",
                )
            })
        })?;
        let next = JournalDerivedIndexHead {
            repository: self.repository,
            branch: cursor.branch.clone(),
            checkpoint: cursor.snapshot,
            checkpoint_generation: cursor.snapshot_generation,
            target: cursor.snapshot_target,
            node_root: cursor.node_root.clone(),
            commit_graph_root: cursor.commit_graph_root.clone(),
            generation,
            indexed_publications: cursor.discovered_publications,
            indexed_commits: cursor.indexed_commits,
            updated_at_millis: now_millis,
        };
        next.validate(
            self.repository,
            &cursor.branch,
            tree_format_digest(&self.format)?,
        )?;
        let bytes = encode_canonical(&next)?;
        let path = self.head_path(&cursor.branch)?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected: loaded.map(|head| head.token),
                bytes: bytes.clone(),
            })
            .await
        {
            Ok(CompareExchangeOutcome::Applied(_)) => Ok(()),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => Ok(()),
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "journal-index rebuild head publication conflicted",
            )
            .retry(RetryAdvice::ReloadHead)),
            Err(error) => {
                if self
                    .plane
                    .load_mutable(&path)
                    .await?
                    .is_some_and(|current| current.bytes == bytes)
                {
                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("journal-index rebuild publication is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation))
                }
            }
        }
    }

    fn rebuild_chunk_prefix(&self, branch: &str, job: OperationId) -> String {
        format!(
            "{}/administration/index-rebuild/{}/{}/chunks/sha256/",
            self.prefix,
            hex::encode(branch.as_bytes()),
            hex::encode(job.as_bytes())
        )
    }

    fn rebuild_chunk_path(
        &self,
        branch: &str,
        job: OperationId,
        id: JournalIndexRebuildChunkId,
    ) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}{}/{}/{}",
            self.rebuild_chunk_prefix(branch, job),
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }

    fn node_pack_mutations(
        &self,
        commit: CommitId,
        pack_id: Option<crate::NodePackId>,
        toc: Option<&NodePackToc>,
        node_region_offset: Option<u64>,
    ) -> Result<Vec<Mutation>> {
        let Some(toc) = toc else {
            return Ok(Vec::new());
        };
        let pack_id = pack_id.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "journal-indexed node pack has no logical reference",
            )
        })?;
        let node_region_offset = node_region_offset.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "journal-indexed node pack has no node-region offset",
            )
        })?;
        let mut mutations = Vec::with_capacity(toc.entries.len());
        for entry in &toc.entries {
            let absolute_offset =
                node_region_offset
                    .checked_add(entry.offset)
                    .ok_or_else(|| {
                        Error::new(ErrorCode::CorruptNode, "journal node offset overflow")
                    })?;
            mutations.push(Mutation::Upsert {
                key: entry.cid.as_bytes().to_vec(),
                val: encode_canonical(&JournalNodeIndexEntry {
                    cid: entry.cid.clone(),
                    container: commit,
                    pack: pack_id,
                    absolute_offset,
                    len: entry.len,
                    sha256: entry.sha256,
                })?,
            });
        }
        Ok(mutations)
    }

    async fn build_commit_graph_entry(
        &self,
        tree: &Tree,
        pending: &BTreeMap<CommitId, JournalCommitGraphEntry>,
        id: CommitId,
        commit: BucketCommit,
    ) -> Result<JournalCommitGraphEntry> {
        let mut jumps = Vec::new();
        if let Some(first_parent) = commit.parents.first().copied() {
            jumps.push(first_parent);
            for level in 1..64usize {
                let ancestor = jumps[level - 1];
                let entry = if let Some(entry) = pending.get(&ancestor) {
                    entry.clone()
                } else {
                    let Some(encoded) = self.graph_engine.get(tree, ancestor.as_bytes()).await?
                    else {
                        break;
                    };
                    decode_canonical(&encoded)?
                };
                let Some(next) = entry.first_parent_jumps.get(level - 1).copied() else {
                    break;
                };
                jumps.push(next);
            }
        }
        Ok(JournalCommitGraphEntry {
            commit: id,
            generation: commit.generation,
            parents: commit.parents,
            first_parent_jumps: jumps,
        })
    }

    /// Apply a chronological journal page with one tree rewrite per derived
    /// index. Newly calculated graph entries remain available in memory while
    /// later entries build their binary-lifting skip pointers.
    async fn index_events(
        &self,
        publisher: &ShardedBranchPublisher<P>,
        node_tree: &mut Tree,
        graph_tree: &mut Tree,
        events: Vec<crate::PublicationEvent>,
    ) -> Result<(usize, usize)> {
        let keys = events
            .iter()
            .map(|event| event.new_target.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let existing = self.graph_engine.get_many(graph_tree, &keys).await?;
        let mut scheduled = BTreeSet::new();
        let missing = events
            .into_iter()
            .zip(existing)
            .filter_map(|(event, existing)| {
                (existing.is_none() && scheduled.insert(event.new_target)).then_some(event)
            })
            .collect::<Vec<_>>();
        let loaded = stream::iter(missing.into_iter().map(|event| async move {
            let object = publisher.load_commit_index(event.new_target).await?;
            // A merge may publish roots containing nodes introduced by a
            // secondary parent. Import those immutable pack locations into
            // the target branch index with the merge event so the merged
            // snapshot remains readable after source-branch deletion/reopen.
            let mut secondary_parents = Vec::new();
            for parent in object.commit.parents.iter().skip(1).copied() {
                secondary_parents.push((parent, publisher.load_commit_index(parent).await?));
            }
            Ok::<_, Error>((event, object, secondary_parents))
        }))
        .buffered(COMMIT_INDEX_LOAD_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

        let mut node_mutations = Vec::new();
        let mut graph_mutations = Vec::new();
        let mut pending = BTreeMap::new();
        let mut indexed_commits = 0usize;
        for loaded in loaded {
            let (event, object, secondary_parents) = loaded?;
            node_mutations.extend(self.node_pack_mutations(
                event.new_target,
                object.commit.node_pack.as_ref().map(|pack| pack.id),
                object.toc.as_ref(),
                object.node_region_offset,
            )?);
            for (parent, parent_index) in secondary_parents {
                node_mutations.extend(self.node_pack_mutations(
                    parent,
                    parent_index.commit.node_pack.as_ref().map(|pack| pack.id),
                    parent_index.toc.as_ref(),
                    parent_index.node_region_offset,
                )?);
            }
            let entry = self
                .build_commit_graph_entry(graph_tree, &pending, event.new_target, object.commit)
                .await?;
            graph_mutations.push(Mutation::Upsert {
                key: event.new_target.as_bytes().to_vec(),
                val: encode_canonical(&entry)?,
            });
            pending.insert(event.new_target, entry);
            indexed_commits = indexed_commits.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "indexed commit count overflow",
                )
            })?;
        }
        let indexed_nodes = node_mutations.len();
        if !node_mutations.is_empty() {
            *node_tree = self.node_engine.batch(node_tree, node_mutations).await?;
        }
        if !graph_mutations.is_empty() {
            *graph_tree = self.graph_engine.batch(graph_tree, graph_mutations).await?;
        }
        Ok((indexed_commits, indexed_nodes))
    }

    async fn collect_unindexed_events(
        &self,
        publisher: &ShardedBranchPublisher<P>,
        branch: &str,
        checkpoint: PublicationEventId,
        checkpoint_generation: RefGeneration,
        checkpoint_target: CommitId,
        current: PublicationEventId,
    ) -> Result<Vec<crate::PublicationEvent>> {
        let mut cursor = publisher.open_journal(branch).await?;
        if cursor.snapshot_head != current {
            return Err(Error::new(
                ErrorCode::RefConflict,
                "branch advanced while opening the journal-index snapshot",
            )
            .retry(RetryAdvice::ReloadHead));
        }
        let mut events = Vec::new();
        loop {
            let page = publisher.read_journal_page(&cursor, 1_000).await?;
            let mut found = false;
            for entry in page.entries {
                if entry.id == checkpoint {
                    if entry.event.generation != checkpoint_generation
                        || entry.event.new_target != checkpoint_target
                    {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "journal-index checkpoint metadata does not match its event",
                        ));
                    }
                    found = true;
                    break;
                }
                if events.len() == self.max_unindexed_events {
                    return Err(Error::new(
                        ErrorCode::HistoryLimitExceeded,
                        "journal-index lag exceeds its bounded catch-up window; run resumable rebuild",
                    ));
                }
                events.push(entry.event);
            }
            if found {
                return Ok(events);
            }
            let Some(next) = page.continuation else {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "journal-derived index checkpoint is absent from the branch journal",
                ));
            };
            cursor = next;
        }
    }

    async fn load_head(&self, branch: &str) -> Result<Option<LoadedHead>> {
        let Some(stored) = self.plane.load_mutable(&self.head_path(branch)?).await? else {
            return Ok(None);
        };
        let value: JournalDerivedIndexHead = decode_canonical(&stored.bytes)?;
        value.validate(self.repository, branch, tree_format_digest(&self.format)?)?;
        Ok(Some(LoadedHead {
            value,
            token: stored.metadata.token,
        }))
    }

    fn tree_from_root(&self, root: &RootManifest) -> Tree {
        Tree {
            root: root.root.clone(),
            config: Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        }
    }

    fn head_path(&self, branch: &str) -> Result<ObjectPath> {
        crate::repository::validate_branch(branch)?;
        ObjectPath::new(format!(
            "{}/journal-index/heads/{}.cbor",
            self.prefix,
            hex::encode(branch.as_bytes())
        ))
    }
}
