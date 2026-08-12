use std::sync::Arc;

use prolly::{AsyncProlly, Config, Mutation, RuntimeConfig, Tree, TreeFormat};
use serde::{Deserialize, Serialize};

use crate::{
    decode_canonical, encode_canonical, tree_format_digest, CommitIdV2, CommitObjectV2,
    CompareExchange, CompareExchangeOutcome, DeleteOutcome, Error, ErrorCode, GetRequest,
    ImmutablePut, ImmutablePutOutcome, JournalCommitGraphEntryV2, JournalDerivedIndexHeadV2,
    JournalIndexRebuildChunkIdV2, JournalIndexRebuildChunkV2, JournalNodeIndexEntryV2, ListRequest,
    MemoryNodeCache, MutableControlStore, NodeCache, ObjectPath, ObjectPlane, OperationId,
    PhysicalVersion, ProllyObjectStore, PublicationEventIdV2, PublicationJournalCursorV2,
    RefGeneration, RepositoryId, Result, RetryAdvice, ShardedBranchPublisherV2, StorageToken,
    TreeRootV1, DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

pub const DEFAULT_JOURNAL_INDEX_MAX_UNINDEXED_EVENTS: usize = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalIndexAdvanceReportV2 {
    pub checkpoint: PublicationEventIdV2,
    pub checkpoint_generation: RefGeneration,
    pub indexed_publications: usize,
    pub indexed_commits: usize,
    pub indexed_nodes: usize,
    pub initialized: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalIndexRebuildPhaseV2 {
    Discovering,
    Applying,
    Complete,
}

/// Constant-size process-independent cursor for rebuilding branch-local node
/// and commit-graph indexes from an immutable publication-journal snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalIndexRebuildCursorV2 {
    pub repository: RepositoryId,
    pub branch: String,
    pub job: OperationId,
    pub snapshot: PublicationEventIdV2,
    pub snapshot_generation: RefGeneration,
    pub snapshot_target: CommitIdV2,
    pub scan: Option<PublicationJournalCursorV2>,
    pub oldest_chunk: Option<JournalIndexRebuildChunkIdV2>,
    pub next_chunk: Option<JournalIndexRebuildChunkIdV2>,
    pub next_chunk_sequence: u64,
    pub node_root: TreeRootV1,
    pub commit_graph_root: TreeRootV1,
    pub discovered_publications: u64,
    pub indexed_commits: u64,
    pub indexed_nodes: u64,
    pub baseline_checkpoint: Option<PublicationEventIdV2>,
    pub baseline_generation: Option<u64>,
    pub phase: JournalIndexRebuildPhaseV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalIndexRebuildStepV2 {
    pub cursor: JournalIndexRebuildCursorV2,
    pub discovered_publications: usize,
    pub indexed_publications: usize,
    pub indexed_commits: usize,
    pub indexed_nodes: usize,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JournalIndexRebuildCleanupV2 {
    pub deleted_objects: usize,
    pub complete: bool,
}

struct LoadedHead {
    value: JournalDerivedIndexHeadV2,
    token: StorageToken,
}

/// Branch-local node and commit-graph indexes advanced exclusively from the
/// immutable v2 publication chain. No S3 namespace listing is used.
pub struct JournalDerivedIndexesV2<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    prefix: String,
    repository: RepositoryId,
    format: TreeFormat,
    node_engine: AsyncProlly<ProllyObjectStore<P>>,
    graph_engine: AsyncProlly<ProllyObjectStore<P>>,
    max_unindexed_events: usize,
}

impl<P: ObjectPlane> JournalDerivedIndexesV2<P> {
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
            format!("{prefix}/journal-index/v2/node-tree"),
            repository,
            2,
            digest,
            node_cache.clone(),
        );
        let graph_store = ProllyObjectStore::new_cached_direct(
            plane.clone(),
            format!("{prefix}/journal-index/v2/graph-tree"),
            repository,
            2,
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
        publisher: &ShardedBranchPublisherV2<P>,
        branch: &str,
        now_millis: u64,
    ) -> Result<JournalIndexAdvanceReportV2> {
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
            return Ok(JournalIndexAdvanceReportV2 {
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
        let mut indexed_commits = 0usize;
        let mut indexed_nodes = 0usize;
        for event in events {
            if self
                .graph_engine
                .get(&graph_tree, event.new_target.as_bytes())
                .await?
                .is_some()
            {
                continue;
            }
            let object = publisher.load_commit_object(event.new_target).await?;
            self.index_node_pack(
                &mut node_tree,
                event.new_target,
                &object,
                &mut indexed_nodes,
            )
            .await?;
            self.index_commit_graph(&mut graph_tree, event.new_target, object)
                .await?;
            indexed_commits += 1;
        }

        let next = JournalDerivedIndexHeadV2 {
            repository: self.repository,
            branch: branch.to_string(),
            checkpoint: current.value.publication,
            checkpoint_generation: current.value.generation,
            target: current.value.target,
            node_root: TreeRootV1 {
                root: node_tree.root.clone(),
                format_digest: digest,
            },
            commit_graph_root: TreeRootV1 {
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
        Ok(JournalIndexAdvanceReportV2 {
            checkpoint: next.checkpoint,
            checkpoint_generation: next.checkpoint_generation,
            indexed_publications,
            indexed_commits,
            indexed_nodes,
            initialized,
        })
    }

    pub async fn start_rebuild(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        branch: &str,
        job: OperationId,
    ) -> Result<JournalIndexRebuildCursorV2> {
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
        Ok(JournalIndexRebuildCursorV2 {
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
            node_root: TreeRootV1 {
                root: None,
                format_digest: digest,
            },
            commit_graph_root: TreeRootV1 {
                root: None,
                format_digest: digest,
            },
            discovered_publications: 0,
            indexed_commits: 0,
            indexed_nodes: 0,
            baseline_checkpoint: baseline.as_ref().map(|head| head.value.checkpoint),
            baseline_generation: baseline.as_ref().map(|head| head.value.generation),
            phase: JournalIndexRebuildPhaseV2::Discovering,
        })
    }

    pub async fn advance_rebuild(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        cursor: &JournalIndexRebuildCursorV2,
        max_events: usize,
        now_millis: u64,
    ) -> Result<JournalIndexRebuildStepV2> {
        self.validate_rebuild_cursor(cursor)?;
        if !(1..=1_000).contains(&max_events) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "journal-index rebuild page must contain 1 to 1,000 events",
            ));
        }
        if cursor.phase == JournalIndexRebuildPhaseV2::Complete {
            return Ok(JournalIndexRebuildStepV2 {
                cursor: cursor.clone(),
                discovered_publications: 0,
                indexed_publications: 0,
                indexed_commits: 0,
                indexed_nodes: 0,
                complete: true,
            });
        }
        if cursor.phase == JournalIndexRebuildPhaseV2::Discovering {
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
            let chunk = JournalIndexRebuildChunkV2 {
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
                next.phase = JournalIndexRebuildPhaseV2::Applying;
                next.next_chunk = next.oldest_chunk;
            }
            return Ok(JournalIndexRebuildStepV2 {
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
        let mut indexed_commits = 0usize;
        let mut indexed_nodes = 0usize;
        for event in chunk.events.iter().rev() {
            if self
                .graph_engine
                .get(&graph_tree, event.new_target.as_bytes())
                .await?
                .is_some()
            {
                continue;
            }
            let object = publisher.load_commit_object(event.new_target).await?;
            self.index_node_pack(
                &mut node_tree,
                event.new_target,
                &object,
                &mut indexed_nodes,
            )
            .await?;
            self.index_commit_graph(&mut graph_tree, event.new_target, object)
                .await?;
            indexed_commits += 1;
        }
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
            next.phase = JournalIndexRebuildPhaseV2::Complete;
        }
        Ok(JournalIndexRebuildStepV2 {
            cursor: next.clone(),
            discovered_publications: 0,
            indexed_publications: chunk.events.len(),
            indexed_commits,
            indexed_nodes,
            complete: next.phase == JournalIndexRebuildPhaseV2::Complete,
        })
    }

    pub async fn cleanup_rebuild(
        &self,
        cursor: &JournalIndexRebuildCursorV2,
        limit: usize,
    ) -> Result<JournalIndexRebuildCleanupV2> {
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
        Ok(JournalIndexRebuildCleanupV2 {
            deleted_objects,
            complete: page.continuation.is_none(),
        })
    }

    pub async fn node_location(
        &self,
        branch: &str,
        cid: &prolly::Cid,
    ) -> Result<Option<JournalNodeIndexEntryV2>> {
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
            .is_some_and(|entry: &JournalNodeIndexEntryV2| entry.cid != *cid)
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
        commit: CommitIdV2,
    ) -> Result<Option<JournalCommitGraphEntryV2>> {
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
            .is_some_and(|entry: &JournalCommitGraphEntryV2| entry.commit != commit)
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "journal commit-graph value does not match its key",
            ));
        }
        Ok(entry)
    }

    pub async fn head(&self, branch: &str) -> Result<Option<JournalDerivedIndexHeadV2>> {
        Ok(self.load_head(branch).await?.map(|head| head.value))
    }

    fn validate_rebuild_cursor(&self, cursor: &JournalIndexRebuildCursorV2) -> Result<()> {
        crate::repository::validate_branch(&cursor.branch)?;
        let digest = tree_format_digest(&self.format)?;
        let phase_shape = match cursor.phase {
            JournalIndexRebuildPhaseV2::Discovering => {
                cursor.scan.is_some() && cursor.next_chunk.is_none()
            }
            JournalIndexRebuildPhaseV2::Applying => {
                cursor.scan.is_none() && cursor.next_chunk.is_some()
            }
            JournalIndexRebuildPhaseV2::Complete => {
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
        chunk: &JournalIndexRebuildChunkV2,
    ) -> Result<JournalIndexRebuildChunkIdV2> {
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
        id: JournalIndexRebuildChunkIdV2,
    ) -> Result<JournalIndexRebuildChunkV2> {
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
        let chunk: JournalIndexRebuildChunkV2 = decode_canonical(&stored.bytes)?;
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
        publisher: &ShardedBranchPublisherV2<P>,
        cursor: &JournalIndexRebuildCursorV2,
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
        let next = JournalDerivedIndexHeadV2 {
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
            "{}/administration/v2/index-rebuild/{}/{}/chunks/sha256/",
            self.prefix,
            hex::encode(branch.as_bytes()),
            hex::encode(job.as_bytes())
        )
    }

    fn rebuild_chunk_path(
        &self,
        branch: &str,
        job: OperationId,
        id: JournalIndexRebuildChunkIdV2,
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

    async fn index_node_pack(
        &self,
        tree: &mut Tree,
        commit: CommitIdV2,
        object: &CommitObjectV2,
        indexed_nodes: &mut usize,
    ) -> Result<()> {
        let Some(pack) = object.node_pack.as_ref() else {
            return Ok(());
        };
        let encoded = object.encode_object()?;
        let payload_offset = CommitObjectV2::node_payload_offset(&encoded)?.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "journal-indexed node pack has no payload offset",
            )
        })?;
        let pack_id = pack.reference()?.id;
        let mut mutations = Vec::with_capacity(pack.entries.len());
        for entry in &pack.entries {
            let absolute_offset = payload_offset.checked_add(entry.offset).ok_or_else(|| {
                Error::new(ErrorCode::CorruptNode, "journal node offset overflow")
            })?;
            mutations.push(Mutation::Upsert {
                key: entry.cid.as_bytes().to_vec(),
                val: encode_canonical(&JournalNodeIndexEntryV2 {
                    cid: entry.cid.clone(),
                    container: commit,
                    pack: pack_id,
                    absolute_offset,
                    len: entry.len,
                    sha256: entry.sha256,
                })?,
            });
        }
        *indexed_nodes = indexed_nodes.checked_add(mutations.len()).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "indexed node count overflow")
        })?;
        if !mutations.is_empty() {
            *tree = self.node_engine.batch(tree, mutations).await?;
        }
        Ok(())
    }

    async fn index_commit_graph(
        &self,
        tree: &mut Tree,
        id: CommitIdV2,
        object: CommitObjectV2,
    ) -> Result<()> {
        let mut jumps = Vec::new();
        if let Some(first_parent) = object.commit.parents.first().copied() {
            jumps.push(first_parent);
            for level in 1..64usize {
                let ancestor = jumps[level - 1];
                let Some(encoded) = self.graph_engine.get(tree, ancestor.as_bytes()).await? else {
                    break;
                };
                let entry: JournalCommitGraphEntryV2 = decode_canonical(&encoded)?;
                let Some(next) = entry.first_parent_jumps.get(level - 1).copied() else {
                    break;
                };
                jumps.push(next);
            }
        }
        let entry = JournalCommitGraphEntryV2 {
            commit: id,
            generation: object.commit.generation,
            parents: object.commit.parents,
            first_parent_jumps: jumps,
        };
        *tree = self
            .graph_engine
            .batch(
                tree,
                vec![Mutation::Upsert {
                    key: id.as_bytes().to_vec(),
                    val: encode_canonical(&entry)?,
                }],
            )
            .await?;
        Ok(())
    }

    async fn collect_unindexed_events(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        branch: &str,
        checkpoint: PublicationEventIdV2,
        checkpoint_generation: RefGeneration,
        checkpoint_target: CommitIdV2,
        current: PublicationEventIdV2,
    ) -> Result<Vec<crate::PublicationEventV2>> {
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
        let value: JournalDerivedIndexHeadV2 = decode_canonical(&stored.bytes)?;
        value.validate(self.repository, branch, tree_format_digest(&self.format)?)?;
        Ok(Some(LoadedHead {
            value,
            token: stored.metadata.token,
        }))
    }

    fn tree_from_root(&self, root: &TreeRootV1) -> Tree {
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
            "{}/journal-index/v2/heads/{}.cbor",
            self.prefix,
            hex::encode(branch.as_bytes())
        ))
    }
}
