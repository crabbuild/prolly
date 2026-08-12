use std::sync::Arc;

use prolly::{AsyncProlly, Config, Mutation, RuntimeConfig, Tree, TreeFormat};

use crate::{
    decode_canonical, encode_canonical, tree_format_digest, CommitIdV2, CommitObjectV2,
    CompareExchange, CompareExchangeOutcome, Error, ErrorCode, JournalCommitGraphEntryV2,
    JournalDerivedIndexHeadV2, JournalNodeIndexEntryV2, MemoryNodeCache, MutableControlStore,
    NodeCache, ObjectPath, ObjectPlane, ProllyObjectStore, PublicationEventIdV2, RefGeneration,
    RepositoryId, Result, RetryAdvice, ShardedBranchPublisherV2, StorageToken, TreeRootV1,
    DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
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
        let controls = MutableControlStore::new(
            plane.clone(),
            prefix.clone(),
            control_versions_to_retain,
        )?;
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
            if head.checkpoint_generation != current.value.generation
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
                vec![publisher
                    .load_publication(current.value.publication)
                    .await?],
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
            self.index_node_pack(&mut node_tree, event.new_target, &object, &mut indexed_nodes)
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
                    Error::new(ErrorCode::InternalInvariant, "publication count exceeds u64")
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
        value.validate(
            self.repository,
            branch,
            tree_format_digest(&self.format)?,
        )?;
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
