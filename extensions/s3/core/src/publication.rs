use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    encode_canonical, AuthorityPermit, AuthorityScope, BranchRefBarrier, BucketCommit, CommitId,
    CommitObject, CompareExchange, CompareExchangeOutcome, Error, ErrorCode, GetRequest,
    ImmutablePut, MutableControlObserver, MutableControlStore, NodePack, ObjectPath, ObjectPlane,
    OperationId, PendingAuthority, PublicationEvent, PublicationEventId, RefGeneration, RefValue,
    ReflogEntry, RepositoryId, Result, RetryAdvice, ShardWriterAuthority, StorageToken,
    DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

#[derive(Clone, Debug)]
pub struct LoadedRef {
    pub value: RefValue,
    pub token: StorageToken,
}

#[derive(Clone, Debug)]
pub struct AppliedBranchBarrier {
    pub reference: LoadedRef,
    barrier: BranchRefBarrier,
}

/// Durable cursor for one immutable snapshot of a branch publication journal.
/// Persist this value between pages; it never depends on the mutable ref after
/// `open_journal` returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationJournalCursor {
    pub repository: RepositoryId,
    pub branch: String,
    pub snapshot_head: PublicationEventId,
    pub next: Option<PublicationEventId>,
    pub next_generation: Option<RefGeneration>,
    pub next_target: Option<CommitId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationJournalEntry {
    pub id: PublicationEventId,
    pub event: PublicationEvent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationJournalPage {
    /// Newest-to-oldest events from the cursor's stable snapshot.
    pub entries: Vec<PublicationJournalEntry>,
    pub continuation: Option<PublicationJournalCursor>,
}

pub struct CommitPublication<'a> {
    pub permit: &'a AuthorityPermit,
    pub branch: &'a str,
    pub commit: &'a BucketCommit,
    pub node_pack: Option<&'a NodePack>,
    pub operation: OperationId,
    pub message: &'a str,
    pub now_millis: u64,
}

pub(crate) struct BranchMovement<'a> {
    pub permit: &'a AuthorityPermit,
    pub branch: &'a str,
    pub target: CommitId,
    pub operation: OperationId,
    pub message: &'a str,
    pub now_millis: u64,
}

impl AppliedBranchBarrier {
    pub fn into_barrier(self) -> BranchRefBarrier {
        self.barrier
    }
}

/// Repository branch publication module. Its interface combines authority
/// validation, immutable commit storage, reflog construction, and ref CAS so
/// callers cannot accidentally publish a commit under the wrong shard stamp.
pub struct ShardedBranchPublisher<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    prefix: String,
    repository: RepositoryId,
    authority: Arc<ShardWriterAuthority<P>>,
}

impl<P: ObjectPlane> ShardedBranchPublisher<P> {
    pub fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        authority: Arc<ShardWriterAuthority<P>>,
    ) -> Result<Self> {
        Self::new_with_control_retention(
            plane,
            prefix,
            repository,
            authority,
            DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
        )
    }

    pub fn new_with_control_retention(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        authority: Arc<ShardWriterAuthority<P>>,
        control_versions_to_retain: usize,
    ) -> Result<Self> {
        Self::new_with_gc_controls(
            plane,
            prefix,
            repository,
            authority,
            control_versions_to_retain,
            None,
            None,
        )
    }

    pub(crate) fn new_with_gc_controls(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        authority: Arc<ShardWriterAuthority<P>>,
        control_versions_to_retain: usize,
        observer: Option<Arc<dyn MutableControlObserver>>,
        barrier: Option<Arc<tokio::sync::RwLock<()>>>,
    ) -> Result<Self> {
        let prefix = prefix.into();
        let mut controls =
            MutableControlStore::new(plane.clone(), prefix.clone(), control_versions_to_retain)?;
        if let Some(observer) = observer {
            controls = controls.with_observer(observer);
        }
        if let Some(barrier) = barrier {
            controls = controls.with_mutation_barrier(barrier);
        }
        Ok(Self {
            plane,
            controls,
            prefix,
            repository,
            authority,
        })
    }

    pub async fn load(&self, branch: &str) -> Result<LoadedRef> {
        let loaded = self.load_including_tombstone(branch).await?;
        if loaded.value.tombstone {
            return Err(Error::new(
                ErrorCode::InvalidRevision,
                "branch ref is deleted",
            ));
        }
        Ok(loaded)
    }

    pub async fn load_including_tombstone(&self, branch: &str) -> Result<LoadedRef> {
        let path = self.ref_path(branch)?;
        let stored = self
            .plane
            .load_mutable(&path)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "branch ref is missing"))?;
        let value: RefValue = crate::decode_canonical(&stored.bytes)?;
        value.validate(self.repository, branch)?;
        Ok(LoadedRef {
            value,
            token: stored.metadata.token,
        })
    }

    /// Create or recreate a branch ref at an already durable  commit. The
    /// selected commit may have been authored under another branch authority;
    /// future publications are fenced by the new branch's own permit.
    pub async fn create_at_target(
        &self,
        permit: &AuthorityPermit,
        branch: &str,
        target: CommitId,
        operation: OperationId,
        message: &str,
        now_millis: u64,
    ) -> Result<LoadedRef> {
        crate::repository::validate_branch(branch)?;
        if operation.is_nil() || message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "branch creation requires an operation and message",
            ));
        }
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        stamp.validate(
            self.repository,
            &AuthorityScope::Branch {
                name: branch.to_string(),
            },
        )?;
        self.load_commit_object(target).await?;
        let existing = match self.load_including_tombstone(branch).await {
            Ok(existing) if !existing.value.tombstone => {
                return Err(Error::new(ErrorCode::RefConflict, "branch already exists"));
            }
            Ok(existing) => Some(existing),
            Err(error) if error.code == ErrorCode::InvalidRevision => None,
            Err(error) => return Err(error),
        };
        let generation = existing.as_ref().map_or(Ok(RefGeneration(0)), |current| {
            current
                .value
                .generation
                .0
                .checked_add(1)
                .map(RefGeneration)
                .ok_or_else(|| Error::new(ErrorCode::InternalInvariant, "ref generation overflow"))
        })?;
        let old_target = existing.as_ref().map(|current| current.value.target);
        let reflog = ReflogEntry {
            branch: branch.to_string(),
            old_target,
            new_target: target,
            operation,
            actor: stamp.writer_id.clone(),
            message: message.to_string(),
            created_at_millis: now_millis,
        };
        let event = PublicationEvent {
            repository: self.repository,
            branch: branch.to_string(),
            generation,
            previous: existing.as_ref().map(|current| current.value.publication),
            old_target,
            new_target: target,
            operation,
            reflog: reflog.id()?,
            authority: stamp.clone(),
            created_at_millis: now_millis,
        };
        let publication = self.store_publication(&event).await?;
        let value = RefValue {
            target,
            previous_target: old_target,
            generation,
            operation,
            reflog: reflog.id()?,
            publication,
            inline_reflog: reflog,
            authority: stamp,
            updated_at_millis: now_millis,
            tombstone: false,
        };
        self.cas_ref(branch, existing.map(|current| current.token), value)
            .await
    }

    pub async fn delete(
        &self,
        permit: &AuthorityPermit,
        branch: &str,
        current: LoadedRef,
        expected: CommitId,
        operation: OperationId,
        now_millis: u64,
    ) -> Result<LoadedRef> {
        current.value.validate(self.repository, branch)?;
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        if current.value.tombstone
            || current.value.target != expected
            || current.value.authority != stamp
            || operation.is_nil()
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch deletion does not match the live ref and authority",
            ));
        }
        let generation =
            RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
            })?);
        let reflog = ReflogEntry {
            branch: branch.to_string(),
            old_target: Some(expected),
            new_target: expected,
            operation,
            actor: stamp.writer_id.clone(),
            message: "delete branch".to_string(),
            created_at_millis: now_millis,
        };
        let event = PublicationEvent {
            repository: self.repository,
            branch: branch.to_string(),
            generation,
            previous: Some(current.value.publication),
            old_target: Some(expected),
            new_target: expected,
            operation,
            reflog: reflog.id()?,
            authority: stamp.clone(),
            created_at_millis: now_millis,
        };
        let publication = self.store_publication(&event).await?;
        let value = RefValue {
            target: expected,
            previous_target: Some(expected),
            generation,
            operation,
            reflog: reflog.id()?,
            publication,
            inline_reflog: reflog,
            authority: stamp,
            updated_at_millis: now_millis,
            tombstone: true,
        };
        self.cas_ref(branch, Some(current.token), value).await
    }

    /// Move a live branch ref to an already durable commit without creating a
    /// new bucket commit. This is the audited primitive used by administrative
    /// reset and reflog recovery; ordinary object writes must continue to use
    /// `store_and_publish`.
    pub(crate) async fn move_target(
        &self,
        current: LoadedRef,
        request: BranchMovement<'_>,
    ) -> Result<LoadedRef> {
        crate::repository::validate_branch(request.branch)?;
        current.value.validate(self.repository, request.branch)?;
        if request.operation.is_nil() || request.message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "branch movement requires an operation and non-empty reason",
            ));
        }
        let stamp = self
            .authority
            .validate_active(request.permit, request.now_millis)
            .await?;
        if current.value.tombstone || current.value.authority != stamp {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch ref is tombstoned or carries another authority epoch",
            ));
        }
        self.load_commit_object(request.target).await?;
        let generation =
            RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
            })?);
        let reflog = ReflogEntry {
            branch: request.branch.to_string(),
            old_target: Some(current.value.target),
            new_target: request.target,
            operation: request.operation,
            actor: stamp.writer_id.clone(),
            message: request.message.to_string(),
            created_at_millis: request.now_millis,
        };
        let event = PublicationEvent {
            repository: self.repository,
            branch: request.branch.to_string(),
            generation,
            previous: Some(current.value.publication),
            old_target: Some(current.value.target),
            new_target: request.target,
            operation: request.operation,
            reflog: reflog.id()?,
            authority: stamp.clone(),
            created_at_millis: request.now_millis,
        };
        let publication = self.store_publication(&event).await?;
        let value = RefValue {
            target: request.target,
            previous_target: Some(current.value.target),
            generation,
            operation: request.operation,
            reflog: reflog.id()?,
            publication,
            inline_reflog: reflog,
            authority: stamp,
            updated_at_millis: request.now_millis,
            tombstone: false,
        };
        self.cas_ref(request.branch, Some(current.token), value)
            .await
    }

    pub async fn create(&self, request: CommitPublication<'_>) -> Result<LoadedRef> {
        let stamp = self
            .authority
            .validate_active(request.permit, request.now_millis)
            .await?;
        self.validate_publication(
            request.branch,
            request.commit,
            &stamp,
            request.operation,
            request.message,
        )?;
        if !request.commit.parents.is_empty() || request.commit.generation.0 != 0 {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "branch creation requires a generation-zero root commit",
            ));
        }
        let target = self
            .store_commit(request.commit, request.node_pack.cloned())
            .await?;
        let reflog = ReflogEntry {
            branch: request.branch.to_string(),
            old_target: None,
            new_target: target,
            operation: request.operation,
            actor: stamp.writer_id.clone(),
            message: request.message.to_string(),
            created_at_millis: request.now_millis,
        };
        let event = PublicationEvent {
            repository: self.repository,
            branch: request.branch.to_string(),
            generation: RefGeneration(0),
            previous: None,
            old_target: None,
            new_target: target,
            operation: request.operation,
            reflog: reflog.id()?,
            authority: stamp.clone(),
            created_at_millis: request.now_millis,
        };
        let publication = self.store_publication(&event).await?;
        let value = RefValue {
            target,
            previous_target: None,
            generation: RefGeneration(0),
            operation: request.operation,
            reflog: reflog.id()?,
            publication,
            inline_reflog: reflog,
            authority: stamp,
            updated_at_millis: request.now_millis,
            tombstone: false,
        };
        self.cas_ref(request.branch, None, value).await
    }

    pub async fn store_and_publish(
        &self,
        current: LoadedRef,
        request: CommitPublication<'_>,
    ) -> Result<LoadedRef> {
        current.value.validate(self.repository, request.branch)?;
        let stamp = self
            .authority
            .validate_active(request.permit, request.now_millis)
            .await?;
        self.validate_publication(
            request.branch,
            request.commit,
            &stamp,
            request.operation,
            request.message,
        )?;
        if current.value.tombstone || current.value.authority != stamp {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch ref is tombstoned or carries another authority epoch",
            ));
        }
        if request.commit.parents.first() != Some(&current.value.target) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "commit does not advance the selected branch ref",
            ));
        }
        let mut parent_generation = None;
        for parent in &request.commit.parents {
            let generation = self.load_commit(*parent).await?.generation.0;
            parent_generation =
                Some(parent_generation.map_or(generation, |current: u64| current.max(generation)));
        }
        let expected_generation =
            parent_generation
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InvalidRequest,
                        "parent generation cannot be advanced",
                    )
                })?;
        if request.commit.generation.0 != expected_generation {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "commit generation does not follow its newest parent",
            ));
        }
        let target = self
            .store_commit(request.commit, request.node_pack.cloned())
            .await?;
        let reflog = ReflogEntry {
            branch: request.branch.to_string(),
            old_target: Some(current.value.target),
            new_target: target,
            operation: request.operation,
            actor: stamp.writer_id.clone(),
            message: request.message.to_string(),
            created_at_millis: request.now_millis,
        };
        let generation =
            RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
            })?);
        let event = PublicationEvent {
            repository: self.repository,
            branch: request.branch.to_string(),
            generation,
            previous: Some(current.value.publication),
            old_target: Some(current.value.target),
            new_target: target,
            operation: request.operation,
            reflog: reflog.id()?,
            authority: stamp.clone(),
            created_at_millis: request.now_millis,
        };
        let publication = self.store_publication(&event).await?;
        let value = RefValue {
            target,
            previous_target: Some(current.value.target),
            generation,
            operation: request.operation,
            reflog: reflog.id()?,
            publication,
            inline_reflog: reflog,
            authority: stamp,
            updated_at_millis: request.now_millis,
            tombstone: false,
        };
        self.cas_ref(request.branch, Some(current.token), value)
            .await
    }

    /// Publish the no-target-change ref barrier required by a pending
    /// authority takeover. The returned receipt is the only constructible
    /// input to `activate_after_barrier`.
    pub async fn publish_takeover_barrier(
        &self,
        branch: &str,
        current: LoadedRef,
        pending: &PendingAuthority,
        operation: OperationId,
        message: &str,
        now_millis: u64,
    ) -> Result<AppliedBranchBarrier> {
        current.value.validate(self.repository, branch)?;
        let stamp = pending.stamp();
        stamp.validate(
            self.repository,
            &AuthorityScope::Branch {
                name: branch.to_string(),
            },
        )?;
        if current.value.tombstone || operation.is_nil() || message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "takeover barrier requires a live ref, operation, and message",
            ));
        }
        if stamp.generation != current.value.authority.generation.saturating_add(1) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "takeover barrier does not advance the ref authority generation",
            ));
        }
        let reflog = ReflogEntry {
            branch: branch.to_string(),
            old_target: Some(current.value.target),
            new_target: current.value.target,
            operation,
            actor: stamp.writer_id.clone(),
            message: message.to_string(),
            created_at_millis: now_millis,
        };
        let generation =
            RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
            })?);
        let event = PublicationEvent {
            repository: self.repository,
            branch: branch.to_string(),
            generation,
            previous: Some(current.value.publication),
            old_target: Some(current.value.target),
            new_target: current.value.target,
            operation,
            reflog: reflog.id()?,
            authority: stamp.clone(),
            created_at_millis: now_millis,
        };
        let publication = self.store_publication(&event).await?;
        let value = RefValue {
            target: current.value.target,
            previous_target: Some(current.value.target),
            generation,
            operation,
            reflog: reflog.id()?,
            publication,
            inline_reflog: reflog,
            authority: stamp.clone(),
            updated_at_millis: now_millis,
            tombstone: false,
        };
        let reference = self.cas_ref(branch, Some(current.token), value).await?;
        Ok(AppliedBranchBarrier {
            reference,
            barrier: BranchRefBarrier::new(stamp),
        })
    }

    fn validate_publication(
        &self,
        branch: &str,
        commit: &BucketCommit,
        stamp: &crate::AuthorityStamp,
        operation: OperationId,
        message: &str,
    ) -> Result<()> {
        crate::repository::validate_branch(branch)?;
        commit.validate_authority(self.repository, branch)?;
        if &commit.authority != stamp
            || commit.author != stamp.writer_id
            || operation.is_nil()
            || message.trim().is_empty()
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "commit publication identity does not match its authority permit",
            ));
        }
        Ok(())
    }

    pub(crate) async fn store_commit(
        &self,
        commit: &BucketCommit,
        node_pack: Option<NodePack>,
    ) -> Result<CommitId> {
        let id = commit.id()?;
        let bytes = CommitObject::new(commit.clone(), node_pack)?.encode_object()?;
        self.plane
            .put_immutable(ImmutablePut {
                path: self.commit_path(id)?,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        Ok(id)
    }

    async fn store_publication(&self, event: &PublicationEvent) -> Result<PublicationEventId> {
        event.validate()?;
        let id = event.id()?;
        let bytes = encode_canonical(event)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: self.publication_path(id)?,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        Ok(id)
    }

    pub async fn load_publication(&self, id: PublicationEventId) -> Result<PublicationEvent> {
        let stored = self
            .plane
            .get(GetRequest {
                path: self.publication_path(id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "publication journal event is missing",
                )
            })?;
        let event: PublicationEvent = crate::decode_canonical(&stored.bytes)?;
        event.validate()?;
        if event.id()? != id || event.repository != self.repository {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "publication event does not match its content address",
            ));
        }
        Ok(event)
    }

    pub async fn open_journal(&self, branch: &str) -> Result<PublicationJournalCursor> {
        let reference = self.load(branch).await?;
        let cursor = PublicationJournalCursor {
            repository: self.repository,
            branch: branch.to_string(),
            snapshot_head: reference.value.publication,
            next: Some(reference.value.publication),
            next_generation: Some(reference.value.generation),
            next_target: Some(reference.value.target),
        };
        self.validate_journal_cursor(&cursor)?;
        Ok(cursor)
    }

    pub async fn read_journal_page(
        &self,
        cursor: &PublicationJournalCursor,
        limit: usize,
    ) -> Result<PublicationJournalPage> {
        self.validate_journal_cursor(cursor)?;
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "publication journal page limit must be between 1 and 1,000",
            ));
        }
        let mut next = cursor.next;
        let mut next_generation = cursor.next_generation;
        let mut next_target = cursor.next_target;
        let mut entries = Vec::with_capacity(limit.min(64));
        while entries.len() < limit {
            let Some(id) = next else { break };
            let event = self.load_publication(id).await?;
            if event.branch != cursor.branch
                || Some(event.generation) != next_generation
                || Some(event.new_target) != next_target
            {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "publication journal event does not match its cursor link",
                ));
            }
            next = event.previous;
            next_generation = event
                .previous
                .map(|_| RefGeneration(event.generation.0.saturating_sub(1)));
            next_target = event.old_target;
            entries.push(PublicationJournalEntry { id, event });
        }
        let continuation = next.map(|next| PublicationJournalCursor {
            repository: cursor.repository,
            branch: cursor.branch.clone(),
            snapshot_head: cursor.snapshot_head,
            next: Some(next),
            next_generation,
            next_target,
        });
        Ok(PublicationJournalPage {
            entries,
            continuation,
        })
    }

    fn validate_journal_cursor(&self, cursor: &PublicationJournalCursor) -> Result<()> {
        crate::repository::validate_branch(&cursor.branch)?;
        let link_presence_matches = cursor.next.is_some()
            && cursor.next_generation.is_some()
            && cursor.next_target.is_some();
        if cursor.repository != self.repository || !link_presence_matches {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "publication journal cursor is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }

    pub async fn load_commit_object(&self, id: CommitId) -> Result<CommitObject> {
        let stored = self
            .plane
            .get(GetRequest {
                path: self.commit_path(id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "parent commit is missing"))?;
        let object = CommitObject::decode_object(&stored.bytes)?;
        if object.commit.id()? != id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "parent commit ID does not match its path",
            ));
        }
        Ok(object)
    }

    pub async fn load_commit(&self, id: CommitId) -> Result<BucketCommit> {
        let path = self.commit_path(id)?;
        let header_len = CommitObject::commit_object_header_len();
        let header = self
            .plane
            .get(GetRequest {
                path: path.clone(),
                range: Some(0..=header_len as u64 - 1),
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "parent commit is missing"))?
            .bytes;
        let commit_len = CommitObject::commit_len_from_header(&header)?;
        let end = header_len
            .checked_add(commit_len)
            .ok_or_else(|| Error::new(ErrorCode::CorruptCommit, "commit length overflow"))?;
        let body = self
            .plane
            .get(GetRequest {
                path,
                range: Some(header_len as u64..=end as u64 - 1),
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "parent commit is missing"))?
            .bytes;
        let mut encoded = header;
        encoded.extend_from_slice(&body);
        let commit = CommitObject::decode_commit_metadata(&encoded)?;
        if commit.id()? != id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "parent commit ID does not match its path",
            ));
        }
        Ok(commit)
    }

    async fn cas_ref(
        &self,
        branch: &str,
        expected: Option<StorageToken>,
        value: RefValue,
    ) -> Result<LoadedRef> {
        value.validate(self.repository, branch)?;
        let path = self.ref_path(branch)?;
        let bytes = encode_canonical(&value)?;
        let outcome = self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected,
                bytes: bytes.clone(),
            })
            .await;
        match outcome {
            Err(error) => {
                if let Some(current) = self.plane.load_mutable(&path).await? {
                    if current.bytes == bytes {
                        return Ok(LoadedRef {
                            value,
                            token: current.metadata.token,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("branch publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation))
            }
            Ok(CompareExchangeOutcome::Applied(metadata)) => Ok(LoadedRef {
                value,
                token: metadata.token,
            }),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {
                Ok(LoadedRef {
                    value,
                    token: current.metadata.token,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "branch ref changed concurrently",
            )),
        }
    }

    fn ref_path(&self, branch: &str) -> Result<ObjectPath> {
        crate::repository::validate_branch(branch)?;
        ObjectPath::new(format!(
            "{}/refs/heads/{}",
            self.prefix,
            hex::encode(branch.as_bytes())
        ))
    }

    fn commit_path(&self, id: CommitId) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/commits/sha256/{}/{}/{}",
            self.prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }

    fn publication_path(&self, id: PublicationEventId) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/publications/sha256/{}/{}/{}",
            self.prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}
