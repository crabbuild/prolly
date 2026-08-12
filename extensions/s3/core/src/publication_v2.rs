use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    encode_canonical, AuthorityPermitV2, AuthorityScopeV2, BranchRefBarrierV2, BucketCommitV2,
    CommitIdV2, CommitObjectV2, CompareExchange, CompareExchangeOutcome, Error, ErrorCode,
    GetRequest, ImmutablePut, MutableControlStore, NodePackV1, ObjectPath, ObjectPlane,
    OperationId, PendingAuthorityV2, PublicationEventIdV2, PublicationEventV2, RefGeneration,
    RefValueV2, ReflogEntryV2, RepositoryId, Result, RetryAdvice, ShardWriterAuthorityV2,
    StorageToken, DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

#[derive(Clone, Debug)]
pub struct LoadedRefV2 {
    pub value: RefValueV2,
    pub token: StorageToken,
}

#[derive(Clone, Debug)]
pub struct AppliedBranchBarrierV2 {
    pub reference: LoadedRefV2,
    barrier: BranchRefBarrierV2,
}

/// Durable cursor for one immutable snapshot of a branch publication journal.
/// Persist this value between pages; it never depends on the mutable ref after
/// `open_journal` returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationJournalCursorV2 {
    pub repository: RepositoryId,
    pub branch: String,
    pub snapshot_head: PublicationEventIdV2,
    pub next: Option<PublicationEventIdV2>,
    pub next_generation: Option<RefGeneration>,
    pub next_target: Option<CommitIdV2>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationJournalEntryV2 {
    pub id: PublicationEventIdV2,
    pub event: PublicationEventV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationJournalPageV2 {
    /// Newest-to-oldest events from the cursor's stable snapshot.
    pub entries: Vec<PublicationJournalEntryV2>,
    pub continuation: Option<PublicationJournalCursorV2>,
}

pub struct CommitPublicationV2<'a> {
    pub permit: &'a AuthorityPermitV2,
    pub branch: &'a str,
    pub commit: &'a BucketCommitV2,
    pub node_pack: Option<&'a NodePackV1>,
    pub operation: OperationId,
    pub message: &'a str,
    pub now_millis: u64,
}

impl AppliedBranchBarrierV2 {
    pub fn into_barrier(self) -> BranchRefBarrierV2 {
        self.barrier
    }
}

/// Protocol-v2 branch publication module. Its interface combines authority
/// validation, immutable commit storage, reflog construction, and ref CAS so
/// callers cannot accidentally publish a commit under the wrong shard stamp.
pub struct ShardedBranchPublisherV2<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    prefix: String,
    repository: RepositoryId,
    authority: Arc<ShardWriterAuthorityV2<P>>,
}

impl<P: ObjectPlane> ShardedBranchPublisherV2<P> {
    pub fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        authority: Arc<ShardWriterAuthorityV2<P>>,
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
        authority: Arc<ShardWriterAuthorityV2<P>>,
        control_versions_to_retain: usize,
    ) -> Result<Self> {
        let prefix = prefix.into();
        let controls =
            MutableControlStore::new(plane.clone(), prefix.clone(), control_versions_to_retain)?;
        Ok(Self {
            plane,
            controls,
            prefix,
            repository,
            authority,
        })
    }

    pub async fn load(&self, branch: &str) -> Result<LoadedRefV2> {
        let loaded = self.load_including_tombstone(branch).await?;
        if loaded.value.tombstone {
            return Err(Error::new(
                ErrorCode::InvalidRevision,
                "v2 branch ref is deleted",
            ));
        }
        Ok(loaded)
    }

    pub async fn load_including_tombstone(&self, branch: &str) -> Result<LoadedRefV2> {
        let path = self.ref_path(branch)?;
        let stored =
            self.plane.load_mutable(&path).await?.ok_or_else(|| {
                Error::new(ErrorCode::InvalidRevision, "v2 branch ref is missing")
            })?;
        let value: RefValueV2 = crate::decode_canonical(&stored.bytes)?;
        value.validate(self.repository, branch)?;
        Ok(LoadedRefV2 {
            value,
            token: stored.metadata.token,
        })
    }

    /// Create or recreate a branch ref at an already durable v2 commit. The
    /// selected commit may have been authored under another branch authority;
    /// future publications are fenced by the new branch's own permit.
    pub async fn create_at_target(
        &self,
        permit: &AuthorityPermitV2,
        branch: &str,
        target: CommitIdV2,
        operation: OperationId,
        message: &str,
        now_millis: u64,
    ) -> Result<LoadedRefV2> {
        crate::repository::validate_branch(branch)?;
        if operation.is_nil() || message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 branch creation requires an operation and message",
            ));
        }
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        stamp.validate(
            self.repository,
            &AuthorityScopeV2::Branch {
                name: branch.to_string(),
            },
        )?;
        self.load_commit_object(target).await?;
        let existing = match self.load_including_tombstone(branch).await {
            Ok(existing) if !existing.value.tombstone => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "v2 branch already exists",
                ));
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
                .ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "v2 ref generation overflow")
                })
        })?;
        let old_target = existing.as_ref().map(|current| current.value.target);
        let reflog = ReflogEntryV2 {
            branch: branch.to_string(),
            old_target,
            new_target: target,
            operation,
            actor: stamp.writer_id.clone(),
            message: message.to_string(),
            created_at_millis: now_millis,
        };
        let event = PublicationEventV2 {
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
        let value = RefValueV2 {
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
        permit: &AuthorityPermitV2,
        branch: &str,
        current: LoadedRefV2,
        expected: CommitIdV2,
        operation: OperationId,
        now_millis: u64,
    ) -> Result<LoadedRefV2> {
        current.value.validate(self.repository, branch)?;
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        if current.value.tombstone
            || current.value.target != expected
            || current.value.authority != stamp
            || operation.is_nil()
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "v2 branch deletion does not match the live ref and authority",
            ));
        }
        let generation =
            RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "v2 ref generation overflow")
            })?);
        let reflog = ReflogEntryV2 {
            branch: branch.to_string(),
            old_target: Some(expected),
            new_target: expected,
            operation,
            actor: stamp.writer_id.clone(),
            message: "delete branch".to_string(),
            created_at_millis: now_millis,
        };
        let event = PublicationEventV2 {
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
        let value = RefValueV2 {
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

    pub async fn create(&self, request: CommitPublicationV2<'_>) -> Result<LoadedRefV2> {
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
                "v2 branch creation requires a generation-zero root commit",
            ));
        }
        let target = self
            .store_commit(request.commit, request.node_pack.cloned())
            .await?;
        let reflog = ReflogEntryV2 {
            branch: request.branch.to_string(),
            old_target: None,
            new_target: target,
            operation: request.operation,
            actor: stamp.writer_id.clone(),
            message: request.message.to_string(),
            created_at_millis: request.now_millis,
        };
        let event = PublicationEventV2 {
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
        let value = RefValueV2 {
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
        current: LoadedRefV2,
        request: CommitPublicationV2<'_>,
    ) -> Result<LoadedRefV2> {
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
                "v2 branch ref is tombstoned or carries another authority epoch",
            ));
        }
        let parent = self.load_commit(current.value.target).await?;
        if request.commit.parents.first() != Some(&current.value.target)
            || request.commit.generation.0 != parent.generation.0.saturating_add(1)
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 commit does not advance the selected branch ref",
            ));
        }
        let target = self
            .store_commit(request.commit, request.node_pack.cloned())
            .await?;
        let reflog = ReflogEntryV2 {
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
                Error::new(ErrorCode::InternalInvariant, "v2 ref generation overflow")
            })?);
        let event = PublicationEventV2 {
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
        let value = RefValueV2 {
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
        current: LoadedRefV2,
        pending: &PendingAuthorityV2,
        operation: OperationId,
        message: &str,
        now_millis: u64,
    ) -> Result<AppliedBranchBarrierV2> {
        current.value.validate(self.repository, branch)?;
        let stamp = pending.stamp();
        stamp.validate(
            self.repository,
            &AuthorityScopeV2::Branch {
                name: branch.to_string(),
            },
        )?;
        if current.value.tombstone || operation.is_nil() || message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 takeover barrier requires a live ref, operation, and message",
            ));
        }
        if stamp.generation != current.value.authority.generation.saturating_add(1) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "v2 takeover barrier does not advance the ref authority generation",
            ));
        }
        let reflog = ReflogEntryV2 {
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
                Error::new(ErrorCode::InternalInvariant, "v2 ref generation overflow")
            })?);
        let event = PublicationEventV2 {
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
        let value = RefValueV2 {
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
        Ok(AppliedBranchBarrierV2 {
            reference,
            barrier: BranchRefBarrierV2::new(stamp),
        })
    }

    fn validate_publication(
        &self,
        branch: &str,
        commit: &BucketCommitV2,
        stamp: &crate::AuthorityStampV2,
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
                "v2 commit publication identity does not match its authority permit",
            ));
        }
        Ok(())
    }

    async fn store_commit(
        &self,
        commit: &BucketCommitV2,
        node_pack: Option<NodePackV1>,
    ) -> Result<CommitIdV2> {
        let id = commit.id()?;
        let bytes = CommitObjectV2::new(commit.clone(), node_pack)?.encode_object()?;
        self.plane
            .put_immutable(ImmutablePut {
                path: self.commit_path(id)?,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        Ok(id)
    }

    async fn store_publication(&self, event: &PublicationEventV2) -> Result<PublicationEventIdV2> {
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

    pub async fn load_publication(&self, id: PublicationEventIdV2) -> Result<PublicationEventV2> {
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
                    "v2 publication journal event is missing",
                )
            })?;
        let event: PublicationEventV2 = crate::decode_canonical(&stored.bytes)?;
        event.validate()?;
        if event.id()? != id || event.repository != self.repository {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 publication event does not match its content address",
            ));
        }
        Ok(event)
    }

    pub async fn open_journal(&self, branch: &str) -> Result<PublicationJournalCursorV2> {
        let reference = self.load(branch).await?;
        let cursor = PublicationJournalCursorV2 {
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
        cursor: &PublicationJournalCursorV2,
        limit: usize,
    ) -> Result<PublicationJournalPageV2> {
        self.validate_journal_cursor(cursor)?;
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 publication journal page limit must be between 1 and 1,000",
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
                    "v2 publication journal event does not match its cursor link",
                ));
            }
            next = event.previous;
            next_generation = event
                .previous
                .map(|_| RefGeneration(event.generation.0.saturating_sub(1)));
            next_target = event.old_target;
            entries.push(PublicationJournalEntryV2 { id, event });
        }
        let continuation = next.map(|next| PublicationJournalCursorV2 {
            repository: cursor.repository,
            branch: cursor.branch.clone(),
            snapshot_head: cursor.snapshot_head,
            next: Some(next),
            next_generation,
            next_target,
        });
        Ok(PublicationJournalPageV2 {
            entries,
            continuation,
        })
    }

    fn validate_journal_cursor(&self, cursor: &PublicationJournalCursorV2) -> Result<()> {
        crate::repository::validate_branch(&cursor.branch)?;
        let link_presence_matches = cursor.next.is_some()
            && cursor.next_generation.is_some()
            && cursor.next_target.is_some();
        if cursor.repository != self.repository || !link_presence_matches {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 publication journal cursor is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }

    pub async fn load_commit_object(&self, id: CommitIdV2) -> Result<CommitObjectV2> {
        let stored = self
            .plane
            .get(GetRequest {
                path: self.commit_path(id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "v2 parent commit is missing"))?;
        let object = CommitObjectV2::decode_object(&stored.bytes)?;
        if object.commit.id()? != id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 parent commit ID does not match its path",
            ));
        }
        Ok(object)
    }

    pub async fn load_commit(&self, id: CommitIdV2) -> Result<BucketCommitV2> {
        Ok(self.load_commit_object(id).await?.commit)
    }

    async fn cas_ref(
        &self,
        branch: &str,
        expected: Option<StorageToken>,
        value: RefValueV2,
    ) -> Result<LoadedRefV2> {
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
                        return Ok(LoadedRefV2 {
                            value,
                            token: current.metadata.token,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("v2 branch publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation))
            }
            Ok(CompareExchangeOutcome::Applied(metadata)) => Ok(LoadedRefV2 {
                value,
                token: metadata.token,
            }),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {
                Ok(LoadedRefV2 {
                    value,
                    token: current.metadata.token,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "v2 branch ref changed concurrently",
            )),
        }
    }

    fn ref_path(&self, branch: &str) -> Result<ObjectPath> {
        crate::repository::validate_branch(branch)?;
        ObjectPath::new(format!(
            "{}/refs/v2/heads/{}",
            self.prefix,
            hex::encode(branch.as_bytes())
        ))
    }

    fn commit_path(&self, id: CommitIdV2) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/commits/v2/sha256/{}/{}/{}",
            self.prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }

    fn publication_path(&self, id: PublicationEventIdV2) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/publications/v2/sha256/{}/{}/{}",
            self.prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}
