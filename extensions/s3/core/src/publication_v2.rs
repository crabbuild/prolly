use std::sync::Arc;

use crate::{
    encode_canonical, AuthorityPermitV2, AuthorityScopeV2, BranchRefBarrierV2, BucketCommitV2,
    CommitIdV2, CommitObjectV2, CompareExchange, CompareExchangeOutcome, Error, ErrorCode,
    GetRequest, ImmutablePut, NodePackV1, ObjectPath, ObjectPlane, OperationId, PendingAuthorityV2,
    RefGeneration, RefValueV2, ReflogEntryV2, RepositoryId, Result, RetryAdvice,
    ShardWriterAuthorityV2, StorageToken,
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
    ) -> Self {
        Self {
            plane,
            prefix: prefix.into(),
            repository,
            authority,
        }
    }

    pub async fn load(&self, branch: &str) -> Result<LoadedRefV2> {
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
        let value = RefValueV2 {
            target,
            previous_target: None,
            generation: RefGeneration(0),
            operation: request.operation,
            reflog: reflog.id()?,
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
        let value = RefValueV2 {
            target,
            previous_target: Some(current.value.target),
            generation: RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(
                || Error::new(ErrorCode::InternalInvariant, "v2 ref generation overflow"),
            )?),
            operation: request.operation,
            reflog: reflog.id()?,
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
        let value = RefValueV2 {
            target: current.value.target,
            previous_target: Some(current.value.target),
            generation: RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(
                || Error::new(ErrorCode::InternalInvariant, "v2 ref generation overflow"),
            )?),
            operation,
            reflog: reflog.id()?,
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

    async fn load_commit(&self, id: CommitIdV2) -> Result<BucketCommitV2> {
        let stored = self
            .plane
            .get(GetRequest {
                path: self.commit_path(id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "v2 parent commit is missing"))?;
        let commit = CommitObjectV2::decode_object(&stored.bytes)?.commit;
        if commit.id()? != id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 parent commit ID does not match its path",
            ));
        }
        Ok(commit)
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
            .plane
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
}
