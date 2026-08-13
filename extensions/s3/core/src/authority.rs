use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    decode_canonical, encode_canonical, CompareExchange, CompareExchangeOutcome, Error, ErrorCode,
    MutableControlStore, ObjectPath, ObjectPlane, OperationId, RepositoryId, Result, StorageToken,
    DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AuthorityScope {
    Branch { name: String },
    System { namespace: String },
}

impl AuthorityScope {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Branch { name } => crate::repository::validate_branch(name),
            Self::System { namespace } => {
                if namespace.is_empty()
                    || namespace.len() > 255
                    || namespace.bytes().any(|byte| byte <= 0x20)
                {
                    return Err(Error::new(
                        ErrorCode::InvalidRequest,
                        "system authority namespace must be 1..=255 visible UTF-8 bytes",
                    ));
                }
                Ok(())
            }
        }
    }

    fn path_component(&self) -> String {
        let (kind, value) = match self {
            Self::Branch { name } => ("branches", name.as_bytes()),
            Self::System { namespace } => ("system", namespace.as_bytes()),
        };
        format!("{kind}/{}", hex::encode(value))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthorityLeaseState {
    Active,
    BarrierPending { previous_generation: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLease {
    pub repository: RepositoryId,
    pub scope: AuthorityScope,
    pub generation: u64,
    pub writer_id: String,
    pub fencing_token: [u8; 32],
    pub state: AuthorityLeaseState,
    pub expires_at_millis: u64,
    pub updated_at_millis: u64,
}

impl AuthorityLease {
    fn validate(&self, repository: RepositoryId, scope: &AuthorityScope) -> Result<()> {
        self.scope.validate()?;
        if self.repository != repository
            || &self.scope != scope
            || self.generation == 0
            || self.writer_id.is_empty()
            || self.fencing_token == [0; 32]
            || self.expires_at_millis <= self.updated_at_millis
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "writer authority lease is malformed",
            ));
        }
        if let AuthorityLeaseState::BarrierPending {
            previous_generation,
        } = self.state
        {
            if previous_generation.checked_add(1) != Some(self.generation) {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "pending authority barrier does not advance exactly one generation",
                ));
            }
        }
        Ok(())
    }

    pub fn stamp(&self) -> AuthorityStamp {
        AuthorityStamp {
            repository: self.repository,
            scope: self.scope.clone(),
            generation: self.generation,
            writer_id: self.writer_id.clone(),
            fencing_token_digest: crate::codec::sha256(&self.fencing_token),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityStamp {
    pub repository: RepositoryId,
    pub scope: AuthorityScope,
    pub generation: u64,
    pub writer_id: String,
    pub fencing_token_digest: [u8; 32],
}

impl AuthorityStamp {
    pub fn validate(&self, repository: RepositoryId, scope: &AuthorityScope) -> Result<()> {
        scope.validate()?;
        if self.repository != repository
            || &self.scope != scope
            || self.generation == 0
            || self.writer_id.is_empty()
            || self.fencing_token_digest == [0; 32]
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "authority stamp is malformed or belongs to another scope",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct AuthorityPermit {
    lease: AuthorityLease,
    token: StorageToken,
}

impl AuthorityPermit {
    pub fn stamp(&self) -> AuthorityStamp {
        self.lease.stamp()
    }

    pub fn expires_at_millis(&self) -> u64 {
        self.lease.expires_at_millis
    }
}

#[derive(Clone, Debug)]
pub struct PendingAuthority(AuthorityPermit);

impl PendingAuthority {
    pub fn stamp(&self) -> AuthorityStamp {
        self.0.stamp()
    }
}

#[derive(Clone, Debug)]
pub struct TakeoverRequest {
    pub scope: AuthorityScope,
    pub expected_writer: String,
    pub expected_generation: u64,
    pub next_writer: String,
    pub handoff_evidence: String,
    pub now_millis: u64,
    pub nonce: OperationId,
}

#[derive(Clone, Debug)]
pub struct BranchRefBarrier {
    stamp: AuthorityStamp,
}

impl BranchRefBarrier {
    #[allow(dead_code)] // Constructed by the repository ref-CAS integration.
    pub(crate) fn new(stamp: AuthorityStamp) -> Self {
        Self { stamp }
    }

    pub fn stamp(&self) -> &AuthorityStamp {
        &self.stamp
    }
}

/// Shard-aware writer authority implementation for repository. A branch is
/// the first assignment adapter: its single ref CAS is the complete takeover
/// barrier. Repository code must publish that barrier before calling
/// `activate_after_barrier`.
pub struct ShardWriterAuthority<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    prefix: String,
    repository: RepositoryId,
    lease_duration: Duration,
}

impl<P: ObjectPlane> ShardWriterAuthority<P> {
    pub fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        lease_duration: Duration,
    ) -> Result<Self> {
        Self::new_with_control_retention(
            plane,
            prefix,
            repository,
            lease_duration,
            DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
        )
    }

    pub fn new_with_control_retention(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        lease_duration: Duration,
        control_versions_to_retain: usize,
    ) -> Result<Self> {
        if lease_duration < Duration::from_secs(10) || lease_duration > Duration::from_secs(86_400)
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "authority lease must be between 10 seconds and 24 hours",
            ));
        }
        let prefix = prefix.into();
        let controls =
            MutableControlStore::new(plane.clone(), prefix.clone(), control_versions_to_retain)?;
        Ok(Self {
            plane,
            controls,
            prefix,
            repository,
            lease_duration,
        })
    }

    pub async fn acquire(
        &self,
        scope: AuthorityScope,
        writer_id: &str,
        now_millis: u64,
        nonce: OperationId,
    ) -> Result<AuthorityPermit> {
        scope.validate()?;
        if writer_id.is_empty() {
            return Err(Error::new(ErrorCode::InvalidRequest, "writer ID is empty"));
        }
        let path = self.path(&scope)?;
        let existing = self.plane.load_mutable(&path).await?;
        let expected = existing
            .as_ref()
            .map(|stored| stored.metadata.token.clone());
        let lease = match existing {
            None => AuthorityLease {
                repository: self.repository,
                scope: scope.clone(),
                generation: 1,
                writer_id: writer_id.to_string(),
                fencing_token: self.fencing_token(&scope, writer_id, nonce, b"acquire")?,
                state: AuthorityLeaseState::Active,
                expires_at_millis: self.expiry(now_millis)?,
                updated_at_millis: now_millis,
            },
            Some(stored) => {
                let mut current: AuthorityLease = decode_canonical(&stored.bytes)?;
                current.validate(self.repository, &scope)?;
                if current.writer_id != writer_id
                    || !matches!(current.state, AuthorityLeaseState::Active)
                {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "authority scope is owned by another writer or awaits a takeover barrier",
                    ));
                }
                if current.expires_at_millis <= now_millis {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "expired authority requires explicit takeover",
                    ));
                }
                current.updated_at_millis = now_millis;
                current.expires_at_millis = self.expiry(now_millis)?;
                current
            }
        };
        self.cas_lease(path, expected, lease).await
    }

    pub async fn validate_active(
        &self,
        permit: &AuthorityPermit,
        now_millis: u64,
    ) -> Result<AuthorityStamp> {
        let stored = self
            .plane
            .load_mutable(&self.path(&permit.lease.scope)?)
            .await?
            .ok_or_else(|| {
                Error::new(ErrorCode::PreconditionFailed, "authority lease is missing")
            })?;
        let current: AuthorityLease = decode_canonical(&stored.bytes)?;
        current.validate(self.repository, &permit.lease.scope)?;
        if current != permit.lease
            || stored.metadata.token != permit.token
            || current.expires_at_millis <= now_millis
            || !matches!(current.state, AuthorityLeaseState::Active)
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "authority permit is stale, expired, or not active",
            ));
        }
        Ok(current.stamp())
    }

    pub async fn renew(&self, permit: AuthorityPermit, now_millis: u64) -> Result<AuthorityPermit> {
        if permit.lease.expires_at_millis <= now_millis
            || !matches!(permit.lease.state, AuthorityLeaseState::Active)
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "only an unexpired active authority permit can renew",
            ));
        }
        let mut renewed = permit.lease;
        renewed.updated_at_millis = now_millis;
        renewed.expires_at_millis = self.expiry(now_millis)?;
        self.cas_lease(self.path(&renewed.scope)?, Some(permit.token), renewed)
            .await
    }

    pub async fn begin_takeover(&self, request: TakeoverRequest) -> Result<PendingAuthority> {
        if request.next_writer.is_empty() || request.handoff_evidence.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "takeover requires a next writer and credential-isolation evidence",
            ));
        }
        let path = self.path(&request.scope)?;
        let stored =
            self.plane.load_mutable(&path).await?.ok_or_else(|| {
                Error::new(ErrorCode::MissingClosure, "authority lease is missing")
            })?;
        let current: AuthorityLease = decode_canonical(&stored.bytes)?;
        current.validate(self.repository, &request.scope)?;
        if current.writer_id == request.next_writer
            && current.generation == request.expected_generation.saturating_add(1)
            && matches!(
                current.state,
                AuthorityLeaseState::BarrierPending { previous_generation }
                    if previous_generation == request.expected_generation
            )
        {
            return Ok(PendingAuthority(AuthorityPermit {
                lease: current,
                token: stored.metadata.token,
            }));
        }
        if current.writer_id != request.expected_writer
            || current.generation != request.expected_generation
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "authority lease does not match takeover expectation",
            ));
        }
        let generation = request.expected_generation.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "authority generation overflow",
            )
        })?;
        let next = AuthorityLease {
            repository: self.repository,
            scope: request.scope.clone(),
            generation,
            writer_id: request.next_writer.clone(),
            fencing_token: self.fencing_token(
                &request.scope,
                &request.next_writer,
                request.nonce,
                request.handoff_evidence.as_bytes(),
            )?,
            state: AuthorityLeaseState::BarrierPending {
                previous_generation: request.expected_generation,
            },
            expires_at_millis: self.expiry(request.now_millis)?,
            updated_at_millis: request.now_millis,
        };
        Ok(PendingAuthority(
            self.cas_lease(path, Some(stored.metadata.token), next)
                .await?,
        ))
    }

    pub async fn activate_after_barrier(
        &self,
        pending: PendingAuthority,
        barrier: BranchRefBarrier,
        now_millis: u64,
    ) -> Result<AuthorityPermit> {
        let AuthorityLeaseState::BarrierPending { .. } = pending.0.lease.state else {
            return Err(Error::new(
                ErrorCode::InternalInvariant,
                "takeover permit is not waiting for a ref barrier",
            ));
        };
        if pending.stamp() != *barrier.stamp() {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch-ref barrier stamp does not match pending authority",
            ));
        }
        let mut active = pending.0.lease;
        active.state = AuthorityLeaseState::Active;
        active.updated_at_millis = now_millis;
        active.expires_at_millis = self.expiry(now_millis)?;
        self.cas_lease(self.path(&active.scope)?, Some(pending.0.token), active)
            .await
    }

    async fn cas_lease(
        &self,
        path: ObjectPath,
        expected: Option<StorageToken>,
        lease: AuthorityLease,
    ) -> Result<AuthorityPermit> {
        lease.validate(self.repository, &lease.scope)?;
        let bytes = encode_canonical(&lease)?;
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
                        return Ok(AuthorityPermit {
                            lease,
                            token: current.metadata.token,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("authority lease CAS outcome is unknown: {error}"),
                ))
            }
            Ok(CompareExchangeOutcome::Applied(metadata)) => Ok(AuthorityPermit {
                lease,
                token: metadata.token,
            }),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {
                Ok(AuthorityPermit {
                    lease,
                    token: current.metadata.token,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::PreconditionFailed,
                "authority lease changed concurrently",
            )),
        }
    }

    fn path(&self, scope: &AuthorityScope) -> Result<ObjectPath> {
        scope.validate()?;
        ObjectPath::new(format!(
            "{}/authority/{}/lease.cbor",
            self.prefix,
            scope.path_component()
        ))
    }

    fn expiry(&self, now_millis: u64) -> Result<u64> {
        now_millis
            .checked_add(u64::try_from(self.lease_duration.as_millis()).map_err(|_| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "authority lease duration overflows u64",
                )
            })?)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "authority lease expiry overflow"))
    }

    fn fencing_token(
        &self,
        scope: &AuthorityScope,
        writer_id: &str,
        nonce: OperationId,
        evidence: &[u8],
    ) -> Result<[u8; 32]> {
        let scope_bytes = encode_canonical(scope)?;
        Ok(crate::codec::sha256(
            &[
                self.repository.as_bytes().as_slice(),
                scope_bytes.as_slice(),
                writer_id.as_bytes(),
                nonce.as_bytes().as_slice(),
                evidence,
            ]
            .concat(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryObjectPlane;

    #[tokio::test]
    async fn only_the_matching_branch_barrier_activates_takeover() {
        let authority = ShardWriterAuthority::new(
            Arc::new(MemoryObjectPlane::new(true)),
            ".prolly",
            RepositoryId::from_hash([0x44; 32]),
            Duration::from_secs(60),
        )
        .unwrap();
        let scope = AuthorityScope::Branch {
            name: "main".to_string(),
        };
        authority
            .acquire(scope.clone(), "writer-a", 1_000, OperationId::new())
            .await
            .unwrap();
        let pending = authority
            .begin_takeover(TakeoverRequest {
                scope,
                expected_writer: "writer-a".to_string(),
                expected_generation: 1,
                next_writer: "writer-b".to_string(),
                handoff_evidence: "credentials revoked".to_string(),
                now_millis: 2_000,
                nonce: OperationId::new(),
            })
            .await
            .unwrap();
        let mut wrong = pending.stamp();
        wrong.generation += 1;
        let error = authority
            .activate_after_barrier(pending.clone(), BranchRefBarrier::new(wrong), 2_001)
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::PreconditionFailed);

        let stamp = pending.stamp();
        let active = authority
            .activate_after_barrier(pending, BranchRefBarrier::new(stamp), 2_002)
            .await
            .unwrap();
        authority.validate_active(&active, 2_003).await.unwrap();
    }
}
