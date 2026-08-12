use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    decode_canonical, encode_canonical, CompareExchange, CompareExchangeOutcome, Error, ErrorCode,
    ObjectPath, ObjectPlane, OperationId, RepositoryId, Result, StorageToken,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AuthorityScopeV2 {
    Branch { name: String },
    System { namespace: String },
}

impl AuthorityScopeV2 {
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
pub enum AuthorityLeaseStateV2 {
    Active,
    BarrierPending { previous_generation: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLeaseV2 {
    pub repository: RepositoryId,
    pub scope: AuthorityScopeV2,
    pub generation: u64,
    pub writer_id: String,
    pub fencing_token: [u8; 32],
    pub state: AuthorityLeaseStateV2,
    pub expires_at_millis: u64,
    pub updated_at_millis: u64,
}

impl AuthorityLeaseV2 {
    fn validate(&self, repository: RepositoryId, scope: &AuthorityScopeV2) -> Result<()> {
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
        if let AuthorityLeaseStateV2::BarrierPending {
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

    pub fn stamp(&self) -> AuthorityStampV2 {
        AuthorityStampV2 {
            scope: self.scope.clone(),
            generation: self.generation,
            writer_id: self.writer_id.clone(),
            fencing_token_digest: crate::codec::sha256(&self.fencing_token),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityStampV2 {
    pub scope: AuthorityScopeV2,
    pub generation: u64,
    pub writer_id: String,
    pub fencing_token_digest: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct AuthorityPermitV2 {
    lease: AuthorityLeaseV2,
    token: StorageToken,
}

impl AuthorityPermitV2 {
    pub fn stamp(&self) -> AuthorityStampV2 {
        self.lease.stamp()
    }

    pub fn expires_at_millis(&self) -> u64 {
        self.lease.expires_at_millis
    }
}

#[derive(Clone, Debug)]
pub struct PendingAuthorityV2(AuthorityPermitV2);

impl PendingAuthorityV2 {
    pub fn stamp(&self) -> AuthorityStampV2 {
        self.0.stamp()
    }
}

#[derive(Clone, Debug)]
pub struct TakeoverRequestV2 {
    pub scope: AuthorityScopeV2,
    pub expected_writer: String,
    pub expected_generation: u64,
    pub next_writer: String,
    pub handoff_evidence: String,
    pub now_millis: u64,
    pub nonce: OperationId,
}

#[derive(Clone, Debug)]
pub struct BranchRefBarrierV2 {
    stamp: AuthorityStampV2,
}

impl BranchRefBarrierV2 {
    #[allow(dead_code)] // Constructed by the protocol v2 repository ref-CAS integration.
    pub(crate) fn new(stamp: AuthorityStampV2) -> Self {
        Self { stamp }
    }

    pub fn stamp(&self) -> &AuthorityStampV2 {
        &self.stamp
    }
}

/// Shard-aware writer authority implementation for protocol v2. A branch is
/// the first assignment adapter: its single ref CAS is the complete takeover
/// barrier. Repository code must publish that barrier before calling
/// `activate_after_barrier`.
pub struct ShardWriterAuthorityV2<P: ObjectPlane> {
    plane: Arc<P>,
    prefix: String,
    repository: RepositoryId,
    lease_duration: Duration,
}

impl<P: ObjectPlane> ShardWriterAuthorityV2<P> {
    pub fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        lease_duration: Duration,
    ) -> Result<Self> {
        if lease_duration < Duration::from_secs(10) || lease_duration > Duration::from_secs(86_400)
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "authority lease must be between 10 seconds and 24 hours",
            ));
        }
        Ok(Self {
            plane,
            prefix: prefix.into(),
            repository,
            lease_duration,
        })
    }

    pub async fn acquire(
        &self,
        scope: AuthorityScopeV2,
        writer_id: &str,
        now_millis: u64,
        nonce: OperationId,
    ) -> Result<AuthorityPermitV2> {
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
            None => AuthorityLeaseV2 {
                repository: self.repository,
                scope: scope.clone(),
                generation: 1,
                writer_id: writer_id.to_string(),
                fencing_token: self.fencing_token(&scope, writer_id, nonce, b"acquire")?,
                state: AuthorityLeaseStateV2::Active,
                expires_at_millis: self.expiry(now_millis)?,
                updated_at_millis: now_millis,
            },
            Some(stored) => {
                let mut current: AuthorityLeaseV2 = decode_canonical(&stored.bytes)?;
                current.validate(self.repository, &scope)?;
                if current.writer_id != writer_id
                    || !matches!(current.state, AuthorityLeaseStateV2::Active)
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
        permit: &AuthorityPermitV2,
        now_millis: u64,
    ) -> Result<AuthorityStampV2> {
        let stored = self
            .plane
            .load_mutable(&self.path(&permit.lease.scope)?)
            .await?
            .ok_or_else(|| {
                Error::new(ErrorCode::PreconditionFailed, "authority lease is missing")
            })?;
        let current: AuthorityLeaseV2 = decode_canonical(&stored.bytes)?;
        current.validate(self.repository, &permit.lease.scope)?;
        if current != permit.lease
            || stored.metadata.token != permit.token
            || current.expires_at_millis <= now_millis
            || !matches!(current.state, AuthorityLeaseStateV2::Active)
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "authority permit is stale, expired, or not active",
            ));
        }
        Ok(current.stamp())
    }

    pub async fn renew(
        &self,
        permit: AuthorityPermitV2,
        now_millis: u64,
    ) -> Result<AuthorityPermitV2> {
        if permit.lease.expires_at_millis <= now_millis
            || !matches!(permit.lease.state, AuthorityLeaseStateV2::Active)
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

    pub async fn begin_takeover(&self, request: TakeoverRequestV2) -> Result<PendingAuthorityV2> {
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
        let current: AuthorityLeaseV2 = decode_canonical(&stored.bytes)?;
        current.validate(self.repository, &request.scope)?;
        if current.writer_id == request.next_writer
            && current.generation == request.expected_generation.saturating_add(1)
            && matches!(
                current.state,
                AuthorityLeaseStateV2::BarrierPending { previous_generation }
                    if previous_generation == request.expected_generation
            )
        {
            return Ok(PendingAuthorityV2(AuthorityPermitV2 {
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
        let next = AuthorityLeaseV2 {
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
            state: AuthorityLeaseStateV2::BarrierPending {
                previous_generation: request.expected_generation,
            },
            expires_at_millis: self.expiry(request.now_millis)?,
            updated_at_millis: request.now_millis,
        };
        Ok(PendingAuthorityV2(
            self.cas_lease(path, Some(stored.metadata.token), next)
                .await?,
        ))
    }

    pub async fn activate_after_barrier(
        &self,
        pending: PendingAuthorityV2,
        barrier: BranchRefBarrierV2,
        now_millis: u64,
    ) -> Result<AuthorityPermitV2> {
        let AuthorityLeaseStateV2::BarrierPending { .. } = pending.0.lease.state else {
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
        active.state = AuthorityLeaseStateV2::Active;
        active.updated_at_millis = now_millis;
        active.expires_at_millis = self.expiry(now_millis)?;
        self.cas_lease(self.path(&active.scope)?, Some(pending.0.token), active)
            .await
    }

    async fn cas_lease(
        &self,
        path: ObjectPath,
        expected: Option<StorageToken>,
        lease: AuthorityLeaseV2,
    ) -> Result<AuthorityPermitV2> {
        lease.validate(self.repository, &lease.scope)?;
        let bytes = encode_canonical(&lease)?;
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
                        return Ok(AuthorityPermitV2 {
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
            Ok(CompareExchangeOutcome::Applied(metadata)) => Ok(AuthorityPermitV2 {
                lease,
                token: metadata.token,
            }),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {
                Ok(AuthorityPermitV2 {
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

    fn path(&self, scope: &AuthorityScopeV2) -> Result<ObjectPath> {
        scope.validate()?;
        ObjectPath::new(format!(
            "{}/authority/v2/{}/lease.cbor",
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
        scope: &AuthorityScopeV2,
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
        let authority = ShardWriterAuthorityV2::new(
            Arc::new(MemoryObjectPlane::new(true)),
            ".prolly/v2",
            RepositoryId::from_hash([0x44; 32]),
            Duration::from_secs(60),
        )
        .unwrap();
        let scope = AuthorityScopeV2::Branch {
            name: "main".to_string(),
        };
        authority
            .acquire(scope.clone(), "writer-a", 1_000, OperationId::new())
            .await
            .unwrap();
        let pending = authority
            .begin_takeover(TakeoverRequestV2 {
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
            .activate_after_barrier(pending.clone(), BranchRefBarrierV2::new(wrong), 2_001)
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::PreconditionFailed);

        let stamp = pending.stamp();
        let active = authority
            .activate_after_barrier(pending, BranchRefBarrierV2::new(stamp), 2_002)
            .await
            .unwrap();
        authority.validate_active(&active, 2_003).await.unwrap();
    }
}
