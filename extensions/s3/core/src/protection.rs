use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    decode_canonical, encode_canonical, Clock, CommitId, CompareExchange, CompareExchangeOutcome,
    Error, ErrorCode, GetRequest, ImmutablePut, ObjectPath, ObjectPlane, OperationId,
    ProtectionSegmentV1, PublicationLeaseStateV1, PublicationLeaseV1, Result, StorageToken,
    SystemClock,
};

#[async_trait::async_trait]
pub trait ProtectionSink: Send + Sync {
    async fn protect(&self, path: ObjectPath) -> Result<()>;
}

struct LeaseRuntime {
    value: PublicationLeaseV1,
    token: StorageToken,
}

pub struct PublicationLease<P: ObjectPlane> {
    plane: Arc<P>,
    prefix: String,
    ttl_millis: u64,
    clock: Arc<dyn Clock>,
    runtime: Arc<Mutex<LeaseRuntime>>,
}

impl<P: ObjectPlane> Clone for PublicationLease<P> {
    fn clone(&self) -> Self {
        Self {
            plane: self.plane.clone(),
            prefix: self.prefix.clone(),
            ttl_millis: self.ttl_millis,
            clock: self.clock.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl<P: ObjectPlane> PublicationLease<P> {
    pub async fn create_or_resume(
        plane: Arc<P>,
        prefix: impl Into<String>,
        operation: OperationId,
        writer: impl Into<String>,
        ttl_millis: u64,
    ) -> Result<Self> {
        Self::create_or_resume_with_clock(
            plane,
            prefix,
            operation,
            writer,
            ttl_millis,
            Arc::new(SystemClock),
        )
        .await
    }

    pub async fn create_or_resume_with_clock(
        plane: Arc<P>,
        prefix: impl Into<String>,
        operation: OperationId,
        writer: impl Into<String>,
        ttl_millis: u64,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        if !(5 * 60 * 1_000..=24 * 60 * 60 * 1_000).contains(&ttl_millis) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "publication lease must be between 5 minutes and 24 hours",
            ));
        }
        let prefix = prefix.into();
        let now = clock.now_millis()?;
        let proposed = PublicationLeaseV1 {
            operation,
            writer: writer.into(),
            generation: 0,
            expires_at_millis: now
                .checked_add(ttl_millis)
                .ok_or_else(|| invariant("publication lease expiry overflow"))?,
            protection_head: None,
            proposal: None,
            state: PublicationLeaseStateV1::Active,
            created_at_millis: now,
            updated_at_millis: now,
        };
        let path = lease_path(&prefix, operation)?;
        let (value, token) = match plane
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected: None,
                bytes: encode_canonical(&proposed)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(metadata) => (proposed, metadata.token),
            CompareExchangeOutcome::Conflict(Some(existing)) => {
                let value: PublicationLeaseV1 = decode_canonical(&existing.bytes)?;
                if value.operation != operation {
                    return Err(invariant(
                        "publication lease path contains another operation",
                    ));
                }
                if matches!(value.state, PublicationLeaseStateV1::Abandoned) {
                    return Err(Error::new(
                        ErrorCode::OperationCanceled,
                        "publication lease was abandoned",
                    ));
                }
                (value, existing.metadata.token)
            }
            CompareExchangeOutcome::Conflict(None) => {
                return Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    "publication lease create conflicted without a readable winner",
                ));
            }
        };
        Ok(Self {
            plane,
            prefix,
            ttl_millis,
            clock,
            runtime: Arc::new(Mutex::new(LeaseRuntime { value, token })),
        })
    }

    pub async fn ensure_active(&self) -> Result<()> {
        let runtime = self.runtime.lock().await;
        match runtime.value.state {
            PublicationLeaseStateV1::Active
                if runtime.value.expires_at_millis > self.clock.now_millis()? =>
            {
                Ok(())
            }
            PublicationLeaseStateV1::Active => Err(Error::new(
                ErrorCode::OperationCanceled,
                "publication lease expired before publication",
            )),
            PublicationLeaseStateV1::Completed { .. } => Ok(()),
            PublicationLeaseStateV1::Abandoned => Err(Error::new(
                ErrorCode::OperationCanceled,
                "publication lease was abandoned",
            )),
        }
    }

    pub async fn set_proposal(&self, commit: CommitId) -> Result<()> {
        self.update(|value, now| {
            value.proposal = Some(commit);
            value.expires_at_millis = now
                .checked_add(self.ttl_millis)
                .ok_or_else(|| invariant("publication lease expiry overflow"))?;
            Ok(())
        })
        .await
    }

    pub async fn complete(&self, commit: CommitId) -> Result<()> {
        self.update(|value, _| {
            match value.state {
                PublicationLeaseStateV1::Completed { commit: existing } if existing == commit => {
                    return Ok(())
                }
                PublicationLeaseStateV1::Completed { .. } => {
                    return Err(invariant("publication lease completed with another commit"));
                }
                PublicationLeaseStateV1::Abandoned => {
                    return Err(Error::new(
                        ErrorCode::OperationCanceled,
                        "cannot complete an abandoned publication lease",
                    ));
                }
                PublicationLeaseStateV1::Active => {}
            }
            value.proposal = Some(commit);
            value.state = PublicationLeaseStateV1::Completed { commit };
            Ok(())
        })
        .await
    }

    pub async fn abandon(&self) -> Result<()> {
        self.update(|value, _| {
            if matches!(value.state, PublicationLeaseStateV1::Active) {
                value.state = PublicationLeaseStateV1::Abandoned;
            }
            Ok(())
        })
        .await
    }

    pub async fn snapshot(&self) -> PublicationLeaseV1 {
        self.runtime.lock().await.value.clone()
    }

    async fn update(
        &self,
        mutate: impl FnOnce(&mut PublicationLeaseV1, u64) -> Result<()>,
    ) -> Result<()> {
        let mut runtime = self.runtime.lock().await;
        let now = self.clock.now_millis()?;
        let mut next = runtime.value.clone();
        mutate(&mut next, now)?;
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or_else(|| invariant("publication lease generation overflow"))?;
        next.updated_at_millis = now;
        match self
            .plane
            .compare_exchange(CompareExchange {
                path: lease_path(&self.prefix, next.operation)?,
                expected: Some(runtime.token.clone()),
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(metadata) => {
                runtime.value = next;
                runtime.token = metadata.token;
                Ok(())
            }
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "publication lease changed concurrently",
            )),
        }
    }
}

#[async_trait::async_trait]
impl<P: ObjectPlane> ProtectionSink for PublicationLease<P> {
    async fn protect(&self, path: ObjectPath) -> Result<()> {
        let mut runtime = self.runtime.lock().await;
        let now = self.clock.now_millis()?;
        match runtime.value.state {
            // An idempotent replay may restage identical immutable objects
            // before its operation-tree record proves the prior result. The
            // committed proposal is already a retained root, so no new lease
            // links are necessary.
            PublicationLeaseStateV1::Completed { .. } => return Ok(()),
            PublicationLeaseStateV1::Active if runtime.value.expires_at_millis > now => {}
            PublicationLeaseStateV1::Active | PublicationLeaseStateV1::Abandoned => {
                return Err(Error::new(
                    ErrorCode::OperationCanceled,
                    "publication lease is not active while protecting an object",
                ));
            }
        }
        let operation = runtime.value.operation;
        let previous = runtime.value.protection_head;
        let segment = ProtectionSegmentV1 {
            operation,
            previous,
            paths: vec![path],
            created_at_millis: now,
        };
        let id = segment.id()?;
        let bytes = encode_canonical(&segment)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: segment_path(&self.prefix, id)?,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        let mut next = runtime.value.clone();
        next.protection_head = Some(id);
        next.expires_at_millis = now
            .checked_add(self.ttl_millis)
            .ok_or_else(|| invariant("publication lease expiry overflow"))?;
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or_else(|| invariant("publication lease generation overflow"))?;
        next.updated_at_millis = now;
        match self
            .plane
            .compare_exchange(CompareExchange {
                path: lease_path(&self.prefix, operation)?,
                expected: Some(runtime.token.clone()),
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(metadata) => {
                runtime.value = next;
                runtime.token = metadata.token;
                Ok(())
            }
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "publication lease changed concurrently",
            )),
        }
    }
}

pub async fn load_publication_lease<P: ObjectPlane>(
    plane: &P,
    prefix: &str,
    operation: OperationId,
) -> Result<Option<PublicationLeaseV1>> {
    let Some(object) = plane
        .get(GetRequest {
            path: lease_path(prefix, operation)?,
            range: None,
            physical_version: None,
        })
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(decode_canonical(&object.bytes)?))
}

pub async fn load_protection_segment<P: ObjectPlane>(
    plane: &P,
    prefix: &str,
    id: crate::ProtectionSegmentId,
) -> Result<Option<ProtectionSegmentV1>> {
    let Some(object) = plane
        .get(GetRequest {
            path: segment_path(prefix, id)?,
            range: None,
            physical_version: None,
        })
        .await?
    else {
        return Ok(None);
    };
    let segment: ProtectionSegmentV1 = decode_canonical(&object.bytes)?;
    if segment.id()? != id {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "protection segment ID mismatch",
        ));
    }
    Ok(Some(segment))
}

fn lease_path(prefix: &str, operation: OperationId) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/publications/{operation}/lease"))
}

fn segment_path(prefix: &str, id: crate::ProtectionSegmentId) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/publications/segments/{id}.cbor"))
}

fn invariant(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::InternalInvariant, message)
}
