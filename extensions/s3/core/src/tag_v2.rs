use std::sync::Arc;

use crate::{
    decode_canonical, encode_canonical, AuthorityPermitV2, AuthorityScopeV2, CommitIdV2,
    CompareExchange, CompareExchangeOutcome, Error, ErrorCode, MutableControlStore, ObjectPath,
    ObjectPlane, OperationId, RefGeneration, ReflogEntryV2, RepositoryId, Result, RetryAdvice,
    ShardWriterAuthorityV2, StorageToken, TagValueV2, DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

#[derive(Clone, Debug)]
pub struct LoadedTagV2 {
    pub value: TagValueV2,
    pub token: StorageToken,
}

pub struct TagStoreV2<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    authority: Arc<ShardWriterAuthorityV2<P>>,
    prefix: String,
    repository: RepositoryId,
}

impl<P: ObjectPlane> TagStoreV2<P> {
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
            authority,
            prefix,
            repository,
        })
    }

    pub async fn load(&self, name: &str) -> Result<LoadedTagV2> {
        let loaded = self.load_including_tombstone(name).await?;
        if loaded.value.tombstone {
            return Err(Error::new(ErrorCode::InvalidRevision, "v2 tag is deleted"));
        }
        Ok(loaded)
    }

    pub async fn load_including_tombstone(&self, name: &str) -> Result<LoadedTagV2> {
        let path = self.path(name)?;
        let stored = self
            .plane
            .load_mutable(&path)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "v2 tag is missing"))?;
        let value: TagValueV2 = decode_canonical(&stored.bytes)?;
        value.validate(self.repository, name)?;
        Ok(LoadedTagV2 {
            value,
            token: stored.metadata.token,
        })
    }

    pub async fn create(
        &self,
        permit: &AuthorityPermitV2,
        name: &str,
        target: CommitIdV2,
        operation: OperationId,
        actor: &str,
        now_millis: u64,
    ) -> Result<LoadedTagV2> {
        crate::repository::validate_branch(name)?;
        if operation.is_nil() || actor.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 tag creation requires an operation and actor",
            ));
        }
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        stamp.validate(
            self.repository,
            &AuthorityScopeV2::System {
                namespace: "tags".to_string(),
            },
        )?;
        let existing = match self.load_including_tombstone(name).await {
            Ok(existing) if !existing.value.tombstone => {
                return Err(Error::new(ErrorCode::RefConflict, "v2 tag already exists"));
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
                    Error::new(ErrorCode::InternalInvariant, "v2 tag generation overflow")
                })
        })?;
        let previous_target = existing.as_ref().map(|current| current.value.target);
        let value = TagValueV2 {
            target,
            previous_target,
            generation,
            operation,
            inline_reflog: ReflogEntryV2 {
                branch: name.to_string(),
                old_target: previous_target,
                new_target: target,
                operation,
                actor: actor.to_string(),
                message: "create tag".to_string(),
                created_at_millis: now_millis,
            },
            authority: stamp,
            updated_at_millis: now_millis,
            tombstone: false,
        };
        self.cas(name, existing.map(|current| current.token), value)
            .await
    }

    pub async fn delete(
        &self,
        permit: &AuthorityPermitV2,
        name: &str,
        current: LoadedTagV2,
        expected: CommitIdV2,
        operation: OperationId,
        now_millis: u64,
    ) -> Result<LoadedTagV2> {
        crate::repository::validate_branch(name)?;
        current.value.validate(self.repository, name)?;
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        stamp.validate(
            self.repository,
            &AuthorityScopeV2::System {
                namespace: "tags".to_string(),
            },
        )?;
        if current.value.tombstone
            || current.value.target != expected
            || current.value.authority != stamp
            || operation.is_nil()
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "v2 tag deletion does not match the live tag and authority",
            ));
        }
        let generation =
            RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "v2 tag generation overflow")
            })?);
        let value = TagValueV2 {
            target: expected,
            previous_target: Some(expected),
            generation,
            operation,
            inline_reflog: ReflogEntryV2 {
                branch: name.to_string(),
                old_target: Some(expected),
                new_target: expected,
                operation,
                actor: stamp.writer_id.clone(),
                message: "delete tag".to_string(),
                created_at_millis: now_millis,
            },
            authority: stamp,
            updated_at_millis: now_millis,
            tombstone: true,
        };
        self.cas(name, Some(current.token), value).await
    }

    async fn cas(
        &self,
        name: &str,
        expected: Option<StorageToken>,
        value: TagValueV2,
    ) -> Result<LoadedTagV2> {
        value.validate(self.repository, name)?;
        let path = self.path(name)?;
        let bytes = encode_canonical(&value)?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected,
                bytes: bytes.clone(),
            })
            .await
        {
            Ok(CompareExchangeOutcome::Applied(metadata)) => Ok(LoadedTagV2 {
                value,
                token: metadata.token,
            }),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {
                Ok(LoadedTagV2 {
                    value,
                    token: current.metadata.token,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "v2 tag changed concurrently",
            )),
            Err(error) => {
                if let Some(current) = self.plane.load_mutable(&path).await? {
                    if current.bytes == bytes {
                        return Ok(LoadedTagV2 {
                            value,
                            token: current.metadata.token,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("v2 tag publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(value.operation.to_string()))
            }
        }
    }

    fn path(&self, name: &str) -> Result<ObjectPath> {
        crate::repository::validate_branch(name)?;
        ObjectPath::new(format!(
            "{}/refs/v2/tags/{}",
            self.prefix,
            hex::encode(name.as_bytes())
        ))
    }
}
