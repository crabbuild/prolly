use std::sync::Arc;

use crate::{
    decode_canonical, encode_canonical, AuthorityPermit, AuthorityScope, CommitId, CompareExchange,
    CompareExchangeOutcome, Error, ErrorCode, MutableControlObserver, MutableControlStore,
    ObjectPath, ObjectPlane, OperationId, RefGeneration, ReflogEntry, RepositoryId, Result,
    RetryAdvice, ShardWriterAuthority, StorageToken, TagValue,
    DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

#[derive(Clone, Debug)]
pub struct LoadedTag {
    pub value: TagValue,
    pub token: StorageToken,
}

pub struct TagStore<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    authority: Arc<ShardWriterAuthority<P>>,
    prefix: String,
    repository: RepositoryId,
}

impl<P: ObjectPlane> TagStore<P> {
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
            authority,
            prefix,
            repository,
        })
    }

    pub async fn load(&self, name: &str) -> Result<LoadedTag> {
        let loaded = self.load_including_tombstone(name).await?;
        if loaded.value.tombstone {
            return Err(Error::new(ErrorCode::InvalidRevision, "tag is deleted"));
        }
        Ok(loaded)
    }

    pub async fn load_including_tombstone(&self, name: &str) -> Result<LoadedTag> {
        let path = self.path(name)?;
        let stored = self
            .plane
            .load_mutable(&path)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "tag is missing"))?;
        let value: TagValue = decode_canonical(&stored.bytes)?;
        value.validate(self.repository, name)?;
        Ok(LoadedTag {
            value,
            token: stored.metadata.token,
        })
    }

    pub async fn create(
        &self,
        permit: &AuthorityPermit,
        name: &str,
        target: CommitId,
        operation: OperationId,
        actor: &str,
        now_millis: u64,
    ) -> Result<LoadedTag> {
        crate::repository::validate_branch(name)?;
        if operation.is_nil() || actor.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "tag creation requires an operation and actor",
            ));
        }
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        stamp.validate(
            self.repository,
            &AuthorityScope::System {
                namespace: "tags".to_string(),
            },
        )?;
        let existing = match self.load_including_tombstone(name).await {
            Ok(existing) if !existing.value.tombstone => {
                return Err(Error::new(ErrorCode::RefConflict, "tag already exists"));
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
                .ok_or_else(|| Error::new(ErrorCode::InternalInvariant, "tag generation overflow"))
        })?;
        let previous_target = existing.as_ref().map(|current| current.value.target);
        let value = TagValue {
            target,
            previous_target,
            generation,
            operation,
            inline_reflog: ReflogEntry {
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
        permit: &AuthorityPermit,
        name: &str,
        current: LoadedTag,
        expected: CommitId,
        operation: OperationId,
        now_millis: u64,
    ) -> Result<LoadedTag> {
        crate::repository::validate_branch(name)?;
        current.value.validate(self.repository, name)?;
        let stamp = self.authority.validate_active(permit, now_millis).await?;
        stamp.validate(
            self.repository,
            &AuthorityScope::System {
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
                "tag deletion does not match the live tag and authority",
            ));
        }
        let generation =
            RefGeneration(current.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "tag generation overflow")
            })?);
        let value = TagValue {
            target: expected,
            previous_target: Some(expected),
            generation,
            operation,
            inline_reflog: ReflogEntry {
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
        value: TagValue,
    ) -> Result<LoadedTag> {
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
            Ok(CompareExchangeOutcome::Applied(metadata)) => Ok(LoadedTag {
                value,
                token: metadata.token,
            }),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {
                Ok(LoadedTag {
                    value,
                    token: current.metadata.token,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "tag changed concurrently",
            )),
            Err(error) => {
                if let Some(current) = self.plane.load_mutable(&path).await? {
                    if current.bytes == bytes {
                        return Ok(LoadedTag {
                            value,
                            token: current.metadata.token,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("tag publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(value.operation.to_string()))
            }
        }
    }

    fn path(&self, name: &str) -> Result<ObjectPath> {
        crate::repository::validate_branch(name)?;
        ObjectPath::new(format!(
            "{}/refs/tags/{}",
            self.prefix,
            hex::encode(name.as_bytes())
        ))
    }
}
