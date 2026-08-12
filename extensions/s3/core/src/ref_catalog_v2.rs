use std::sync::Arc;

use prolly::{AsyncProlly, Config, RuntimeConfig, Tree, TreeFormat};
use serde::{Deserialize, Serialize};

use crate::{
    decode_canonical, encode_canonical, tree_format_digest, CompareExchange,
    CompareExchangeOutcome, Error, ErrorCode, GetRequest, ImmutablePut, MemoryNodeCache,
    MutableControlStore, NativeRefCatalogEntryV2, NodeCache, ObjectPath, ObjectPlane, OperationId,
    RefCatalogEventIdV2, RefCatalogEventV2, RefCatalogShardHeadV2, RefGeneration, RefKindV2,
    RepositoryId, Result, RetryAdvice, StorageToken, TreeRootV1,
    DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

pub const REF_CATALOG_SHARDS_V2: u8 = 16;
const MAX_HEAD_CAS_ATTEMPTS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefCatalogCursorV2 {
    pub repository: RepositoryId,
    pub kind: RefKindV2,
    pub shard: u8,
    pub after: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogRefV2 {
    pub kind: RefKindV2,
    pub name: String,
    pub target: crate::CommitIdV2,
    pub generation: RefGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefCatalogPageV2 {
    pub entries: Vec<CatalogRefV2>,
    pub continuation: Option<RefCatalogCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefCatalogUpdateV2 {
    pub event: RefCatalogEventIdV2,
    pub shard: u8,
    pub generation: u64,
    pub already_indexed: bool,
}

struct LoadedHead {
    value: RefCatalogShardHeadV2,
    token: StorageToken,
}

/// Event-driven, prefix-sharded catalog for native protocol-v2 refs.
///
/// Authoritative refs remain separate mutable objects. Each successful ref
/// transition records one immutable lifecycle event and advances only the
/// derived shard selected by the ref name. A missed update is repaired from
/// authoritative refs; catalog state is never used to authorize mutation.
pub struct ShardedRefCatalogV2<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    prefix: String,
    repository: RepositoryId,
    format: TreeFormat,
    engine: AsyncProlly<crate::ProllyObjectStore<P>>,
}

impl<P: ObjectPlane> ShardedRefCatalogV2<P> {
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
            DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
        )
    }

    pub fn new_with_limits(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        format: TreeFormat,
        node_cache: Arc<dyn NodeCache>,
        control_versions_to_retain: usize,
    ) -> Result<Self> {
        let prefix = prefix.into();
        let digest = tree_format_digest(&format)?;
        let store = crate::ProllyObjectStore::new_cached_direct(
            plane.clone(),
            format!("{prefix}/ref-catalog/v2/tree"),
            repository,
            2,
            digest,
            node_cache,
        );
        let controls =
            MutableControlStore::new(plane.clone(), prefix.clone(), control_versions_to_retain)?;
        let config = Config {
            format: format.clone(),
            runtime: RuntimeConfig::default(),
        };
        Ok(Self {
            plane,
            controls,
            prefix,
            repository,
            format,
            engine: AsyncProlly::new(store, config),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record(
        &self,
        kind: RefKindV2,
        name: &str,
        target: crate::CommitIdV2,
        generation: RefGeneration,
        operation: OperationId,
        tombstone: bool,
        created_at_millis: u64,
    ) -> Result<RefCatalogUpdateV2> {
        crate::repository::validate_branch(name)?;
        if operation.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 ref-catalog update requires a non-nil operation ID",
            ));
        }
        let shard = ref_catalog_shard_v2(kind, name);
        let path = self.head_path(shard)?;
        let key = catalog_key(kind, name);
        let desired = NativeRefCatalogEntryV2 {
            target,
            generation,
            operation,
            tombstone,
            updated_at_millis: created_at_millis,
        };

        for _ in 0..MAX_HEAD_CAS_ATTEMPTS {
            let loaded = self.load_head(shard).await?;
            let tree = self.tree_from_head(loaded.as_ref());
            if let Some(encoded) = self.engine.get(&tree, &key).await? {
                let existing: NativeRefCatalogEntryV2 = decode_canonical(&encoded)?;
                if existing.generation.0 > generation.0 {
                    return Ok(RefCatalogUpdateV2 {
                        event: loaded
                            .as_ref()
                            .expect("an indexed entry requires a shard head")
                            .value
                            .latest_event,
                        shard,
                        generation: loaded.as_ref().expect("head checked").value.generation,
                        already_indexed: true,
                    });
                }
                if existing.generation == generation {
                    if existing == desired {
                        return Ok(RefCatalogUpdateV2 {
                            event: loaded
                                .as_ref()
                                .expect("an indexed entry requires a shard head")
                                .value
                                .latest_event,
                            shard,
                            generation: loaded.as_ref().expect("head checked").value.generation,
                            already_indexed: true,
                        });
                    }
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "v2 ref-catalog generation is already bound to another state",
                    ));
                }
            }

            let event = RefCatalogEventV2 {
                repository: self.repository,
                shard,
                previous: loaded.as_ref().map(|head| head.value.latest_event),
                kind,
                name: name.to_string(),
                target,
                generation,
                operation,
                tombstone,
                created_at_millis,
            };
            event.validate(self.repository, shard)?;
            let event_id = self.store_event(&event).await?;
            let next_tree = self
                .engine
                .put(&tree, key.clone(), encode_canonical(&desired)?)
                .await?;
            let head_generation = loaded.as_ref().map_or(Ok(0), |head| {
                head.value.generation.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "v2 ref-catalog shard generation overflow",
                    )
                })
            })?;
            let next = RefCatalogShardHeadV2 {
                repository: self.repository,
                shard,
                latest_event: event_id,
                root: TreeRootV1::from_tree(&next_tree)?,
                generation: head_generation,
                updated_at_millis: created_at_millis,
            };
            next.validate(self.repository, shard, tree_format_digest(&self.format)?)?;
            let bytes = encode_canonical(&next)?;
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
                Ok(CompareExchangeOutcome::Applied(_)) => {
                    return Ok(RefCatalogUpdateV2 {
                        event: event_id,
                        shard,
                        generation: next.generation,
                        already_indexed: false,
                    });
                }
                Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {
                    return Ok(RefCatalogUpdateV2 {
                        event: event_id,
                        shard,
                        generation: next.generation,
                        already_indexed: false,
                    });
                }
                Ok(CompareExchangeOutcome::Conflict(_)) => continue,
                Err(error) => {
                    if self
                        .plane
                        .load_mutable(&path)
                        .await?
                        .is_some_and(|current| current.bytes == bytes)
                    {
                        return Ok(RefCatalogUpdateV2 {
                            event: event_id,
                            shard,
                            generation: next.generation,
                            already_indexed: false,
                        });
                    }
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("v2 ref-catalog shard publication outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation)
                    .operation(operation.to_string()));
                }
            }
        }
        Err(Error::new(
            ErrorCode::RefConflict,
            "v2 ref-catalog shard remained contended after bounded retries",
        )
        .retry(RetryAdvice::After(std::time::Duration::from_millis(10))))
    }

    pub async fn list(
        &self,
        kind: RefKindV2,
        cursor: Option<RefCatalogCursorV2>,
        limit: usize,
    ) -> Result<RefCatalogPageV2> {
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 ref-catalog page limit must be between 1 and 1,000",
            ));
        }
        let cursor = cursor.unwrap_or(RefCatalogCursorV2 {
            repository: self.repository,
            kind,
            shard: 0,
            after: None,
        });
        self.validate_cursor(&cursor, kind)?;
        let mut entries = Vec::with_capacity(limit);
        let mut shard = cursor.shard;
        let mut after = cursor.after;
        while shard < REF_CATALOG_SHARDS_V2 {
            let Some(head) = self.load_head(shard).await? else {
                shard = shard.saturating_add(1);
                after = None;
                continue;
            };
            let tree = self.tree_from_head(Some(&head));
            let prefix = catalog_prefix(kind);
            let mut stream = self.engine.prefix(&tree, prefix).await?;
            while let Some(entry) = stream.next().await {
                let (key, encoded) = entry?;
                let name = decode_catalog_name(&key, prefix)?;
                if after
                    .as_ref()
                    .is_some_and(|after| name.as_str() <= after.as_str())
                {
                    continue;
                }
                let value: NativeRefCatalogEntryV2 = decode_canonical(&encoded)?;
                if !value.tombstone {
                    entries.push(CatalogRefV2 {
                        kind,
                        name: name.clone(),
                        target: value.target,
                        generation: value.generation,
                    });
                    if entries.len() == limit {
                        return Ok(RefCatalogPageV2 {
                            entries,
                            continuation: Some(RefCatalogCursorV2 {
                                repository: self.repository,
                                kind,
                                shard,
                                after: Some(name),
                            }),
                        });
                    }
                }
            }
            shard = shard.saturating_add(1);
            after = None;
        }
        Ok(RefCatalogPageV2 {
            entries,
            continuation: None,
        })
    }

    pub async fn load_event(&self, id: RefCatalogEventIdV2) -> Result<RefCatalogEventV2> {
        let stored = self
            .plane
            .get(GetRequest {
                path: self.event_path(id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(ErrorCode::MissingClosure, "v2 ref-catalog event is missing")
            })?;
        let event: RefCatalogEventV2 = decode_canonical(&stored.bytes)?;
        let expected_shard = ref_catalog_shard_v2(event.kind, &event.name);
        event.validate(self.repository, expected_shard)?;
        if event.id()? != id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 ref-catalog event does not match its content address",
            ));
        }
        Ok(event)
    }

    async fn load_head(&self, shard: u8) -> Result<Option<LoadedHead>> {
        if shard >= REF_CATALOG_SHARDS_V2 {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 ref-catalog shard is outside the configured range",
            ));
        }
        let Some(stored) = self.plane.load_mutable(&self.head_path(shard)?).await? else {
            return Ok(None);
        };
        let value: RefCatalogShardHeadV2 = decode_canonical(&stored.bytes)?;
        value.validate(self.repository, shard, tree_format_digest(&self.format)?)?;
        Ok(Some(LoadedHead {
            value,
            token: stored.metadata.token,
        }))
    }

    fn tree_from_head(&self, head: Option<&LoadedHead>) -> Tree {
        Tree {
            root: head.and_then(|head| head.value.root.root.clone()),
            config: Config {
                format: self.format.clone(),
                runtime: RuntimeConfig::default(),
            },
        }
    }

    async fn store_event(&self, event: &RefCatalogEventV2) -> Result<RefCatalogEventIdV2> {
        let id = event.id()?;
        let bytes = encode_canonical(event)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: self.event_path(id)?,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        Ok(id)
    }

    fn validate_cursor(&self, cursor: &RefCatalogCursorV2, kind: RefKindV2) -> Result<()> {
        if cursor.repository != self.repository
            || cursor.kind != kind
            || cursor.shard >= REF_CATALOG_SHARDS_V2
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 ref-catalog cursor belongs to another repository, kind, or shard",
            ));
        }
        if let Some(after) = &cursor.after {
            crate::repository::validate_branch(after)?;
        }
        Ok(())
    }

    fn head_path(&self, shard: u8) -> Result<ObjectPath> {
        ObjectPath::new(format!(
            "{}/ref-catalog/v2/shards/{shard:02x}/head.cbor",
            self.prefix
        ))
    }

    fn event_path(&self, id: RefCatalogEventIdV2) -> Result<ObjectPath> {
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/ref-events/v2/sha256/{}/{}/{}",
            self.prefix,
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}

pub fn ref_catalog_shard_v2(kind: RefKindV2, name: &str) -> u8 {
    let kind = match kind {
        RefKindV2::Branch => 0_u8,
        RefKindV2::Tag => 1_u8,
    };
    let mut shard_key = Vec::with_capacity(name.len() + 1);
    shard_key.push(kind);
    shard_key.extend_from_slice(name.as_bytes());
    crate::codec::sha256(&shard_key)[0] % REF_CATALOG_SHARDS_V2
}

fn catalog_prefix(kind: RefKindV2) -> &'static [u8] {
    match kind {
        RefKindV2::Branch => b"b\0",
        RefKindV2::Tag => b"t\0",
    }
}

fn catalog_key(kind: RefKindV2, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.len() + 2);
    key.extend_from_slice(catalog_prefix(kind));
    key.extend_from_slice(name.as_bytes());
    key
}

fn decode_catalog_name(key: &[u8], prefix: &[u8]) -> Result<String> {
    let name = key.strip_prefix(prefix).ok_or_else(|| {
        Error::new(
            ErrorCode::CorruptNode,
            "v2 ref-catalog entry escaped its kind prefix",
        )
    })?;
    String::from_utf8(name.to_vec())
        .map_err(|_| Error::new(ErrorCode::CorruptNode, "v2 ref-catalog name is not UTF-8"))
}
