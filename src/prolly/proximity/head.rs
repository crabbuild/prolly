use super::{
    AsyncProximityBuildOptions, AsyncProximityMap, ProximityBuildStats, ProximityConfig,
    ProximityMutation, ProximityMutationStats, ProximityRecord, SearchIo, SearchRuntime,
};
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{
    compare_and_swap_named_content_root_with_limits_async,
    compare_and_swap_prevalidated_content_root_async,
    load_named_content_root_with_cached_validation_async, ContentGraphLimits,
    ContentManifestUpdate, ContentObjectKind, ContentRootManifest, ContentRootPublication,
    TypedContentRoot,
};
use crate::prolly::error::Error;
use crate::prolly::manifest::AsyncManifestStore;
use crate::prolly::store::AsyncStore;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// One validated immutable snapshot opened through a managed async head.
pub struct AsyncProximitySnapshot<S>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    publication: ContentRootPublication,
    map: AsyncProximityMap<SearchIo<S>>,
}

impl<S> AsyncProximitySnapshot<S>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    pub fn publication(&self) -> &ContentRootPublication {
        &self.publication
    }

    pub fn map(&self) -> &AsyncProximityMap<SearchIo<S>> {
        &self.map
    }

    pub fn into_map(self) -> AsyncProximityMap<SearchIo<S>> {
        self.map
    }
}

/// Result of a managed build or mutation commit.
pub enum AsyncProximityHeadCommit<S, T>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    Applied {
        snapshot: AsyncProximitySnapshot<S>,
        stats: T,
        attempts: usize,
    },
    Conflict {
        current_manifest_cid: Option<Cid>,
        attempts: usize,
    },
}

/// Durable named-head orchestration for async proximity maps.
///
/// The head owns validation reuse, a long-lived authenticated search runtime,
/// expected-head CAS, and bounded conflict retry. Immutable map bytes remain
/// readable through the underlying AsyncStore without this manager.
pub struct AsyncProximityHead<S: AsyncStore> {
    store: S,
    io: SearchIo<S>,
    name: Vec<u8>,
    limits: ContentGraphLimits,
    max_conflict_retries: usize,
    validated: Mutex<Option<ContentRootPublication>>,
}

impl<S> AsyncProximityHead<S>
where
    S: AsyncStore + AsyncManifestStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    pub fn new(store: S, name: impl Into<Vec<u8>>) -> Self {
        Self::with_runtime(store, name, Arc::new(SearchRuntime::default()))
    }

    pub fn with_runtime(store: S, name: impl Into<Vec<u8>>, runtime: Arc<SearchRuntime>) -> Self {
        Self {
            io: SearchIo::new(store.clone(), runtime),
            store,
            name: name.into(),
            limits: ContentGraphLimits::default(),
            max_conflict_retries: 3,
            validated: Mutex::new(None),
        }
    }

    pub fn with_limits(mut self, limits: ContentGraphLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_max_conflict_retries(mut self, retries: usize) -> Self {
        self.max_conflict_retries = retries;
        self
    }

    pub fn runtime(&self) -> &Arc<SearchRuntime> {
        self.io.runtime()
    }

    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Open the current head, reusing a validation receipt only when the named
    /// manifest CID is unchanged on this manager and store instance.
    pub async fn open(&self) -> Result<Option<AsyncProximitySnapshot<S>>, Error> {
        let cached = self
            .validated
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let Some(publication) = load_named_content_root_with_cached_validation_async(
            &self.store,
            &self.name,
            &self.limits,
            cached.as_ref(),
        )
        .await?
        else {
            return Ok(None);
        };
        if publication.manifest.root.kind != ContentObjectKind::ProximityDescriptor {
            return Err(invalid_head("named root is not a proximity descriptor"));
        }
        let map = AsyncProximityMap::load_with_search_io(
            self.io.clone(),
            publication.manifest.root.cid.clone(),
        )
        .await?;
        self.remember(&publication);
        Ok(Some(AsyncProximitySnapshot { publication, map }))
    }

    /// Build a canonical map and install it only when the named head is absent.
    pub async fn build_if_absent(
        &self,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
        options: AsyncProximityBuildOptions,
        created_at_millis: u64,
        metadata: BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<AsyncProximityHeadCommit<S, ProximityBuildStats>, Error> {
        let (map, stats) =
            AsyncProximityMap::build_with_options(self.io.clone(), config, records, options)
                .await?;
        let manifest = manifest_for(&map, 1, created_at_millis, metadata);
        match compare_and_swap_named_content_root_with_limits_async(
            &self.store,
            &self.name,
            None,
            manifest,
            &self.limits,
        )
        .await?
        {
            ContentManifestUpdate::Applied(publication) => {
                self.remember(&publication);
                Ok(AsyncProximityHeadCommit::Applied {
                    snapshot: AsyncProximitySnapshot { publication, map },
                    stats,
                    attempts: 1,
                })
            }
            ContentManifestUpdate::Conflict {
                current_manifest_cid,
            } => Ok(AsyncProximityHeadCommit::Conflict {
                current_manifest_cid,
                attempts: 1,
            }),
        }
    }

    /// Apply a mutation batch against the current validated head and atomically
    /// publish the resulting descriptor. Conflicts reopen fresh state and retry
    /// up to the configured bound.
    pub async fn mutate_with_retry(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
        created_at_millis: u64,
        metadata: BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<AsyncProximityHeadCommit<S, ProximityMutationStats>, Error> {
        let mutations: Vec<_> = mutations.into_iter().collect();
        let attempts_allowed = self.max_conflict_retries.saturating_add(1);
        let mut last_conflict = None;
        for attempt in 1..=attempts_allowed {
            let Some(current) = self.open().await? else {
                return Err(invalid_head("cannot mutate an absent proximity head"));
            };
            if mutations.is_empty() {
                return Ok(AsyncProximityHeadCommit::Applied {
                    snapshot: current,
                    stats: ProximityMutationStats::default(),
                    attempts: 0,
                });
            }
            let logical_version = current
                .publication
                .manifest
                .logical_version
                .checked_add(1)
                .ok_or_else(|| invalid_head("logical version overflow"))?;
            let expected = current.publication.manifest_cid.clone();
            let (map, stats) = current.map.mutate_batch(mutations.clone()).await?;
            let manifest = manifest_for(&map, logical_version, created_at_millis, metadata.clone());
            match compare_and_swap_prevalidated_content_root_async(
                &self.store,
                &self.name,
                Some(&expected),
                manifest,
            )
            .await?
            {
                ContentManifestUpdate::Applied(publication) => {
                    self.remember(&publication);
                    return Ok(AsyncProximityHeadCommit::Applied {
                        snapshot: AsyncProximitySnapshot { publication, map },
                        stats,
                        attempts: attempt,
                    });
                }
                ContentManifestUpdate::Conflict {
                    current_manifest_cid,
                } => last_conflict = current_manifest_cid,
            }
        }
        Ok(AsyncProximityHeadCommit::Conflict {
            current_manifest_cid: last_conflict,
            attempts: attempts_allowed,
        })
    }

    fn remember(&self, publication: &ContentRootPublication) {
        *self
            .validated
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(publication.clone());
    }
}

fn manifest_for<S>(
    map: &AsyncProximityMap<SearchIo<S>>,
    logical_version: u64,
    created_at_millis: u64,
    metadata: BTreeMap<Vec<u8>, Vec<u8>>,
) -> ContentRootManifest
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    ContentRootManifest {
        root: TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone()),
        logical_version,
        created_at_millis,
        metadata,
    }
}

fn invalid_head(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "managed proximity head",
        reason: reason.into(),
    }
}
