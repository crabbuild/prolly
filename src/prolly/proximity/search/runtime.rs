use crate::prolly::cid::Cid;
use crate::prolly::content_graph::ContentObjectKind;
use crate::prolly::error::Error;
use crate::prolly::node::Node;
use crate::prolly::proximity::accelerator::hnsw::storage::Manifest as HnswManifest;
use crate::prolly::proximity::accelerator::pq::Manifest as PqManifest;
use crate::prolly::proximity::accelerator::{
    catalog::Manifest as CatalogManifest, composite::Manifest as CompositeManifest,
};
use crate::prolly::proximity::storage::quantized::ScalarQuantized;
use crate::prolly::proximity::storage::vector::ExternalVector;
use crate::prolly::proximity::storage::{Descriptor, ProximityNode};
#[cfg(feature = "async-store")]
use crate::prolly::store::AsyncStore;
use crate::prolly::store::{BatchOp, Store};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
#[cfg(feature = "async-store")]
use std::task::{Poll, Waker};

static NEXT_STORE_NAMESPACE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StoreCacheNamespace(u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchRuntimePolicy {
    pub max_entries: usize,
    pub max_bytes: usize,
    pub authoritative_max_bytes: usize,
    pub hnsw_max_bytes: usize,
    pub pq_max_bytes: usize,
}

impl Default for SearchRuntimePolicy {
    fn default() -> Self {
        Self {
            max_entries: 16_384,
            max_bytes: 256 * 1024 * 1024,
            authoritative_max_bytes: 128 * 1024 * 1024,
            hnsw_max_bytes: 96 * 1024 * 1024,
            pq_max_bytes: 32 * 1024 * 1024,
        }
    }
}

impl SearchRuntimePolicy {
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_entries == 0 || self.max_bytes == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "search runtime entry and byte limits must be positive".to_owned(),
            });
        }
        let partitions = self
            .authoritative_max_bytes
            .saturating_add(self.hnsw_max_bytes)
            .saturating_add(self.pq_max_bytes);
        if partitions < self.max_bytes {
            return Err(Error::InvalidProximityConfig {
                reason: "search runtime partition byte limits must cover the total limit"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SearchIo<S> {
    store: S,
    namespace: StoreCacheNamespace,
    runtime: Arc<SearchRuntime>,
    kind: ContentObjectKind,
    physical_bytes_read: Arc<AtomicUsize>,
    physical_reads: Arc<AtomicUsize>,
    dimensions: Option<u32>,
}

impl<S> SearchIo<S> {
    pub fn new(store: S, runtime: Arc<SearchRuntime>) -> Self {
        Self {
            store,
            namespace: StoreCacheNamespace(NEXT_STORE_NAMESPACE.fetch_add(1, Ordering::Relaxed)),
            runtime,
            kind: ContentObjectKind::OrderedNode,
            physical_bytes_read: Arc::new(AtomicUsize::new(0)),
            physical_reads: Arc::new(AtomicUsize::new(0)),
            dimensions: None,
        }
    }

    pub fn namespace(&self) -> StoreCacheNamespace {
        self.namespace
    }

    pub fn runtime(&self) -> &Arc<SearchRuntime> {
        &self.runtime
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn physical_bytes_read(&self) -> usize {
        self.physical_bytes_read.load(Ordering::Relaxed)
    }

    pub fn physical_reads(&self) -> usize {
        self.physical_reads.load(Ordering::Relaxed)
    }

    /// Bind proximity decoder context so authoritative PRXN objects can be
    /// validated before shared cache admission, including through AsyncStore.
    pub fn with_proximity_dimensions(mut self, dimensions: u32) -> Self {
        self.kind = ContentObjectKind::ProximityNode;
        self.dimensions = Some(dimensions);
        self
    }

    pub(crate) fn for_kind(&self, kind: ContentObjectKind) -> Self
    where
        S: Clone,
    {
        let mut binding = self.clone();
        binding.kind = kind;
        binding
    }

    pub(crate) fn for_kind_with_dimensions(&self, kind: ContentObjectKind, dimensions: u32) -> Self
    where
        S: Clone,
    {
        let mut binding = self.for_kind(kind);
        binding.dimensions = Some(dimensions);
        binding
    }
}

impl<S: Store + Clone> Store for SearchIo<S> {
    type Error = S::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let Ok(cid) = <[u8; 32]>::try_from(key).map(Cid) else {
            return self.store.get(key);
        };
        match self.runtime.load(self, self.kind, &cid, 2, |bytes| {
            validate_cached_object(bytes, self.dimensions)
        }) {
            Ok(loaded) => Ok(Some(loaded.bytes.as_ref().to_vec())),
            Err(Error::NotFound(_)) => Ok(None),
            Err(Error::Store(error)) => match error.downcast::<S::Error>() {
                Ok(error) => Err(*error),
                Err(_) => self.store.get(key),
            },
            Err(_) => self.store.get(key),
        }
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        let Ok(cid) = <[u8; 32]>::try_from(key).map(Cid) else {
            return self.store.get_shared(key);
        };
        match self.runtime.load(self, self.kind, &cid, 2, |bytes| {
            validate_cached_object(bytes, self.dimensions)
        }) {
            Ok(loaded) => Ok(Some(loaded.bytes)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(Error::Store(error)) => match error.downcast::<S::Error>() {
                Ok(error) => Err(*error),
                Err(_) => self.store.get_shared(key),
            },
            Err(_) => self.store.get_shared(key),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.store.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.store.delete(key)
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        self.store.batch(ops)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let cids = keys
            .iter()
            .map(|key| <[u8; 32]>::try_from(*key).map(Cid))
            .collect::<Result<Vec<_>, _>>();
        let Ok(cids) = cids else {
            return self.store.batch_get_ordered(keys);
        };
        match self.runtime.load_batch(self, &cids, self.kind, 2) {
            Ok(values) => Ok(values
                .into_iter()
                .map(|value| value.map(|bytes| bytes.as_ref().to_vec()))
                .collect()),
            Err(Error::Store(error)) => match error.downcast::<S::Error>() {
                Ok(error) => Err(*error),
                Err(_) => self.store.batch_get_ordered(keys),
            },
            Err(_) => self.store.batch_get_ordered(keys),
        }
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.batch_get_ordered(keys)
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        let cids = keys
            .iter()
            .map(|key| <[u8; 32]>::try_from(*key).map(Cid))
            .collect::<Result<Vec<_>, _>>();
        let Ok(cids) = cids else {
            return self.store.batch_get_shared_ordered_unique(keys);
        };
        match self.runtime.load_batch(self, &cids, self.kind, 2) {
            Ok(values) => Ok(values),
            Err(Error::Store(error)) => match error.downcast::<S::Error>() {
                Ok(error) => Err(*error),
                Err(_) => self.store.batch_get_shared_ordered_unique(keys),
            },
            Err(_) => self.store.batch_get_shared_ordered_unique(keys),
        }
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    fn prefers_batch_reads(&self) -> bool {
        self.store.prefers_batch_reads()
    }

    fn supports_hints(&self) -> bool {
        self.store.supports_hints()
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.store.get_hint(namespace, key)
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.store.put_hint(namespace, key, value)
    }
}

#[cfg(feature = "async-store")]
impl<S: AsyncStore + Clone> AsyncStore for SearchIo<S>
where
    S::Error: Send + Sync,
{
    type Error = S::Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let Ok(cid) = <[u8; 32]>::try_from(key).map(Cid) else {
            return self.store.get(key).await;
        };
        match self
            .runtime
            .load_async(self, self.kind, &cid, 2, |bytes| {
                validate_cached_object(bytes, self.dimensions)
            })
            .await
        {
            Ok(loaded) => Ok(Some(loaded.bytes.as_ref().to_vec())),
            Err(Error::NotFound(_)) => Ok(None),
            Err(Error::Store(error)) => match error.downcast::<S::Error>() {
                Ok(error) => Err(*error),
                Err(_) => self.store.get(key).await,
            },
            Err(_) => self.store.get(key).await,
        }
    }

    async fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        let Ok(cid) = <[u8; 32]>::try_from(key).map(Cid) else {
            return self.store.get_shared(key).await;
        };
        match self
            .runtime
            .load_async(self, self.kind, &cid, 2, |bytes| {
                validate_cached_object(bytes, self.dimensions)
            })
            .await
        {
            Ok(loaded) => Ok(Some(loaded.bytes)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(Error::Store(error)) => match error.downcast::<S::Error>() {
                Ok(error) => Err(*error),
                Err(_) => self.store.get_shared(key).await,
            },
            Err(_) => self.store.get_shared(key).await,
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.store.put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.store.delete(key).await
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.store.batch(ops).await
    }

    async fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_shared(key).await?);
        }
        Ok(values)
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    fn prefers_batch_reads(&self) -> bool {
        self.store.prefers_batch_reads()
    }

    fn read_parallelism(&self) -> usize {
        self.store.read_parallelism()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Partition {
    Authoritative,
    Hnsw,
    Pq,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    namespace: StoreCacheNamespace,
    kind: ContentObjectKind,
    cid: Cid,
    decoder_version: u8,
}

struct CacheEntry {
    bytes: Arc<[u8]>,
    partition: Partition,
    generation: u64,
}

#[derive(Default)]
struct RuntimeState {
    entries: HashMap<CacheKey, CacheEntry>,
    access_log: VecDeque<(CacheKey, u64)>,
    in_flight: HashSet<CacheKey>,
    generation: u64,
    bytes: usize,
    authoritative_bytes: usize,
    hnsw_bytes: usize,
    pq_bytes: usize,
    #[cfg(feature = "async-store")]
    async_waiters: HashMap<CacheKey, Vec<Waker>>,
}

pub struct SearchRuntime {
    policy: SearchRuntimePolicy,
    state: Mutex<RuntimeState>,
    wake: Condvar,
}

impl Default for SearchRuntime {
    fn default() -> Self {
        Self::new(SearchRuntimePolicy::default()).expect("default search runtime policy is valid")
    }
}

impl SearchRuntime {
    pub fn new(policy: SearchRuntimePolicy) -> Result<Self, Error> {
        policy.validate()?;
        Ok(Self {
            policy,
            state: Mutex::new(RuntimeState::default()),
            wake: Condvar::new(),
        })
    }

    pub fn clear(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.entries.clear();
        state.access_log.clear();
        state.bytes = 0;
        state.authoritative_bytes = 0;
        state.hnsw_bytes = 0;
        state.pq_bytes = 0;
    }

    pub(crate) fn load<S: Store, F>(
        &self,
        io: &SearchIo<S>,
        kind: ContentObjectKind,
        cid: &Cid,
        decoder_version: u8,
        validate: F,
    ) -> Result<RuntimeLoad, Error>
    where
        F: FnOnce(&[u8]) -> Result<(), Error>,
    {
        let key = CacheKey {
            namespace: io.namespace,
            kind,
            cid: cid.clone(),
            decoder_version,
        };
        loop {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let generation = state.generation.wrapping_add(1);
            state.generation = generation;
            if let Some(entry) = state.entries.get_mut(&key) {
                entry.generation = generation;
                let bytes = entry.bytes.clone();
                state.access_log.push_back((key.clone(), generation));
                compact_access_log(&mut state);
                return Ok(RuntimeLoad { bytes });
            }
            if state.in_flight.insert(key.clone()) {
                break;
            }
            state = self
                .wake
                .wait(state)
                .unwrap_or_else(|poison| poison.into_inner());
            drop(state);
        }

        let loaded = io
            .store
            .get(cid.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))
            .and_then(|bytes| bytes.ok_or_else(|| Error::NotFound(cid.clone())))
            .and_then(|bytes| {
                let actual = Cid::from_bytes(&bytes);
                if actual == *cid {
                    Ok(bytes)
                } else {
                    Err(Error::CidMismatch {
                        expected: cid.clone(),
                        actual,
                    })
                }
            })
            .and_then(|bytes| {
                validate(&bytes)?;
                Ok(bytes)
            });
        if let Ok(bytes) = &loaded {
            io.physical_reads.fetch_add(1, Ordering::Relaxed);
            io.physical_bytes_read
                .fetch_add(bytes.len(), Ordering::Relaxed);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.in_flight.remove(&key);
        let result = loaded.map(|bytes| {
            let bytes = Arc::<[u8]>::from(bytes.into_boxed_slice());
            self.insert(&mut state, key.clone(), bytes.clone());
            RuntimeLoad { bytes }
        });
        self.wake.notify_all();
        #[cfg(feature = "async-store")]
        if let Some(waiters) = state.async_waiters.remove(&key) {
            for waiter in waiters {
                waiter.wake();
            }
        }
        result
    }

    fn load_batch<S: Store>(
        &self,
        io: &SearchIo<S>,
        cids: &[Cid],
        kind: ContentObjectKind,
        decoder_version: u8,
    ) -> Result<Vec<Option<Arc<[u8]>>>, Error> {
        let keys = cids
            .iter()
            .cloned()
            .map(|cid| CacheKey {
                namespace: io.namespace,
                kind,
                cid,
                decoder_version,
            })
            .collect::<Vec<_>>();
        let mut results = vec![None; keys.len()];
        let mut leaders = Vec::<usize>::new();
        let mut waiters = Vec::<usize>::new();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            for (index, key) in keys.iter().enumerate() {
                let generation = state.generation.wrapping_add(1);
                state.generation = generation;
                if let Some(entry) = state.entries.get_mut(key) {
                    entry.generation = generation;
                    results[index] = Some(entry.bytes.clone());
                    state.access_log.push_back((key.clone(), generation));
                    compact_access_log(&mut state);
                } else if state.in_flight.insert(key.clone()) {
                    leaders.push(index);
                } else {
                    waiters.push(index);
                }
            }
        }
        if !leaders.is_empty() {
            let store_keys = leaders
                .iter()
                .map(|index| cids[*index].as_bytes() as &[u8])
                .collect::<Vec<_>>();
            let loaded = match io.store.batch_get_ordered_unique(&store_keys) {
                Ok(loaded) => loaded,
                Err(error) => {
                    let mut state = self
                        .state
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    for index in &leaders {
                        state.in_flight.remove(&keys[*index]);
                    }
                    self.wake.notify_all();
                    return Err(Error::Store(Box::new(error)));
                }
            };
            io.physical_reads.fetch_add(1, Ordering::Relaxed);
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            #[cfg(feature = "async-store")]
            let mut async_wakers = Vec::new();
            for index in &leaders {
                state.in_flight.remove(&keys[*index]);
                #[cfg(feature = "async-store")]
                if let Some(waiters) = state.async_waiters.remove(&keys[*index]) {
                    async_wakers.extend(waiters);
                }
            }
            self.wake.notify_all();
            #[cfg(feature = "async-store")]
            for waiter in async_wakers {
                waiter.wake();
            }
            for (index, bytes) in leaders.iter().copied().zip(loaded) {
                let key = &keys[index];
                if let Some(bytes) = bytes {
                    let actual = Cid::from_bytes(&bytes);
                    if actual != key.cid {
                        self.wake.notify_all();
                        return Err(Error::CidMismatch {
                            expected: key.cid.clone(),
                            actual,
                        });
                    }
                    validate_cached_object(&bytes, io.dimensions)?;
                    io.physical_bytes_read
                        .fetch_add(bytes.len(), Ordering::Relaxed);
                    let bytes = Arc::<[u8]>::from(bytes.into_boxed_slice());
                    self.insert(&mut state, key.clone(), bytes.clone());
                    results[index] = Some(bytes);
                }
            }
        }
        for index in waiters {
            results[index] = match self.load(io, kind, &cids[index], decoder_version, |bytes| {
                validate_cached_object(bytes, io.dimensions)
            }) {
                Ok(loaded) => Some(loaded.bytes),
                Err(Error::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
        }
        Ok(results)
    }

    #[cfg(feature = "async-store")]
    pub(crate) async fn load_async<S: AsyncStore, F>(
        &self,
        io: &SearchIo<S>,
        kind: ContentObjectKind,
        cid: &Cid,
        decoder_version: u8,
        validate: F,
    ) -> Result<RuntimeLoad, Error>
    where
        S::Error: Send + Sync,
        F: FnOnce(&[u8]) -> Result<(), Error>,
    {
        let key = CacheKey {
            namespace: io.namespace,
            kind,
            cid: cid.clone(),
            decoder_version,
        };
        let cached = futures_util::future::poll_fn(|context| {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let generation = state.generation.wrapping_add(1);
            state.generation = generation;
            if let Some(entry) = state.entries.get_mut(&key) {
                entry.generation = generation;
                let bytes = entry.bytes.clone();
                state.access_log.push_back((key.clone(), generation));
                compact_access_log(&mut state);
                return Poll::Ready(Some(RuntimeLoad { bytes }));
            }
            if state.in_flight.insert(key.clone()) {
                return Poll::Ready(None);
            }
            let waiters = state.async_waiters.entry(key.clone()).or_default();
            if !waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                waiters.push(context.waker().clone());
            }
            Poll::Pending
        })
        .await;
        if let Some(cached) = cached {
            return Ok(cached);
        }
        let mut in_flight_guard = AsyncInFlightGuard {
            runtime: self,
            key: key.clone(),
            armed: true,
        };

        let loaded = io
            .store
            .get(cid.as_bytes())
            .await
            .map_err(|error| Error::Store(Box::new(error)))
            .and_then(|bytes| bytes.ok_or_else(|| Error::NotFound(cid.clone())))
            .and_then(|bytes| {
                let actual = Cid::from_bytes(&bytes);
                if actual != *cid {
                    return Err(Error::CidMismatch {
                        expected: cid.clone(),
                        actual,
                    });
                }
                validate(&bytes)?;
                Ok(bytes)
            });
        if let Ok(bytes) = &loaded {
            io.physical_reads.fetch_add(1, Ordering::Relaxed);
            io.physical_bytes_read
                .fetch_add(bytes.len(), Ordering::Relaxed);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.in_flight.remove(&key);
        in_flight_guard.armed = false;
        let result = loaded.map(|bytes| {
            let bytes = Arc::<[u8]>::from(bytes.into_boxed_slice());
            self.insert(&mut state, key.clone(), bytes.clone());
            RuntimeLoad { bytes }
        });
        let waiters = state.async_waiters.remove(&key).unwrap_or_default();
        self.wake.notify_all();
        drop(state);
        for waiter in waiters {
            waiter.wake();
        }
        result
    }

    fn insert(&self, state: &mut RuntimeState, key: CacheKey, bytes: Arc<[u8]>) {
        let partition = partition(key.kind);
        let partition_limit = self.partition_limit(partition);
        if bytes.len() > self.policy.max_bytes || bytes.len() > partition_limit {
            return;
        }
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        state.bytes = state.bytes.saturating_add(bytes.len());
        *partition_bytes_mut(state, partition) =
            partition_bytes(state, partition).saturating_add(bytes.len());
        state.entries.insert(
            key.clone(),
            CacheEntry {
                bytes,
                partition,
                generation,
            },
        );
        state.access_log.push_back((key, generation));
        while state.entries.len() > self.policy.max_entries
            || state.bytes > self.policy.max_bytes
            || partition_bytes(state, partition) > partition_limit
        {
            let Some((candidate, candidate_generation)) = state.access_log.pop_front() else {
                break;
            };
            if state
                .entries
                .get(&candidate)
                .is_some_and(|entry| entry.generation == candidate_generation)
            {
                let removed = state
                    .entries
                    .remove(&candidate)
                    .expect("checked cache entry");
                state.bytes = state.bytes.saturating_sub(removed.bytes.len());
                *partition_bytes_mut(state, removed.partition) =
                    partition_bytes(state, removed.partition).saturating_sub(removed.bytes.len());
            }
        }
        compact_access_log(state);
    }

    fn partition_limit(&self, partition: Partition) -> usize {
        match partition {
            Partition::Authoritative => self.policy.authoritative_max_bytes,
            Partition::Hnsw => self.policy.hnsw_max_bytes,
            Partition::Pq => self.policy.pq_max_bytes,
        }
    }
}

#[cfg(feature = "async-store")]
struct AsyncInFlightGuard<'a> {
    runtime: &'a SearchRuntime,
    key: CacheKey,
    armed: bool,
}

#[cfg(feature = "async-store")]
impl Drop for AsyncInFlightGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .runtime
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.in_flight.remove(&self.key);
        let waiters = state.async_waiters.remove(&self.key).unwrap_or_default();
        self.runtime.wake.notify_all();
        drop(state);
        for waiter in waiters {
            waiter.wake();
        }
    }
}

pub(crate) struct RuntimeLoad {
    pub bytes: Arc<[u8]>,
}

fn partition(kind: ContentObjectKind) -> Partition {
    match kind {
        ContentObjectKind::HnswManifest
        | ContentObjectKind::HnswPage
        | ContentObjectKind::CompositeAccelerator => Partition::Hnsw,
        ContentObjectKind::ProductQuantization => Partition::Pq,
        _ => Partition::Authoritative,
    }
}

fn partition_bytes(state: &RuntimeState, partition: Partition) -> usize {
    match partition {
        Partition::Authoritative => state.authoritative_bytes,
        Partition::Hnsw => state.hnsw_bytes,
        Partition::Pq => state.pq_bytes,
    }
}

fn partition_bytes_mut(state: &mut RuntimeState, partition: Partition) -> &mut usize {
    match partition {
        Partition::Authoritative => &mut state.authoritative_bytes,
        Partition::Hnsw => &mut state.hnsw_bytes,
        Partition::Pq => &mut state.pq_bytes,
    }
}

fn compact_access_log(state: &mut RuntimeState) {
    let maximum = state.entries.len().saturating_mul(8).saturating_add(1_024);
    if state.access_log.len() <= maximum {
        return;
    }
    let mut current = state
        .entries
        .iter()
        .map(|(key, entry)| (key.clone(), entry.generation))
        .collect::<Vec<_>>();
    current.sort_by_key(|(_, generation)| *generation);
    state.access_log = current.into();
}

fn validate_cached_object(bytes: &[u8], dimensions: Option<u32>) -> Result<(), Error> {
    let magic = bytes
        .get(..4)
        .ok_or_else(|| Error::InvalidProximityObject {
            kind: "runtime cache",
            reason: "content object has no codec magic".to_owned(),
        })?;
    match magic {
        b"CRAB" => Node::from_bytes(bytes).map(|_| ()),
        b"PRXI" => Descriptor::decode(bytes).map(|_| ()),
        b"PRXN" => ProximityNode::decode(
            bytes,
            dimensions.ok_or_else(|| Error::InvalidProximityObject {
                kind: "runtime cache",
                reason: "PRXN cache admission requires vector dimensions".to_owned(),
            })?,
        )
        .map(|_| ()),
        b"PRXV" => ExternalVector::decode(bytes).map(|_| ()),
        b"PQS8" => ScalarQuantized::decode(bytes).map(|_| ()),
        b"HNSW" => HnswManifest::decode(bytes).map(|_| ()),
        b"PQPQ" => PqManifest::decode(bytes).map(|_| ()),
        b"PCOM" => CompositeManifest::decode(bytes).map(|_| ()),
        b"PACL" => CatalogManifest::decode(bytes).map(|_| ()),
        _ => Err(Error::InvalidProximityObject {
            kind: "runtime cache",
            reason: "content codec is not cache-admissible".to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(label: &[u8], kind: ContentObjectKind) -> CacheKey {
        CacheKey {
            namespace: StoreCacheNamespace(7),
            kind,
            cid: Cid::from_bytes(label),
            decoder_version: 2,
        }
    }

    #[test]
    fn weighted_cache_evicts_lru_bypasses_oversized_and_keeps_pinned_bytes_alive() {
        let runtime = SearchRuntime::new(SearchRuntimePolicy {
            max_entries: 2,
            max_bytes: 16,
            authoritative_max_bytes: 16,
            hnsw_max_bytes: 16,
            pq_max_bytes: 16,
        })
        .unwrap();
        let first_key = key(b"first", ContentObjectKind::OrderedNode);
        let second_key = key(b"second", ContentObjectKind::OrderedNode);
        let third_key = key(b"third", ContentObjectKind::OrderedNode);
        let oversized_key = key(b"oversized", ContentObjectKind::OrderedNode);
        let first = Arc::<[u8]>::from(vec![1; 8]);
        let pinned = first.clone();
        let mut state = runtime.state.lock().unwrap();
        runtime.insert(&mut state, first_key.clone(), first);
        runtime.insert(&mut state, second_key.clone(), Arc::from(vec![2; 8]));
        runtime.insert(&mut state, third_key.clone(), Arc::from(vec![3; 8]));
        assert!(!state.entries.contains_key(&first_key));
        assert!(state.entries.contains_key(&second_key));
        assert!(state.entries.contains_key(&third_key));
        assert_eq!(state.bytes, 16);
        runtime.insert(&mut state, oversized_key.clone(), Arc::from(vec![4; 17]));
        assert!(!state.entries.contains_key(&oversized_key));
        assert_eq!(state.bytes, 16);
        drop(state);
        assert_eq!(pinned.as_ref(), &[1; 8]);
    }
}
