# Production Engine Foundation Design

Status: draft for written review; phase direction approved on 2026-07-14

## Purpose

Make `prolly-map` safe to embed as authoritative, long-running infrastructure in
a single-host application such as an agent runtime. The first milestone turns
implicit assumptions about storage, durability, resources, recovery, and
observability into explicit contracts that the engine validates before serving
managed maps.

This design does not add a second tree engine. `Prolly<S>` remains the engine
and `VersionedMap` remains the preferred authoritative-map API. The new
foundation adds a builder, deployment profiles, store capabilities, bounded
limits, startup health, structured error guidance, and dependency-neutral
observability around those existing types.

The milestone is the first part of a larger production program. Subsequent
specifications will cover crash-safe packed local storage, mutation-memory
hardening, operational maintenance, and distributed storage. Those phases must
conform to the contracts defined here.

## Current Evidence

The current implementation already provides the hard algorithmic foundation:

- immutable content-addressed trees;
- configurable canonical chunking and node layouts;
- bounded `WriteSession` overlays;
- optimized append and scattered value-update paths;
- structural diff and three-way merge;
- strict transaction overlays and managed `VersionedMap` publication;
- named roots, retention, garbage collection, backup, sync, and proofs;
- decoded-node caches and node-I/O counters;
- memory, file, SQLite, RocksDB, SlateDB, PGlite, and remote store adapters;
- language bindings with a shared verification matrix.

The production gaps are contract and operations gaps:

1. `RuntimeConfig::default()` leaves the decoded-node cache unbounded.
2. `supports_transactions() -> bool` does not distinguish process atomicity,
   crash atomicity, power-loss durability, cross-process compare-and-swap, or
   distributed compare-and-swap.
3. `FileNodeStore` serializes transactions under an in-process mutex, writes
   nodes and roots as separate files, and can expose a partially published
   multi-root transaction after a process or machine crash.
4. Store errors are boxed after losing machine-readable retry and recovery
   information.
5. Bindings reduce many failures to strings or broad error variants.
6. Startup does not prove that configured limits and store guarantees satisfy
   the intended deployment profile.
7. Existing metrics describe node I/O but not operation outcomes, latency,
   retries, conflicts, health, or maintenance.

This design makes those limitations observable. It must not label a backend
production-ready merely because it implements the required Rust traits.

## Scope

This milestone implements all of the following for the synchronous engine:

- `DeploymentProfile` and exact built-in profile requirements;
- `EngineLimits` and enforcement at public allocation/amplification boundaries;
- `StoreCapabilities` with conservative defaults and forwarding wrappers;
- `ProductionStore` as the compile-time managed-store contract;
- `EngineBuilder<S>` producing `Prolly<S>`;
- read-only and read-write bounded startup health;
- stable error codes, categories, retry advice, and recovery actions;
- store-specific error classification without losing the source error;
- a dependency-neutral operation observer;
- extended cumulative metrics;
- binding records and conformance fixtures for the new public contract;
- capability declarations for every checked-in store adapter;
- snapshot-consistent, bounded portable managed-map backup;
- a hard-cutover managed-map catalog generation guard;
- a hard cutover to root manifests whose identity contains persisted format but
  excludes runtime cache and I/O tuning;
- documentation and examples for the embedded durable profile.

## Non-Goals

This milestone does not:

- make the current `FileNodeStore` crash-atomic;
- implement packed files, a transaction journal, or cross-process file locks;
- add background threads or a maintenance scheduler;
- add an object-store backend or distributed ref protocol;
- change canonical node bytes, node CIDs, tree shape, or logical map contents;
- add application keys, values, root names, or payload bytes to telemetry;
- claim that runtime probes can prove behavior under physical power loss;
- keep source compatibility for custom `Store` implementations or bindings.

There are no production users requiring backward compatibility. Public API and
root-manifest changes in this milestone are a deliberate hard cutover. Node
bytes, node CIDs, and root CIDs remain unchanged. Root-manifest bytes and
content-derived map-version IDs intentionally change because runtime tuning is
removed from persisted identity.

## Design Principles

1. **Capabilities are facts, not optimization hints.** A false positive can
   cause data loss, so unknown capabilities are always reported as the weakest
   level.
2. **Profiles fail closed.** The engine refuses to build a managed production
   profile when required guarantees are missing.
3. **Resource use is bounded by default.** Unbounded behavior requires an
   explicit opt-in and is rejected by production profiles.
4. **Health is evidence, not a boolean.** Reports retain every check, bound,
   failure, and skipped probe.
5. **Errors are actionable across languages.** Stable codes and guidance do not
   depend on parsing English messages.
6. **Observability cannot alter correctness.** Observer absence, failure, or
   slowness never changes tree identity or transaction semantics.
7. **Privacy is the default.** Operational events contain counts, sizes, CIDs
   only where explicitly requested, and no logical key/value data.

## Module Boundaries

Create focused modules under `src/prolly/`:

- `engine.rs`: deployment profiles, limits, `EngineBuilder`, validation;
- `capabilities.rs`: store capability enums and `ProductionStore`;
- `health.rs`: bounded startup checks and health report types;
- `guidance.rs`: error metadata, stable codes, retry/recovery policy;
- `observe.rs`: observer interface, operation events, outcome recording.

Existing modules keep their current responsibilities:

- `config.rs` retains persisted `TreeFormat` and runtime cache/I/O tuning;
- `error.rs` retains the top-level error enum and delegates metadata to
  `guidance.rs`;
- `store/mod.rs` retains storage operations and exposes capability methods;
- `mod.rs` retains tree algorithms and owns the observer/metrics fields;
- `versioned_map.rs` remains the authoritative managed-map facade.

## Store Capability Contract

### Capability types

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistenceLevel {
    Volatile,
    ProcessPersistent,
    CrashConsistent,
    PowerLossDurable,
    ReplicatedDurable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionAtomicity {
    None,
    ProcessAtomic,
    CrashAtomic,
    DistributedAtomic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CasScope {
    None,
    Process,
    Host,
    Distributed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupConsistency {
    None,
    RequiresQuiescence,
    ConcurrentSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreCapabilities {
    pub persistence: PersistenceLevel,
    pub transaction_atomicity: TransactionAtomicity,
    pub cas_scope: CasScope,
    pub ordered_batch_reads: bool,
    pub atomic_batch_writes: bool,
    pub root_enumeration: bool,
    pub node_enumeration: bool,
    pub performance_hints: bool,
    pub backup_consistency: BackupConsistency,
}
```

`StoreCapabilities::conservative()` returns `Volatile`, `None`, `None`, and
`BackupConsistency::None`, plus `false` for every boolean. `Default` is the
conservative value.

Capability levels are not inferred from trait implementation. The store
instance reports them because configuration can change the answer. For example,
a file-backed SQLite store using `synchronous=FULL` has different acknowledged
write durability from an in-memory database or a file database using
`synchronous=NORMAL`.

### Store methods

Add these methods to `Store`:

```rust
fn capabilities(&self) -> StoreCapabilities {
    StoreCapabilities::conservative()
}

fn classify_error(&self, error: &Self::Error) -> ErrorMetadata {
    let _ = error;
    ErrorMetadata::unknown_store()
}
```

`Arc`, `Box`, synchronous-to-async, Tokio-blocking, transaction-overlay, and
remote wrappers forward both methods exactly. Existing
`prefers_batch_reads()`, `supports_hints()`, and `supports_transactions()` are
removed. Algorithms consult the matching capability fields.

Add the managed-store marker:

```rust
pub trait ProductionStore:
    Store + ManifestStore + ManifestStoreScan + TransactionalStore + NodeStoreScan
{
}

impl<T> ProductionStore for T where
    T: Store + ManifestStore + ManifestStoreScan + TransactionalStore + NodeStoreScan
{
}
```

The marker proves API availability. Runtime capabilities prove semantics.

### Capability semantics

- `ProcessPersistent`: bytes normally survive closing and reopening the
  process, but acknowledged writes are not promised after abrupt termination.
- `CrashConsistent`: reopening after process/OS failure yields a structurally
  valid old or new state, but the latest acknowledged transaction may be lost.
- `PowerLossDurable`: once success is returned, the backend promises the
  transaction survives power loss under its documented filesystem/storage
  assumptions.
- `ReplicatedDurable`: acknowledged writes satisfy a documented remote
  replication quorum in addition to durability.
- `ProcessAtomic`: readers in the same process cannot observe partial staged
  node/root publication; crash recovery may expose partial publication.
- `CrashAtomic`: after reopening, all root and node writes from one transaction
  are visible or none are visible.
- `DistributedAtomic`: independent hosts share the same atomic transaction
  boundary.
- `Host` CAS coordinates independent processes on one host.
- `Distributed` CAS coordinates independent writers on different hosts.
- `RequiresQuiescence` backup means a documented external write barrier is
  required for a consistent export or backend copy.
- `ConcurrentSnapshot` backup means the adapter exposes a documented and tested
  backend-copy or snapshot operation that remains consistent while independent
  writers continue. It does not describe `VersionedMapBackup`, whose logical
  catalog-consistency contract is defined separately below.

Capabilities are promises documented and tested by each adapter. Health probes
can disprove a promise but cannot prove physical power-loss behavior.

### Initial declarations

The first implementation declares capabilities conservatively:

| Store/configuration | Persistence | Transaction | CAS | Backup | Embedded profile |
| --- | --- | --- | --- | --- | --- |
| `MemStore` | `Volatile` | `ProcessAtomic` | `Process` | `None` | rejected |
| current `FileNodeStore` | `ProcessPersistent` | `ProcessAtomic` | `Process` | `RequiresQuiescence` | rejected |
| SQLite in-memory | `Volatile` | `ProcessAtomic` | `Process` | `None` | rejected |
| SQLite file, WAL + NORMAL | `CrashConsistent` | `CrashAtomic` | `Host` | `RequiresQuiescence` | rejected for acknowledged-write durability |
| SQLite file, WAL + FULL | `PowerLossDurable` | `CrashAtomic` | `Host` | `RequiresQuiescence` | accepted |
| current RocksDB adapter | `CrashConsistent` | `CrashAtomic` | `Process` | `RequiresQuiescence` | rejected for host-wide CAS and acknowledged-write durability |
| current SlateDB adapter | backend/config dependent | `ProcessAtomic` | `Process` | provider dependent | rejected |
| browser/in-process adapters | declared from actual host contract | conservative unless proved | conservative | conservative | rejected unless all requirements pass |
| remote adapters | declared from provider contract | provider contract | provider contract | provider contract | evaluated against service profile |

SQLite configuration gains an explicit synchronous-mode enum. The embedded
durable example uses WAL plus `FULL`; it does not silently change custom SQLite
configurations. The initial implementation does not claim a generic concurrent
backend-copy API that the adapter does not expose. Operators must quiesce writes
before copying the SQLite database files. Adding an online backend snapshot is
a later adapter feature and requires a concrete public operation plus a
concurrent-writer conformance test before the capability can be upgraded.

## Deployment Profiles

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentProfile {
    Memory,
    EmbeddedDurable,
    Service,
    Custom,
}
```

No profile is implicit. `EngineBuilder::new(store)` requires a subsequent
`.profile(...)` call before `build()` or `build_managed()` is available.

### Memory

- accepts volatile persistence and process-scoped atomicity;
- requires bounded cache and operation limits;
- does not require manifest or node enumeration when using `build()`;
- reports actual durability capabilities without implying a durable-readiness
  claim.

### EmbeddedDurable

Requires all of:

- `PowerLossDurable` or `ReplicatedDurable` persistence;
- `CrashAtomic` or `DistributedAtomic` transaction atomicity;
- `Host` or `Distributed` CAS;
- atomic batch writes;
- root and node enumeration;
- `RequiresQuiescence` or `ConcurrentSnapshot` backup support, with the
  application-visible procedure recorded in health;
- a `ProductionStore` passed to `build_managed()`;
- all limits finite;
- successful read-write startup health before reporting `Ready`.

### Service

Requires all embedded guarantees plus:

- `ReplicatedDurable` persistence;
- `DistributedAtomic` transactions;
- `Distributed` CAS;
- ordered batch reads;
- an installed observer;
- finite retry elapsed time and traversal limits.

The synchronous builder can validate a synchronous service store. Async engine
profiles will receive a separate specification so cancellation and async health
do not inherit blocking assumptions.

### Custom

Requires callers to provide an explicit `ProfileRequirements` value. A custom
profile is still bounded unless `EngineLimits::explicitly_unbounded()` is
selected. Health reports record the exact custom requirements.

## Engine Limits

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineLimits {
    pub node_cache_max_nodes: usize,
    pub node_cache_max_bytes: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_root_name_bytes: usize,
    pub max_map_id_bytes: usize,
    pub max_pending_write_bytes: usize,
    pub max_batch_mutations: usize,
    pub max_eager_result_entries: usize,
    pub max_page_entries: usize,
    pub max_traversal_nodes: usize,
    pub max_traversal_bytes: u64,
    pub max_health_roots: usize,
    pub max_health_nodes: usize,
    pub max_health_bytes: u64,
    pub max_health_elapsed: Duration,
    pub ordered_read_parallelism: usize,
    pub max_optimistic_retries: usize,
    pub max_retry_elapsed: Duration,
}
```

Exact embedded defaults:

| Limit | Value |
| --- | ---: |
| decoded cache nodes | 8,192 |
| decoded cache serialized-byte weight | 64 MiB |
| logical key bytes | 1 MiB |
| logical value bytes | 16 MiB |
| named-root bytes | 4,096 |
| managed-map ID bytes | 1,024 |
| pending write session | 64 MiB |
| batch mutations | 100,000 |
| eager result entries | 100,000 |
| page entries | 10,000 |
| traversal nodes | 1,000,000 |
| traversal encoded bytes | 4 GiB |
| startup roots | 100,000 |
| startup nodes | 1,000,000 |
| startup encoded bytes | 4 GiB |
| startup elapsed time | 60 seconds |
| ordered read parallelism | 4 |
| optimistic attempts | 8 |
| total retry elapsed time | 10 seconds |

Memory defaults use 4,096 cache nodes, 32 MiB cache bytes, 16 MiB pending
writes, and otherwise the embedded limits. Service defaults use 32,768 cache
nodes, 256 MiB cache bytes, 128 MiB pending writes, 32 ordered reads, and the
same logical safety ceilings until workload measurements justify changes.

Every numeric limit must be greater than zero. `max_retry_elapsed` can be zero
only when `max_optimistic_retries` is one. `explicitly_unbounded()` uses
`usize::MAX`, `u64::MAX`, and `Duration::MAX`, sets an internal
`unbounded_opt_in` flag, and is rejected by `EmbeddedDurable` and `Service`.

Eager APIs fail before crossing their result bound. Streaming iterators can
return entries until a traversal bound is reached, then yield one structured
resource-exhaustion error and terminate. Transaction publication never occurs
after a bound failure. Keys, values, root names, and managed-map IDs are checked
before cloning or encoding when their length is already known, and immediately
after bounded serialization otherwise. Internal root names are checked against
`max_root_name_bytes` after canonical construction.

`EngineLimits`, not `Config.runtime`, is authoritative for manager cache and
I/O bounds. `RuntimeConfig::default()` remains byte-compatible during this
milestone so old node/root test inputs can be compared deliberately, but the
production builder ignores its cache limits and applies `EngineLimits`.
Runtime-only `ConfigBuilder` methods are removed in the hard cutover; equivalent
methods live on `EngineBuilder` and `EngineLimits`.

### Root-manifest identity correction

The current root-manifest wire payload serializes the complete `Config`, which
accidentally includes `RuntimeConfig`. `MapVersionId::for_tree` hashes that
payload. Two managers using identical persisted `TreeFormat` values but
different cache sizes can therefore produce different logical map-version IDs.
That violates the documented separation between persisted identity and local
tuning.

Root-manifest wire version 2 stores:

```rust
struct RootManifestWire {
    version: u64,
    root: Option<Cid>,
    format: TreeFormat,
    created_at_millis: Option<u64>,
    updated_at_millis: Option<u64>,
}
```

`RootManifest` exposes `format: TreeFormat` instead of `config: Config`.
`RootManifest::to_tree_with_runtime(runtime)` reconstructs a `Tree` using the
persisted format and the active engine runtime. Managed-map code always uses
the active engine runtime. `RootManifest::to_tree()` uses bounded memory-profile
runtime settings for standalone inspection.

Only wire version 2 is accepted after the hard cutover. Version 1 returns the
stable incompatible-format error `format.root_manifest_version`. Node objects
and root CIDs remain readable because their bytes do not include runtime config.
All manifest, snapshot, version-ID, binding, and cross-language fixtures are
regenerated deliberately. Tests prove that changing only `EngineLimits` or
runtime tuning no longer changes root-manifest bytes or `MapVersionId`.

## Portable Managed-Map Backup Consistency

`VersionedMap::backup()` currently reads the head and version catalog in
separate operations. A concurrent publication or prune can therefore produce a
backup whose head and version set never existed together. Repeating an
unprotected enumeration is not a proof because a store scan need not be a
transactional snapshot.

The managed-map hard cutover moves its roots below the internal prefix
`\0prolly/versioned-map/v2/` and adds one catalog-generation root per map. A
map's suffix remains the lowercase hex encoding of its exact byte ID; its roots
end in `/head`, `/versions/<version-cid>`, and `/catalog-generation`. The guard
manifest points to the canonical empty tree and carries a strictly increasing
`updated_at_millis` generation. Every operation that can change the head or
version-root set—including initialize, publish, import, restore, prune, and
multi-map transactions—must condition on the observed generation and write
`max(current_time_millis, previous_generation + 1)` in the same store
transaction as the catalog changes. Creation conditions on absence. Generation
overflow returns `limit.catalog_generation`; a generation is never reused or
rolled back. Direct application root APIs reject every name beginning with
`0x00`; private engine methods accept only a recognized internal prefix and
validate its complete name grammar.

Portable backup is then an optimistic, bounded snapshot operation:

1. Read catalog generation `G1`.
2. Capture one canonical catalog state containing the head manifest and the
   exact sorted `(root_name, manifest_bytes)` set under the map's version
   prefix.
3. Read `G2`; retry unless `G1 == G2`.
4. Reject malformed catalog names, duplicate version IDs, an absent head, or a
   head that is not present in that captured version set before exporting any
   nodes.
5. Export exactly the immutable trees named by the captured state, subject to
   eager-result and traversal limits.
6. Read `G3` and return the verified backup only when `G2 == G3`. If export
   failed and the generation changed, retry; if it did not change, return the
   original structured export failure.

All retries are bounded by both `max_optimistic_retries` and
`max_retry_elapsed`. Because catalog changes and generation changes share one
atomic transaction and generations cannot repeat, equal generations prove that
the captured roots described one logical catalog state. Content-addressed
nodes are immutable, so their bundles remain valid while the generation is
unchanged. A changing target catalog eventually returns
`backup.catalog_changed` with category `Conflict`, retry advice `AfterBackoff`,
and recovery action `ReloadHead`; it never returns a mixed backup. Limit or
stable store failures terminate immediately with their own metadata.

The operation records attempts, conflicts, exported versions, traversed nodes,
and bytes. Tests place deterministic publication and pruning hooks before,
during, and after bundle export to prove retry, success, exhaustion, and the
absence of mixed-state results. Store conformance also proves the generation
condition and catalog writes commit atomically. Backend backup capability
remains relevant to whole-database disaster recovery and does not weaken this
portable logical backup guarantee.

## Engine Builder

```rust
let engine = EngineBuilder::new(store)
    .profile(DeploymentProfile::EmbeddedDurable)
    .config(config)
    .limits(EngineLimits::embedded_default())
    .observer(observer)
    .build_managed()?;
```

Builder behavior:

1. Require an explicit profile.
2. Validate `TreeFormat` without writing storage.
3. Validate every engine limit and cross-limit invariant.
4. Compare instance capabilities with profile requirements.
5. Reject unknown or malformed reserved internal roots discovered by root scan.
6. Construct `Prolly<S>` with bounded caches, limits, observer, and profile.
7. Run read-only health for memory/custom managed profiles.
8. Run read-write health for embedded/service profiles.
9. Return the engine only when the required health mode is `Ready`. A failed
   build returns `Error::StartupHealth` containing the complete report.

`build()` is available for any `Store` and is intended for raw/memory tree use.
`build_managed()` requires `ProductionStore`. Direct `Prolly::new` is removed in
the hard cutover so production callers cannot accidentally bypass validation.
Internal tests use a test-only unchecked constructor when they intentionally
exercise malformed configurations.

The engine exposes `profile()`, `limits()`, and `capabilities()` read-only.

## Startup Health

### API

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthMode {
    ReadOnly,
    ReadWrite,
}

pub fn health(&self, mode: HealthMode) -> EngineHealthReport;
```

The report is returned even when checks fail. Store errors become failed check
records with structured error metadata. Health returns no top-level `Result`,
so callers never lose partial evidence.

```rust
pub struct EngineHealthReport {
    pub status: HealthStatus,
    pub profile: DeploymentProfile,
    pub capabilities: StoreCapabilities,
    pub limits: EngineLimits,
    pub checks: Vec<HealthCheck>,
    pub roots_examined: usize,
    pub nodes_examined: usize,
    pub bytes_examined: u64,
    pub started_at_millis: u64,
    pub elapsed: Duration,
}

pub enum HealthStatus { Ready, Degraded, NotReady }
pub enum HealthCheckStatus { Passed, Warning, Failed, Skipped }
```

Checks execute in the stable table order below. Any failed check whose failure
effect is `NotReady` makes the report `NotReady`. Otherwise, one or more
warnings make it `Degraded`; only passed and skipped checks make it `Ready`.
When several failures exist, `Error::StartupHealth` derives its category and
recovery action from the first highest-severity failed check in this order, but
retains every check in the report.

### Stable checks

| Code | Mode | Failure effect | Evidence |
| --- | --- | --- | --- |
| `engine.format.valid` | both | `NotReady` | validated persisted format |
| `engine.limits.valid` | both | `NotReady` | exact limit violation |
| `store.capabilities.profile` | both | `NotReady` | missing guarantee list |
| `store.roots.enumerable` | both | `NotReady` for managed profiles | root count/error |
| `store.roots.reserved_namespace` | both | `NotReady` for unknown or malformed names | counts by recognized internal prefix |
| `store.roots.manifests_valid` | both | `NotReady` | offending root hash only, not name |
| `store.roots.formats_valid` | both | `NotReady` | incompatible format fingerprint |
| `store.roots.reachable` | both | `NotReady` | missing CID and bounded walk counts |
| `store.nodes.cid_valid` | both | `NotReady` | expected/actual CID |
| `store.gc.enumeration_ready` | both | `NotReady` for embedded/service | enumeration outcome |
| `store.backup.consistency` | both | `NotReady` when required and absent; `Skipped` when the profile does not require it | declared guarantee and, for `RequiresQuiescence`, the required operator procedure |
| `store.transaction.probe` | read-write | `NotReady` | CAS/apply/cleanup outcomes |
| `store.transaction.probe_cleanup` | read-write | `NotReady` | leftover internal roots |
| `engine.health.within_budget` | both | `NotReady` | first exceeded bound |
| `engine.observer.installed` | both | `NotReady` only for service | observer state |

Read-only health never writes or deletes data. It walks roots in deterministic
name order and nodes in deterministic CID order, stopping exactly at configured
bounds. The elapsed bound is cooperative: it is checked before and after every
store call and traversal step, but the synchronous engine cannot interrupt a
store implementation that blocks inside one call.

Read-write health uses the reserved prefix
`00 70 72 6f 6c 6c 79 2f 68 65 61 6c 74 68 2f 76 31 2f`
(`\0prolly/health/v1/`). Public root APIs reject names beginning with `0x00`.
The probe name appends a SHA-256 digest of process ID, timestamp, and a process
counter. It atomically publishes an empty-tree manifest under an absent-root
condition, reads it back, atomically deletes it under the observed condition,
and confirms absence.

Before a new probe, read-write health removes abandoned roots in the reserved
namespace through conditional transactions. Because application APIs reject
the namespace, every syntactically valid health root is engine-owned. It never
removes versioned-map roots or unknown reserved roots. Cleanup failure is
`NotReady`.

Health messages never include logical root names, keys, values, or store
credentials. Debug-only formatting may expose a root-name hash.

## Structured Errors

### Metadata

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorMetadata {
    pub code: String,
    pub category: ErrorCategory,
    pub retry: RetryAdvice,
    pub recovery: RecoveryAction,
}

pub enum ErrorCategory {
    Conflict,
    TransientInfrastructure,
    Corruption,
    Incompatible,
    ResourceExhausted,
    InvalidInput,
    Unsupported,
    Permanent,
    Unknown,
}

pub enum RetryAdvice {
    Never,
    Immediate,
    AfterBackoff,
    AfterReload,
}

pub enum RecoveryAction {
    None,
    ReloadHead,
    IncreaseLimit,
    Reconfigure,
    Upgrade,
    RepairFromReplica,
    RestoreBackup,
    FreeStorage,
    Abort,
}
```

`Error::metadata()` uses an exhaustive match without a wildcard. Adding an
`Error` variant therefore fails compilation until metadata is assigned.

Core mapping rules:

| Error family | Code prefix | Category | Retry | Recovery |
| --- | --- | --- | --- | --- |
| optimistic/transaction conflict | `conflict.*` | `Conflict` | `AfterReload` | `ReloadHead` |
| missing node | `node.not_found` | `Corruption` | `Never` | `RepairFromReplica` |
| invalid node/CID/manifest/snapshot | `corruption.*` | `Corruption` | `Never` | `RepairFromReplica` or `RestoreBackup` |
| format/version mismatch | `format.*` | `Incompatible` | `Never` | `Upgrade` or `Reconfigure` |
| entry/buffer/index/content limits | `limit.*` | `ResourceExhausted` | `Never` | `IncreaseLimit` |
| malformed input/duplicate/unsorted/cursor | `input.*` | `InvalidInput` | `Never` | `None` |
| unsupported transaction/operation | `unsupported.*` | `Unsupported` | `Never` | `Reconfigure` |
| serialization of caller value | `serialization.*` | `InvalidInput` | `Never` | `None` |
| store classified transient | adapter code | `TransientInfrastructure` | adapter advice | adapter action |
| store classified disk full | adapter code | `ResourceExhausted` | `AfterBackoff` | `FreeStorage` |
| unclassified custom store | `store.unknown` | `Unknown` | `Never` | `Abort` |
| startup health not ready | `engine.startup_not_ready` | category of highest-severity failed check | `Never` | failed check action |

`Error::Store` becomes:

```rust
Store {
    source: Box<dyn std::error::Error + Send + Sync>,
    metadata: ErrorMetadata,
}
```

All store-error conversions call `Error::from_store(&store, error)`, which asks
the instance to classify the error before boxing it. Direct
`Error::Store(Box::new(...))` construction is removed. Adapter classifications
must be based on typed provider error codes, never message substring matching.

Retry advice describes whether repeating the same logical operation is safe;
it does not perform retries. `VersionedMap` uses profile retry limits for
conflicts and transient store errors marked safe. It records attempts and
returns the last structured error when either attempt or elapsed budget is
exhausted.

## Observability

### Observer

```rust
pub trait EngineObserver: Send + Sync + 'static {
    fn on_event(&self, event: &EngineEvent);
}

pub struct EngineEvent {
    pub operation: OperationKind,
    pub elapsed: Duration,
    pub outcome_code: String,
    pub logical_items: u64,
    pub nodes_read: u64,
    pub bytes_read: u64,
    pub nodes_written: u64,
    pub bytes_written: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub retries: u32,
    pub conflicts: u32,
}
```

`OperationKind` has stable variants for create/open, get/get-many,
range/prefix/page, put/delete/batch/write-session, build/append, diff/merge,
transaction/version publication, backup/restore/sync, proof, verification,
health, and garbage collection.

Events contain no logical key, value, root name, map ID, blob bytes, query
vector, or credentials. An optional future tracing adapter may attach
application-owned correlation IDs outside the engine event.

The engine stores `Option<Arc<dyn EngineObserver>>`. The no-observer path does
not allocate strings on success: outcome codes use static identifiers until an
observer event is constructed. Observer callbacks run after engine locks and
store transactions are released. A callback panic is caught, increments
`observer_failures`, disables that observer instance, and never changes the
operation result. Abort-on-panic builds retain normal abort behavior.

### Metrics

Extend `ProllyMetricsSnapshot` with:

- total operations, successes, and failures;
- conflicts and retry attempts;
- resource-limit rejections;
- health runs and failed health checks;
- observer failures;
- transaction commits and commit failures;
- eager result entries and streaming entries produced.

Counters saturate at `u64::MAX`. Latency distributions belong to observers;
the core does not implement histograms.

No-op observer overhead must remain within 3% of observer-disabled execution
for build, get, batch, range, diff, and merge at 100K records under the
confidence-interval method below. Any statistically demonstrated larger
regression blocks the milestone.

## Binding Contract

The UniFFI facade becomes the binding source of truth for:

- deployment profiles;
- engine-limit records;
- store-capability records;
- health reports and check records;
- error metadata records;
- metrics additions.

Every foreign exception exposes:

- `code`;
- `category`;
- `retryAdvice`/idiomatic equivalent;
- `recoveryAction`/idiomatic equivalent;
- human-readable message.

Node errors attach the fields to the JavaScript `Error` object. WASM exports a
plain metadata record alongside the exception. Python, Kotlin, Java, Ruby,
Swift, and Go expose generated/idiomatic enums rather than requiring string
parsing.

A checked-in JSON fixture contains every stable enum spelling and representative
error mapping. Each binding test loads the fixture and asserts exact parity.
Generated binding sources are regenerated in the same change as the Rust ABI.

Host-provided stores default to conservative capabilities and `store.unknown`
errors. Host APIs can opt into stronger values only by implementing the full
capability and classification callbacks. The embedded production profile
rejects a host store whose callbacks are absent.

## Validation and Testing

### Unit and compile-time tests

- every profile has exact default limits;
- every invalid zero/cross-limit combination is rejected;
- oversized keys, values, root names, and map IDs fail before publication;
- every capability requirement reports a stable missing-capability code;
- wrapper stores forward capabilities and error classification exactly;
- `Error::metadata()` covers every variant without a wildcard;
- unknown store errors are never marked retryable;
- reserved root names are rejected by every public root API;
- observer events contain no application bytes;
- observer panics do not change operation results.

### Portable backup tests

- every managed catalog mutation conditions on and advances its generation in
  the same atomic transaction;
- generation timestamps increase when the wall clock repeats or moves backward;
- unknown, missing, malformed, and overflowed generations fail closed;
- a stable catalog returns a backup whose head and exact version set match the
  captured catalog;
- publication or prune at each deterministic export hook causes retry and never
  produces a mixed catalog;
- retry-attempt, elapsed-time, eager-version, node, and byte limits return their
  exact structured errors without a partial backup;
- an export error is retried only when a changed generation proves concurrent
  catalog mutation; otherwise the original error is preserved.

### Health tests

Use fault-injecting production-store fixtures to verify:

- empty and populated stores report `Ready` within bounds;
- missing root nodes report `NotReady` with the exact CID;
- corrupt node bytes report the expected and actual CID;
- malformed manifests and incompatible formats are distinguished;
- root, node, byte, and elapsed budgets stop deterministically;
- read-only health performs zero writes/deletes;
- read-write probe applies CAS, verifies, deletes, and leaves no root;
- failure after probe publication is cleaned on the next run;
- cleanup conflict never deletes a non-matching root;
- store failures remain in the report with metadata and partial counts.

### Store conformance

Extend sync and async conformance suites with declared-capability tests.
Capability-specific tests run only when a capability is claimed, but every
claim has at least one behavioral test. Crash/power-loss declarations also
require adapter-specific subprocess fault tests; an in-process mock is not
sufficient evidence.

### Binding verification

Run every command in `bindings/VERIFICATION.md`. Add fixture-parity tests to
each binding and verify that representative conflict, corruption, limit,
unsupported, and transient-store errors retain exact metadata.

### Regression gates

- canonical roots match pre-change fixtures for all built-in tree formats;
- node bytes and root CIDs match pre-change fixtures;
- root-manifest/version-ID fixtures change only by removing runtime tuning from
  identity and match across different `EngineLimits`;
- all existing Rust tests pass with all features;
- clippy with warnings denied, rustdoc warnings denied, and formatting pass;
- all store adapter suites pass;
- all language binding suites pass;
- no-op observer performance stays within 3%;
- bounded defaults do not regress the validated 10K–10M workload matrix by
  more than the 3% confidence-interval gate below;
- every remaining performance regression is reported explicitly.

Performance comparisons use the feature branch's merge base with `main` as the
no-regression baseline. Baseline and candidate binaries are built from clean
release-profile worktrees with the same Rust toolchain and features. Each case
uses five warmups and at least twenty measured samples, alternates baseline and
candidate order, and reports a bootstrapped 95% confidence interval for the
median elapsed-time ratio. A case blocks when the interval's lower bound is
greater than `1.03`; an apparent gain is claimed only when its upper bound is
less than `1.00`. Otherwise it is reported as statistically inconclusive.

The matrix covers 10K, 50K, 1M, and 10M records; append-only, uniformly random,
and clustered keys; and build, point insert/delete/update, batch mutation,
range scan, prefix scan, sparse diff, and conflict/non-conflict merge. The 10M
cases may run in a scheduled job, but cannot be skipped in the completion
audit. Peak resident memory and bytes read/written are reported beside latency
and throughput and must remain inside the configured limits.

Raw measurements and generated reports stay in ignored local or CI artifacts;
they are not committed and are not included in pull-request prose. The final
user handoff reports gains, regressions, inconclusive cases, machine details,
commands, and artifact locations honestly.

## Security and Privacy

- Health and observer output excludes logical data and credentials.
- Error messages from providers are preserved for local logs, while public
  metadata remains stable and sanitized.
- Reserved internal roots cannot be created through application APIs.
- Health traversal validates CIDs before decoding nodes.
- Read-write health performs no destructive repair outside the reserved root
  namespace.
- Capability declarations are not accepted from serialized untrusted input;
  they come from the live adapter instance.
- Production examples use restrictive filesystem permissions and SQLite FULL
  synchronous mode.

## Documentation

Update:

- `README.md` with one embedded durable startup example;
- `docs/getting-started.md` with profile selection;
- `docs/performance.md` with bounded defaults and observer overhead;
- `docs/versioned-map.md` with startup health and retry guidance;
- `docs/architecture.md` with capability versus trait semantics;
- `docs/roadmap.md` to mark the Phase 1 items shipped only after verification;
- every store README with its exact capability table and durability caveats;
- `bindings/VERIFICATION.md` with metadata fixture commands.

Documentation must say that the current file store does not pass the embedded
durable profile. SQLite WAL plus FULL synchronous mode is the initial reference
backend for that profile.

## Delivery Sequence

The implementation plan will split Phase 1 into independently reviewable
changes:

1. capability types, forwarding, and adapter declarations;
2. engine limits, profiles, and builder hard cutover;
3. structured error metadata and store classification;
4. bounded read-only health;
5. reserved-root read-write health and cleanup;
6. catalog generation and snapshot-consistent portable backup;
7. observer events and metrics;
8. UniFFI and all language-binding parity;
9. documentation, performance gates, and completion audit.

Each change follows test-driven development and must leave the repository
passing its relevant focused tests before the next change begins.

## Completion Criteria

Phase 1 is complete only when all of the following are proved by current-state
evidence:

1. Every checked-in store instance reports an exact capability record.
2. No store passes a profile whose guarantees it cannot document and test.
3. `EngineBuilder` is the only public synchronous engine constructor.
4. Embedded and service profiles reject unbounded limits.
5. SQLite WAL plus FULL passes embedded read-write health while reporting its
   required write-quiescence procedure for backend-file backup.
6. The current file store is rejected with actionable capability failures.
7. Health detects every injected corruption, missing-node, format, budget, and
   transaction-probe failure described above.
8. Every public `Error` variant and tested provider failure has stable metadata.
9. Every supported language binding exposes identical metadata and health
   semantics.
10. Observability covers every public operation family without logical-data
    leakage.
11. Every portable backup is proved to contain a head and exact version set from
    one catalog generation, or fails without returning a backup.
12. Persisted node bytes, node CIDs, and root CIDs are unchanged. Manifest bytes
    depend on logical root, persisted format, and explicit timestamp metadata;
    `MapVersionId` depends only on logical root and persisted format. Neither
    identity depends on runtime tuning.
13. Correctness, store, binding, lint, documentation, formatting, and
    performance gates all pass.
14. The completion audit maps each criterion to command output, test names, or
    generated fixture evidence; absence of a failure is not sufficient proof.

After these criteria pass, the next specification will hard-cut over the local
file backend to crash-safe grouped packs and journaled root publication. The
larger production objective remains active until storage, mutation memory,
maintenance, crash/soak testing, and distributed-ready contracts are also
implemented and verified.
