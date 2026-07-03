# Language Bindings Technical Design

This document describes how to expose the Rust `prolly-map` implementation to
other languages without reimplementing the tree in each ecosystem. The goal is
to keep one authoritative engine in Rust, then ship thin language bindings that
share the same conformance fixtures, wire format, and release process.

## Goals

- Keep Rust as the native implementation and source of truth.
- Ship first-class bindings for tier 1 languages: Python, Java, Go,
  Node/TypeScript, and Kotlin.
- Ship browser WASM support as part of the tier 1 Node/TypeScript delivery.
- Ship tier 2 bindings for Ruby, Swift, and standalone WASM embedding after
  the binding ABI is stable.
- Prefer UniFFI for shared interface definition and generated bindings.
- Allow language-specific adapters when they provide a better package,
  runtime, or developer experience than UniFFI for that language.
- Reuse `crates/prolly/conformance/prolly-fixtures.v1.json` for every binding.
- Treat tier labels as delivery order only. A tier 2 language can ship later,
  but it must still reach the same Rust feature parity before it is marked
  complete.

Non-goals for the first binding release:

- no independent native implementation in each language;
- no direct exposure of Rust generics, lifetimes, traits, or borrowed data;
- no async FFI surface until the synchronous API is stable, although async
  feature parity remains required before a language binding is complete.

## Source Of Truth

The current Rust crate remains the core:

```text
crates/prolly
  src/                 Rust implementation
  bindings/uniffi/     Rust FFI facade and UniFFI generator config
  bindings/python/     Python package and generated bindings
  bindings/go/         Go package and generated bindings
  bindings/node/       Node/TypeScript package
  bindings/wasm/       Browser WASM package
  bindings/java/       Java facade package
  bindings/kotlin/     Kotlin/JVM package
  bindings/ruby/       Ruby gem
  bindings/swift/      Swift package
  conformance/         shared fixtures generated from Rust
  docs/                wire format, behavior, and binding design
```

Bindings should call Rust code through a small facade instead of binding the
entire public Rust API. The facade keeps language APIs stable even if internal
Rust modules continue to evolve.

The binding crate location is fixed:

```text
crates/prolly/bindings/uniffi/
  Cargo.toml           package `prolly-bindings`
  src/lib.rs           FFI-safe facade over `prolly-map`
  uniffi.toml          language generator settings
```

The binding crate should depend on the Rust library by path:

```toml
prolly-map = { path = "../.." }
```

This keeps UniFFI, Node-API, JNI, or WASM dependencies out of the core
`prolly-map` crate unless we intentionally enable a binding feature.

## Resolved Design Decisions

- The shared Rust UniFFI facade lives under
  `crates/prolly/bindings/uniffi`.
- Every language binding package lives under its own
  `crates/prolly/bindings/<language>` folder.
- Java ships as a Java-friendly wrapper over generated Kotlin/JVM UniFFI
  bindings.
- Node/TypeScript ships both Node-API and browser WASM packages in the same
  tier 1 release.
- SQLite is the first required persistent store beyond memory and file for
  non-WASM binding artifacts.
- Generated binding sources are checked in to support vendored and offline
  builds; compiled binaries and package outputs are not checked in.

## Feature Parity Contract

All language bindings must be able to expose the same behavior as the Rust
crate. Release tiers only decide when a language ships, not whether the
language receives a reduced API.

Parity levels:

```text
P0 Core read/write        create, get, get_many, put, delete, batch, bulk build, range
P1 Wire/helpers           nodes, CIDs, config, key helpers, value/blob envelopes
P2 Diff/merge             eager/page/structural diff, merge, conflicts, resolvers
P3 Named roots/stores     manifests, CAS, retention, concrete Rust stores
P4 Operational APIs       stats, debug views, cache metrics, hints, cursors
P5 Advanced data flows    large values, GC, sync, CRDT, tombstones, streaming/async
P6 Host extensibility     custom stores, custom resolvers, custom merge policy
```

A binding can publish an early preview at P0 or P1, but a stable language
package must document its current parity level. A language is "feature
complete" only when it passes P0 through P6 for every Rust feature compiled
into that package.

Parity is behavioral, not a promise to mirror every Rust helper type as a
foreign class. For example, a binding can expose `BatchBuilder`,
`SortedBatchBuilder`, `BatchWriter`, and `MutationBuffer` behavior through
bulk-build and batch methods as long as ordering, deduplication, append
semantics, errors, stats, and root CIDs match Rust.

### Rust API Parity Matrix

| Rust surface | Required binding shape | Parity notes |
| --- | --- | --- |
| `Config`, `Encoding`, constants | records and helper constructors | Cache limits must round-trip but must never affect CIDs. |
| `Cid`, `Node`, `Tree` | byte-first records plus node encode/decode/CID helpers | `CRAB` compact node bytes must remain byte-for-byte compatible. |
| `Prolly::create/get/get_many/put/delete/batch`, `BatchBuilder`, `SortedBatchBuilder`, `append_batch` | `ProllyEngine` methods | Batch must preserve Rust last-write-wins semantics for duplicate keys. |
| `range`, `range_after`, `range_from_cursor`, `range_page` | eager list plus page/cursor APIs | Iterators must be represented by page tokens, not borrowed Rust iterators. |
| `diff`, `range_diff`, `diff_page`, `structural_diff_page` | eager and paged diff records | Structural diff markers can be records; streaming maps to pages. |
| `merge`, `merge_explain`, `merge_range`, `merge_prefix` | merge methods plus explanation records | Built-in resolvers must be available in every binding. |
| `Conflict`, `Resolution`, `Resolver`, `resolver::*` | records/enums, built-in resolver names, and P6 custom callbacks | Custom resolver callbacks require callback safety rules per language. |
| `RootManifest`, named root load/publish/CAS/retention | manifest records and engine methods | CAS result must preserve applied/current/missing semantics. |
| `Store`, `NodeStoreScan`, concrete stores | store-kind constructors, scan APIs, and P6 host store callbacks | Rust-backed stores ship before host-language stores. |
| `MemStore`, `FileNodeStore` | required store kinds | Available in every non-WASM binding. |
| `SqliteStore`, `RocksDBStore`, `PgliteStore`, `SlateDbStore` | optional feature-gated store kinds | Bindings expose these only in artifacts compiled with the matching Rust feature. |
| `ValueRef`, `BlobRef`, `LargeValueConfig`, blob stores | value/blob records and large-value methods | Blob GC must use the same reachability records as Rust. |
| JSON/CBOR/versioned value codecs | encode/decode helper functions | Language-specific JSON values should remain outside the core byte API. |
| key helpers and `KeyBuilder` | functions plus segment builder | Numeric encodings must match Rust signed/unsigned byte ordering. |
| `TreeStats`, stats diff, debug tree/compare | records from Rust debug/stat APIs | Debug output can be structured records, not formatted text only. |
| GC APIs | plan/reachability/sweep records and methods | GC must work for nodes, named roots, and blobs where the store supports scans. |
| sync APIs | missing-node plan/copy records and methods | Sync APIs copy Rust content-addressed nodes; transports remain language-specific. |
| CRDT helpers | timestamped value, multi-value set, strategy records, and P6 custom callbacks | Built-in strategies ship before callback strategies; current bindings expose value/delete CRDT resolver callbacks. |
| merge policy registry | prefix/exact rule records, methods, and P6 custom callbacks | Current bindings expose default, prefix, and exact-key rules with named built-ins and host callbacks. |
| tombstone helpers | tombstone record/functions | Tombstone bytes must remain compatible with Rust helper output. |
| streaming diff/conflicts | page or callback-safe APIs | Full iterator objects are not exposed; bindings use pages to avoid lifetime leaks. |
| async store/async prolly | async language APIs after sync parity | Async parity is required before a language is marked complete, except WASM preview builds. |
| cache metrics, parallel config, and hints | metrics records, option records, and hint methods | Performance hints are optional for correctness but part of complete parity. |

### Platform Exceptions

Feature parity is defined against the Rust features compiled into the binding
artifact. A binding may omit a store backend when the platform cannot support
it, but the package must say so explicitly.

Examples:

- WASM/browser packages do not expose filesystem stores in browser builds.
- Node packages must expose memory, file, and SQLite stores. Browser WASM
  packages should start with memory and browser-hosted stores.
- SQLite is the baseline persistent store for non-WASM binding artifacts.
- RocksDB, PGlite, and SlateDB are optional artifacts because their Rust crates
  and native dependencies change package size and platform support.
- Host-language custom callbacks for stores, merge resolvers, CRDT resolvers,
  and merge policy functions are P6 features. A language without P6 can be
  useful, but it is not fully feature-parity complete.

## Binding Tool Strategy

UniFFI is the preferred default because it is designed to generate bindings for
Rust libraries from one object model. Its current guide says interfaces can be
described with proc macros or a WebIDL-like UDL file, and that generated
bindings avoid writing binding code by hand:
<https://mozilla.github.io/uniffi-rs/latest/index.html>.

The first implementation uses UniFFI proc macros, matching the SlateDB binding
layout this project follows. A UDL file can still be added later if reviewers
want a language-neutral interface artifact, but the authoritative contract for
the initial binding is the proc-macro facade in
`crates/prolly/bindings/uniffi/src/lib.rs` plus `uniffi.toml`.

UniFFI support status from the current guide:

- full support: Kotlin, Swift, Python;
- partial legacy support: Ruby;
- third-party bindings: Go, Java, JavaScript/TypeScript, Node, and others;
- WASM uses external binding generators and requires WASM-specific scaffolding
  configuration.

References:

- UniFFI guide: <https://mozilla.github.io/uniffi-rs/latest/index.html>
- UniFFI binding generation: <https://mozilla.github.io/uniffi-rs/latest/tutorial/foreign_language_bindings.html>
- UniFFI WASM configuration: <https://mozilla.github.io/uniffi-rs/latest/wasm/configuration.html>
- UniFFI repository and third-party bindings list: <https://github.com/mozilla/uniffi-rs>

## Public FFI Model

The FFI API should be byte-first and record-oriented. Do not expose `Prolly<S>`
or `Store` directly because the Rust API is generic over store type and uses
Rust traits with associated error types.

### Records

```text
ConfigRecord
  min_chunk_size: u64
  max_chunk_size: u64
  chunking_factor: u32
  hash_seed: u64
  encoding: EncodingRecord
  node_cache_max_nodes: optional<u64>
  node_cache_max_bytes: optional<u64>

EncodingRecord
  kind: string            "raw", "cbor", "json", or "custom"
  custom_name: optional<string>

TreeHandle
  root: optional<bytes>   exactly 32 bytes when present
  config: ConfigRecord

EntryRecord
  key: bytes
  value: bytes

DiffRecord
  kind: string            "added", "removed", or "changed"
  key: bytes
  value: optional<bytes>  used for added/removed
  old: optional<bytes>    used for changed
  new: optional<bytes>    used for changed

MutationRecord
  kind: string            "upsert" or "delete"
  key: bytes
  value: optional<bytes>  required for upsert

RangePageRecord
  entries: sequence<EntryRecord>
  next_cursor: optional<RangeCursorRecord>

RangeCursorRecord
  after_key: optional<bytes>

RangeBoundsRecord
  start: bytes
  end: optional<bytes>

ChangedSpanRecord
  start: bytes
  end: optional<bytes>

DiffPageRecord
  diffs: sequence<DiffRecord>
  next_cursor: optional<RangeCursorRecord>

ConflictPageRecord
  conflicts: sequence<ConflictRecord>
  next_cursor: optional<RangeCursorRecord>

StructuralDiffPageRecord
  markers: sequence<StructuralDiffMarkerRecord>
  next_cursor: optional<string>

StructuralDiffMarkerRecord
  kind: string
  left_cid: optional<bytes>
  right_cid: optional<bytes>
  key_start: optional<bytes>
  key_end: optional<bytes>

ConflictRecord
  key: bytes
  base: optional<bytes>
  left: optional<bytes>
  right: optional<bytes>

ResolutionRecord
  kind: string            "value", "delete", or "unresolved"
  value: optional<bytes>

NamedRootRecord
  name: bytes
  tree: TreeHandle

RootManifestRecord
  tree: TreeHandle
  created_at_millis: optional<u64>
  updated_at_millis: optional<u64>

NamedRootSelectionRecord
  roots: sequence<NamedRootRecord>
  missing_names: sequence<bytes>

ManifestUpdateRecord
  applied: bool
  conflict: bool
  current: optional<TreeHandle>

NamedRootRetentionRecord
  kind: string            "all", "exact", "prefix", "newest_by_name", or "updated_since"
  names: sequence<bytes>
  prefix: optional<bytes>
  count: optional<u64>
  min_updated_at_millis: optional<u64>

MergeExplanationRecord
  merged: TreeHandle
  conflicts: sequence<ConflictRecord>
  trace_json: string

ValueRefRecord
  kind: string            "inline" or "blob"
  value: optional<bytes>
  blob_cid: optional<bytes>
  blob_len: optional<u64>

BlobRefRecord
  cid: bytes
  len: u64

VersionedValueRecord
  schema: string
  version: u64
  encoding: EncodingRecord
  payload: bytes

TimestampedValueRecord
  value: bytes
  timestamp_millis: u64

GcPlanRecord
  live_cids: sequence<bytes>
  live_nodes: u64
  live_bytes: u64
  leaf_nodes: u64
  internal_nodes: u64
  candidate_nodes: u64
  reclaimable_cids: sequence<bytes>
  reclaimable_nodes: u64
  reclaimable_bytes: u64
  missing_candidates: u64

GcSweepRecord
  plan: GcPlanRecord
  deleted_nodes: u64
  deleted_bytes: u64

BlobGcPlanRecord
  live_blobs: sequence<BlobRefRecord>
  live_blob_count: u64
  live_blob_bytes: u64
  scanned_nodes: u64
  scanned_values: u64
  candidate_blobs: u64
  reclaimable_blobs: sequence<BlobRefRecord>
  reclaimable_blob_count: u64
  reclaimable_blob_bytes: u64
  missing_candidates: u64

BlobGcSweepRecord
  plan: BlobGcPlanRecord
  deleted_blobs: u64
  deleted_blob_bytes: u64

MissingNodePlanRecord
  required_node_cids: sequence<bytes>
  required_nodes: u64
  required_bytes: u64
  missing_node_cids: sequence<bytes>
  missing_nodes: u64
  missing_bytes: u64

MissingNodeCopyRecord
  plan: MissingNodePlanRecord
  copied_nodes: u64
  copied_bytes: u64

StatsRecord
  fields: string          JSON produced by the Rust facade until stable UDL

DebugRecord
  fields: string          JSON produced by the Rust facade until stable UDL

NodeRecord
  keys: sequence<bytes>
  vals: sequence<bytes>
  leaf: bool
  level: u8
  min_chunk_size: u64
  max_chunk_size: u64
  chunking_factor: u32
  hash_seed: u64
  encoding: EncodingRecord
```

Use records rather than deeply nested enums for the first release. That makes
Java, Go, and TypeScript adapters easier because every target can map optional
byte fields predictably.

### Objects

```text
ProllyEngine
  constructor memory(config: ConfigRecord)
  constructor file(path: string, config: ConfigRecord)
  constructor sqlite(path: string, config: ConfigRecord)
  constructor rocksdb(path: string, config: ConfigRecord)
  constructor pglite(path: string, config: ConfigRecord)
  constructor slatedb(path: string, config: ConfigRecord)

  create() -> TreeHandle
  get(tree: TreeHandle, key: bytes) -> optional<bytes>
  get_many(tree: TreeHandle, keys: sequence<bytes>) -> sequence<optional<bytes>>
  put(tree: TreeHandle, key: bytes, value: bytes) -> TreeHandle
  delete(tree: TreeHandle, key: bytes) -> TreeHandle
  batch(tree: TreeHandle, mutations: sequence<MutationRecord>) -> TreeHandle
  build_from_entries(entries: sequence<EntryRecord>) -> TreeHandle
  build_from_sorted_entries(entries: sequence<EntryRecord>) -> TreeHandle
  append_batch(tree: TreeHandle, mutations: sequence<MutationRecord>) -> TreeHandle

  range(tree: TreeHandle, start: bytes, end: optional<bytes>) -> sequence<EntryRecord>
  range_page(tree: TreeHandle, cursor: optional<RangeCursorRecord>, end: optional<bytes>, limit: u64) -> RangePageRecord
  range_after(tree: TreeHandle, after_key: bytes, end: optional<bytes>) -> sequence<EntryRecord>
  range_from_cursor(tree: TreeHandle, cursor: optional<RangeCursorRecord>, end: optional<bytes>) -> sequence<EntryRecord>

  diff(base: TreeHandle, other: TreeHandle) -> sequence<DiffRecord>
  range_diff(base: TreeHandle, other: TreeHandle, start: bytes, end: optional<bytes>) -> sequence<DiffRecord>
  diff_page(base: TreeHandle, other: TreeHandle, cursor: optional<RangeCursorRecord>, end: optional<bytes>, limit: u64) -> DiffPageRecord
  structural_diff_page(base: TreeHandle, other: TreeHandle, cursor: optional<string>, limit: u64) -> StructuralDiffPageRecord
  conflict_page(base: TreeHandle, left: TreeHandle, right: TreeHandle, cursor: optional<RangeCursorRecord>, limit: u64) -> ConflictPageRecord

  merge(base: TreeHandle, left: TreeHandle, right: TreeHandle, resolver: optional<string>) -> TreeHandle
  merge_explain(base: TreeHandle, left: TreeHandle, right: TreeHandle, resolver: optional<string>) -> MergeExplanationRecord
  merge_range(base: TreeHandle, left: TreeHandle, right: TreeHandle, start: bytes, end: optional<bytes>, resolver: optional<string>) -> TreeHandle
  merge_prefix(base: TreeHandle, left: TreeHandle, right: TreeHandle, prefix: bytes, resolver: optional<string>) -> TreeHandle
  merge_with_resolver(base: TreeHandle, left: TreeHandle, right: TreeHandle, resolver: MergeResolverCallback) -> TreeHandle
  merge_explain_with_resolver(base: TreeHandle, left: TreeHandle, right: TreeHandle, resolver: MergeResolverCallback) -> MergeExplanationRecord
  merge_range_with_resolver(base: TreeHandle, left: TreeHandle, right: TreeHandle, start: bytes, end: optional<bytes>, resolver: MergeResolverCallback) -> TreeHandle
  merge_prefix_with_resolver(base: TreeHandle, left: TreeHandle, right: TreeHandle, prefix: bytes, resolver: MergeResolverCallback) -> TreeHandle
  crdt_merge(base: TreeHandle, left: TreeHandle, right: TreeHandle, config: CrdtConfigRecord) -> TreeHandle
  crdt_merge_with_resolver(base: TreeHandle, left: TreeHandle, right: TreeHandle, delete_policy: CrdtDeletePolicyKind, resolver: CrdtResolverCallback) -> TreeHandle

  load_named_root(name: bytes) -> optional<TreeHandle>
  load_named_roots(names: sequence<bytes>) -> NamedRootSelectionRecord
  list_named_roots() -> sequence<NamedRootRecord>
  load_retained_named_roots(retention: NamedRootRetentionRecord) -> NamedRootSelectionRecord
  publish_named_root(name: bytes, tree: TreeHandle)
  publish_named_root_at_millis(name: bytes, tree: TreeHandle, now_millis: u64)
  compare_and_swap_named_root(name: bytes, expected: optional<TreeHandle>, replacement: optional<TreeHandle>) -> ManifestUpdateRecord
  compare_and_swap_named_root_at_millis(name: bytes, expected: optional<TreeHandle>, replacement: optional<TreeHandle>, now_millis: u64) -> ManifestUpdateRecord

  get_value_ref(tree: TreeHandle, key: bytes) -> optional<ValueRefRecord>
  get_large_value(tree: TreeHandle, key: bytes) -> optional<bytes>
  put_large_value(tree: TreeHandle, key: bytes, value: bytes, threshold: u64) -> TreeHandle

  collect_stats(tree: TreeHandle) -> StatsRecord
  metrics() -> StatsRecord
  reset_metrics()
  clear_cache()
  publish_prefix_path_hint(tree: TreeHandle, prefix: bytes) -> bool
  publish_changed_spans_hint(tree: TreeHandle, spans: sequence<ChangedSpanRecord>) -> u64
  stats_diff(before: TreeHandle, after: TreeHandle) -> StatsRecord
  debug_tree(tree: TreeHandle) -> DebugRecord
  debug_compare_trees(left: TreeHandle, right: TreeHandle) -> DebugRecord

  plan_store_gc(roots: sequence<TreeHandle>) -> GcPlanRecord
  sweep_store_gc(roots: sequence<TreeHandle>) -> GcSweepRecord
  plan_blob_store_gc(roots: sequence<TreeHandle>) -> BlobGcPlanRecord
  sweep_blob_store_gc(roots: sequence<TreeHandle>) -> BlobGcSweepRecord
  plan_missing_nodes(tree: TreeHandle, destination: ProllyEngine) -> MissingNodePlanRecord
  copy_missing_nodes(tree: TreeHandle, destination: ProllyEngine) -> MissingNodeCopyRecord
  list_node_cids() -> sequence<bytes>

  node_from_bytes(bytes: bytes) -> NodeRecord
  node_to_bytes(node: NodeRecord) -> bytes
cid_from_bytes(bytes: bytes) -> bytes
```

Async bindings should expose the same method set through each language's
native async style:

- Python: `async` methods or an `AsyncProllyEngine` wrapper;
- Kotlin: suspend functions;
- Java: `CompletableFuture`;
- Go: context-aware methods where cancellation matters;
- Node/TypeScript: `Promise` methods;
- Swift: `async` functions;
- Ruby: sync methods first, then fibers/promises only if package conventions
  are clear;
- WASM: `Promise` methods for browser APIs.

Utility functions are part of the same binding namespace:

```text
prefix_end(prefix: bytes) -> optional<bytes>
prefix_range(prefix: bytes) -> RangeBoundsRecord
u64_key(value: u64) -> bytes
u128_key(value: string) -> bytes
i64_key(value: i64) -> bytes
i128_key(value: string) -> bytes
timestamp_millis_key(value: u64) -> bytes
encode_segment(segment: bytes) -> bytes
decode_segments(key: bytes) -> sequence<bytes>
debug_key(key: bytes) -> string

encode_json_canonical(json: string) -> bytes
decode_json_canonical(bytes: bytes) -> string
encode_cbor_from_json(json: string) -> bytes
decode_cbor_to_json(bytes: bytes) -> string
versioned_value_to_bytes(record: VersionedValueRecord) -> bytes
versioned_value_from_bytes(bytes: bytes) -> VersionedValueRecord
value_ref_to_bytes(record: ValueRefRecord) -> bytes
value_ref_from_bytes(bytes: bytes) -> ValueRefRecord
root_manifest_to_bytes(manifest: RootManifestRecord) -> bytes
root_manifest_from_bytes(bytes: bytes) -> RootManifestRecord

timestamped_value_to_bytes(value: bytes, timestamp_millis: u64) -> bytes
timestamped_value_from_bytes(bytes: bytes) -> TimestampedValueRecord
multi_value_set_to_bytes(values: sequence<bytes>) -> bytes
multi_value_set_from_bytes(bytes: bytes) -> sequence<bytes>

tombstone_upsert(value: bytes) -> bytes
tombstone_compaction(value: bytes) -> optional<bytes>
is_tombstone_value(value: bytes) -> bool
```

`ProllyEngine` should own an internal concrete Rust store. Initial store kinds:

- memory store for tests, examples, and short-lived snapshots;
- file node store for local persistent use;
- SQLite store for the first persistent cross-language backend beyond file;
- RocksDB, PGlite, and SlateDB store constructors only in artifacts compiled
  with those Rust features.

Language-provided stores are P6. UniFFI callback interfaces can model host
callbacks, but store callbacks add cross-language overhead to every node load,
complicate threading, and make browser support harder.

`MergeResolverCallback`, `CrdtResolverCallback`, and merge policy callback
rules are the first host callback APIs. `MergeResolverCallback` must be
available for full-tree, range-limited, and prefix-limited merge methods in
every binding that claims P6 resolver parity. `CrdtResolverCallback` must be
available for CRDT merges and returns only value/delete resolutions because
CRDT merges cannot remain unresolved. Merge policy registries must support
default, prefix, and exact-key rules with both named built-in resolvers and
host callback resolvers. Host callback implementations must keep handle maps
alive for the full FFI call or for the lifetime of a policy registry, convert
callback exceptions into binding errors or unresolved/delete fallback
resolutions according to the language facade, and must not panic or unwind
across the FFI boundary.

### Errors

Expose one binding error enum:

```text
ProllyBindingError
  InvalidArgument(message)
  InvalidCid(message)
  InvalidNode(message)
  NotFound(message)
  Conflict(message)
  Store(message)
  Serialization(message)
  Internal(message)
```

All FFI entrypoints must return typed errors and must not panic across the FFI
boundary. Convert Rust `prolly::Error` into `ProllyBindingError` inside the
facade. Validate byte lengths at the boundary, especially CIDs and root fields.

## Rust Facade Design

The facade should wrap the existing Rust implementation without duplicating
tree logic.

```rust
pub struct BindingEngine {
    inner: BindingStore,
    config: prolly::Config,
}

enum BindingStore {
    Memory(prolly::Prolly<prolly::MemStore>),
    File(prolly::Prolly<prolly::FileNodeStore>),
    #[cfg(feature = "sqlite")]
    Sqlite(prolly::Prolly<prolly::SqliteStore>),
    #[cfg(feature = "rocksdb")]
    RocksDb(prolly::Prolly<prolly::RocksDBStore>),
    #[cfg(feature = "pglite")]
    Pglite(prolly::Prolly<prolly::PgliteStore>),
    #[cfg(feature = "slatedb")]
    SlateDb(prolly::Prolly<prolly::SlateDbStore>),
}
```

Each method matches on `BindingStore` and calls the native Rust method. This
avoids trait objects around `Store`, which are awkward because `Store` has an
associated error type.

Facade rules:

- convert `ConfigRecord` to `prolly::Config` once at construction;
- convert `TreeHandle` to `prolly::Tree` on each call;
- convert results back to FFI records;
- copy byte buffers at the boundary;
- keep cursors as explicit page tokens, not borrowed Rust iterators;
- keep all node serialization and CID work delegated to Rust.

Any Rust return type that is too large or unstable for UDL in the first pass
can cross the boundary as deterministic JSON generated by the Rust facade.
Those JSON placeholder fields must be replaced by typed records before the
binding is marked feature complete.

## Language Matrix

| Target | Tier | Recommended binding path | Rationale |
| --- | --- | --- | --- |
| Rust | Native | direct `prolly-map` crate | Native API is the reference implementation. |
| Python | 1 | UniFFI official Python generator, packaged with maturin or setuptools-rust | UniFFI officially supports Python, and Python is a high-value scripting and tooling target for inspection, tests, and data workflows. |
| Kotlin | 1 | UniFFI official Kotlin generator | UniFFI officially supports Kotlin and Android-style packaging. |
| Java | 1 | Kotlin/JVM UniFFI artifact plus Java-friendly facade | Java is not first-party UniFFI today, but the JVM can consume Kotlin-generated artifacts. Use a Java adapter to avoid Kotlin idioms leaking into Java examples. |
| Go | 1 | third-party UniFFI Go generator first, cgo wrapper fallback | UniFFI lists third-party Go support. If maturity is not enough, keep the same Rust facade and expose a small C ABI for cgo. |
| Node/TypeScript | 1 | `napi-rs` Node-API package plus browser WASM package | Node users expect npm packages with prebuilt native modules and TypeScript declarations, while browser users need WASM. Both artifacts share the Rust facade and ship together. |
| Ruby | 2 | UniFFI Ruby generator | UniFFI keeps Ruby working but describes it as partial legacy support, so ship after tier 1. |
| Swift | 2 | UniFFI official Swift generator | Strong official support; the repository now includes a SwiftPM package with generated sources, fixture checks, and runnable cookbook examples. |
| WASM | 2 | standalone `wasm-bindgen` wrapper or external UniFFI WASM generator | Browser WASM ships with Node/TypeScript in tier 1. This row covers later standalone WASM embedding outside the TypeScript package. |

## Per-Language Packaging

### Python

Deliverables:

- Python package under `crates/prolly/bindings/python`;
- generated UniFFI Python module and native library via maturin;
- `bytes`-first public API;
- unittest/pytest conformance tests;
- wheels for Linux, macOS, and Windows.

Use UniFFI as the primary binding path because Python is officially supported.
Package with `maturin` first, following the implemented
`crates/prolly/bindings/python/pyproject.toml`.

The existing `crates/prolly/bindings/python/src` implementation remains a
temporary fixture harness and source-tree fallback. The package-level
`prolly` module loads the generated Rust binding once built with maturin.

### Kotlin

Deliverables:

- generated Kotlin sources;
- native libraries for Linux, macOS, Windows, and Android targets as needed;
- Gradle module;
- examples for `ByteArray` keys and values.

Use UniFFI as the primary path. Keep JVM identifiers stable:

```text
group/artifact namespace: build.crab
Kotlin package: build.crab.prolly
Java package: build.crab.prolly
```

### Java

Deliverables:

- Maven artifact with Java facade classes;
- native library loader;
- JUnit conformance tests.

Chosen first version:

- generate Kotlin/JVM bindings with UniFFI;
- write a Java-friendly wrapper that exposes `byte[]`, `List<Entry>`,
  `Optional<byte[]>`, and checked exceptions;
- keep Kotlin runtime dependencies explicit in the Maven artifact.

Do not start with a third-party Java UniFFI generator. Revisit a direct Java
generator or JNI wrapper only if the Kotlin/JVM artifact cannot satisfy Java
package quality, Android compatibility, or offline build requirements.

### Go

Deliverables:

- Go module under `crates/prolly/bindings/go`;
- generated bindings or cgo wrapper;
- `[]byte` API and table-driven conformance tests.

Preferred first version:

- prototype with a third-party UniFFI Go generator;
- keep the public Go API handwritten and small so generator changes do not
  break users.

Fallback:

- expose a C ABI from `prolly-bindings`;
- wrap it with cgo and explicit memory-free helpers.

### Node/TypeScript

Deliverables:

- npm package under `crates/prolly/bindings/node`;
- browser WASM package under `crates/prolly/bindings/wasm`;
- TypeScript declarations;
- prebuilt binaries for common Node targets;
- WASM bundle for browser use;
- Node test runner conformance tests;
- browser runner conformance tests for the WASM package.

Chosen first version:

- implement Node bindings with `napi-rs` over the same Rust facade;
- implement browser bindings with `wasm-bindgen` or an external UniFFI
  WASM/TypeScript generator over the same Rust facade;
- expose `Uint8Array` for bytes;
- publish ESM and CJS entrypoints for Node if packaging cost is reasonable;
- publish an ESM-first browser package for WASM;
- ship Node-API and browser WASM artifacts in the same tier 1 release.

### Ruby

Deliverables:

- gem under `crates/prolly/bindings/ruby`;
- generated Ruby bindings;
- fixture tests.

Use UniFFI Ruby after tier 1. Keep the public API bytes-first with Ruby
`String` objects forced to binary encoding.

### Swift

Deliverables:

- Swift Package Manager module;
- XCFramework for Apple platforms;
- executable fixture checks;
- XCTest conformance tests when the local Apple toolchain provides XCTest.

Use UniFFI Swift. Because UniFFI has official Swift support, Swift can move up
if mobile clients become a priority. The checked-in Swift package links against
the same `prolly-bindings` facade and keeps generated Swift/FFI sources in the
package for offline builds.

### WASM

Deliverables:

- browser package under `crates/prolly/bindings/wasm` as part of the
  Node/TypeScript tier 1 delivery;
- standalone WASM embedding package after the binding ABI is stable;
- generated TypeScript declarations;
- fixture tests in Node and a browser runner.

Preferred browser version:

- direct `wasm-bindgen` wrapper over the Rust facade;
- optional external UniFFI WASM/TypeScript generator once the generator is
  stable enough for production packaging.

WASM-specific notes:

- avoid exposing filesystem stores in browser builds;
- use memory store first;
- add IndexedDB/OPFS-backed stores through explicit JS host integration later;
- consider UniFFI's `wasm-unstable-single-threaded` feature only in a separate
  WASM binding crate so native builds keep normal `Send`/`Sync` checks.

## Generated Vs Handwritten Code

Check in:

- binding facade source and generator config
  (`bindings/uniffi/src/lib.rs` and `bindings/uniffi/uniffi.toml`);
- generated UniFFI and language binding sources;
- generation metadata that records tool versions, commands, features, and the
  Rust binding ABI version;
- handwritten language facades;
- packaging manifests;
- conformance tests;
- small examples.

Do not check in:

- compiled dynamic libraries;
- platform-specific native artifacts;
- npm, wheel, gem, Maven, or Swift build outputs.

Release automation should generate bindings and native artifacts in CI, then
publish signed packages. CI must also regenerate the checked-in binding sources
and fail if the generated output differs, so offline builds stay reproducible
without letting generated code drift from the UniFFI facade.

Generated sources should live inside each language package, not in a shared
scratch directory. Suggested paths:

- Python: `crates/prolly/bindings/python/prolly/uniffi`;
- Kotlin/JVM: `crates/prolly/bindings/kotlin/src/main/kotlin/build/crab/prolly/generated`;
- Java facade: `crates/prolly/bindings/java/src/main/java/build/crab/prolly`;
- Go: `crates/prolly/bindings/go/internal/generated`;
- Node/TypeScript: `crates/prolly/bindings/node/src/generated`;
- browser WASM: `crates/prolly/bindings/wasm/src` for the handwritten ESM
  loader/types and `crates/prolly/bindings/wasm/pkg` for `wasm-bindgen`
  generated JS/TS glue during release builds. The release `pkg` directory and
  compiled `.wasm` output are built in CI and not checked in.
- Swift: `crates/prolly/bindings/swift/Sources/Prolly` and
  `crates/prolly/bindings/swift/Sources/prollyFFI/include`.

Each generated directory should include a small provenance file with the
generator name, generator version, command line, enabled Cargo features, and
`prolly-bindings` ABI version.

## Conformance Strategy

Every binding must pass the same fixture suite:

- load `conformance/prolly-fixtures.v1.json`;
- decode node bytes and verify CIDs through Rust binding calls;
- reproduce boundary decisions;
- validate key helpers;
- load Rust-generated store images;
- run `get`, `range`, and `diff`;
- decode value and blob envelopes;
- validate manifest bytes when the binding supports named roots.

Because the binding calls Rust, these tests primarily verify:

- packaging found the native library;
- byte arrays are not corrupted by the language boundary;
- optional fields map correctly;
- errors and null/none values preserve semantics;
- generated facades expose the expected API.

Feature-complete conformance also requires parity scenarios for:

- batch mutation sorting, deduplication, delete no-op behavior, and root CID
  equality with Rust;
- `get_many` order preservation, duplicate keys, and missing keys;
- range cursors, range pages, and range-after resumption;
- eager, range, paged, and structural diff;
- three-way merge, built-in resolvers, conflict reporting, merge explanations,
  range merge, and prefix merge;
- named root publish, load, batch load, compare-and-swap, delete, timestamps,
  and retention selection;
- value codecs, large-value inline/blob envelopes, blob resolution, and blob
  GC;
- node GC, sync missing-node planning/copying, and scan support;
- stats, stats diff, debug tree, and debug compare tree records;
- key helpers for unsigned/signed integers, prefixes, escaped segments, and
  debug formatting;
- CRDT timestamped values, multi-value sets, built-in merge strategies, delete
  policies, and custom value/delete resolver callbacks;
- tombstone helper bytes and tombstone compaction behavior;
- async/page equivalents for every iterator-like Rust API;
- P6 host callbacks for custom merge resolvers, custom CRDT resolvers, custom
  merge policies, and custom stores in languages that claim full parity.

Add smoke tests for each language:

```text
engine = ProllyEngine.memory(default_config)
tree = engine.create()
tree = engine.put(tree, b"a", b"1")
assert engine.get(tree, b"a") == b"1"
assert engine.range(tree, b"", None) == [(b"a", b"1")]
```

## Build And Release Plan

Phase 0: binding contract

- add `prolly-bindings` crate at `crates/prolly/bindings/uniffi`;
- implement the proc-macro UniFFI facade and `uniffi.toml`;
- expose P0 and P1: memory/file stores, tree handles, get, get_many, put,
  delete, batch, range, range page, CID, node bytes, key helpers, and
  value/blob envelope helpers;
- run Rust unit tests against the facade.

Phase 1: official UniFFI targets

- generate Python bindings and replace the temporary Python `src` internals;
- generate Kotlin bindings;
- run P0/P1 conformance tests against both generated packages.

Phase 2: tier 1 package adapters

- Java wrapper over generated Kotlin/JVM bindings;
- Go generator prototype plus cgo fallback decision;
- Node/TypeScript package with `napi-rs`;
- browser WASM package shipped with the Node/TypeScript release;
- P0/P1 conformance CI for all tier 1 packages.

Phase 3: parity expansion

- add P2 diff/merge parity, including conflict records and built-in resolvers;
- add P3 named roots, manifest CAS, retention, SQLite, and feature-gated store
  constructors for RocksDB, PGlite, and SlateDB;
- add P4 stats, debug views, cursors, metrics, and hints;
- add P5 large values, GC, sync, CRDT, tombstones, streaming/page APIs, and
  async equivalents;
- do not mark any language package stable until it documents and passes its
  target parity level.

Phase 4: tier 2 targets

- Ruby with UniFFI;
- Swift with UniFFI;
- standalone WASM embedding with direct `wasm-bindgen` or external UniFFI
  generator.

Phase 5: callback and host integration

- keep custom merge resolver, custom CRDT resolver, and custom merge-policy
  callback parity green across Python, Go, Node/TypeScript, Kotlin/JVM, Java,
  and Ruby;
- language-hosted store callbacks;
- browser IndexedDB/OPFS stores;
- remote/object-store adapters;
- async store callbacks if profiling shows they are worth the complexity.

## CI Matrix

Minimum CI before publishing any binding:

- Rust: `cargo test -p prolly-map --test conformance_fixtures`
- Binding crate: facade unit tests and generated binding smoke tests
- Generated source freshness: regenerate bindings and fail on any diff
- Kotlin/JVM: Gradle test on Linux
- Java: Maven or Gradle JUnit conformance tests
- Go: `go test ./...`
- Node/TypeScript: `npm test` for Node-API and browser WASM packages
- Python: `python -m unittest discover -s crates/prolly/bindings/python/tests`
- SQLite: conformance tests for every non-WASM package

Feature-complete CI must also run one parity test group per P-level. The CI
status for each package should report the highest passing P-level, for example
`python:P6`, `node:P3`, or `wasm:P1-preview`.

Release CI should build native artifacts for:

- macOS arm64 and x86_64;
- Linux x86_64 and arm64;
- Windows x86_64;
- Android targets if Kotlin Android support is shipped;
- Apple iOS/macOS targets if Swift is shipped.

## Versioning And Compatibility

Use three versions:

- Rust crate version: `prolly-map`;
- binding ABI version: `prolly-bindings`;
- fixture schema version: `prolly-conformance-v1`.

Rules:

- Rust patch releases may add binding methods but must not change existing
  method behavior.
- Binding method removals or record field changes require a binding major
  version bump.
- Fixture schema changes require a new fixture file, not in-place mutation of
  `prolly-fixtures.v1.json`.
- Language packages should declare the exact Rust binding ABI version they
  bundle.

## Risks And Mitigations

FFI overhead:

- expose page APIs for range and diff;
- avoid one FFI call per node or per row for large scans;
- keep high-volume traversal in Rust.

Generator maturity:

- prefer official UniFFI targets first;
- keep language-specific adapters small and tested;
- use the Kotlin/JVM wrapper path for Java;
- use `napi-rs` plus browser WASM for Node/TypeScript;
- keep a cgo fallback for Go if third-party generator maturity is not enough.

Packaging complexity:

- build artifacts in CI;
- keep native library loading code language-specific;
- publish examples for local development and installed package usage.

Memory safety:

- never return borrowed Rust slices;
- copy byte arrays at the boundary;
- use UniFFI-managed objects or explicit free functions for non-UniFFI
  adapters;
- validate all inbound byte lengths.

Semantic drift:

- keep conformance fixtures generated from Rust;
- run the same fixtures in every language;
- keep `wire-format.md` as the low-level reference and this document as the
  binding architecture reference.

## Finalized Decisions

- `prolly-bindings` is a nested crate under `crates/prolly/bindings/uniffi`.
- Language packages live under `crates/prolly/bindings/<language>`.
- Java is a wrapper over generated Kotlin/JVM bindings.
- Node/TypeScript ships Node-API and browser WASM artifacts together.
- SQLite is the first persistent store beyond memory and file.
- Generated binding sources are checked in for vendored and offline builds.
