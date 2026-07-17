# Language-Native Store Bindings Design

Status: approved design

Date: 2026-07-16

Scope: shared asynchronous store protocol; Node/TypeScript, Python, Go,
Java/Kotlin, Ruby, Swift, and browser/WASM store packages; SQLite, PostgreSQL,
MySQL, Redis, DynamoDB, Cosmos DB, Spanner, PGlite, IndexedDB, and OPFS

## Summary

The language bindings will support pluggable stores through one versioned,
asynchronous protocol. The Rust engine remains responsible for all Prolly tree
semantics. Independently versioned language-native packages translate protocol
operations to an injected database or cloud SDK client.

The implementation proceeds provider first. Each provider is completed across
every supported language before work moves to the next provider. A provider
slice is complete only after protocol conformance, bidirectional compatibility
with the Rust provider, packaging isolation, cancellation, lifecycle, and
transaction tests pass for every supported language cell.

This specification refines the provider coverage and rollout described in
[`docs/language-store-adapters-design.md`](../../language-store-adapters-design.md).
Where the two documents differ, this approved specification controls the
language/provider matrix and delivery order.

## Context

The repository already contains the main Rust storage layers:

- `Store` and `Prolly` for synchronous embedded stores;
- `AsyncStore` and `AsyncProlly` for asynchronous stores;
- `RemoteStoreBackend` for provider operations;
- `RemoteProllyStore` for CID and manifest validation;
- separate Rust store packages under `stores/`;
- synchronous host-store callbacks in the language bindings.

The synchronous callback is suitable for small embedded host stores but not
for remote database clients. Blocking a JavaScript promise, Python event loop,
JVM coroutine dispatcher, or Swift cooperative executor can deadlock or erase
the concurrency benefit of `AsyncProlly`. Compiling all Rust provider crates
into every binding would also add unrelated SDKs, runtimes, and native build
requirements to core packages.

## Goals

1. Define one stable `prolly-store-protocol-v1` contract for host-language
   stores.
2. Keep canonical tree algorithms, CIDs, manifests, proofs, diff, merge, sync,
   GC, and transaction coordination in Rust.
3. Use established SDKs and drivers native to each language ecosystem.
4. Keep provider dependencies out of core language packages.
5. Preserve physical compatibility with the existing Rust provider stores.
6. Support real asynchronous I/O and bounded concurrency without blocking host
   event loops.
7. Expose provider capabilities and limits conservatively.
8. Make every supported language/provider claim machine-checkable through a
   compatibility manifest and shared conformance suite.
9. Allow provider and core packages to release independently within a declared
   protocol compatibility range.

## Non-goals

- Reimplementing Prolly tree algorithms in host languages.
- Compiling all Rust provider adapters into every language artifact.
- Defining a dynamic Rust plugin ABI or sharing Rust trait-object pointers
  between native libraries.
- Supporting RocksDB or SlateDB outside Rust.
- Building an HTTP storage gateway.
- Connecting browser/WASM directly to PostgreSQL, MySQL, Redis, DynamoDB,
  Cosmos DB, or Spanner.
- Writing custom Cosmos DB or Spanner clients for a language without an
  established provider SDK.
- Claiming strict transactions where a provider or selected SDK cannot supply
  them.

## Architecture

```text
Application
    |-- core language binding
    |     `-- Rust AsyncProlly engine
    |           `-- validated foreign-store bridge
    `-- optional provider package
          `-- injected host-language SDK client or pool
```

The architecture has four layers.

### Rust engine

The `prolly-map` crate remains authoritative for deterministic encoding,
content identifiers, immutable tree operations, validation, proofs, diff,
merge, retention, sync, garbage collection, and transaction coordination.

### Portable store protocol

`prolly-store-protocol-v1` defines owned records, operations, ordering,
capabilities, limits, errors, and transaction results. It carries opaque byte
strings and never exposes Rust generics, lifetimes, iterators, or associated
error types.

### Runtime bridge

Each native binding converts the portable protocol into
`RemoteStoreBackend`. `RemoteProllyStore` continues to validate fetched node
bytes against their requested CIDs and to validate canonical root manifests.
Browser/WASM implements the same logical protocol through a separate
single-thread-compatible bridge.

### Provider packages

Each provider package owns its database SDK dependency, schema initialization,
provider limits, retry behavior, error classification, and translation to the
portable protocol. Provider packages do not implement tree behavior.

## Support Matrix

The mandatory version 1 matrix is:

| Store | Node/TS | Python | Go | JVM | Ruby | Swift server/macOS | Browser/WASM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite | yes | yes | yes | yes | yes | yes | no |
| PostgreSQL | yes | yes | yes | yes | yes | yes | no |
| MySQL | yes | yes | yes | yes | yes | yes | no |
| Redis | yes | yes | yes | yes | yes | yes | no |
| DynamoDB | yes | yes | yes | yes | yes | yes | no |
| Cosmos DB | yes | yes | yes | yes | unsupported | unsupported | no |
| Spanner | yes | yes | yes | yes | yes | unsupported | no |
| PGlite | yes | no | no | no | no | no | yes |
| IndexedDB | no | no | no | no | no | no | yes |
| OPFS | no | no | no | no | no | no | yes |

Additional platform rules:

- iOS supports SQLite only. Direct server-database and cloud packages target
  macOS and server-side Swift.
- Java and Kotlin share one JVM implementation. Kotlin exposes suspending APIs;
  Java exposes `CompletableFuture` facades.
- Unsupported cells have no placeholder package and appear explicitly in the
  compatibility manifest.
- Browser packages contain no server credential or direct database transport.

Google currently publishes Spanner libraries for Node.js, Python, Go, Java,
and Ruby, but not Swift. Microsoft's Cosmos DB SDK surface does not include
Ruby or Swift. The matrix follows those established SDK boundaries rather than
adding project-owned cloud protocol clients.

## Packaging

Provider code lives beside its language build tooling:

```text
bindings/
  uniffi/                         shared native foreign-store bridge
  node/stores/<provider>/
  python/stores/<provider>/
  go/stores/<provider>/
  kotlin/stores/<provider>/      shared JVM implementation
  java/stores/<provider>/        Java facade artifacts
  ruby/stores/<provider>/
  swift/stores/<provider>/
  wasm/stores/<provider>/

conformance/
  store-protocol-v1/
    protocol.json
    cases.json
    failure-cases.json
    providers/<provider>/
```

Artifacts preserve the repository's current ecosystem namespaces, including
`@trail`, `build.crab`, and the existing Go module path. Every provider is a
separately installable artifact and declares:

- the supported core binding version range;
- the supported protocol major;
- the physical provider schema version;
- its runtime and SDK requirements;
- its exact capabilities and limits.

Each package binds one named SDK or driver rather than accepting any object
that happens to expose similar methods. Cloud packages use the provider's
official SDK when the support matrix marks the combination as supported.
Database packages use a maintained ecosystem driver that preserves opaque
bytes and the required transaction behavior. The chosen dependency and client
type are public package metadata and appear in the compatibility manifest. A
package does not fall back to a project-owned cloud REST client.

The core artifacts contain memory and file support where the platform permits,
but no optional database or cloud SDK dependency. Provider constructors prefer
an injected client, connection, or pool. Convenience connection constructors
may be provided inside provider packages. Schema initialization and migration
are always explicit.

## Protocol Model

Every adapter supplies immutable metadata during construction:

```text
StoreDescriptor
  protocol_major: u32
  adapter_name: string
  provider: string
  schema_version: u32
  capabilities: StoreCapabilities
  limits: StoreLimits

StoreCapabilities
  native_batch_reads: bool
  atomic_batch_writes: bool
  node_scan: bool
  hints: bool
  atomic_nodes_and_hint: bool
  root_scan: bool
  root_compare_and_swap: bool
  transactions: bool
  read_parallelism: u32

StoreLimits
  max_batch_read_items: optional<u32>
  max_batch_write_items: optional<u32>
  max_transaction_operations: optional<u32>
  max_node_bytes: optional<u64>
```

The descriptor is validated before engine construction. The protocol major
must equal 1, schema version must be supported by the selected provider, and
read parallelism must be at least one. Capabilities do not change during an
adapter's lifetime. Unknown guarantees default to unsupported. An absent
numeric limit means that the protocol cannot preflight that limit; it does not
mean the provider is unbounded.

### Node operations

```text
async get_node(cid) -> optional<bytes>
async put_node(cid, node)
async delete_node(cid)
async batch_nodes(mutations)
async batch_get_nodes_ordered(cids) -> sequence<optional<bytes>>
async list_node_cids() -> sequence<bytes>

NodeMutation
  kind: upsert | delete
  cid: bytes
  node: optional<bytes>
```

A CID is exactly 32 bytes. Ordered batch reads return one value for every
input position, including duplicates and missing CIDs. Node scans contain only
valid CIDs and are ordered by raw bytes. When a logical batch is split across
multiple non-atomic provider requests, `atomic_batch_writes` is false.

### Hint operations

```text
async get_hint(namespace, key) -> optional<bytes>
async put_hint(namespace, key, value)
async batch_put_nodes_with_hint(entries, namespace, key, value)
```

Hints never affect correctness. Adapters without hints return missing values
and report the capability as false. A non-atomic combined write publishes
nodes before its hint so a stale hint always has a correct traversal fallback.

### Named-root operations

```text
async get_root_manifest(name) -> optional<bytes>
async put_root_manifest(name, manifest)
async delete_root_manifest(name)
async compare_and_swap_root_manifest(name, expected, replacement)
    -> RootCasResult
async list_root_manifests() -> sequence<RawNamedRoot>
```

Root CAS compares exact canonical manifest bytes, including missing state.
Conflicts return the current bytes as a normal result. Root scans are ordered
by raw name bytes.

```text
RootCasResult
  applied: bool
  current: optional<bytes>

RawNamedRoot
  name: bytes
  manifest: bytes
```

An applied CAS has `applied = true` and no `current` payload. A conflict has
`applied = false`; `current` contains the exact stored manifest or is absent
when the root is missing. Deletes of missing nodes and unconditional deletes
of missing roots are idempotent.

### Transaction operation

```text
async commit_transaction(node_writes, root_conditions, root_writes)
    -> TransactionResult

RootCondition
  name: bytes
  expected: optional<bytes>

RootWrite
  kind: put | delete
  name: bytes
  manifest: optional<bytes>

TransactionResult
  applied: bool
  conflict: optional<TransactionConflict>

TransactionConflict
  name: bytes
  expected: optional<bytes>
  current: optional<bytes>
```

When `transactions` is true, commit atomically validates all root conditions,
applies all node writes, and applies all root writes. A conflict applies no
writes and returns a typed conflict. An adapter that cannot guarantee the
entire logical operation returns `unsupported` and advertises transactions as
false.

## Rust Binding Changes

The shared native binding adds an asynchronous foreign trait conceptually
named `ForeignRemoteStore`. All method arguments and results are owned
FFI-safe records.

`ForeignRemoteBackend` retains the callback and validated descriptor and
implements `RemoteStoreBackend`. The binding constructs:

```text
RemoteProllyStore<ForeignRemoteBackend>
AsyncProlly<RemoteProllyStore<ForeignRemoteBackend>>
```

A separate exported `AsyncProllyEngine` owns this stack. It does not expand the
current synchronous `ProllyEngine` store enum. Its initial remote surface
includes creation, point and batch reads, put/delete/batch, range pages, diff,
merge, named roots, CAS, scans, GC, sync, and transactions when supported.

The core also adds `OwnedAsyncProllyTransaction<S>`. It owns a cloned store
handle, overlay, and transaction state instead of borrowing a manager. Commit
and rollback consume the live transaction state. Calls after completion return
a stable completed-transaction error. Dropping an uncommitted transaction
performs no provider writes.

The existing synchronous host-store callback remains for compatibility and
small embedded adapters. New database and cloud packages never block through
that callback.

## Language Runtime Bridges

### Node/TypeScript

Node-API sends owned requests to the JavaScript thread through a thread-safe
function. JavaScript invokes the provider adapter, and promise fulfillment or
rejection completes a Rust one-shot channel. Cancellation invalidates the
pending completion token and safely ignores late promise results.

### Python

UniFFI asynchronous foreign traits call Python coroutines. Synchronous SDKs
run in a bounded executor and never on the asyncio event-loop thread.

### Go

The public API accepts `context.Context`. The cgo layer uses start-operation
and completion handles. Context cancellation cancels the Rust future and the
provider request when its SDK supports cancellation.

### Kotlin and Java

Kotlin owns the JVM implementation and exposes suspending operations. Blocking
JDBC and SQLite calls execute on a bounded dispatcher. Java artifacts provide
thin `CompletableFuture` facades and do not duplicate provider logic.

### Ruby

The generated completion bridge invokes asynchronous adapters and uses a
bounded worker pool for synchronous gems. Completion handles prevent callbacks
from resolving more than once.

### Swift

The API uses `async throws`. Blocking client calls execute outside Swift's
cooperative executor. Server/cloud packages are restricted to macOS and
server-side Swift; iOS receives SQLite only.

### Browser/WASM

`wasm-bindgen` futures await JavaScript promises. Implementations may be
non-`Send` and operate on the browser thread. Browser packages cover
IndexedDB, OPFS, and PGlite only.

## Provider Layouts and Capabilities

Each provider has one versioned physical layout shared with its Rust adapter.

| Provider | Canonical layout | Atomicity model |
| --- | --- | --- |
| SQLite | `prolly_nodes`, `prolly_hints`, and `prolly_roots` BLOB tables | SQLite transactions |
| PostgreSQL | Existing three-table `BYTEA` schema | SQL transactions and locked root CAS |
| MySQL | Existing `VARBINARY`/`LONGBLOB` schema | SQL transactions and conditional root updates |
| Redis | Binary `node:`, `hint:`, and `root:` key families under a prefix | Lua or `MULTI` transaction scripts; production use requires documented persistence settings |
| DynamoDB | One table with binary `pk` and `value` attributes | Conditional root writes and bounded transactional writes |
| Cosmos DB | One `/kind` partition containing node, root, and hint documents | ETag CAS and single-partition transactional batches |
| Spanner | `ProllyNodes`, `ProllyHints`, and `ProllyRoots` tables | Read-write transactions and mutations |
| PGlite | PostgreSQL-compatible three-table schema | PGlite transactions |
| IndexedDB | Versioned node, hint, and root object stores | IndexedDB read-write transactions |
| OPFS | Binary node objects and versioned root metadata | Conservative; no strict multi-object transaction claim |

Provider constraints are part of the protocol descriptor:

- DynamoDB batch writes contain at most 25 items per provider request. A larger
  logical batch is chunked and is not advertised as atomic.
- DynamoDB ordered batch reads preserve caller order after provider chunking
  and retries of unprocessed items.
- Cosmos DB strict transactions stay within one `/kind` partition and at most
  100 operations.
- PostgreSQL, MySQL, SQLite, PGlite, and Spanner use native transactions for
  strict node-plus-root commits.
- Redis transaction capabilities are conservative, and its operational
  documentation states the persistence configuration required for primary
  storage.
- OPFS does not advertise strict transactions or crash-atomic multi-object
  publication.

SQL table names, cloud attribute names, binary prefixes, encodings, and
partition rules are normative. Adapters do not invent a new layout under an
official provider name. Initialization is non-destructive and safe under
concurrent startup. Breaking layouts require a new schema version and an
explicit migration.

## End-to-End Data Flow

1. The application constructs a provider adapter and injects its SDK client or
   pool.
2. The core validates protocol major, schema version, capabilities, and limits.
3. `AsyncProlly` plans tree work and requests ordered node batches through the
   foreign bridge.
4. The language adapter executes provider operations and returns opaque bytes.
5. Rust verifies every fetched node CID and decodes canonical manifests.
6. Rust computes canonical replacement nodes.
7. The adapter stores immutable nodes and publishes a named root through CAS or
   a strict transaction.
8. The binding returns portable tree and root records to the application.

Applications own injected clients. Closing an engine releases its adapter
reference but does not close a shared injected client. An owned-client
constructor may expose explicit close behavior. A foreign store never retains
its engine, preventing reference cycles.

## Error, Retry, and Security Contract

```text
StoreError
  code: StoreErrorCode
  message: string
  retryable: bool
  provider_code: optional<string>

StoreErrorCode
  invalid_argument
  invalid_data
  unavailable
  permission_denied
  resource_exhausted
  unsupported
  cancelled
  internal
```

Missing nodes and roots are optional results. CAS and transaction conflicts
are result variants. Unexpected host exceptions become `internal` errors and
never unwind across FFI.

Provider packages own SDK retries. Rust does not automatically retry an
ambiguous root CAS or transaction commit. Reads and idempotent node puts may be
retried according to the injected SDK policy. Calculable provider limits are
checked before sending a request.

Credentials, authorization headers, account keys, connection passwords, and
full connection URLs are removed from messages before they cross FFI. Returned
node bytes remain untrusted until Rust validates their CID.

Every pending call has one completion token and exactly one terminal state.
Cancellation discards late completions safely and releases all owned buffers.

## Conformance and Compatibility Testing

Every runtime implements the shared fake store before a real provider. The
fake can delay operations, inject structured failures, simulate partial
non-atomic batches, return malformed data, force CAS and transaction conflicts,
and complete after cancellation.

The shared protocol suite covers:

- missing, inserted, updated, and deleted nodes;
- duplicate and missing ordered batch reads;
- CID length and node-hash mismatch rejection;
- batch semantics and claimed atomicity;
- optional hints and correct stale-hint fallback;
- deterministic node and root scans;
- root manifest round trips and every CAS state;
- transaction success and rollback on conflict;
- capability-gated unsupported behavior;
- provider limit enforcement;
- error classification and secret redaction;
- cancellation and late completion;
- bounded parallel reads;
- GC and missing-node sync when scans are supported.

Every provider slice also runs:

1. provider-native unit tests;
2. emulator or container integration tests where available;
3. Rust-write/language-read compatibility;
4. language-write/Rust-read compatibility;
5. cross-language CAS races that produce exactly one winner;
6. strict transaction rollback tests;
7. package installation and dependency-isolation tests;
8. batching tests that reject one FFI round trip per requested CID;
9. lifecycle tests requiring zero leaked engines, transactions, buffers, and
   pending completions.

Managed-cloud cases run in credentialed CI. Ordinary pull requests use local
emulators, containers, and fakes. Compilation alone never qualifies a provider
cell as supported.

## Provider-First Delivery

### Phase 0: protocol foundation

- Add protocol records, capability and limit records, errors, and fixtures.
- Add native and WASM foreign-store bridges.
- Add `AsyncProllyEngine` and `OwnedAsyncProllyTransaction`.
- Run fake-store conformance in every runtime.

### Phase 1: SQLite

- Implement Node, Python, Go, JVM, Ruby, and Swift packages.
- Validate the existing SQLite physical layout bidirectionally with Rust.
- Run synchronous drivers on bounded workers.

### Phase 2: PostgreSQL and PGlite

- Implement PostgreSQL across all native/server languages.
- Implement PGlite for Node and browser/WASM using the same logical SQL schema.
- Establish the full cross-language transaction and CAS race harness.

### Phase 3: MySQL

- Implement all native/server language packages.
- Verify binary ordering, length limits, and conditional root updates.

### Phase 4: Redis

- Implement all native/server language packages.
- Test Lua or transaction scripts and document required persistence settings.

### Phase 5: DynamoDB

- Implement all native/server language packages.
- Test ordered chunked reads, unprocessed items, conditional roots, and
  transaction limits.

### Phase 6: Cosmos DB

- Implement Node, Python, Go, and JVM packages.
- Test `/kind` partition compatibility, ETags, and 100-operation transaction
  limits.

### Phase 7: Spanner

- Implement Node, Python, Go, JVM, and Ruby packages.
- Test GoogleSQL layout, mutations, and read-write transaction conflicts.

### Phase 8: browser-native stores

- Implement IndexedDB and OPFS packages.
- Run the same logical protocol cases with their conservative capabilities.

### Phase 9: release completion

- Publish the checked compatibility manifest.
- Deprecate built-in SQLite constructors after every external SQLite package
  passes conformance.
- Verify registry artifacts contain only their intended provider SDKs.

## Release Gates

A provider phase is complete only when every supported cell in that phase:

- passes the protocol suite;
- reads data written by the Rust adapter;
- writes data read by the Rust adapter;
- preserves canonical roots and CIDs;
- reports exact capabilities and limits;
- passes cancellation and leak tests;
- passes package installation tests;
- documents schema, migration, credentials, ownership, durability, and limits.

The complete program is done when every `yes` cell in the support matrix meets
these gates, unsupported cells are explicit, core packages contain no provider
SDKs, and browser packages contain no direct server/cloud transports.

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Async callback deadlock | Runtime-specific completion bridges; never block an event loop waiting for a foreign future |
| Cross-language schema drift | One normative provider schema and bidirectional Rust compatibility tests |
| Capability overstatement | Conservative defaults and capability-specific conformance cases |
| Provider batch limits | Descriptor limits, preflight validation, ordered chunking, and lowered atomicity claims |
| Cancellation leaks | Single-use completion tokens and late-completion tests |
| Core dependency growth | Separate provider artifacts and installation dependency audits |
| Secret exposure | Structured redacted errors and explicit negative tests |
| Incomplete matrix presented as parity | Checked compatibility manifest and per-cell release gates |

## References

- [`docs/language-store-adapters-design.md`](../../language-store-adapters-design.md)
- [`docs/language-bindings-design.md`](../../language-bindings-design.md)
- [`docs/async-store.md`](../../async-store.md)
- [`docs/wire-format.md`](../../wire-format.md)
- [Google Cloud Spanner client libraries](https://docs.cloud.google.com/spanner/docs/reference/libraries)
- [Azure Cosmos DB SDK overview](https://learn.microsoft.com/en-us/azure/cosmos-db/sdk-python)
- [AWS SDK support for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/sdk-general-information-section.html)
