# Cross-Language Store Adapters Technical Design

- Status: proposed implementation design
- Target protocol: `prolly-store-protocol-v1`
- Applies to: Rust, Node/TypeScript, Python, Go, Java/Kotlin, Swift, Ruby, and
  browser/WASM bindings

## Summary

Language bindings should ship only memory and file stores in their core
packages. Database and cloud stores should ship as independently versioned
adapter packages that use the host language's normal database SDK and implement
one versioned, asynchronous Prolly store protocol.

The Rust engine remains responsible for tree algorithms, node encoding, CID
verification, root-manifest decoding, diff, merge, sync, and garbage
collection. A language adapter is responsible only for mapping protocol
operations to a provider such as DynamoDB, PostgreSQL, MySQL, Redis, Cosmos DB,
or Spanner.

```text
application
    |
    +-- prolly core language package
    |       |
    |       +-- Rust AsyncProlly engine
    |               |
    |               +-- ForeignRemoteStore bridge
    |
    +-- optional language store package
            |
            +-- host database SDK/client
                    |
                    +-- database or cloud service
```

This design prevents database SDKs from becoming transitive dependencies of
the core language bindings, lets applications inject already-configured
clients, and keeps stored nodes and roots interoperable with the existing Rust
`prolly-store-*` crates.

## Context

The repository already contains the main pieces needed for this design:

- `Store` and `Prolly` support embedded synchronous stores.
- `AsyncStore` and `AsyncProlly` support runtime-neutral asynchronous stores.
- `RemoteStoreBackend` and `RemoteProllyStore` separate provider I/O from CID
  verification and manifest semantics.
- The binding facade exposes a synchronous `HostStoreCallback` for custom
  stores.
- Node, Python, Go, Java/Kotlin, Ruby, and Swift already test host-store
  callbacks.

The current callback is not sufficient for remote adapters:

- it is synchronous, while most Node and browser database clients return
  promises;
- the current language-level async facades generally schedule synchronous
  engine calls rather than use `AsyncProlly` end to end;
- it does not expose the strict remote transaction contract;
- it represents failures as optional strings instead of stable error codes;
- compiling concrete Rust adapters into the shared FFI crate would add every
  selected provider SDK to the binding binary.

This design adds an async remote-store boundary rather than expanding the
existing synchronous callback.

## Goals

- Keep the core Rust crate and core language packages free of optional database
  SDKs.
- Let users install only the adapters they use.
- Use idiomatic provider clients, credentials, pools, TLS, tracing, and runtime
  behavior in every host language.
- Preserve Rust's canonical node, CID, manifest, CAS, transaction, sync, and GC
  semantics.
- Permit a store written through one language to be read through another
  language or the Rust adapter for the same provider.
- Support true asynchronous I/O without blocking a language event loop or a
  Rust executor worker.
- Preserve native batching, bounded read parallelism, conditional writes, and
  transactions.
- Provide one reusable conformance suite for official and community adapters.
- Allow adapters and core bindings to release independently through a versioned
  protocol.

## Non-goals

- Reimplementing the Prolly tree algorithms in every language.
- Loading arbitrary Rust dynamic libraries as store plugins.
- Passing Rust store pointers between independently built native libraries.
- Bundling every Rust `prolly-store-*` crate into every language artifact.
- Making physical database schemas identical across different providers.
- Exposing application credentials through the Rust core configuration.
- Supporting direct privileged cloud access from untrusted browser code.
- Guaranteeing strict transactions on a provider that cannot implement them.

## Design Decisions

### 1. Host-native adapters are the default

Remote adapter packages use the host ecosystem's database SDK:

| Language | DynamoDB | PostgreSQL | MySQL |
| --- | --- | --- | --- |
| Node/TypeScript | AWS SDK for JavaScript | `pg` | `mysql2` |
| Python | `boto3` or an async AWS client | `psycopg` | an idiomatic MySQL client |
| Go | AWS SDK for Go | `pgx` or `database/sql` | `database/sql` |
| Java/Kotlin | AWS SDK for Java | JDBC/R2DBC | JDBC/R2DBC |
| Swift | AWS SDK or application client | application-selected driver | application-selected driver |
| Ruby | AWS SDK for Ruby | `pg` | `mysql2` |

The adapter constructor accepts a client or pool whenever practical. The core
never owns credential discovery, connection pooling, TLS roots, proxy settings,
or provider retry configuration.

### 2. The remote boundary is async

The new contract mirrors `RemoteStoreBackend`, not the synchronous `Store`
trait. All I/O operations are asynchronous. Capability accessors are
synchronous because their values are immutable for an adapter instance.

Embedded host adapters may complete operations immediately. They still
implement the same async language interface so applications do not need a
second remote API.

### 3. Rust validates portable semantics

The host adapter returns raw node and manifest bytes. The Rust bridge wraps it
in `RemoteProllyStore`, which continues to:

- verify that each fetched node hashes to its requested CID;
- verify node bytes before writes;
- decode and validate canonical `RootManifest` bytes;
- preserve ordered batch-read positions;
- translate raw root CAS results into Prolly manifest results;
- expose async store, manifest, scan, and transactional capabilities to
  `AsyncProlly`.

Provider packages should not duplicate this logic.

### 4. Core and adapter packages are separate

The core language package declares no database SDK dependencies. Each adapter
package declares the core package and exactly one provider SDK family.

Suggested public package names:

| Ecosystem | Core | Example adapter |
| --- | --- | --- |
| npm | `@prolly/core` | `@prolly/store-dynamodb` |
| PyPI | `prolly` | `prolly-store-dynamodb` |
| Go | core module | separate `store/dynamodb` module |
| Maven | `build.crab:prolly` | `build.crab:prolly-store-dynamodb` |
| RubyGems | `prolly` | `prolly-store-dynamodb` |
| SwiftPM | `Prolly` | separate `ProllyStoreDynamoDB` package/product |

The exact core npm name may retain the existing package name during migration;
the separation rule is normative, not the spelling of the core package.

### 5. Provider schemas are cross-language contracts

Each provider has one schema and namespace layout shared by its Rust and
language adapters. For example, every PostgreSQL adapter uses the same node,
root, and hint tables as `prolly-store-postgres`. Every DynamoDB adapter uses
the same binary key prefixes and attribute names as
`prolly-store-dynamodb`.

Schema definitions move into provider-specific normative documents or
machine-readable migration files. An adapter must not invent a new physical
layout under an existing official package name.

## Protocol Model

`prolly-store-protocol-v1` is a logical protocol. It defines operations,
values, ordering, atomicity, and errors. Provider schema documents define how
those operations are persisted.

All byte fields are opaque byte strings. Foreign bindings must not convert
keys, CIDs, root names, manifests, or node values through text encodings.

### Protocol metadata

Every adapter returns immutable metadata when constructed:

```text
StoreDescriptor
  protocol_major: u32            must be 1
  adapter_name: string           stable implementation name
  provider: string               e.g. "postgres" or "dynamodb"
  capabilities: StoreCapabilities

StoreCapabilities
  native_batch_reads: bool
  atomic_batch_writes: bool
  node_scan: bool
  hints: bool
  atomic_nodes_and_hint: bool
  root_scan: bool
  root_compare_and_swap: bool
  transactions: bool
  read_parallelism: u32          at least 1
```

Construction fails before the engine is created if `protocol_major` is not
supported. Capabilities must not change during the adapter's lifetime.

### Node operations

```text
async get_node(cid: bytes) -> optional<bytes>
async put_node(cid: bytes, node: bytes)
async delete_node(cid: bytes)
async batch_nodes(ops: sequence<NodeMutation>)
async batch_get_nodes_ordered(cids: sequence<bytes>)
    -> sequence<optional<bytes>>
async list_node_cids() -> sequence<bytes>

NodeMutation
  kind: "upsert" | "delete"
  cid: bytes
  node: optional<bytes>           required for upsert
```

Requirements:

- A CID is exactly 32 bytes.
- `batch_get_nodes_ordered` returns exactly one result per input position,
  including duplicate and missing CIDs.
- `list_node_cids` returns only node CIDs, sorted by raw bytes.
- Node upserts are idempotent.
- When `atomic_batch_writes` is true, `batch_nodes` applies all mutations or
  none.
- When `node_scan` is false, `list_node_cids` returns `Unsupported`; GC APIs
  requiring scans are unavailable, but ordinary tree operations still work.

Provider adapters may chunk requests internally to respect service limits.
For example, DynamoDB may split a logical batch into provider-sized calls. The
capability must report `atomic_batch_writes = false` if the complete logical
batch is not atomic.

### Hint operations

```text
async get_hint(namespace: bytes, key: bytes) -> optional<bytes>
async put_hint(namespace: bytes, key: bytes, value: bytes)
async batch_put_nodes_with_hint(
  entries: sequence<NodeEntry>,
  namespace: bytes,
  key: bytes,
  value: bytes
)

NodeEntry
  cid: bytes
  node: bytes
```

Hints are optional and never affect correctness. An adapter with `hints =
false` may return missing values and treat writes as no-ops. If
`atomic_nodes_and_hint` is false, the combined operation writes nodes before
the hint. A stale or absent hint must always have a correct traversal fallback.

### Root-manifest operations

```text
async get_root_manifest(name: bytes) -> optional<bytes>
async put_root_manifest(name: bytes, manifest: bytes)
async delete_root_manifest(name: bytes)
async compare_and_swap_root_manifest(
  name: bytes,
  expected: optional<bytes>,
  replacement: optional<bytes>
) -> RootCasResult
async list_root_manifests() -> sequence<RawNamedRoot>

RootCasResult
  applied: bool
  current: optional<bytes>

RawNamedRoot
  name: bytes
  manifest: bytes
```

Requirements:

- Manifest values are the canonical bytes defined by the Rust
  `RootManifest` encoder.
- Root names are arbitrary bytes.
- Root CAS compares exact manifest bytes, including missing state.
- An applied delete returns `applied = true`.
- A conflict returns `applied = false` and the current value, or missing state.
- Root CAS is one atomic provider operation. Adapters that cannot guarantee it
  must report `root_compare_and_swap = false`; publishing with optimistic
  concurrency is then unavailable.
- Root scans are sorted by raw root-name bytes.

### Transaction operations

```text
async commit_transaction(
  node_writes: sequence<NodeMutation>,
  root_conditions: sequence<RootCondition>,
  root_writes: sequence<RootWrite>
) -> TransactionResult

RootCondition
  name: bytes
  expected: optional<bytes>

RootWrite
  kind: "put" | "delete"
  name: bytes
  manifest: optional<bytes>       required for put

TransactionResult
  kind: "applied" | "conflict"
  conflict: optional<TransactionConflict>

TransactionConflict
  name: bytes
  expected: optional<bytes>
  current: optional<bytes>
```

When `transactions = true`, the operation must atomically:

1. validate every root condition;
2. apply all node writes;
3. apply all root writes.

On conflict it applies no writes. Providers may return the first conflicting
root. When `transactions = false`, the method returns `Unsupported` and the
binding does not expose strict engine transactions for that store.

PostgreSQL, MySQL, and Spanner should implement this with native transactions.
DynamoDB may implement it with transactional writes within provider item and
request limits. Redis requires a durable configuration plus Lua or
`WATCH`/`MULTI`; capability advertising must be conservative.

## Error Contract

Foreign adapters return a structured store error:

```text
StoreError
  code: StoreErrorCode
  message: string
  retryable: bool

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

Missing nodes and roots are normal optional results, not errors. CAS and
transaction conflicts are normal result variants, not errors.

Provider retries belong in the adapter or its SDK. The Rust engine must not
automatically retry root CAS or a transaction after an ambiguous failure.
Content-addressed node puts are idempotent, but adapter retry policy must still
respect cancellation and provider limits.

Unexpected foreign exceptions must become `StoreErrorCode.internal`; they
must never unwind or panic across the FFI boundary.

## Rust Binding Architecture

### Foreign trait

Add a new foreign async trait to `bindings/uniffi` conceptually shaped as:

```rust
#[uniffi::export(foreign)]
#[async_trait::async_trait]
pub trait ForeignRemoteStore: Send + Sync {
    fn descriptor(&self) -> StoreDescriptorRecord;

    async fn get_node(&self, cid: Vec<u8>)
        -> Result<Option<Vec<u8>>, ForeignStoreError>;
    async fn put_node(&self, cid: Vec<u8>, node: Vec<u8>)
        -> Result<(), ForeignStoreError>;
    // The production trait contains every protocol method.
}
```

Use a foreign trait rather than the legacy callback-interface annotation. All
arguments cross by value because foreign traits cannot borrow Rust slices. The
production definition contains every protocol method and uses records for
mutations, CAS, roots, and transaction results. It implements conversion from
unexpected UniFFI callback errors into `ForeignStoreError`, the Rust/FFI
representation of the protocol's structured `StoreError`.

### Bridge to the Rust engine

Add a concrete `ForeignRemoteBackend`:

```text
ForeignRemoteBackend
  callback: Arc<dyn ForeignRemoteStore>
  descriptor: validated StoreDescriptor
```

It implements `RemoteStoreBackend` by converting borrowed Rust inputs to owned
FFI records and awaiting the foreign trait. Then construct:

```text
RemoteProllyStore<ForeignRemoteBackend>
AsyncProlly<RemoteProllyStore<ForeignRemoteBackend>>
```

This concrete wrapper avoids requiring `AsyncStore` itself to become object
safe and reuses the existing remote validation layer.

### Async engine object

Export a separate `AsyncProllyEngine` binding object. Do not add an async store
variant to the current synchronous `ProllyEngine` enum.

Required constructor:

```text
async ProllyEngine.open_remote(
  store: ForeignRemoteStore,
  config: ConfigRecord
) -> AsyncProllyEngine
```

All I/O-bearing methods on `AsyncProllyEngine` are true exported async Rust
methods. The foreign runtime polls the Rust future; the Rust future awaits the
foreign store implementation. No Tokio dependency is added to `prolly-map` or
to the generic UniFFI bridge.

The first implementation must cover the minimum useful remote surface:

- create, get, get-many, put, delete, and batch;
- range and range page;
- diff and merge;
- named-root load, list, publish, delete, and CAS;
- list-node-CIDs, GC planning/sweep, and missing-node sync when supported;
- transaction begin/commit when the adapter advertises transactions.

The remaining `AsyncProlly` surface can then be exported using the existing
binding records.

### FFI-safe async transactions

The current `AsyncProllyTransaction<'a, S>` borrows its manager and therefore
cannot be stored in a long-lived foreign object. Before exposing transaction
objects, add `OwnedAsyncProllyTransaction<S>` to the core, paralleling the
existing owned synchronous transaction.

The owned form requires `S: Clone`, owns a cloned store handle plus transaction
state, and uses an owned async overlay store. `RemoteProllyStore` and
`ForeignRemoteBackend` are cloneable because provider clients are retained by
reference-counted handles.

The binding exposes an `AsyncProllyTransaction` object whose methods stage
create/get/put/delete/batch and named-root operations. `commit` and `rollback`
atomically consume its internal transaction; every later call returns a
completed-transaction error. Dropping an uncommitted foreign transaction is a
rollback and performs no provider writes.

### Existing synchronous custom stores

Keep `HostStoreCallback` for backward compatibility and simple synchronous
host stores. Rename it at the handwritten language facade level to
`SyncHostStore` when a breaking binding release permits it.

Do not implement remote Node clients by blocking inside `HostStoreCallback`.
Do not call a JavaScript promise and wait synchronously for its completion.

### Removing SQLite from the core binding

The current UniFFI crate enables SQLite by default. Change the next binding
major release to:

```toml
[features]
default = []
```

Memory and file remain constructors on the core binding. SQLite moves to a
language adapter package. If already-published language packages require a
migration window, retain their SQLite constructor for one deprecated minor
release, but new core artifacts must not make SQLite a default dependency.

## Runtime-Specific Bridges

### Python, Kotlin, Swift, and Ruby

Use UniFFI async foreign traits and exported async functions. The generated
binding runs the adapter coroutine/future on the foreign runtime and completes
the Rust future through UniFFI's completion callback.

Python adapters provide both an async-native implementation and, where useful,
a documented worker wrapper for synchronous drivers. A synchronous driver must
not run on the asyncio event-loop thread.

Java consumes the Kotlin/JVM artifact. Its handwritten facade converts
`CompletableFuture` to and from the generated suspend API without blocking.

### Node/TypeScript

The Node package continues to use Node-API. Add a dedicated async host-store
bridge in `bindings/node/native`:

1. Rust sends an owned request to the JavaScript main thread through a
   thread-safe function.
2. JavaScript invokes the adapter method and resolves its promise.
3. Promise fulfillment or rejection completes a Rust one-shot channel.
4. `ForeignRemoteBackend` awaits that channel.
5. Cancellation drops or marks the pending request and ignores late promise
   completion safely.

The Node native crate may depend on a runtime integration feature; that
dependency stays in the Node artifact and does not enter `prolly-map`.

The TypeScript interface uses native `Promise` results and `Uint8Array` values.

### Go

Expose context-aware handwritten methods. The cgo bridge uses the same
start-operation/completion model as the UniFFI async ABI. Cancelling a Go
context signals cancellation to the Rust future and, when supported, the
provider SDK call.

Adapter methods accept `context.Context` in the public Go API even if the
generated internal interface represents cancellation with a handle.

### Browser/WASM

Use `wasm-bindgen` futures and JavaScript promises. Browser-first official
adapters are IndexedDB, OPFS, and PGlite. Direct DynamoDB, PostgreSQL, or MySQL
access is not an official browser target because credentials and network
protocol support usually belong behind an application service.

WASM adapters may be non-`Send`. The WASM bridge is separate from the native
`Send + Sync` foreign trait but implements the same logical protocol and runs
the same conformance cases.

## Public Language API

Language facades should hide generated FFI records where idiomatic wrappers
are useful, but must preserve byte values and optional states.

TypeScript example:

```ts
import { AsyncProlly } from "@prolly/core";
import { DynamoDBStore } from "@prolly/store-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const store = new DynamoDBStore({
  client: new DynamoDBClient({}),
  tableName: "prolly",
  keyPrefix: new TextEncoder().encode("tenant-42:"),
});

await store.initializeSchema();
const db = await AsyncProlly.open({ store });
const tree = await db.create();
```

Python example:

```python
from prolly import AsyncProlly
from prolly_store_postgres import PostgresStore

store = await PostgresStore.connect(pool=pool)
await store.initialize_schema()
db = await AsyncProlly.open(store=store)
tree = await db.create()
```

Go example:

```go
store, err := prollypostgres.New(pool)
if err != nil { return err }

db, err := prolly.OpenRemote(ctx, store, prolly.DefaultConfig())
```

Configuration rules:

- prefer injecting an existing client or pool;
- provide convenience `connect` constructors only in adapter packages;
- use the provider's documented isolation mechanism: a binary key prefix where
  supported, or a dedicated SQL database/schema/table set where it is not;
- never log credentials or full connection URLs;
- expose schema initialization explicitly rather than performing hidden DDL on
  the first read;
- document whether schema initialization is safe under concurrent startup.

## Repository Layout

Keep store packages close to their language build tooling:

```text
bindings/
  uniffi/                         async foreign-store bridge
  node/stores/{dynamodb,postgres,mysql}/
  python/stores/{dynamodb,postgres,mysql}/
  go/stores/{dynamodb,postgres,mysql}/
  java/stores/{dynamodb,postgres,mysql}/
  kotlin/stores/
  ruby/stores/
  swift/stores/

conformance/
  store-protocol-v1/
    cases.json
    failure-cases.json
    providers/
      postgres.md
      mysql.md
      dynamodb.md
```

Go adapters use separate `go.mod` files when dependency isolation requires
them. Other language package managers may require a slightly different
physical project boundary, but provider SDK dependencies must not be added to
the core package manifest.

## Provider Compatibility

For each official provider, publish a normative schema document containing:

- schema version;
- tables, collections, or key prefixes;
- binary column and attribute types;
- namespace/prefix construction;
- root CAS implementation;
- transaction guarantees and limits;
- batch limits and chunking behavior;
- scan ordering;
- schema initialization and migration procedure;
- compatibility test vectors.

Version 1 provider layouts are the layouts already implemented by their Rust
adapter crates. Before publishing a language adapter, extract those layouts
into migrations or documents and make both Rust and language tests consume the
same definition where practical.

Schema changes follow expand/migrate/contract rules. A package must not make a
destructive schema change during ordinary initialization. Breaking physical
layout changes require a new schema version and a migration command.

### PostgreSQL schema v1

PostgreSQL adapters use the existing Rust schema exactly:

```sql
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid bytea PRIMARY KEY,
  node bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace bytea NOT NULL,
  key bytea NOT NULL,
  value bytea NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name bytea PRIMARY KEY,
  manifest bytea NOT NULL
);
```

Node batches, root CAS, and protocol transactions use database transactions.
Scans order `cid` or `name` by their binary value. Because table names are
fixed in schema v1, tenant isolation uses a dedicated database/schema or an
explicit future table-set option shared by every implementation; adapters must
not silently prepend tenant bytes to CIDs or root names.

### MySQL schema v1

MySQL adapters use the existing Rust schema exactly:

```sql
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid VARBINARY(32) PRIMARY KEY,
  node LONGBLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace VARBINARY(255) NOT NULL,
  `key` VARBINARY(255) NOT NULL,
  value LONGBLOB NOT NULL,
  PRIMARY KEY(namespace, `key`)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name VARBINARY(255) PRIMARY KEY,
  manifest LONGBLOB NOT NULL
);
```

Node batches, root CAS, and protocol transactions use database transactions.
Adapters must use binary parameters and ordering; they must not route these
columns through a connection collation or text conversion. Namespaces and hint
keys longer than 255 bytes are invalid for this schema version.

### DynamoDB schema v1

DynamoDB adapters use one table with:

- binary partition key `pk`;
- no sort key;
- binary payload attribute `value`;
- on-demand billing when the adapter creates the table.

The default application prefix is the bytes `prolly:`. Callers may replace it
with an arbitrary binary prefix. Item keys are concatenated without text or
base64 conversion:

```text
node item: prefix || "node:" || cid
root item: prefix || "root:" || root_name
hint item: prefix || "hint:" || u64be(len(namespace)) || namespace || hint_key
```

Reads that participate in protocol behavior are strongly consistent. Adapter
request limits match the provider and current Rust adapter:

- batch get: 100 items per provider request;
- batch write: 25 items per provider request;
- transaction write: 100 items per transaction;
- unprocessed batch items are retried with a bounded policy.

A logical batch larger than one batch-write request is not atomic. Root CAS
uses a conditional put/delete. `commit_transaction` uses a DynamoDB transaction
and returns `resource_exhausted` before sending when the logical transaction
exceeds the supported item count.

### Initial capability matrix

The first official implementations advertise capabilities conservatively:

| Capability | PostgreSQL | MySQL | DynamoDB |
| --- | --- | --- | --- |
| Native/coalesced batch reads | yes | yes | yes |
| Atomic logical node batch | yes | yes | no |
| Node scan | yes | yes | yes |
| Hints | yes | yes | yes |
| Atomic nodes plus hint | yes | yes | no |
| Root scan | yes | yes | yes |
| Atomic root CAS | yes | yes | yes |
| Strict transactions | yes | yes | yes, up to provider limit |

An adapter must advertise a lower capability if its chosen client or deployment
cannot provide the listed behavior.

## Conformance Testing

### Protocol contract suite

Every adapter runs the same logical cases:

- missing, inserted, updated, and deleted node behavior;
- CID length rejection and Rust-side CID mismatch detection;
- duplicate and missing ordered batch reads;
- batch last-write behavior and advertised atomicity;
- deterministic node scans;
- optional hint behavior and stale-hint fallback;
- root manifest round trips;
- missing/expected/value/delete root CAS cases;
- root scan ordering;
- transaction success and rollback on conflict;
- unsupported capability behavior;
- structured error propagation;
- cancellation and late completion;
- bounded parallel reads;
- GC and missing-node sync when scans are supported.

### Cross-language compatibility suite

For every official provider:

1. initialize the schema with the Rust adapter;
2. write nodes and named roots with Rust;
3. read and mutate them through each language adapter;
4. verify roots and values again with Rust;
5. reverse the writer and reader roles;
6. race root CAS from two implementations and require exactly one winner.

PostgreSQL, MySQL, Redis, and local DynamoDB tests run in the existing service
containers. Managed-cloud tests run separately with explicit credentials and
must not be required for ordinary pull requests.

### Failure injection

The reference fake adapter supports delayed reads, configured failures,
partial non-atomic batches, malformed bytes, CAS/transaction conflicts, and
cancellation before or after provider completion.

Run the fake adapter in every language binding before adding a real provider.
This isolates FFI correctness from database behavior.

## Performance Requirements

- Adapter packages must implement native batch APIs when the provider has
  them.
- `batch_get_nodes_ordered` must avoid one FFI round trip per CID.
- `read_parallelism` must be bounded and default conservatively.
- Rust's node cache remains above the foreign adapter boundary.
- Large ranges and diffs stay inside Rust; only node batches and final result
  pages cross FFI.
- Adapters expose provider metrics through their native SDK rather than
  expanding the core protocol in version 1.

Benchmarks compare the Rust provider adapter with the language adapter through
async FFI for cold/warm point lookup, get-many, batch update, range page, diff,
root CAS, and sync workloads. The initial release has no universal latency
target, but it must demonstrate that batching avoids linear FFI-call growth
for get-many and batch workloads.

## Security and Resource Ownership

- Applications own provider clients and credential configuration.
- Adapters may clone or retain safe client handles for their lifetime.
- Closing an engine releases its foreign store reference but does not close an
  injected shared client unless the adapter explicitly owns it.
- Foreign store objects must not retain the engine, preventing reference
  cycles across FFI.
- Every pending async call has exactly one completion path.
- Late completions after cancellation are ignored and released safely.
- Returned node bytes are untrusted until Rust verifies their CID.
- Provider errors must redact secrets, authorization headers, and connection
  strings before crossing the FFI boundary.

## Versioning and Release Policy

There are four independent compatibility versions:

- `prolly-map` Rust API version;
- language binding API/ABI version;
- `prolly-store-protocol` major version;
- provider physical schema version.

Adapter packages declare the supported core binding range and protocol major.
A core package may support more than one protocol major during migrations.

Release ordering:

1. publish a core language binding that supports the protocol;
2. publish or update adapter packages;
3. run installation tests against registry artifacts;
4. publish compatibility documentation.

Adapters do not need lockstep patch versions with core. Removing a protocol
method, changing record meaning, or changing atomicity requirements requires a
new protocol major.

## Migration from Current Bindings

1. Add the async protocol alongside the existing synchronous callback.
2. Add `AsyncProllyEngine.open_remote` without changing current constructors.
3. Build and test at least one external adapter package.
4. Deprecate core SQLite constructors in published language packages.
5. Change the binding crate's default feature set to empty in the next allowed
   breaking release.
6. Publish SQLite as an adapter package for each supported native language.
7. Remove deprecated core constructors after the documented migration window.

Existing memory, file, and synchronous custom-store APIs remain available.
Rust users continue to use the independently published `prolly-store-*`
crates directly.

## Implementation Plan

### Phase 0: Protocol and documentation

- Add protocol records and capability definitions.
- Extract PostgreSQL, MySQL, and DynamoDB schemas into normative documents or
  migrations.
- Add protocol fixtures and a language-neutral fake-store behavior file.
- Update existing binding documentation so SQLite is no longer a required
  core dependency.

Exit criteria: reviewers approve operation semantics, transaction guarantees,
error codes, package boundaries, and schema ownership.

### Phase 1: Rust and UniFFI vertical slice

- Add `ForeignRemoteStore` as an async foreign trait.
- Add `ForeignRemoteBackend` implementing `RemoteStoreBackend`.
- Add `AsyncProllyEngine.open_remote`.
- Add `OwnedAsyncProllyTransaction` and the owned async overlay store.
- Export create, put, get, batch, range page, and named-root CAS.
- Implement an in-memory async foreign fake in Python or Kotlin.
- Add cancellation, error, and lifetime tests.

Exit criteria: an async store implemented outside Rust drives `AsyncProlly`
without blocking and passes the fake-store protocol suite.

### Phase 2: Node/TypeScript PostgreSQL reference adapter

- Implement the Node promise/one-shot bridge.
- Publish the TypeScript store interface.
- Implement `@prolly/store-postgres` with injected `pg` pool support.
- Reuse the Rust PostgreSQL schema.
- Run bidirectional Rust/TypeScript compatibility tests.

PostgreSQL is first because its transactional semantics are strong, its local
test service is simple, and it exercises every protocol capability.

Exit criteria: Node can perform remote tree operations, strict transactions,
root CAS, GC, and sync against a database also readable by Rust.

### Phase 3: DynamoDB and MySQL for Node/TypeScript

- Implement provider packages.
- Document DynamoDB non-atomic batch and transactional-write limits.
- Validate MySQL transaction isolation and binary ordering.
- Add registry installation tests proving the core npm package does not install
  AWS, PostgreSQL, or MySQL SDKs.

### Phase 4: Python and Go

- Implement Python async foreign-store facade and PostgreSQL/DynamoDB/MySQL
  packages.
- Implement Go context/cancellation bridge and isolated adapter modules.
- Run the same cross-language compatibility suite.

### Phase 5: JVM, Ruby, and Swift

- Implement Kotlin suspend adapters and Java `CompletableFuture` facades.
- Add Ruby and Swift according to provider ecosystem demand.
- Keep official provider coverage explicit rather than claiming every
  language/store combination automatically.

### Phase 6: Browser stores and remaining providers

- Implement IndexedDB, OPFS, and PGlite adapters for WASM/browser use.
- Add Redis, Cosmos DB, Spanner, and other providers based on demand.
- Consider an HTTP gateway adapter for browsers and thin clients.

## Acceptance Criteria

The design is implemented when:

- core language packages install without optional database SDKs;
- the UniFFI core binding has no default SQLite dependency;
- a true async foreign store can drive Rust `AsyncProlly` end to end;
- Node promise callbacks never block the JavaScript event loop;
- provider capabilities correctly gate scans, CAS, hints, and transactions;
- Rust verifies every fetched node against its CID;
- adapter errors and cancellation cross FFI without panics or leaked handles;
- PostgreSQL, MySQL, and DynamoDB have at least one language adapter passing the
  protocol suite;
- Rust and language adapters can read and update the same provider storage;
- adapter SDKs appear only in adapter package dependency graphs;
- published package installation tests pass on supported platforms.

## Rejected Alternatives

### Compile all Rust adapters into the shared binding

Rejected because it adds large provider SDKs, native dependencies, runtimes,
and platform build requirements to every user.

### Publish a native Rust store plugin and pass its handle to core

Rejected for version 1 because independent native modules cannot safely share
Rust trait objects or UniFFI object handles without a separately stabilized C
plugin ABI. Such an ABI would add allocator, lifetime, panic, symbol, and
versioning complexity.

### Use only the existing synchronous `HostStoreCallback`

Rejected for remote stores because blocking promise-based or event-loop SDKs
can deadlock and lose true async traversal and read concurrency.

### Implement tree algorithms in each adapter package

Rejected because it creates semantic drift in node encoding, boundaries,
merge, proofs, GC, and sync. Rust remains the single engine.

### Require a Prolly storage gateway service

Rejected as the default because many applications can connect directly to
their database and should not need another service. A gateway can later
implement the same logical protocol for browsers or restricted clients.

## References

- [`async-store.md`](async-store.md): Rust async store and `AsyncProlly` design.
- [`language-bindings-design.md`](language-bindings-design.md): general binding
  architecture and parity requirements.
- [`wire-format.md`](wire-format.md): canonical node, CID, and manifest bytes.
- [`architecture.md`](architecture.md): tree, store, manifest, GC, and sync
  layers.
- [UniFFI async overview](https://mozilla.github.io/uniffi-rs/latest/internals/async-overview.html).
- [UniFFI foreign traits](https://mozilla.github.io/uniffi-rs/latest/foreign_traits.html).
- [UniFFI async/future support](https://mozilla.github.io/uniffi-rs/latest/futures.html).
