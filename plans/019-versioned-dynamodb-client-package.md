# Versioned DynamoDB Rust Client Package Technical Design

> **For implementers:** execute phases in order. Each phase has an independently
> usable deliverable, verification plan, exit gate, and rollback boundary.

**Goal:** Publish a Rust client crate that follows the AWS SDK for Rust DynamoDB
fluent call shape, executes logical operations directly through the existing
Rust `prolly-store-dynamodb` adapter, and adds immutable table history, diff,
restore, and optimistic table-version controls without requiring a proxy or
compatible service deployment.

**Reference client:** Rust 1.91.1+, Tokio, `aws-sdk-dynamodb = 1.73.0`, and
`prolly-store-dynamodb = 0.6.0`, matching the current Rust store adapter's
dependency line. Later compatible dependency ranges require explicit compile
and conformance testing.

**Relationship to plan 018:** This plan is the active and sole product design
and has no dependency on `018-versioned-dynamodb-compatible-service.md`. Plan
018 is a superseded contingency, not a planned deliverable. The Rust client and
logical core must be implementable, testable, and releasable without any
service crate, HTTP protocol, or server executor. Service work is excluded from
this plan's phases, estimates, acceptance criteria, and release gates.

**Deployment decision:** Ship and operate the client-side package without a
Versioned DynamoDB compatibility service. The client-side package is the sole
required product boundary and owns all logical DynamoDB and versioning
semantics through the shared Rust core. A future server is justified only by a
separate requirement for DynamoDB wire compatibility, non-Rust clients, or an
independent credential/policy boundary; it must not be introduced as an
internal hop for Rust client requests.

**Delivery consequence:** Completing this plan completes the Versioned
DynamoDB product. There is no subsequent service phase, service migration, or
server deployment gate. Applications link the Rust client, and that client
executes directly against the configured `prolly-store-dynamodb` backend.

**Architecture decision:** A compatible server is not part of the Versioned
DynamoDB design or delivery roadmap. The in-process Rust client is sufficient
because it owns the same logical core, durable metadata, worker coordination,
and physical DynamoDB adapter that a wrapper service would call. Adding a
service would change deployment and trust boundaries, but would not add
versioning capability. Plan 018 is therefore evidence retained for a rejected
alternative, not a second component of this system.

The only reasons to propose a server later are requirements that cannot be met
by a linked Rust crate: unmodified non-Rust AWS SDK clients, DynamoDB wire
protocol compatibility, or an independently administered network and security
boundary. Such a proposal requires a new architecture decision and plan; it is
not an extension or completion phase of this design.

### Normative deployment contract

The production data path is entirely client-side:

```text
Rust application
  -> prolly_dynamodb_client::Client
  -> prolly_dynamodb_core
  -> prolly_store_dynamodb::DynamoDbBackend
  -> ordinary AWS DynamoDB tables
```

Only the Rust application and its AWS resources are required. There is no
Versioned DynamoDB endpoint, proxy, sidecar, gateway, or compatibility server
to provision, discover, scale, authenticate, monitor, or upgrade.

Background work such as stream materialization, TTL expiry, retention, and
garbage collection uses explicit Rust worker processes that link the same
client/core crates and coordinate through durable DynamoDB state. These workers
are not request-serving services: foreground reads and writes do not traverse
them, and their absence cannot change the client API into a remote API.

| Component | Required | Deployment role |
| --- | --- | --- |
| Rust application using `prolly_dynamodb_client` | Yes | Executes the DynamoDB-compatible and version APIs in-process |
| Physical DynamoDB node/root/blob tables | Yes | Durable storage and cross-process coordination |
| Explicit maintenance workers | Only for enabled asynchronous features | Run the same Rust packages out of band; never serve client requests |
| Versioned DynamoDB compatibility service | No | Excluded from this product and every release gate |

Any future wire-compatible service is a separate product proposal. It must not
alter this crate's storage format, introduce a transport abstraction into the
client, or become required for existing client deployments.

## Status

- State: in progress
- Priority: P1
- Effort: XL
- Risk: high
- First implementation target: Rust AWS SDK DynamoDB fluent client API
- Storage target: `prolly_store_dynamodb::{DynamoDbBackend, DynamoDbStore}`
- Initial aligned versions: `prolly-store-dynamodb 0.6.0`,
  `aws-sdk-dynamodb 1.73.0`, Rust 1.91.1
- Semantic implementation: Rust `prolly-dynamodb-core`
- Superseded contingency: `plans/018-versioned-dynamodb-compatible-service.md`

## 1. Executive summary

The package is a local Rust database client, not a transparent HTTP endpoint.
An application constructs a `prolly_dynamodb_client::Client` from the same
configured `DynamoDbBackend` used by the existing store crate. Common fluent
call sites retain the AWS SDK for Rust shape and `AttributeValue` types:

```rust,no_run
use aws_sdk_dynamodb::types::AttributeValue;
use prolly_dynamodb_client::Client;
use prolly_store_dynamodb::DynamoDbBackend;

async fn open(
    physical: aws_sdk_dynamodb::Client,
) -> Result<Client, prolly_dynamodb_client::Error> {
    let backend = DynamoDbBackend::new(physical, "prolly-versioned-dynamodb")
        .with_root_table_name("prolly-versioned-dynamodb-roots")
        .with_key_prefix(b"orders-prod:".to_vec())
        .with_read_parallelism(16)
        .with_batch_get_parallelism(16)
        .with_batch_write_parallelism(16);

    // Provisioning stays explicit and uses the existing store API.
    backend.initialize_schema().await?;
    Client::open(backend).await
}

async fn example(client: &Client) -> Result<(), prolly_dynamodb_client::Error> {
    client
        .put_item()
        .table_name("Orders")
        .item("accountId", AttributeValue::S("acct-1".into()))
        .item("orderId", AttributeValue::S("order-7".into()))
        .item("status", AttributeValue::S("OPEN".into()))
        .send()
        .await?;

    let current = client
        .get_item()
        .table_name("Orders")
        .key("accountId", AttributeValue::S("acct-1".into()))
        .key("orderId", AttributeValue::S("order-7".into()))
        .send()
        .await?;

    assert!(current.item().is_some());
    Ok(())
}
```

The logical `Orders` table is not a native DynamoDB item table. The physical
table stores content-addressed Prolly nodes, blobs, and root manifests. The
client translates built AWS operation inputs into operations on one
authoritative `AsyncVersionedMap` per logical table. Distributed correctness
comes from DynamoDB strong reads, conditional root updates, and transactions;
it never depends on one Rust process, a local mutex, or a sticky session.

History functionality is additive:

```rust,no_run
let orders = client.table("Orders");
let head = orders.head().await?;

let old_order = orders
    .at(head.id.clone())
    .get_item()
    .key("accountId", AttributeValue::S("acct-1".into()))
    .key("orderId", AttributeValue::S("order-7".into()))
    .send()
    .await?;

let mut changes = orders.diff(earlier_version, head.id).page_size(256);
while let Some(page) = changes.next_page().await? {
    for change in page.diffs {
        println!("{change:?}");
    }
}
```

The package has one execution path: it runs the Rust logical core over
`DynamoDbStore = RemoteProllyStore<DynamoDbBackend>` in the application
process. No HTTP wrapper, endpoint emulation, protocol translation, or server
executor is required. Node, Python, and other language packages are possible
later bindings over the same Rust core and fixtures, not the reference
implementation.

Consequently, the package must not expose abstractions whose only purpose is a
future service: no transport interface, remote endpoint setting, service
discovery, HTTP error model, or client/server consistency mode belongs in the
v1 public API.

## 2. Background and current foundation

### 2.1 Why a client package is sufficient

Trusted backend applications already own AWS credentials and can safely access
a dedicated physical DynamoDB namespace. The Rust package can therefore provide
the complete logical database behavior in-process without a second deployment,
network hop, authentication layer, or HTTP compatibility surface.

The package is not wire-compatible DynamoDB. It is fluent-call compatible for
an explicitly supported set of operations and reuses public AWS SDK Rust
`AttributeValue`, input, and output types. Applications must install the crate
and replace the concrete client type. Code that explicitly names
`aws_sdk_dynamodb::Client`, explicitly names generated `*FluentBuilder` types,
calls `.customize()`, or expects `SdkError<E, HttpResponse>` is not automatically
source-compatible.

### 2.2 Existing reusable capabilities

The repository already provides most low-level components:

- `AsyncVersionedMap` owns immutable versions, a mutable head, historical
  reads, optimistic updates, diff, rollback, and retention semantics.
- `AsyncProlly` and the async store traits keep remote I/O off blocking paths.
- `RemoteProllyStore` verifies CIDs and root manifests across host-language
  adapters.
- `prolly_store_dynamodb::DynamoDbBackend` accepts a caller-owned
  `aws_sdk_dynamodb::Client`, exposes the underlying client and configured
  physical/root table names, strongly reads nodes/roots, implements root
  compare-and-swap, chunks provider batches, and exposes strict transactions.
- `DynamoDbStore` is already the native async
  `RemoteProllyStore<DynamoDbBackend>` path used by `AsyncProlly`.
- `AsyncIndexedMap` already provides native asynchronous strict derived-index
  coordination for remote stores.
- Language-neutral conformance fixtures already establish the repository's
  cross-language testing style.

### 2.3 Gaps that block the package

The following are not implemented today:

- `AsyncVersionedMap` does not yet expose every listing, diff-page, CAS restore,
  retention, and multi-map lifecycle operation required by the logical client.
- The DynamoDB store's current strict transaction counts rewritten nodes
  against DynamoDB's transaction-action limit. Larger legal logical writes
  require immutable prepublication followed by an atomic root transaction.
- The current default Prolly node limit can exceed DynamoDB's physical item
  limit.
- The host DynamoDB adapter has no chunked blob store for near-400-KB logical
  items.
- There is no canonical DynamoDB `AttributeValue`, item, key, number, expression,
  catalog, or commit implementation.
- There are no Rust compatibility builders that accept AWS SDK operation fields
  and route their completed input into the logical core.
- There is no stable version-aware package API, metadata surface, capability
  contract, or explicit worker ownership model.

This design closes those gaps without making the physical store adapter own
logical DynamoDB semantics and without forking the Rust store configuration or
ordered-tree algorithms.

## 3. Product scope and compatibility contract

### 3.1 Goals

- Preserve AWS SDK for Rust `AttributeValue`, operation input, and operation
  output types for advertised operations.
- Preserve the inferred fluent call shape—`client.get_item().table_name(...)
  .key(...).send().await`—for common backend code.
- Accept an already configured `DynamoDbBackend` as the primary constructor so
  the existing store API remains the single owner of physical configuration.
- Make ordinary operations target the current committed table head.
- Expose historical reads, versions, diffs, restore, and whole-table optimistic
  conditions through additive APIs.
- Coordinate independent processes safely through persistent roots and
  DynamoDB transactions.
- Keep caller-owned AWS SDK clients, credentials, HTTP runtime, retry policies,
  and lifecycle under application control.
- Fail closed for unsupported commands and fields.
- Publish explicit workload, compatibility, security, and cost envelopes.

### 3.2 Non-goals

- Being type-identical to `aws_sdk_dynamodb::Client` or its generated fluent
  builders.
- Supporting `.customize()`, per-operation Smithy config overrides, generated
  waiters, or third-party generic bounds without contract tests.
- Allowing native DynamoDB clients to read logical items directly from the
  physical storage table.
- Preserving native DynamoDB request count, latency, capacity-unit reporting,
  partition scaling, global tables, backups, IAM semantics, streams topology,
  or TTL timing.
- Running in untrusted browsers, mobile apps, or public edge code with physical
  table credentials; those deployments require an application-owned API and
  are outside this client package.
- Starting hidden TTL, stream, migration, retention, or GC workers when a client
  is opened.
- Reimplementing Prolly node, diff, merge, or root-manifest behavior in the
  compatibility facade.
- Supporting every DynamoDB command in the first release. PartiQL, global
  tables, native backups, import/export service APIs, DAX, and table-class
  operations remain unsupported until separately designed.

### 3.3 Initial trust and deployment boundary

The client is for trusted server-side code. Code with physical write
credentials can bypass the package, publish malformed internal records, delete
content, or defeat logical authorization. A local library cannot be a security
boundary against its own process.

Do not expose the client when any of the following is true:

- clients are untrusted or user-controlled;
- policies must be enforced independently of application code;
- physical credentials must not reach the application;
- browser, mobile, or external partner access is required;
- centralized audit identity must be authoritative.

Those cases require an independently secured application API. A domain-specific
API is usually the smaller security boundary; reviving a wire-compatible
DynamoDB service would require a separate architecture decision. Neither is a
dependency or deliverable of this plan.

### 3.4 Compatibility levels

Every operation and request field is classified as:

| Level | Meaning |
| --- | --- |
| Exact | The observable request/result/error behavior matches the declared DynamoDB contract. |
| Compatible stronger | The package provides a stronger guarantee without invalidating callers, such as an operation-pinned immutable snapshot. |
| Subset | Only documented fields or forms are accepted; all others fail explicitly. |
| Extension | Additive Prolly behavior with no native DynamoDB equivalent. |
| Unsupported | Rejected with a stable operation/field capability error. |

The package exports a machine-readable capability report. A command is never
accepted merely because an AWS SDK input builder exposes it.

## 4. Public package design

### 4.1 Crate names and dependency alignment

The public crate is `prolly-dynamodb-client`. The transport-independent crate is
`prolly-dynamodb-core`. This separation keeps canonical database semantics out
of AWS compatibility builders and supports focused testing; it does not imply
or require a service transport.

Initial dependencies intentionally match the existing store crate:

```toml
[dependencies]
prolly-map = { version = "0.7.0", features = ["async-store", "tokio"] }
prolly-store-dynamodb = "0.6.0"
prolly-dynamodb-core = { path = "../core" }
aws-sdk-dynamodb = "=1.73.0"
aws-smithy-types = "=1.5.0"
tokio = { version = "1.45", features = ["rt-multi-thread", "time"] }
```

The exact Smithy transitive versions follow `prolly-store-dynamodb`; they are
not independently widened. The client re-exports its supported
`aws_sdk_dynamodb` and `prolly_store_dynamodb` crates so consumers can avoid
accidentally constructing values from a second incompatible SDK release.

### 4.2 Direct construction from the store API

The primary constructor consumes a configured `DynamoDbBackend`:

```rust,no_run
let backend = DynamoDbBackend::new(aws_client.clone(), "prolly-data")
    .with_root_table_name("prolly-data-roots")
    .with_key_prefix(b"tenant-a:".to_vec())
    .with_read_parallelism(16)
    .with_batch_get_parallelism(16)
    .with_batch_write_parallelism(8)
    .with_scan_parallelism(4);

backend.initialize_schema().await?;

let client = Client::builder()
    .backend(backend)
    .logical_retry_limit(8)
    .open()
    .await?;
```

Callers that already constructed the native async store use the store overload
so `RemoteStoreConfig` is not rebuilt or lost:

```rust,no_run
use prolly_store_dynamodb::{DynamoDbBackend, DynamoDbStore};

let store = DynamoDbStore::with_config(backend, remote_store_config);
let client = Client::builder()
    .store(store)
    .logical_retry_limit(8)
    .open()
    .await?;
```

Normative rules:

- The client does not recreate `DynamoDbBackend` configuration in another
  struct. Primary table, companion root table, key prefix, concurrency, schema
  validation, AWS credentials, endpoint, HTTP client, and provider retry
  behavior remain owned by the existing adapter.
- `open` does not call `initialize_schema`; provisioning is explicit.
- `DynamoDbBackend` and the AWS SDK client are cheaply cloned handles. Dropping
  the versioned client does not destroy shared SDK state.
- Opening from a backend converts it through `DynamoDbStore::new(backend)`.
  Opening from a store uses that store unchanged. Both create `AsyncProlly`
  with the approved Dynamo-safe tree configuration.
- Opening rejects an unsupported store capability, oversized node profile, or
  durable database-format mismatch before accepting logical operations.
- A convenience `from_aws_client(client, table_name)` may exist, but it must
  delegate to `DynamoDbBackend::new` and clearly expose every adapter option.

#### 4.2.1 Rust store API compatibility matrix

The initial compatibility target is the current native async API in
`prolly-store-dynamodb 0.6.0`:

| Existing Rust store surface | Versioned client contract |
| --- | --- |
| `DynamoDbBackend::new(client, table_name)` | Accepted directly by `open`/`builder().backend(...)`; the caller's AWS client is retained. |
| `.with_root_table_name(...)` | Preserved exactly; the client does not derive or replace the configured companion roots table. |
| `.with_key_prefix(...)` | Preserved exactly and treated as the complete physical namespace boundary. |
| `.with_read_parallelism(...)` | Preserved by the backend and used by remote reads. |
| `.with_batch_get_parallelism(...)` | Preserved by the backend and used by immutable-node reads. |
| `.with_batch_write_parallelism(...)` | Preserved by the backend and used by prepublication/batch writes. |
| `.with_scan_parallelism(...)` | Preserved by the backend and used by bounded enumeration/maintenance. |
| `.initialize_schema().await` | Remains an explicit provisioning call before client open; logical `CreateTable` never creates the physical tables. |
| `DynamoDbStore::new(backend)` | Accepted through `open_store`/`builder().store(...)`; no adapter layer is inserted between the client and store. |
| `DynamoDbStore::with_config(backend, config)` | Accepted unchanged so caller-selected `RemoteStoreConfig` survives. |
| `store.backend()` and backend getters | Direct-mode client diagnostics expose a read-only `backend()` view with the same `client`, `table_name`, `root_table_name`, and `key_prefix` values. |
| `SyncDynamoDbStore` | Not accepted by the async v1 client. A future blocking facade must wrap the same core deliberately and prove nested-runtime and cancellation behavior. |

The builder rejects setting both `backend` and `store`, or neither. Contract
tests compile against the exact public store types; the client must not replace
them with look-alike configuration structs.

The client exposes `pub fn backend(&self) -> &DynamoDbBackend` for read-only
inspection. Mutating physical configuration after open is intentionally
impossible because doing so would change the namespace below an active logical
database.

### 4.3 AWS SDK Rust fluent compatibility

The public client mirrors supported AWS operation method names:

```rust,no_run
let output: aws_sdk_dynamodb::operation::get_item::GetItemOutput = client
    .get_item()
    .table_name("Orders")
    .key("pk", AttributeValue::S("order-1".into()))
    .consistent_read(true)
    .projection_expression("#s, total")
    .expression_attribute_names("#s", "status")
    .send()
    .await?;
```

Each compatibility builder owns:

```rust
pub struct GetItemFluentBuilder {
    client: Client,
    input: aws_sdk_dynamodb::operation::get_item::builders::GetItemInputBuilder,
    context: ReadContext,
}
```

The wrapper delegates field accumulation to the public AWS `*InputBuilder`,
calls `build()` at send time, validates the advertised field subset, converts
the built input into the independent core model, and constructs the official
AWS `*Output` through its public output builder.

The AWS generated `*FluentBuilder` itself cannot be reused: its constructor and
service handle are private and its `send()` always invokes Smithy HTTP
orchestration. Compatibility therefore means:

- same inferred method chain for advertised fields;
- same AWS model/input/output value types;
- different client and fluent-builder concrete types;
- a crate-owned error wrapper rather than an HTTP `SdkError`;
- no direct-mode `.customize()` or `send_with(&aws_sdk_dynamodb::Client)`.

### 4.4 Input-first execution API

Every advertised operation also accepts a completed official AWS input. This is
the stable integration path for generated code and tests that do not need the
fluent facade:

```rust,no_run
let input = aws_sdk_dynamodb::operation::get_item::GetItemInput::builder()
    .table_name("Orders")
    .key("pk", AttributeValue::S("order-1".into()))
    .build()?;

let output = client.execute_get_item(input, ReadOptions::default()).await?;
```

Fluent builders are thin wrappers over this API. Core behavior must never be
implemented in generated compatibility setters.

### 4.5 Outputs and version metadata

Normal `.send()` returns the official AWS operation output type. Rust cannot
attach out-of-band fields to that value, so version metadata is explicit:

```rust,no_run
let WithMetadata { output, metadata } = client
    .put_item()
    .table_name("Orders")
    .item("pk", AttributeValue::S("order-1".into()))
    .send_with_metadata()
    .await?;
```

```rust
pub struct WithMetadata<T> {
    pub output: T,
    pub metadata: OperationMetadata,
}

pub struct OperationMetadata {
    pub operation_id: OperationId,
    pub commit_id: Option<CommitId>,
    pub transitions: Vec<TableTransitionMetadata>,
    pub logical_retries: u32,
    pub execution_mode: ExecutionMode,
}
```

`transitions` is a vector because non-atomic `BatchWriteItem` may create more
than one transition for a table. A no-op accepted event may have equal
before/after state IDs while retaining a distinct commit ID.

### 4.6 Version extensions on fluent builders

Historical reads and whole-table compare-and-swap use additive methods:

```rust,no_run
let historical = client
    .get_item()
    .table_name("Orders")
    .key("pk", AttributeValue::S("order-1".into()))
    .at(old_version.clone())
    .send()
    .await?;

let updated = client
    .update_item()
    .table_name("Orders")
    .key("pk", AttributeValue::S("order-1".into()))
    .update_expression("SET #s = :s")
    .expression_attribute_names("#s", "status")
    .expression_attribute_values(":s", AttributeValue::S("PAID".into()))
    .expected_head(expected_head)
    .request_token(operation_token)
    .send_with_metadata()
    .await?;
```

Rules:

- `at` exists only on read builders.
- `expected_head` exists only on write builders.
- Historical membership is verified against the resolved table ID.
- Item conditions and expected head both must succeed.
- An expected-head mismatch is not automatically retried.
- Builders without version extensions target the current head.

### 4.7 Table facade

The table facade scopes the name and exposes history:

```rust
impl Table {
    pub fn get_item(&self) -> ScopedGetItemFluentBuilder;
    pub fn put_item(&self) -> ScopedPutItemFluentBuilder;
    pub fn at(&self, id: TableVersionId) -> Snapshot;
    pub fn if_head(&self, id: TableVersionId) -> ConditionalTable;

    pub async fn head(&self) -> Result<TableHead, Error>;
    pub fn versions(&self, input: ListVersionsInput) -> VersionStream;
    pub fn commits(&self, input: ListCommitsInput) -> CommitStream;
    pub async fn commit(&self, id: CommitId) -> Result<Commit, Error>;
    pub fn diff(&self, from: TableVersionId, to: TableVersionId) -> DiffBuilder;
    pub fn restore(&self, target: TableVersionId) -> RestoreBuilder;
}
```

A restore remains an explicit head compare-and-swap even though its verb is
short:

```rust,no_run
let restored = orders
    .restore(old_version)
    .expected_head(current_head.version_id)
    .request_token(operation_token)
    .send()
    .await?;
```

Scoped builders omit `table_name` or validate any supplied table name equals the
facade name. `Snapshot` exposes only `GetItem`, `Query`, `Scan`, and
declared read operations at the type level.

#### 4.7.1 Concise version-context naming

Public Rust methods omit a `version` suffix when the receiver, parameter type,
or returned type already supplies that context:

| Verbose draft name | Public name | Reason |
| --- | --- | --- |
| `VersionedDynamoDbClient` | `Client` | The crate path already identifies the versioned DynamoDB client. |
| `VersionedTable` / `HistoricalTable` | `Table` / `Snapshot` | The receiver and return type express current versus pinned state. |
| `VersionedOutput<T>` | `WithMetadata<T>` | Describes the wrapper's actual purpose without repeating the product name. |
| Historical table selection | `at(...)` | The `TableVersionId` or typed table-pin collection makes the target explicit. |
| `diff_versions(input)` | `diff(from, to)` | A table diff is necessarily between two table states. |
| `restore_version(input)` | `restore(target)` | The returned builder carries `expected_head` and the request token. |
| `if_version(version)` | `if_head(version)` | Names the actual current-head precondition instead of repeating “version.” |

Nouns that identify a durable concept remain explicit: `TableVersionId`,
`VersionStream`, `versions()`, `version_id`, and `commit_id`. This preserves the
important distinction between content-derived state versions and event commits.
Because the package is not published yet, the verbose draft methods are
removed rather than retained as aliases.

### 4.8 Error compatibility

The package returns a typed local error:

```rust
pub enum Error<E = aws_sdk_dynamodb::error::BuildError> {
    Service(E),
    Construction(aws_sdk_dynamodb::error::BuildError),
    Backend(prolly_store_dynamodb::DynamoDbBackendError),
    Engine(prolly::Error),
    Core(prolly_dynamodb_core::Error),
    Cancelled { operation_id: OperationId },
    OutcomeUnknown { operation_id: OperationId },
}
```

Operation builders may specialize `E` to the corresponding generated operation
error. Core semantic errors map to generated service-error values when the AWS
model exposes a suitable public variant/builder; otherwise they retain a stable
core category and `ProvideErrorMetadata` code.

The result is intentionally not `SdkError<E, HttpResponse>` because local
execution has no HTTP response, Smithy retry context, or request serializer.
The crate provides `as_service_error`, `is_retryable`, `operation_id`, and
stable category accessors. Ordinary `?`/`anyhow` use remains straightforward;
code matching concrete `SdkError` variants requires adaptation.

### 4.9 No transport abstraction in v1

The v1 crate does not expose a server mode, endpoint option, transport trait, or
service-client feature. Every operation executes through the configured
`DynamoDbStore`. This avoids carrying a speculative abstraction through every
builder and error type.

A future network product may translate its protocol into
`prolly-dynamodb-core`, but it must define its own client and cannot expand or
destabilize this package's v1 surface merely to preserve hypothetical mode
switching.

### 4.10 Hooks, paginators, and waiters

The Rust client uses `tracing` spans and optional typed diagnostic subscribers.
It does not expose Smithy per-operation customization.

Query and Scan builders gain crate-owned `.into_paginator()` implementations
only after page-level contract tests pass. The paginator returns official AWS
output/item types and can be version-pinned. Generated AWS waiters are not
compatible with the versioned client unless a separate adapter implements and
tests their required operation subset.

## 5. Architecture and ownership

### 5.1 Layering

```text
application code
  |
  | AWS SDK Rust fluent fields/types + Prolly version extensions
  v
prolly-dynamodb-client
  |
  +-- AWS input builder compatibility / model conversion
  |
  v
prolly-dynamodb-core
  |
  v
AsyncVersionedMap / AsyncIndexedMap / AsyncProlly
  |
  v
DynamoDbStore / DynamoDbBackend
  |
  v
caller-owned aws_sdk_dynamodb::Client -> DynamoDB
```

### 5.2 Normative ownership rules

- Rust core owns DynamoDB value validation, exact number behavior, canonical
  item/key encodings, expression parsing/evaluation, query planning, table
  lifecycle, commits, indexes, version operations, and error categories.
- Prolly core owns tree algorithms, managed root naming, version identity,
  diff, transactions, retention reachability, and CID verification.
- The Rust client facade owns public AWS input/core conversions, compatible
  fluent setters, AWS output construction, error wrapping, and client
  ergonomics.
- The physical DynamoDB adapter owns provider batching, retries of unprocessed
  physical requests, conditional writes, physical schema validation, and AWS
  SDK calls.

No layer may decode raw Prolly nodes, manufacture managed root names, or
reimplement expression semantics merely to avoid crossing a binding boundary.

### 5.3 Proposed repository layout

```text
dynamodb/
  core/
    Cargo.toml                         # crate: prolly-dynamodb-core
    src/
      lib.rs
      capability.rs
      error.rs
      model/{attribute,number,item,key,schema,version}.rs
      expression/{lexer,parser,ast,eval,update,projection}.rs
      catalog/{record,store}.rs
      engine/{database,table,read,write,query,transaction,index}.rs
      history/{commit,diff,restore,retention}.rs
      worker/{stream,ttl}.rs
    tests/

  client/
    Cargo.toml                         # crate: prolly-dynamodb-client
    src/
      lib.rs
      client.rs
      config.rs
      error.rs
      metadata.rs
      conversion/{attribute,input,output,error}.rs
      operation/{create_table,get_item,put_item,delete_item,update_item}.rs
      operation/{query,scan,batch,transaction}.rs
      table.rs
      paginator.rs
    tests/
      aws_sdk_compile.rs
      builder_parity.rs
      direct_contract.rs

  conformance/
    model-v1.json
    expressions-v1.json
    operations-v1.json
    errors-v1.json
    history-v1.json
```

The exact top-level directory spelling can change before implementation. The
separation between logical core, Rust client facade, and physical store is
normative.

### 5.4 Core operation boundary

The Rust logical core exposes command-independent operations, not AWS SDK or
HTTP structs:

```rust
pub struct Database<S: AsyncStore> { /* ... */ }

impl<S> Database<S>
where
    S: AsyncStore
      + AsyncManifestStore
      + AsyncTransactionalStore,
{
    pub async fn create_table(&self, input: CreateTable) -> Result<CreateTableResult>;
    pub async fn get_item(&self, input: GetItem, ctx: ReadContext) -> Result<GetItemResult>;
    pub async fn put_item(&self, input: PutItem, ctx: WriteContext) -> Result<WriteResult>;
    pub async fn query(&self, input: Query, ctx: ReadContext) -> Result<QueryResult>;
    pub async fn transact_write(&self, input: TransactWrite, ctx: WriteContext)
        -> Result<TransactionResult>;
}
```

Input types contain this plan's independent core `AttributeValue` model. They
do not depend on `aws-sdk-dynamodb`, Axum, Smithy HTTP orchestration, or a
specific transport.

### 5.5 AWS model conversion boundary

The client crate is the only layer that depends on AWS operation
input/output types. Conversion is lossless:

- `AttributeValue::N` remains a decimal string until parsed by `DynamoNumber`;
- `Blob` remains raw bytes;
- maps are converted without trusting `HashMap` iteration order;
- sets are validated and canonicalized only in the core;
- unknown non-exhaustive enum variants fail with a capability error;
- output conversion uses public AWS output builders;
- no AWS input/output is used as a persisted encoding.

The conversion layer has round-trip fixtures for every supported AWS model
variant and compile tests against the exact SDK line used by the store crate.

## 6. Persistent format

All client instances use the same versioned logical model and must read and
write the same canonical namespace and format.

### 6.1 Database format record

Every physical key prefix contains one durable database-format record created
by compare-and-swap:

```text
DatabaseFormatRecord
  format_version
  logical_protocol_major
  logical_protocol_minor
  item_codec_digest
  key_codec_digest
  catalog_codec_digest
  commit_codec_digest
  tree_format_digest
  publication_mode
  minimum_reader_version
  minimum_writer_version
```

Every client open validates this record. A change that alters canonical bytes,
state IDs, root visibility, or write interpretation requires an explicit format
upgrade and migration. Package semver alone never silently changes persisted
output.

### 6.2 Logical namespace

Use these namespaces:

```text
["system", "dynamodb", "catalog", "v1"]
["system", "dynamodb", "table", table_id, "items"]
["system", "dynamodb", "table", table_id, "commits"]
["system", "dynamodb", "table", table_id, "index", index_id]
["system", "dynamodb", "idempotency", "v1", token_hash[0]]
```

Logical table names resolve through the catalog to immutable 32-byte table IDs.
Deleting and recreating a name allocates a new ID. Every write validates the
resolved table incarnation in its transaction read set so a concurrent delete
or same-name recreation cannot redirect a prepared mutation.

### 6.3 Canonical keys and items

The core implements these formats:

- ordered primary key format version `0x01`;
- string, binary, and exact ordered decimal components;
- canonical `DDBI v1` items with sorted attribute names and sets;
- DynamoDB-compatible logical size validation;
- a conservative inline threshold with content-addressed blob offload;
- table descriptors carrying a tree-format digest.

The Rust client never persists an AWS SDK input/output object or an incidental
serializer representation.
Equivalent logical values, including equivalent number spellings, must create
identical canonical bytes and table versions across independent client
processes.

### 6.4 Physical DynamoDB storage profile

The current Rust adapter uses two physical tables:

- the primary table has binary `pk`, binary `value`, and stores
  content-addressed nodes plus traversal hints under the configured key prefix;
- the companion root registry, `<primary>-roots` by default, has binary `pk`
  namespace plus binary `sk` root name and is the canonical named-root store.

The client consumes `DynamoDbBackend` or an existing `DynamoDbStore` and must
not assume an older one-table root layout. Its configured `table_name`,
`root_table_name`, `key_prefix`, parallelism settings, and `RemoteStoreConfig`
are authoritative. Credentials, endpoint, HTTP client, timeouts, and provider
retry configuration remain in the caller-owned
`aws_sdk_dynamodb::Client`.

Required Phase 0 additions are client prerequisites:

- Dynamo-safe logical-byte tree chunking;
- serialized physical nodes no larger than the tested safety ceiling;
- verified immutable node prepublication;
- atomic root conditions/writes after prepublication;
- a chunked `DynamoDbBlobStore`;
- capability reporting for provider batch and root-transaction limits.

Client versions may share a namespace only when database-format and writer
capability negotiation succeeds. A writer must refuse a namespace whose minimum
writer version or publication mode it cannot honor.

### 6.5 State and event identity

`TableVersionId` is the content-derived `MapVersionId` and identifies state.
`CommitId` identifies an accepted event. Therefore:

- a no-op may produce a commit without a new state ID;
- restoring an earlier state reuses its state ID but creates a new commit;
- identical states in different tables may share a hash;
- all external version references include table identity;
- commit metadata is never included in canonical item/tree bytes.

Audit identity is caller-supplied and is not authoritative against a
compromised process. Applications requiring independently authenticated
principal identity must enforce it outside this package.

## 7. Command compatibility surface

### 7.1 Initial command matrix

| Command | Initial level | Planned phase | Notes |
| --- | --- | --- | --- |
| `CreateTable` | Subset | 2 | Keys and billing metadata; no native resource creation per logical table |
| `DescribeTable` | Subset | 2 | Logical descriptor and declared compatibility metadata |
| `ListTables` | Exact/subset | 2 | Logical names with bounded pagination |
| `DeleteTable` | Subset | 2 | Catalog tombstone; physical GC is separate |
| `GetItem` | Compatible stronger | 2 | Operation-pinned immutable state |
| `PutItem` | Subset then exact | 2/3 | Conditions and full return values arrive in Phase 3 |
| `DeleteItem` | Subset then exact | 2/3 | Conditions and full return values arrive in Phase 3 |
| `UpdateItem` | Subset | 3 | Advertised expression grammar only |
| `Query` | Subset | 4 | Base table first; indexes in Phase 7 |
| `Scan` | Subset | 4 | Serial scan first; parallel scan separately gated |
| `BatchGetItem` | Subset | 4 | One pinned version per logical table |
| `BatchWriteItem` | Subset | 4 | Whole batch remains non-atomic |
| `TransactGetItems` | Subset | 5 | Declared logical/physical limits |
| `TransactWriteItems` | Subset | 5 | Strict multi-table root publication |
| GSI/LSI `Query`/`Scan` | Subset | 7 | Strict synchronous index versions |
| Streams/TTL APIs | Extension/subset | 7+ | Require explicitly run leased workers |
| PartiQL commands | Unsupported | — | Separate design required |
| Native backup/global-table APIs | Unsupported | — | Different infrastructure semantics |

The generated capability report lists fields, expression forms, limits, and
known semantic differences for each command.

### 7.2 Capacity and consistency fields

The package must not report physical Prolly storage requests as native logical
DynamoDB capacity units. Initial behavior is:

- `ReturnConsumedCapacity=NONE` or absence: accepted.
- `TOTAL`/`INDEXES`: rejected until a documented compatibility response exists.
- physical request/byte metrics: available only through Prolly diagnostics.
- `ConsistentRead=true`: accepted where the logical operation is supported.
- `ConsistentRead=false` or absence: may still receive the stronger
  operation-pinned committed snapshot.
- strong reads on GSI-like indexes: rejected to match DynamoDB's restriction.

### 7.3 Pagination

Standard `LastEvaluatedKey`/`ExclusiveStartKey` remain ordinary logical key
objects. Standard pages resolve the current head independently and therefore
may observe a newer head between calls, consistent with DynamoDB's lack of a
whole-query snapshot contract across requests.

The table facade offers repeatable pagination pinned to a version:

```rust,no_run
let mut pages = orders
    .at(version)
    .query()
    .key_condition_expression("pk = :pk")
    .expression_attribute_values(":pk", AttributeValue::S("acct-1".into()))
    .into_paginator();

while let Some(page) = pages.next_page().await? {
    // Every page resolves exactly the same immutable table version.
    consume(page);
}
```

The in-process package never exposes an internal structural cursor. Repeatable
pagination carries a typed immutable version in the builder and continues with
the ordinary logical `LastEvaluatedKey`; there is no untrusted serialized-token
boundary to sign. If a future cross-process cursor serialization extension is
added, it must use a separately configured signing key and must never place its
opaque token inside standard `LastEvaluatedKey` values.

## 8. Execution algorithms

### 8.1 Open and capability negotiation

Client open performs:

1. Validate options without provider calls.
2. Consume the configured `DynamoDbStore`, or wrap the supplied
   `DynamoDbBackend` with `DynamoDbStore::new`.
3. Describe and validate the explicitly initialized primary and roots tables;
   `open` never provisions them.
4. Read the store descriptor and provider limits.
5. Require strong manifest reads, root CAS, transactions, approved publication
   mode, and Dynamo-safe maximum node bytes.
6. Load or CAS-create `DatabaseFormatRecord`.
7. Validate codec/tree digests and reader/writer ranges.
8. Open the logical database handle.
9. Publish a frozen capability report and begin accepting calls.

Failure drops partially opened Rust resources but never changes or shuts down
the caller-configured shared SDK client.

### 8.2 Command dispatch

For every command:

1. Build the official AWS operation input through its public input builder.
2. Convert AWS values into owned, lossless core values.
3. Validate the supported field subset before starting logical work.
4. Allocate one operation ID and one cancellation scope.
5. Invoke the core database operation over the configured store.
6. Convert the core result or error into the declared AWS output/error shape.
7. Record version metadata and diagnostics.
8. Resolve only after ambiguous commit outcomes are reconciled or clearly
   reported.

Built inputs are owned values. Binary/map data is converted before the first
await that could allow caller state to change.

### 8.3 Point read

`GetItem`:

1. Resolve the logical name to one active table descriptor.
2. Select the requested historical version or strongly read the current head.
3. Pin an immutable snapshot for the operation.
4. Validate and encode the complete primary key from the descriptor.
5. Read the canonical value, reassemble/verify a blob if necessary, and decode
   the item.
6. Apply the projection expression.
7. Return the official AWS output; `send_with_metadata` additionally returns
   explicit version metadata.

An unknown, pruned, or wrong-table version fails closed. A historical read
never falls forward to head.

### 8.4 Single-item write

`PutItem`, `DeleteItem`, and `UpdateItem` use the following loop:

1. Resolve and pin the active table descriptor/incarnation.
2. Strongly read the current head and item.
3. Evaluate conditions against the immutable old item.
4. Build the complete new item and validate schema, key immutability, nesting,
   expression, and logical item limits.
5. Canonically encode the item and prepare any blob content.
6. Apply the mutation to a candidate tree.
7. Verify and prepublish immutable nodes/blobs in provider-sized batches.
8. Atomically condition the catalog lifecycle fence and old head, then publish
   the new version/head and commit records.
9. On a known head race, reopen the new head and repeat all item/condition/update
   evaluation within the retry budget.
10. Return old/new values according to the request and the committed transition.

Unreachable prepared content after a lost race is safe and later reclaimable.
No head references missing content.

### 8.5 Expressions

The expression engine is pure Rust and lives in the logical core:

```text
expression text
  -> lexer with source spans
  -> parser
  -> name/value substitution
  -> typed AST and validation
  -> evaluation against immutable old item
  -> boolean, projection, or update plan
```

Update operands resolve against the old item before any action is applied. The
Rust compatibility facade forwards strings and AWS values but does not
interpret paths, functions, reserved words, or number arithmetic.

### 8.6 Query and Scan

`Query` encodes the equality partition-key prefix and optional sort-key bounds
into one Prolly range. It never scans the full table for a valid key condition.
Results follow canonical key order and support reverse traversal.

`Query` and `Scan` count evaluated logical bytes before filter/projection,
observe the supported 1-MB boundary, and may return an empty filtered page with
a continuation key. One call remains pinned to one immutable version even if
head advances during evaluation.

### 8.7 Batch operations

`BatchGetItem` resolves one snapshot per participating table and reconstructs
results according to DynamoDB's unordered response contract. It reports
unprocessed keys only for bounded work/resource behavior, not to hide semantic
errors.

`BatchWriteItem` remains non-atomic as a whole. Each item operation is an
independent conditional head transition and may be retried independently.
Multiple versions/commits can result from one batch. Duplicate-key and request
limit validation occurs before writes where DynamoDB rejects the entire batch.

### 8.8 Transactions

`TransactWriteItems`:

1. Resolves every logical table descriptor and validates unique item targets.
2. Pins every participating table head and item read set.
3. Evaluates conditions and updates in request order for cancellation reasons.
4. Builds candidate roots for every participating table and its synchronous
   indexes.
5. Prepublishes verified immutable nodes/blobs.
6. Performs one physical root transaction containing lifecycle fences, old-head
   conditions, new heads/version roots, commit-log roots, and optional
   idempotency root.
7. Returns all transitions under one shared `CommitId`.

The standard 100 logical-item limit and the smaller effective root-action limit
are separate. Capability discovery reports both, and validation fails before
prepublication when the declared operation shape cannot fit.

### 8.9 Historical reads, diff, and restore

Historical `GetItem`, `Query`, and `Scan` use the same execution plans after
snapshot selection. Diff streams structural Prolly changes and decodes logical
items lazily. Restore conditions the current head and moves it to an already
cataloged immutable version; it does not copy every item or delete the version
being left.

Restore requires `expectedHead` unless an explicitly privileged force option is
used. The client treats force as an administrative API but cannot independently
authorize the caller; deployments must use separate credentials/processes or an
application-owned authorization boundary for enforceable policy.

## 9. Concurrency, retries, and ambiguous outcomes

### 9.1 Distributed correctness

Local locks may reduce duplicate work inside one process but are never in the
correctness proof. Independent client processes coordinate solely through
durable root conditions and physical transactions.

One table head is the initial write serialization point. The package exposes
conflict, retry, and candidate-rebuild metrics. It does not claim native
DynamoDB partition write scaling.

### 9.2 Cache rules

Safe caches:

- immutable nodes and blobs keyed by verified content ID;
- parsed expressions keyed by exact expression/substitution shape;
- immutable table descriptors keyed by `(table_id, descriptor_version)`;
- historical snapshots keyed by `(table_id, version_id)` until retention
  invalidation is observed.

Unsafe by default:

- a current head reused across independent operations;
- a table-name resolution used for writes without a lifecycle fence;
- negative version existence cached across retention/admin changes;
- condition results reused after a head conflict.

Phase 2 starts with no cross-operation head cache. Any later cache must preserve
the advertised consistency classification and have concurrency tests.

### 9.3 Retry layers

There are three distinct retry layers:

1. The caller-owned AWS SDK retries transport/throttling according to its own
   configuration.
2. The physical adapter retries unprocessed batch entries within bounded limits.
3. The logical core retries known optimistic head conflicts and rebuilds the
   operation against the new immutable state.

The package records each layer separately and prevents unbounded multiplicative
retry. Validation, failed item conditions, expected-head conflicts, unsupported
fields, and known transaction cancellation are never automatically retried.
`ClientBuilder::logical_retry_limit(n)` controls layer 3 and counts retries
after the first attempt. It defaults to seven (eight total attempts), accepts
zero through 63, and rejects a larger value before provider access. The AWS SDK
and adapter retain their independently configured retry limits.

The decoded-node cache is process-local and bounded to 64 MiB of retained
serialized-node weight by default.
`node_cache_max_nodes` and `node_cache_max_bytes` add explicit simultaneous
ceilings; zero disables caching. Correctness pins may temporarily exceed these
ceilings until unpinned. Retry and cache tuning is reported in the capability
document but excluded from durable format identity.

### 9.4 Idempotency and operation identity

Each invocation allocates an `OperationId`. `TransactWriteItems` honors the
standard `ClientRequestToken`. Write builders permit `.request_token(...)` as an
extension for single-item writes and maintenance operations.

For an ambiguous physical root-transaction response, the core reads the durable
idempotency/commit record and conditioned heads before deciding whether the
operation applied. It does not blindly replay a non-idempotent update.

An automatically generated in-memory operation ID can reconcile ambiguity only
while the invocation/process survives. Applications that need safe retry across
process restart must supply a durable request token or use the standard
transaction token.

### 9.5 Cancellation

Dropping an operation future requests Rust cancellation but cannot retract a
physical request already accepted by DynamoDB. Explicit APIs that need
cooperative cancellation accept a `CancellationToken`. If cancellation occurs
after root commit submission, the returned error indicates that outcome
reconciliation may be required and includes a safe operation ID, never a
guessed rollback.

## 10. Error model

The Rust core emits stable categories and structured details. The client maps
semantic failures to the matching generated AWS operation error when the SDK
model exposes one, while its outer error preserves non-HTTP storage and
reconciliation failures:

| Core category | Client result |
| --- | --- |
| Invalid item/key/schema/expression | `ValidationException` |
| Missing logical table | `ResourceNotFoundException` |
| Failed item condition | `ConditionalCheckFailedException` |
| Transaction semantic failure | `TransactionCanceledException` with ordered reasons |
| Known root conflict exhaustion | `TransactionConflictException` or declared throttling class |
| Unsupported command/field | `ValidationException` plus stable Prolly capability code |
| Physical throttling/resource exhaustion | Declared retryable AWS-compatible service exception |
| Corrupt/missing content | Internal error with operation ID; no raw CID in normal message |
| Unknown commit outcome | Retryable reconciliation error with operation ID/token guidance |

The error implements `std::error::Error`, stable category accessors, operation
ID access, retry classification, and `ProvideErrorMetadata` where practical.
Physical table names, keys, CIDs, root names, credentials, and raw provider
bodies are omitted from normal messages. Diagnostics can expose redacted
provider codes to trusted operators.

Fluent-builder, input-first, and core-level tests run the same operation/error
fixture corpus. Exact wording may differ where AWS does not specify it; type,
category, cancellation order, retryability, and stable capability code are
normative.

## 11. Client/core contract

### 11.1 One semantic implementation

`prolly-dynamodb-core` is the sole owner of logical DynamoDB behavior. The
client facade converts official AWS inputs into core operations and converts
core results back into official AWS outputs. Fluent setters, paginators, and
metadata wrappers must not reimplement validation, expressions, transactions,
history, or index behavior.

Given the same database format, starting roots, operation, and deterministic
time/IDs in a test harness, fluent-builder, input-first, and core-level paths
must produce the same canonical state versions and logical results.

### 11.2 Direct call boundary

The client contains one concrete in-process path rather than an executor
abstraction:

```rust
struct ClientInner {
    database: Database<DynamoDbStore>,
    capabilities: Capabilities,
    cancellation: CancellationToken,
    diagnostics: Diagnostics,
}
```

Every public builder holds a cheap `Client` clone and invokes the typed core
operation directly. No transport selection, endpoint dispatch, boxed executor,
or HTTP error variant exists in v1.

### 11.3 Independent-process coordination

Multiple client processes may share a physical namespace only when format and
writer capability negotiation succeeds. They coordinate through durable roots
and DynamoDB transactions, not shared process memory. Caches are always local
and never participate in correctness.

### 11.4 Mixed client versions and upgrades

Rolling deployments are allowed only when the durable format record declares
the reader/writer versions mutually compatible. Before any breaking format or
semantic change:

1. publish backward-compatible readers;
2. record an upgrade intent/maintenance fence;
3. migrate or shadow-build new state;
4. verify canonical equality or declared transformation;
5. atomically select the new format generation;
6. retain the old generation for rollback.

Two client versions with unverified canonical codecs must never write the same
namespace.

## 12. Security and operational ownership

### 12.1 IAM profiles

Recommended roles are separate:

- **Provisioner:** create/describe/update the physical table.
- **Runtime client:** read/write nodes, blobs, and approved root records;
  no physical table deletion.
- **Maintenance worker:** scan roots/nodes/blobs and perform retention/GC under
  leases.
- **Migration operator:** source export/stream access plus target build/cutover.

Provider IAM cannot prove that calls came through the local package. Dedicated
physical tables or prefixes should align with application trust domains.

### 12.2 Secrets and data exposure

- Credentials remain inside the caller-owned AWS SDK client.
- `DynamoDbBackend` holds the normal clone of that client; the versioned package
  never extracts, serializes, or independently resolves credentials.
- Diagnostic hooks default to table IDs, operation kinds, counts, and sizes;
  they do not log attribute values, keys, expressions with values, or raw
  physical records.
- Version and commit IDs are not secrets and must not be treated as
  authorization tokens.
- Enforceable per-tenant/item authorization requires an application-owned API
  boundary; the local package cannot supply it.

### 12.3 Maintenance ownership

Opening a client never starts background workers. The package may expose
explicit worker constructors, but the caller must run, lease, monitor, and stop
them:

```rust
let worker = client
    .workers()
    .ttl(TtlWorkerOptions::new(owner_id).lease_duration(lease_duration))
    .await?;
worker.run(cancellation_token).await?;
```

Exactly one leased worker owner performs TTL, stream materialization,
migrations, retention, and GC for a namespace. Leases and durable checkpoints,
not convention, prevent duplicate unsafe maintenance.

## 13. Observability and resource lifecycle

### 13.1 Diagnostics

Required events and metrics include:

- operation type, logical table ID, duration, success/error category;
- selected version and transition count;
- logical items/bytes evaluated and returned;
- physical node/blob/root reads and writes;
- physical requests and bytes by DynamoDB operation;
- head conflicts, logical retries, and candidate rebuild time;
- AWS SDK attempts when available through public metadata;
- blob offloads, chunks, verification failures;
- transaction logical items, participating roots, and physical root actions;
- cache hits/misses and bounded memory use;
- version/commit counts, diff work, retention, and worker lag.

Hooks run outside correctness-critical locks and cannot modify operation data.
Hook failure is reported diagnostically and does not turn a committed write into
an apparent rollback.

### 13.2 Shutdown and drop semantics

The client is cloneable and follows normal Rust ownership. Dropping a clone
releases only package state and never performs blocking I/O or shuts down the
caller-configured AWS client. Normal clients own no background workers.

If the configured client owns tasks, `shutdown().await` stops accepting new
work, requests cooperative cancellation, drains for the configured timeout,
and clears caches. It is idempotent; calls begun after shutdown fail before a
provider request. Explicit workers have their own cancellation and join handle,
so dropping an ordinary request client cannot silently stop maintenance.

## 14. Packaging and release strategy

### 14.1 Rust crates and features

The reference distribution is ordinary Rust source, not a native addon:

- `prolly-dynamodb-core`: transport-independent logical semantics;
- `prolly-dynamodb-client`: client facade, AWS-compatible types/builders, and
  the `DynamoDbBackend`/`DynamoDbStore` construction paths.

Client execution is unconditional rather than feature-gated. Optional `admin`
and `workers` features add their corresponding APIs without changing persisted
format. Feature combinations are checked in CI with minimal versions where the
workspace policy permits. Later language bindings wrap this Rust surface; they
are not part of the first release gate. Cargo crate names remain unscoped by
Rust convention; any later npm binding must use the `@crabbuild` scope (for
example, `@crabbuild/prolly-dynamodb-client`).

### 14.2 Version axes

Track these independently:

- Rust crate semver for core and client;
- Rust MSRV, initially the repository's Rust 1.91.1;
- `aws-sdk-dynamodb` compatibility, initially aligned exactly with the store's
  `=1.73.0` dependency so public AWS model types unify;
- `prolly-store-dynamodb` and `prolly-map` compatibility ranges;
- remote store protocol major/schema version;
- logical DynamoDB protocol version;
- persisted item/key/catalog/commit format versions.

A package release may expand command support without changing persisted format.
A persisted format change requires fixtures, migration, reader/writer ranges,
and an upgrade note.

### 14.3 Capability handshake

Capabilities include:

- client and core implementation versions;
- supported commands and request fields;
- expression grammar/features;
- logical item/key/batch/transaction limits;
- physical root-action limit;
- index, history, diff, restore, retention, stream, and TTL support;
- database format and codec digests;
- publication mode and atomicity description;
- declared consistency and capacity-reporting behavior.

Applications can assert required capabilities at startup rather than discover
incompatibility on a production request.

## 15. Testing strategy

### 15.1 Test layers

1. **Rust unit tests:** exact numbers, values, item/key codecs, expressions,
   catalog records, commits, and error categories.
2. **Property tests:** codec injectivity, order preservation, canonical
   equality, expression determinism, and mutation/rebuild equivalence.
3. **Rust AWS model tests:** every supported AWS `AttributeValue`, operation
   input/output conversion, blob ownership, builder field, and error mapping.
   Compile tests pin the documented fluent call shape.
4. **Remote-store conformance:** provider limits, strong reads, CAS,
   prepublication, blobs, transactions, and injected ambiguous outcomes.
5. **Store API compatibility tests:** compile and execute backend-first and
   store-first construction while preserving roots table, key prefix,
   parallelism, and `RemoteStoreConfig`.
6. **Client integration tests:** Rust client through DynamoDB Local with a
   dedicated physical namespace.
7. **Differential tests:** supported logical requests against a separate native
   DynamoDB Local table, comparing items, order, counts, and error category.
8. **API-path parity tests:** identical operation traces through fluent,
   input-first, and core-level paths, comparing logical results and state IDs.
9. **Multi-process tests:** independent Rust processes racing writes,
   conditions, table recreation, transactions, restore, and shutdown.
10. **Fault injection:** every node/blob prepare, root condition/write, response,
   cancellation, retry, and reconciliation boundary.
11. **Performance tests:** cold/warm latency and physical request/byte/cost
    amplification as release gates.

### 15.2 Golden fixtures

Version and publish fixtures for:

- Dynamo number spelling, arithmetic, and ordered bytes;
- canonical attribute/item bytes and logical size;
- PK/SK tuple bytes and query bounds;
- expression AST, evaluation, update, projection, and errors;
- logical operation input/result/error traces;
- table descriptors, database format, commits, cursors, and retention records;
- core/client final state roots for deterministic traces.

Core, client-facade, and AWS-conversion tests consume the same fixtures.

### 15.3 Reference environments

Integration tests use two isolated DynamoDB Local tables or prefixes:

- a physical Prolly storage table for the client;
- a native logical reference table for differential behavior.

Tests must never mistake DynamoDB Local performance for AWS production capacity.
An opt-in AWS test profile validates item-size, transaction, throttling, IAM,
and ambiguous network behavior that the local emulator cannot faithfully model.

## 16. Phased implementation plan

| Phase | Dependency | Relative effort | Primary risk |
| --- | --- | --- | --- |
| 0. Async engine/backend readiness | existing Rust engine and Dynamo store | L | root visibility and provider action limits |
| 1. Logical DynamoDB core | Phase 0 | L | canonical model correctness |
| 2. Rust low-level CRUD client | Phases 0-1 | L | fluent/input compatibility |
| 3. Expressions and conditional writes | Phase 2 | XL | semantic divergence and error precedence |
| 4. Query, Scan, batches, history | Phase 3 | L | pagination and evaluated-byte behavior |
| 5. Transactions, commits, idempotency | Phases 0-4 | XL | multi-root atomicity and ambiguity |
| 6. Version administration and maintenance tools | Phase 5 | L | bounded history and safe maintenance |
| 7. Secondary indexes and explicit Rust workers | Phases 5-6 | XL | index publication and worker leases |
| 8. Rust release and language expansion | all declared features | L | dependency drift, contention, security, recovery |

### Phase 0: Async engine and backend readiness

#### Context and background

The native Rust engine already exposes `AsyncVersionedMap`, `AsyncProlly`, and
`AsyncIndexedMap`, and the store crate already provides
`RemoteProllyStore<DynamoDbBackend>`. The remaining version-list/diff-page,
CAS-restore, retention, and multi-map conveniences must be completed in the
engine. The adapter also publishes rewritten nodes and roots in one strict
transaction, so legal logical writes can exceed the provider action limit. A
client built before these gaps close would work only for small examples or
duplicate engine lifecycle rules.

This phase is owned by the client plan and has no service prerequisite.

#### Scope and implementation

**Core:** `src/prolly/versioned_map.rs`, `src/prolly/transaction.rs`, store
capabilities, tests, and docs.

**Provider:** `stores/prolly-store-dynamodb` plus shared store conformance.

- [x] Complete native async version listing, pinned snapshots, diff pages,
  CAS restore, retention, and change-subscription primitives.
- [x] Implement `AsyncVersionedMapsTransaction` or equivalent core-owned
  multi-map coordination.
- [x] Add explicit transaction publication modes and provider action limits.
- [x] Implement verified immutable upsert prepublication for DynamoDB; keep all
  conditioned root movements atomic and forbid staged node deletes.
- [x] Add a Dynamo-safe tree configuration and serialized-node safety test.
- [x] Add chunked `DynamoDbBlobStore` behavior to the native Rust provider path.
- [x] Add ambiguous root-transaction reconciliation primitives.
- [x] Add DynamoDB Local and fake-store failure tests at every publication
  boundary.

#### Deliverable

A Rust example constructs `DynamoDbBackend` from a caller-configured AWS SDK
client, opens `DynamoDbStore`, writes/reads a near-limit blob-backed value,
lists and diffs versions, CAS-restores a version, and atomically updates two
maps. No logical DynamoDB item model exists yet.

#### Verification

```text
cargo test
cargo test --manifest-path stores/prolly-store-dynamodb/Cargo.toml
cargo run --manifest-path stores/prolly-store-dynamodb/Cargo.toml --example versioned_map
```

Run the provider integration suite against DynamoDB Local with an isolated
non-empty key prefix.

#### Exit gate

- A committed head never references a missing node or blob under injected
  failures.
- No serialized physical item exceeds the tested DynamoDB safety ceiling.
- Multi-map conflict leaves every participating head unchanged.
- Async lifecycle results match the synchronous semantic fixtures where both
  APIs overlap.
- Provider limits and publication mode are discoverable before logical open.

#### Rollback boundary

All new capabilities are additive. Existing raw remote-engine and atomic-all
store paths remain available. The client package is not published in this phase.

---

### Phase 1: Logical DynamoDB core

#### Context and background

Prolly stores ordered byte keys and values. DynamoDB defines typed values, exact
decimal behavior, schema-bound keys, item limits, expressions, and operation
errors. These semantics must have one implementation in the core before the
client facade is allowed to expose them.

This phase defines that logical model in `prolly-dynamodb-core` for the client.

#### Scope and implementation

- [x] Create the standalone Rust core crate and conformance fixture directory.
- [x] Implement independent `AttributeValue`, `DynamoNumber`, `Item`, key schema,
  and table descriptor types.
- [x] Implement ordered PK/SK encoding and canonical `DDBI v1` item encoding.
- [x] Make durable format/blob field extraction panic-free and validate
  deserialized expression paths before evaluation; malformed or crafted input
  fails as typed corruption/validation errors.
- [x] Implement logical item-size validation and blob `ValueRef` integration.
- [x] Implement `DatabaseFormatRecord`, codec digests, and open negotiation.
- [x] Implement catalog create/describe/list/delete with immutable table IDs and
  lifecycle fences.
- [x] Implement command-independent `get_item`, unconditional `put_item`, and
  unconditional `delete_item` over `AsyncVersionedMap`.
- [x] Expose current head and exact historical point read.
- [x] Define stable core errors and capability records.
- [x] Add deterministic ID/time injection used only by tests.

#### Deliverable

An embedded Rust example creates a string-PK/numeric-SK table, writes canonical
items, reads current and historical values, and proves equivalent insertion
orders and decimal spellings produce the same state ID.

#### Verification

```text
cargo test --manifest-path dynamodb/core/Cargo.toml
cargo run --manifest-path dynamodb/core/Cargo.toml --example embedded_table
```

Property tests cover encode/decode, ordering, table recreation, format mismatch,
and cross-store deterministic roots.

#### Exit gate

- Canonical fixtures have explicit versions and pass in Rust and a fixture
  reader outside the core crate.
- Legal keys round-trip and sort identically to the declared DynamoDB subset.
- Equivalent items produce identical bytes and state IDs.
- Delete/recreate cannot expose the old table's versions through the new name.
- The core has no AWS SDK, HTTP, client-facade, or transport dependency.

#### Rollback boundary

The crate is not yet a public application API. It can be removed without
altering existing Prolly package interfaces; any experimental namespace is test
only.

---

### Phase 2: Rust low-level CRUD client

#### Context and background

This is the first application-facing vertical slice. It validates the central
claim: familiar AWS SDK for Rust fluent calls and official low-level model types
can target a local Prolly logical database while the package owns no credentials
and deploys no service.

Conditions, update expressions, Query, batches, and transactions remain gated
so the package cannot silently emulate them incorrectly.

#### Scope and implementation

- [x] Create `dynamodb/client` as the `prolly-dynamodb-client` crate with no
  language-binding or service dependency.
- [x] Align Rust 1.91.1, `aws-sdk-dynamodb = 1.73.0`,
  `prolly-store-dynamodb = 0.6.0`, and the store's `prolly-map` version.
- [x] Implement `open(DynamoDbBackend)` and `open_store(DynamoDbStore)`
  constructors plus mutually exclusive builder inputs; preserve
  the backend's physical table, roots table, key prefix, parallelism, AWS
  endpoint, credentials, retry configuration, and any existing
  `RemoteStoreConfig`.
- [x] Implement official AWS input/output conversions and crate-owned fluent
  builders for `CreateTable`, `DescribeTable`, `ListTables`, `DeleteTable`,
  `GetItem`, unconditional `PutItem`, and unconditional `DeleteItem`.
- [x] Implement input-first `execute_*` methods for the same operations.
- [x] Implement async open, capability negotiation, tracing, cooperative
  cancellation, Rust drop semantics, and optional idempotent shutdown without
  shutting down the caller's shared AWS client.
- [x] Implement official output construction and the typed local error wrapper.
- [x] Implement `send_with_metadata`, `table`, `head`, and historical
  `GetItem`.
- [x] Reject every unsupported input field before logical mutation work.
- [x] Add client README examples and a DynamoDB Local starter.
- [x] Add compile tests for advertised fluent chains and multi-process Rust
  point-write/head-conflict tests.
- [x] Add store-API contract tests covering custom roots table, non-empty key
  prefix, all four parallelism settings, backend/store constructors, explicit
  schema initialization, and read-only backend inspection. Initialization
  revalidates the final ACTIVE description after create/`ResourceInUse` races;
  deterministic AWS-protocol replay tests prove incompatible primary and roots
  race winners fail closed. The provider crate forbids unsafe Rust.

#### Deliverable

A Rust backend changes its client type and construction while keeping the
normal `client.get_item()...send().await` shape and AWS `AttributeValue` and
output types. It can inspect the resulting table version and read the previous
version without an HTTP service.

#### Verification

```bash
cargo test --manifest-path dynamodb/client/Cargo.toml
cargo test --manifest-path dynamodb/client/Cargo.toml --test fluent_compile
cargo run --manifest-path dynamodb/client/Cargo.toml --example direct_crud
cargo package --manifest-path dynamodb/client/Cargo.toml
```

Run one contract example twice in separate processes against the same physical
namespace and verify both observe committed state.

#### Exit gate

- Advertised fluent inputs compile using official AWS SDK model types.
- Standard outputs contain no enumerable Prolly extension fields.
- Unsupported fields fail with stable capability errors; unsupported operations
  are absent or exposed only as explicit unsupported stubs.
- Conversion into the core owns all bytes needed after `.send()` begins.
- The client never changes the caller-configured backend or shuts down the
  shared AWS SDK client.

#### Rollback boundary

The package is published as an experimental prerelease and uses an isolated
physical prefix. Existing Prolly/adapter packages remain source-compatible.

---

### Phase 3: Expressions, conditional writes, and fluent parity

#### Context and background

Real DynamoDB CRUD depends heavily on condition, update, and projection
expressions. A superficially compatible `UpdateItem` is unsafe: differences in
path resolution, old-value evaluation, number arithmetic, or error precedence
can change application data. Rust removes JavaScript number coercion from this
surface, but exact decimal value and canonical spelling, expression value typing, validation order,
and generated-builder field behavior still require deliberate compatibility.

#### Scope and implementation

- [x] Implement the shared Rust expression lexer, parser, typed AST, condition
  evaluator, update planner, and projection evaluator.
- [x] Support the declared expression names/values, functions, paths, limits,
  return values, and condition-failure old values.
- [x] Route conditional `PutItem`/`DeleteItem` and `UpdateItem` through the
  optimistic retry loop with complete re-evaluation.
- [x] Add whole-table `expected_head` and `table.if_head` APIs.
- [x] Complete compatibility setters and input-first execution for the
  corresponding official AWS operation inputs.
- [x] Preserve exact base-10 values from `AttributeValue::N(String)` without
  binary floating point, canonicalizing equivalent spellings by trimming
  leading/trailing zeroes as required by DynamoDB's documented number model.
- [x] Implement stable validation/error precedence fixtures.
- [x] Differential-test supported expressions against a native DynamoDB Local
  reference table.
- [x] Add fuzz/property tests for parser termination, depth, paths, and
  deterministic update plans.

#### Deliverable

The Rust client executes common conditional CRUD/update workloads with AWS SDK
model types. A caller can combine an item condition with an expected table head
and receive an official AWS output plus explicit version metadata.

#### Verification

```text
cargo test --manifest-path dynamodb/core/Cargo.toml expressions
cargo test --manifest-path dynamodb/client/Cargo.toml expressions
cargo test --manifest-path dynamodb/client/Cargo.toml --test fluent_compile
```

The differential suite compares final items and error categories, including
failed conditions and nested update paths. Its native DynamoDB Local matrix
covers all advertised condition-function families, boolean/range forms,
update clauses, `ALL_NEW`/`UPDATED_NEW`, and `ALL_OLD` condition-failure
images with explicit expected result sets.

#### Exit gate

- Update operands are proven to resolve against the immutable old item.
- Root-conflict retries re-evaluate every condition and update expression.
- Precision-sensitive numbers remain validated decimal strings and never pass
  through binary floating-point.
- Unsupported grammar fails before any node/blob publication.
- Fluent-builder and input-first paths produce identical canonical items.

#### Rollback boundary

Expression commands are advertised by capabilities independently. Disabling
them leaves Phase 2 unconditional CRUD and historical point reads intact.

---

### Phase 4: Query, Scan, batches, pagination, and historical collections

#### Context and background

Query and Scan introduce ordering, range planning, evaluated-byte limits,
post-read filters/projections, and pagination. Batch operations add partial
success and cross-table snapshot choices. Historical collection reads must pin
every page intentionally instead of accidentally following head.

#### Scope and implementation

- [x] Implement base-table key-condition planning for every supported PK/SK
  type and operator.
- [x] Implement forward/reverse `Query` on a pinned immutable snapshot.
- [x] Implement serial `Scan`, select/count modes, filter/projection order, and
  the evaluated 1-MB boundary.
- [x] Implement standard `ExclusiveStartKey`/`LastEvaluatedKey` behavior.
- [x] Implement first-party query/scan paginator helpers after fluent/input-first
  compatibility tests.
- [x] Implement `BatchGetItem` with one pinned version per table.
- [x] Implement non-atomic `BatchWriteItem`, duplicate validation, bounded work,
  and `UnprocessedItems`.
- [x] Add `table.at(...).query/scan`, async Rust page streams, and
  `client.batch_get_item().at(pins)` reads across explicitly pinned table
  versions.
- [x] Keep structural cursors private and use typed pinned versions plus standard
  logical continuation keys; defer signed tokens until a cross-process cursor
  serialization extension creates a trust boundary.
- [x] Differential-test order, counts, empty filtered pages, limits, and
  continuation behavior.

#### Deliverable

The package supports the common single-table access plane and batch operations.
Applications can choose standard moving-head pagination or repeatable
version-pinned iteration explicitly.

#### Verification

- Golden tests cover every key type, operator, direction, and boundary.
- Query never uses full-table scan for a valid key condition.
- Filtering happens after evaluated-byte accounting.
- A concurrency test advances head between pages and proves standard and pinned
  behavior are distinct and documented.
- Batch fault injection returns valid partial success without claiming atomicity.

#### Exit gate

- Client and native-reference results match for the supported subset.
- Every page is internally pinned to one version.
- Historical page iterators never switch versions.
- Batch metadata represents multiple transitions per table when necessary.

#### Rollback boundary

Query, Scan, batches, and each paginator are separately capability-gated.
Disabling them preserves earlier CRUD support.

---

### Phase 5: Transactions, durable commits, and idempotency

#### Context and background

DynamoDB transactions require one coherent multi-table read set, ordered
cancellation reasons, unique item targets, idempotency, and atomic publication.
Prolly supplies multi-root primitives after Phase 0, but logical transaction
limits and physical root actions are different. Ambiguous network outcomes are
especially dangerous for non-idempotent updates.

#### Scope and implementation

- [x] Implement shared-core `TransactGetItems` and `TransactWriteItems` models.
- [x] Use the core async multi-map coordinator; never sequence per-table commits
  in the compatibility facade.
- [x] Evaluate all conditions from one transaction read set and preserve
  cancellation reason order.
- [x] Implement per-table commit-log maps, sequence allocation, shared
  `CommitId`, and before/after transitions.
- [x] Route accepted writes through commit recording, including no-op events.
- [x] Implement standard `ClientRequestToken` fingerprint/replay/expiry.
- [x] Implement extension tokens for `send_with_metadata` single-write/admin
  calls.
- [x] Validate logical item and physical root-action limits before preparation.
- [x] Add ambiguous response reconciliation and process-restart token tests.
- [x] Expose commit/transition metadata without changing standard outputs.

#### Deliverable

Rust low-level transaction builders atomically update multiple logical tables
from independent client processes. Token replay returns the original result and
one commit resolves to every exact table transition.

#### Verification

- Inject failure at every validation, prepare, root condition/write, response,
  and reconciliation boundary.
- Race same-table and disjoint-table transactions across processes.
- Restart the process between ambiguous submission and token reconciliation.
- Compare supported cancellation categories/order with DynamoDB Local.
- Verify a failed transaction advances no participating head.

#### Exit gate

- Every successful transaction has exactly one logical commit identity.
- Every participating table references that commit in ordered table history.
- Token replay never creates another transition.
- Unknown outcomes are reconciled without blind replay.
- Capability discovery reports both logical and effective physical limits.

#### Rollback boundary

Transaction commands and durable commit recording are feature-gated. If commit
recording becomes mandatory for all writes, the namespace format record is
advanced only after all active writers support it.

---

### Phase 6: Version administration and maintenance tools

#### Context and background

Earlier phases create durable history but expose only basic access. Production
users need bounded discovery, diff, restore, retention, backup/import, and
explicit operational tooling. Destructive maintenance should not run
implicitly in request processes.

#### Scope and implementation

- [x] Complete `head`, paginated `versions`, `commits`, `commit`, streaming
  `diff`, CAS `restore`, and retention APIs.
- [x] Add bounded, resumable structural diff and history cursors.
- [x] Implement descriptor plus snapshot-bundle export/import.
- [x] Add an explicit Rust administrative CLI/crate entry point for bootstrap,
  verify, backup, import, retention, and bounded global GC planning.
- [x] Add GC application only after exact-plan replay/audit and partial-delete
  recovery tests prove the executor verifies the same lease and root digest.
- [x] Require a durable fail-closed global writer fence for sweep or migration
  cutover; expiry alone never re-admits writers, and release/break is audited.
- [x] Run fluent/input-first/core parity traces against the same physical
  namespace and isolated equivalent namespaces.
- [x] Document worker deployment, maintenance authority, and rollback.

#### Deliverable

Operators can inspect and compare history, restore with CAS, plan retention/GC,
and export/import a logical table using the client crate and explicit
administrative tooling.

#### Verification

- Restore loses safely to a concurrent writer unless its expected head matches.
- Retained/protected versions remain complete after a verified GC test; pruned
  versions fail closed.
- Export/import preserves descriptor semantics and table state ID.
- Fluent/input-first/core traces produce identical logical results and
  canonical roots.
- Worker restart preserves checkpoints and subsequent history.

#### Exit gate

- Large version lists/diffs are bounded and resumable.
- No destructive maintenance method runs without an explicit call and authority
  context.
- Administrative and worker capabilities are machine-readable.
- Backup/import and retention plans are dry-run capable before mutation.

#### Rollback boundary

Administrative mutations and workers are separate features. Read-only history
remains usable when restore, prune, import, or GC is disabled.

---

### Phase 7: Secondary indexes and explicit background workers

#### Context and background

Many DynamoDB schemas need alternate access paths. The repository already has
native `AsyncIndexedMap`, so this phase does not invent another asynchronous
tree or index coordinator. It maps DynamoDB LSI/GSI semantics onto those strict
derived-index primitives. TTL and stream materialization still require durable
leases/checkpoints and must not be incidental work tied to arbitrary
request-client lifetime.

#### Scope and implementation

- [x] Reuse `AsyncIndexedMap` for base/index atomic coordination and add only
  DynamoDB-specific definitions, projections, descriptor linkage, and
  historical pairing above it.
- [x] Add LSI/GSI descriptors, canonical index entries, projections, and
  base/index atomic publication.
- [x] Implement `IndexName` Query/Scan in the Rust fluent and input-first APIs.
- [x] Pair historical base versions with exact historical index versions.
- [x] Add shadow build, verify, activate, retire, and retention workflows.
- [x] Define explicit Rust `Worker` traits/types for
  commit-to-stream materialization, TTL expiry, and maintenance.
- [x] Implement leases, checkpoints, idempotent records, backpressure, and
  graceful cancellation.
- [x] Do not start any worker from normal client `open`; require an explicit
  worker constructor and cancellation token.
- [x] Prove that a dedicated leased worker process interoperates with ordinary
  client readers and writers.

#### Deliverable

Applications can query declared secondary indexes with state-version fidelity.
Operators may deliberately run stream/TTL workers in a dedicated process.

#### Verification

- Every base mutation matches a clean index rebuild oracle.
- Injected failure never exposes a base version without its declared synchronous
  index versions.
- Sparse/non-unique indexes and all supported projections are covered.
- Historical index queries never use a current index head.
- Worker crash/restart misses no commit; duplicate delivery has stable IDs.
- TTL races never delete an item whose expiry value changed.

#### Exit gate

- DynamoDB index coordination owns no duplicate tree algorithm and delegates
  strict publication to `AsyncIndexedMap`.
- Index activation is CAS-based and rollback retains the previous generation.
- Worker ownership is lease-enforced across independent processes.
- Stream/TTL retention cannot silently invalidate an active checkpoint.

#### Rollback boundary

Indexes activate through descriptor/catalog CAS. Workers are independent
processes and can be disabled without changing synchronous base-table CRUD.

---

### Phase 8: Rust production release, performance envelope, and language expansion

#### Context and background

The client package moves database semantics into every application process. A
production release therefore depends on Cargo dependency compatibility, mixed
writer correctness, bounded resource use, contention measurements, and clear
operational/security guidance. The single-table-head architecture must be
measured before a sharded alternative is selected.

#### Scope and implementation

- [ ] Publish reproducible, provenance-attested Rust crates and verify
  `cargo package` contents from clean downstream applications.
  All release/qualification manifests now carry checked-in lockfiles, including
  the root and DynamoDB store graphs that were previously ignored despite CI
  using `--locked`. Package creation and extracted archive tests are locked.
  A reviewed clean commit, signed provenance, registry publication, and
  post-publication consumer verification are still required, so this gate
  remains open.
- [x] Test the supported Rust target, TLS, Tokio runtime, default-feature, and
  minimal-feature matrices.
  Locked local evidence covers Rust 1.91.1 on native
  `aarch64-apple-darwin` plus GCC-15.2.0-linked
  `aarch64-unknown-linux-gnu` and `x86_64-unknown-linux-gnu`. Every declared
  target passes root minimal/default/Tokio and provider/core/client/admin
  all-target matrices. Rustls with exact `aws-lc-rs 1.17.3` and
  `aws-lc-sys 0.43.0` is the only supported TLS configuration. DynamoDB Local
  lifecycles pass on caller-owned Tokio current-thread and multi-thread
  runtimes. Cross-target results prove compilation/linking, not Linux runtime
  or hosted AWS behavior; those remain explicit gates below.
- [ ] Add multi-process/mixed-version soak tests with throttling, cancellation,
  process death, and ambiguous outcomes.
  The current-binary DynamoDB Local slice passes with four independent writer
  processes and 50 acknowledged writes each. One writer is killed after
  starting operation five, restarted from zero with stable request tokens, and
  converges to exactly 200 items, 201 versions, and 201 sequential unique
  commits. Existing provider fault tests cover cancellation and ambiguous
  acceptance. Independently released mixed binaries, provider throttling, and
  hosted duration remain required, so this gate stays open.
- [ ] Benchmark cold/warm reads, writes, Query, Scan, batches, transactions,
  history, indexes, blobs, and GC with physical request/byte/cost reporting.
  `benchmarks/dynamodb-client` covers the complete logical facade described
  above and records SDK executions, HTTP attempts/retries, wire bytes, per-API
  fan-out, physical transaction actions, exact returned transitions, machine
  and binary provenance, process CPU, and peak RSS. Runner v13 makes full and
  focused-history workloads and teardown mode explicit and requires a
  revision/sample-bound `gc-reachability.csv` artifact for full runs. The
  artifact records a checked, history-scaled protected-tree ceiling; the
  validator fails closed on absent, malformed, duplicate,
  revision-mismatched, over-limit, or empty-graph evidence. An opt-in physical
  teardown path is restricted to runner-owned explicitly ephemeral Compose
  projects and executes on success or failure; shared/external endpoints retain
  namespace cleanup. Runner v15 binds the client node-cache byte ceiling into
  the executable CLI, per-sample cache evidence, run manifest, and validators,
  preventing memory evidence from silently comparing different cache policies.
  The resumable matrices define 10K items at 1/16/64/near-400 KiB, 1M items at
  1 KiB, and history depths 10/100/1K/10K/100K; qualification refuses dirty or
  narrowed runs. Matrix-v2 aggregate validators enforce exact schemas, case
  order, revision, cache ceiling, sample count, result directories, and
  completion manifests. Full 1/64/399-KiB and history 10/100/1K smoke matrices
  pass both initial execution and validation-only resume, with runner-owned
  DynamoDB Local removed after every case.

  Format 12 removes the former 1,024-write indexed-history ceiling using one
  bounded active coordinator plus a current-only per-table catalog of compact
  locators. Each locator binds the exact indexed snapshot ID and a detached,
  content-addressed one-record tree containing the full immutable manifest.
  Current-only commit roots, transaction-pinned reads, and ordered strongly
  consistent named-root batches reduce history amplification. The 1,100-write
  core contract passes and verifies 1,110 exact roots, including 1,103 immutable
  version roots. An isolated DynamoDB Local format-12 depth-1,000 append now
  takes 35.552 seconds, 22,797 SDK executions, 322,173,160 request bytes,
  505,937,748 response bytes, 9,000 physical transaction actions, and
  46,956,544-byte peak RSS. Format 11 took 41.423 seconds, 24,520 executions,
  383,827,260 request bytes, and 752,415,229 response bytes; the superseded
  monolithic layout took 68.830-103.696 seconds,
  58,768-59,812 executions, and 336,330,752-420,921,344-byte RSS.

  The current client reduces this amplification without a format change or a
  service hop. Transaction caches promote only CID-validated node bytes after
  an applied commit; conflict and rollback writes remain isolated. Ordered
  batches pin required global/table roots. Indexed-publication readback is
  skipped only through an explicit durable-publication capability whose
  conservative default is false. DynamoDB opts in because a successful write
  is durably persisted and the adapter retries returned `UnprocessedItems`
  until empty, otherwise failing before root publication. Five depth-100 local
  samples now use exactly 803 executions per 100 appends, 7,729,455 mean
  request bytes, 291,579 mean response bytes, and the same 900 transaction
  actions: 52.9%, 3.1%, and 95.6% reductions from the accepted format-12
  baselines. Median local time is 1.522 seconds, not an AWS latency claim. A
  depth-1,000 one-sample rerun passes in 16.709 seconds and 8,003 executions,
  versus 35.552 seconds and 22,797 executions before this optimization.
  A runner-v14 depth-10,000 rerun with the selected 64-MiB default passes all
  exact rows in 246.989 seconds with 101,177 executions, 3.430 GB request bytes,
  0.917 GB response bytes, 90,000 transaction actions, and 184,401,920-byte
  peak client RSS. This reduces append time 44.0%, executions 64.7%, request
  bytes 21.3%, and response bytes 91.2% from the prior format-12 run. A separate
  256-MiB diagnostic used 98,763 executions and 233.487 seconds but
  545,619,968-byte RSS; selecting 64 MiB cuts that RSS 66.2% for 2.4% more calls
  and 5.8% more one-sample local time. Cache weight is not a hard RSS limit, so
  production memory qualification remains open. Runner v15 records a
  single-lock cache-occupancy snapshot per sample and fail-closed validation
  rejects configuration drift, impossible pinned counts, or unpinned
  serialized-node weight above the declared ceiling; peak RSS remains a
  separate gate. A depth-10,000 v15 rerun passes all six rows and records 2,936
  entries, 67,104,564 retained bytes, zero pins, and 183,484,416-byte peak RSS
  under the 67,108,864-byte ceiling. Its 279.640-second append and 101,004 total
  SDK executions remain dirty DynamoDB Local evidence, not an AWS envelope.

  GC expands snapshot-catalog protection directly and consults an append-only
  per-table registry of blobs introduced by successful writes/imports. Full
  depth-1,000 GC passes with 984 retained roots, 2,861 protected trees, 2,855
  live nodes, 56 blob-scan nodes, and 3,073 scanned values. The additional
  protected trees are detached manifests; retention proves removed locators
  remove their trees from this set. Plans canonically
  bind `protected_trees`, `scanned_blob_nodes`, and `scanned_values`, and apply
  recomputes them before deletion. The registry is deliberately conservative:
  blobs referenced only by removed history can remain pending a future exact,
  audited registry compaction. Clean 10K/100K/full-size and hosted-AWS matrices
  remain unexecuted, so this checkbox remains open.

  A runner-v12 pre-optimization format-12 10K history diagnostic passes all six exact
  rows plus namespace cleanup. Append creates 10,000 versions in 441.004
  seconds using 286,898 SDK executions, 90,000 transaction actions, 4.357 GB
  request bytes, 10.392 GB response bytes, and 89,948,160-byte peak client RSS;
  enumeration returns 10,001 unique versions in 230.768 milliseconds and 33
  calls. Relative to format 11, append latency falls 31.5%, request bytes 15.0%,
  response bytes 24.3%, and executions 1.8%; RSS rises 4.6% but stays bounded.
  Per-item namespace cleanup leaves whole-run time at 1,462.62 seconds, down
  from 2,123 seconds. This proves 10K correctness and bounded client memory.
  The improved result above removes most call/response amplification but does
  not make a 100K run safe on the current 31-GiB emulator: remaining duration
  and storage pressure plus the measured 184-MB client RSS require an explicit
  envelope. Formal clean ten-sample 10K and 100K qualification remain open.

  Runner v13 removes the obsolete fixed 10,000-tree harness ceiling and records
  a checked history-scaled bound in GC evidence. A full depth-10K diagnostic
  passes all 40 exact rows under a 50,000-tree bound with 9,984 retained roots,
  29,861 protected trees, 29,969 live nodes (23,346,531 bytes), 170 blob-scan
  nodes, and 30,073 scanned values. GC plan/apply use 321/32 SDK executions and
  1.296/0.320 seconds; whole-process peak RSS is 245,334,016 bytes. Isolated
  volume teardown completes the run in 508 seconds. This closes the local 10K
  GC diagnostic only; the dirty one-sample result does not close clean repeated,
  100K, full-size, or hosted-AWS qualification.
- [ ] Publish supported table/item sizes, concurrency, latency, memory/cache,
  and transaction shapes.
  Client-level controls now bound logical retries (default seven after the
  first attempt, hard maximum 63) and decoded-node caching (default 64 MiB of
  retained serialized-node weight, optional node-count and byte ceilings, zero
  disables). Core format-neutrality,
  exact injected-conflict attempt behavior, pre-provider validation, fluent
  compile, and DynamoDB Local capability-report contracts cover these controls.
  Production RSS and concurrency envelopes remain measurement gates, so this
  checkbox remains open.
- [ ] Test upgrade/downgrade and rolling compatibility across client versions
  for every persisted format.
  Format 12 now has a checked-in exact-byte fixture plus semantic decode;
  formats 10 and 11 remain historical decode guards only. The current format has
  malformed-envelope and every-field fail-closed tests. This closes the
  current-source negotiation prerequisite but not the checkbox: genuine
  rolling upgrade/rollback evidence requires an independently built historical
  package and must not be simulated with two builds of the same source.
  The packaged `rolling_compatibility_probe` and fail-closed Python coordinator
  now provide the executable cross-binary gate, including exact format-record
  comparison, concurrent writes, dual state/history verification, reciprocal
  immutable reads, binary hashes, and forensic reports. Its identical-binary
  diagnostic passes, but cannot close this checkbox.
- [x] Add backup/restore, corruption, retention/GC, and lost-process drills.
- [x] Decide whether hot tables retain one head, use admission/microbatching, or
  migrate to partition-sharded roots plus a snapshot manifest.
  Version 1 retains one serializable table head, admits writes locally before
  speculative work, and relies on provider CAS plus bounded retries across
  processes. Implicit microbatching is rejected because it changes request
  acknowledgement/isolation semantics. Partition-sharded roots remain a
  separately formatted future design, triggered only by hosted contention
  evidence that exceeds the published single-head envelope.
- [x] Stabilize the Rust API, MSRV policy, AWS/store dependency policy, and
  compatibility matrix.
  `dynamodb/client/public-api.txt` freezes every reviewed public signature and
  trait implementation except generic blanket impls. A pinned
  `cargo-public-api 0.52.0`/`nightly-2026-06-19` verifier fails CI on drift,
  and package verification requires the exact baseline in the archive. The
  compatibility reference defines 0.1.x source compatibility, pre-1.0 minor
  migration rules, Rust 1.91.1, exact AWS SDK/AWS-LC inputs, aligned store/core
  releases, and the three supported compilation targets.
- [x] Keep version 1 Rust-only. Any separately approved language package must
  bind the same core and consume the same fixtures; it must not port semantics
  manually or introduce a service dependency into this client.

#### Deliverable

A production-ready Rust client crate, compatibility/capability reference,
benchmark and cost guide, security/deployment profiles, disaster-recovery
runbook, and an evidence-based scale decision.

The security profile is delivered in `dynamodb/client/SECURITY.md` with
exact-table runtime/provisioner policy templates under `deploy/aws`. Policy
tests derive the physical SDK action set from the provider source. The profile
correctly treats prefixes as collision isolation rather than hostile IAM
tenancy and treats client history as mutable application evidence, not WORM.

#### Verification

- CI installs packaged crates in clean downstream workspaces rather than using
  repository path dependencies.
- Every supported Rust target/runtime feature combination compiles, and hosted
  targets run CRUD, transaction, history, and shutdown smoke tests.
- Multi-process soak tests maintain catalog/head/commit integrity.
- Restore/replay drills recover the declared RPO/RTO.
- Production-shaped AWS tests validate limits not represented by DynamoDB Local.

#### Exit gate

- No production mode requires unaudited raw physical-table administration in an
  application request process.
- Workload and cost amplification limits are published.
- Operators can identify and reconcile every ambiguous committed operation.
- Mixed client versions pass the declared reader/writer compatibility tests.
- The scale decision is backed by measured contention thresholds.

#### Rollback boundary

Breaking scale/layout changes use a new format generation and shadow migration.
The previous generation remains readable until verification and atomic cutover
complete. Package rollback is allowed only within the durable format record's
declared writer range.

## 17. Capability progression

| Capability | P0 | P1 | P2 | P3 | P4 | P5 | P6 | P7 | P8 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Remote async version lifecycle | yes | yes | yes | yes | yes | yes | yes | yes | hardened |
| DynamoDB logical core | no | yes | yes | yes | yes | yes | yes | yes | stable |
| Rust construction from backend/store | no | no | yes | yes | yes | yes | yes | yes | stable |
| AWS-model fluent CRUD | no | Rust internal | subset | yes | yes | yes | yes | yes | stable |
| Conditions/UpdateItem | no | no | no | yes | yes | yes | yes | yes | stable |
| Official-input `execute_*` API | no | no | CRUD | CRUD | CRUD/batch | transactions | history | indexes | stable |
| Query/Scan/batches | no | no | no | no | yes | yes | yes | yes | stable |
| Historical collection reads | engine only | point | point | point | yes | yes | yes | yes | stable |
| Transactions/commits | engine only | no | no | no | no | yes | yes | yes | stable |
| Version admin/diff/restore | engine only | minimal | minimal | minimal | reads | yes | yes | yes | stable |
| Admin/maintenance tooling | no | no | no | no | no | no | yes | yes | stable |
| Secondary indexes | async engine primitive | no | no | no | no | no | no | yes | stable |
| Explicit stream/TTL workers | no | no | no | no | no | commit base | interfaces | yes | stable |

## 18. Performance and cost model

A client `GetItem` can require a catalog/head read plus several Prolly
node reads and a blob fetch. A logical write can rewrite a tree path, prepublish
multiple immutable records, and transact several roots. In-process execution
removes a service hop but does not remove this physical amplification.

Every benchmark reports:

```text
logical operation latency and bytes
physical DynamoDB request counts and bytes by API
nodes/blob chunks read and written
tree height and cache/hint hit rate
head conflicts and retry amplification
logical items versus physical root transaction actions
versions/commits created and nodes reused
estimated provider request/storage cost
client/core CPU time and process memory
```

Required matrices include 1-KB, 16-KB, 64-KB, and near-400-KB items; 10K and 1M
item tables; cold/warm point reads; uniform and hot-table writers; small and
1-MB queries; 1/10/100-item logical transactions where effective limits allow;
and history depths from 10 to 100K transitions.

### 18.1 Bulk ingestion and large commits

High-cardinality migration must not be implemented as one compatible
`PutItem` or `BatchWriteItem` call per source record. Two explicit extension
paths preserve the compatible API while reducing version and commit count:

```rust
let imported = client
    .bulk_import_sorted(
        "Events",
        KeyAttribute { name: "id".into(), kind: KeyKind::String },
        None,
        primary_key_sorted_items,
        BulkImportOptions::default(),
    )
    .await?;
assert_eq!(imported.item_count, 1_000_000);

let mut writes = client
    .table("Events")
    .write_session()
    .options(LargeWriteOptions::default())
    .if_head(imported.version);
writes.put(next_item)?;
writes.delete(expired_key)?;
let commit = writes.commit().await?;
```

`bulk_import_sorted` creates a fresh table. It consumes strictly increasing
canonical primary keys through the memory-bounded sorted tree builder,
prepublishes immutable nodes and blobs, and atomically exposes the catalog,
table head, version root, index snapshot, blob registry, and one commit. Thus a
one-million-record import creates one table version and one commit, not one
million versions.

`WriteSession` is an explicit buffered extension for an existing table. One
successful `commit()` produces one version and one commit for all distinct
buffered item targets. Its caller-selected `LargeWriteOptions` bounds item
count and logical bytes. It requires `PrepublishImmutableNodes`; compatible
`TransactWriteItems` remains limited to 100 items and 4 MiB.

The optimized commit path loads the global commit catalog and all participating
table logs with one ordered root batch. Immutable node publication is divided
into DynamoDB's 25-request batches and bounded by the backend's
`batch_write_parallelism`; large-value preparation overlaps up to eight
independent content-addressed blob uploads. A final root transaction remains
the only visibility point.

## 19. Risks and mitigations

| Risk | Consequence | Mitigation |
| --- | --- | --- |
| AWS SDK generated API or model types drift | Fluent calls stop compiling or map differently after an SDK upgrade | Initial exact SDK alignment, public input/output conversion only, compile fixtures, SDK-version upgrade CI |
| Client is mistaken for a security boundary | Raw physical access bypasses invariants | Trusted-backend scope, dedicated roles/tables, application-owned API for enforceable policy |
| One table head is hot | Conflict retries and write ceiling | Metrics, bounded retries/admission, Phase 8 measured scale decision |
| Physical amplification surprises users | Higher latency/cost than native DynamoDB | Diagnostics, benchmarks, published workload/cost envelope |
| Facade and core semantics drift | Fluent and input-first calls produce different results | One Rust core, thin conversions, shared fixtures, API-path parity CI |
| Mixed package versions corrupt format | Different writers produce incompatible state | Durable format record, min reader/writer ranges, fail-closed open |
| Cargo resolves incompatible SDK/store versions | Duplicate public model types or compile failure | Re-export the aligned dependencies, exact initial version, clean downstream resolution tests, documented upgrade policy |
| Rust compile time or binary size is excessive | Poor developer/deployment experience | Feature-gate admin/workers, measure clean builds, avoid duplicated AWS clients and transport stacks |
| Ambiguous provider response duplicates update | Incorrect non-idempotent state | Durable tokens, operation IDs, reconciliation, no blind replay |
| Background work starts in many app processes | Duplicate/unsafe TTL, stream, or GC behavior | No implicit workers, explicit leases, one elected worker owner |
| Physical node/item exceeds provider limit | Backend rejection after work | Dynamo-safe config, blob offload, serialized-size preflight tests |
| Retention/GC races readers | Missing historical content | Retained-root proof, leases/fences, plan then sweep, fault tests |
| SDK middleware-dependent libraries fail | Source compatibility overclaimed | No concrete-client claim; explicit helpers and tested integration matrix |

## 20. Alternatives considered

### 20.1 Use TypeScript as the reference client implementation

Rejected. It would duplicate canonical numbers/items, expressions, table
lifecycle, commits, version handling, and eventually indexes from the Rust
core and would not satisfy compatibility with the native Rust store API.
TypeScript can be a later binding over the same Rust core and fixtures.

### 20.2 Reuse the generated AWS `*FluentBuilder` directly

Rejected. The generated builder contains a private service handle and its
`send()` path invokes Smithy HTTP. Public `*InputBuilder`, `*Input`, and output
types are reusable; local execution requires a crate-owned fluent builder.

### 20.3 Dereference or transparently wrap `aws_sdk_dynamodb::Client`

Rejected. Unsupported calls could escape to the physical Prolly storage table,
and generated middleware/error behavior would imply an HTTP exchange that did
not occur. A separate client type with an explicit operation matrix is honest
and type-safe.

### 20.4 Add a DynamoDB-compatible service to the client roadmap

Rejected. It adds HTTP protocol compatibility, independent authentication,
network admission, deployment, and another client surface without being needed
by trusted Rust backends. Plan 018 is archived only as prior research. Reviving
it requires a new architecture decision based on a concrete wire-compatibility
or independent-security-boundary requirement.

### 20.5 Store logical items as native DynamoDB rows and keep history separately

Rejected for authoritative version semantics. Two stores cannot atomically
agree on every write without a durable coordination protocol, and historical
whole-table snapshots/diffs would no longer derive from the authoritative
state. It remains a migration or asynchronous shadow-history option.

### 20.6 Start TTL/stream/GC timers inside every client

Rejected. Application process lifetime is not a durable scheduler, multiple
instances duplicate work, and request roles often should not have maintenance
permissions. Workers must be explicit and leased.

## 21. Whole-program done criteria

The client package is complete for its declared v1 scope when:

- supported AWS SDK for Rust fluent call chains use official AWS model and
  output types and require only the documented client/error type adaptations;
- every accepted operation and input field appears in a machine-readable
  compatibility matrix and every unsupported field fails closed;
- fluent, input-first, and core-level paths use one logical core and pass
  parity traces;
- canonical key/item/expression fixtures pass across core and Rust client;
- current and historical reads are version-correct under concurrent writers;
- conditions, retries, transactions, and restores never partially publish;
- near-limit legal logical items work without oversized physical records;
- commit IDs, state IDs, tokens, and no-op behavior are distinct and documented;
- clients in separate processes coordinate without local correctness
  assumptions;
- version list/diff/restore/retention/backup workflows are bounded and
  restartable;
- no background worker starts implicitly;
- production artifacts, security guidance, upgrade/rollback, recovery, and
  workload/cost envelopes are published and tested;
- packaged crates compile in clean downstream workspaces at the declared MSRV
  and aligned AWS/store dependency versions;
- both `DynamoDbBackend`-first and existing `DynamoDbStore`-first construction
  preserve the current Rust adapter configuration in contract tests.

## 22. STOP conditions

Stop implementation and return to design review if any phase requires:

- implementing managed version roots, tree mutation, or diff logic in the
  compatibility facade rather than Prolly core;
- duplicating canonical DynamoDB semantics between the client facade and core;
- depending on private AWS SDK fluent constructors, command serialization, or
  Smithy internals;
- accepting an unknown command or request field with guessed behavior;
- treating a caller-owned raw SDK client as a security boundary;
- moving a mutable head before every referenced node/blob is durable;
- retrying an ambiguous non-idempotent mutation without durable reconciliation;
- using an in-process mutex/queue as the distributed correctness mechanism;
- claiming `BatchWriteItem` is one atomic version/commit;
- starting TTL, stream, retention, or GC work implicitly during client open;
- changing canonical bytes or writer behavior without format negotiation and a
  migration/rollback path;
- claiming native DynamoDB performance, capacity, IAM, global-table, backup,
  stream, or TTL equivalence without explicit evidence and compatibility scope.

## 23. External and repository references

External semantic references:

- AWS SDK for Rust DynamoDB examples and fluent builder pattern:
  <https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_dynamodb_code_examples.html>
- DynamoDB low-level API actions:
  <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html>
- DynamoDB constraints:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html>
- Query key ordering and limits:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.KeyConditionExpressions.html>
- Query filter ordering:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.FilterExpression.html>
- BatchWriteItem partial atomicity and limits:
  <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html>
- DynamoDB transactions:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html>

Repository references:

- `plans/018-versioned-dynamodb-compatible-service.md` (archived alternative)
- `docs/versioned-map.md`
- `docs/secondary-index-design.md`
- `docs/async-first-api-inventory.md`
- `docs/language-store-adapters-design.md`
- `stores/prolly-store-dynamodb/Cargo.toml`
- `stores/prolly-store-dynamodb/src/lib.rs`
- `stores/prolly-store-dynamodb/README.md`
