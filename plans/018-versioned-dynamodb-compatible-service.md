# Versioned DynamoDB-Compatible Service Research (Closed, Archived Alternative)

> **Decision:** The active product does not include this service. The in-process
> Rust client in plan 019 is sufficient and is the only planned deliverable.
> This document is superseded contingency research. It contributes no phases,
> estimates, dependencies, or release gates to the client package. This document
> is retained as prior research, not as queued work. Reopening it requires a new
> architecture decision based on a concrete need for wire-compatible non-Rust
> SDK clients or an independently deployed credential/security boundary. Do not
> build it merely to support `prolly-dynamodb-client`.

> **Default disposition:** do not implement or deploy this service. The active
> architecture is the in-process Rust client in plan 019. Keeping this document
> does not reserve engineering capacity or imply a future delivery commitment.

> **Not a component:** Nothing in this document is part of the Versioned
> DynamoDB system described by plan 019. In particular, the client package must
> not depend on this protocol, deployment model, authentication boundary, or
> server lifecycle. The document may inform a future, separately approved
> product only; it must not be used to derive current implementation tasks.

> **Historical material only:** the phases below are preserved to retain the
> protocol and deployment research that led to the client-side decision. They
> are not executable backlog, acceptance criteria, or implied follow-on work.
> Reactivation requires a new design and approval; this archived phase list
> must not be implemented directly as though it were current.

> **Accepted replacement:** Plan 019's in-process Rust client is the complete
> production architecture. It talks directly to the existing Rust DynamoDB
> store adapter and ordinary DynamoDB tables. Optional maintenance workers link
> the same crates and are not request-serving services. No item in the archived
> phase list below is needed to ship, operate, or complete Versioned DynamoDB.

**Superseded contingency goal:** If a network boundary is later required and a
new architecture decision reactivates this work, build an
authoritative DynamoDB-compatible data service whose current table state is
exposed through familiar DynamoDB data-plane operations while every distinct
table state remains addressable as an immutable Prolly version.

**Architecture:** A shared logical core owns DynamoDB schemas, attribute values,
expressions, request semantics, and version behavior. A thin service owns the
AWS-compatible network protocol. One `AsyncVersionedMap` owns each logical
table. The existing `prolly-store-dynamodb` crate remains the physical
content-addressed node/root backend and is not confused with the logical
DynamoDB API being emulated.

**Tech stack:** Rust 1.91.1, the existing async-first Prolly engine,
`prolly-store-dynamodb 0.6.0` with its aligned
`aws-sdk-dynamodb 1.73.0`, `RemoteProllyStore<DynamoDbBackend>`, Tokio, an
HTTP/AWS JSON 1.0 server, deterministic binary codecs, and DynamoDB Local plus
official AWS SDK clients for compatibility tests.

> **Boundary update:**
> `019-versioned-dynamodb-client-package.md` is authoritative and independently
> owns the logical core, Rust client, version administration, and explicit
> workers. If this service is revived, it may reuse `prolly-dynamodb-core`, but
> the core and client must never acquire a service dependency. This plan would
> own only AWS JSON/HTTP, authentication, network admission, and deployment.
> Rust crate names follow Cargo conventions. Any later npm service/client
> binding uses the `@crabbuild` scope.

## Status

- **Priority:** none / superseded contingency
- **Effort:** XL
- **Risk:** High
- **Status:** Superseded; not planned and not required for the Rust client
- **Activation condition:** a new approved architecture decision requiring
  DynamoDB wire compatibility or an independent credential/security boundary
- **Depends on:** the stabilized plan 019 logical core/format, existing
  `AsyncVersionedMap`, async transactions, large-value references, remote-store
  protocol, and `prolly-store-dynamodb`
- **Planned at:** `b3d69526b4dd`, 2026-08-08
- **Decision owner:** Prolly maintainers

## 1. Executive summary

This design makes Prolly authoritative for logical DynamoDB items. AWS SDK
clients send normal DynamoDB requests to a configurable service endpoint. The
service translates table/item operations into deterministic ordered keys and
canonical values, executes them against immutable Prolly trees, and atomically
advances table heads.

The initial architecture deliberately provides stronger current-table reads
than DynamoDB's default eventual reads: all base-table reads use a committed
Prolly head or an explicitly pinned historical version. Returning the latest
state when an eventually consistent read was permitted is compatible with the
client contract. Exact AWS capacity accounting, global tables, DAX, and the
full AWS control plane are not goals.

There are two identities:

- `TableVersionId` identifies one unique table state. It is the existing
  `MapVersionId`; identical states have the same identifier.
- `CommitId` identifies an applied service transaction or write event and may
  reference one or more table transitions. It is added only when the durable
  per-table commit logs are implemented. It does not replace table state
  identity.

The MVP retains one mutable head per table. This gives exact table snapshots,
historical reads, diffs, and rollback, but serializes successful writes through
one head CAS. It is intended first for moderate write concurrency, not as a
claim of native DynamoDB partition-scale throughput.

## 2. Background and current foundation

### 2.1 Why a compatibility layer is required

`prolly-store-dynamodb` currently answers a different question: it stores
content-addressed Prolly nodes, named roots, and hints in a physical DynamoDB
table. It does not store application items as native DynamoDB rows and does not
implement `GetItem`, `Query`, expressions, table schemas, or other DynamoDB
application semantics.

The new component therefore sits above the engine:

```text
AWS SDK / CLI
      |
      | DynamoDB AWS JSON requests
      v
Versioned DynamoDB compatibility service
      |
      | canonical byte keys and values
      v
AsyncVersionedMap / AsyncProllyTransaction
      |
      | nodes, roots, hints, blobs
      v
RemoteProllyStore<DynamoDbBackend>
      |
      v
physical AWS DynamoDB table
```

The physical table is an implementation detail. Logical table names, primary
keys, item attributes, indexes, and versions are owned by the compatibility
service and never exposed as physical `pk`/`value` records.

### 2.2 Existing reusable capabilities

| Existing capability | Current owner | Use in this design |
| --- | --- | --- |
| Immutable ordered trees | `ProllyEngine` | Authoritative table state |
| Current head plus immutable versions | `AsyncVersionedMap` | Table state/version lifecycle |
| Historical pinned snapshots | `AsyncMapSnapshot` | Repeatable `GetItem`, `Query`, and `Scan` |
| Optimistic root conditions | async transaction overlay | Conditional publication and retries |
| Multi-root atomic commit primitive | `AsyncProllyTransaction` | Cross-table transactions and indexes |
| Deterministic diff | async engine diff | Version comparison and stream materialization |
| Large value references | `ValueRef` plus `AsyncBlobStore` | Near-400-KB logical items |
| Physical DynamoDB backend | `DynamoDbBackend` | Durable nodes, roots, and hints |
| Async secondary-index coordinator | `AsyncIndexedMap` | Strict base/index publication for logical LSI/GSI mapping |
| Retention and GC | version/root and reachability APIs | Version lifecycle |

### 2.3 Gaps that must be closed

1. There is no DynamoDB `AttributeValue` model or deterministic item codec.
2. There is no order-preserving codec for DynamoDB's 38-digit number key type.
3. There is no DynamoDB expression parser/evaluator.
4. There is no logical table catalog or DynamoDB control-plane subset.
5. There is no AWS JSON-compatible server.
6. DynamoDB LSI/GSI descriptors and projections are not yet mapped onto the
   existing `AsyncIndexedMap` coordinator.
7. Async version listing, rollback, retention, and high-level multi-map
   transactions are less complete than the synchronous facade.
8. The physical DynamoDB transaction currently counts rewritten Prolly nodes,
   root conditions, and root writes against DynamoDB's 100-action limit.
9. Default Prolly nodes may be much larger than DynamoDB's 400-KB item limit.
10. The physical DynamoDB adapter has no blob implementation for offloaded
    logical items.

## 3. Product and compatibility contract

### 3.1 Goals

- Let existing applications keep the normal DynamoDB request and response
  shapes for the supported data-plane operations.
- Require only a client endpoint/configuration change for supported official
  AWS SDKs.
- Make the current table head the default target of every normal operation.
- Make historical versions immutable and directly readable.
- Preserve primary-key uniqueness, sort-key ordering, conditional-write
  behavior, update-expression behavior, pagination, and transaction atomicity.
- Keep logical table schemas and version retention explicit and durable.
- Reuse the existing async engine instead of creating a second tree algorithm.
- Make each implementation phase deployable, reversible, and measurable.

### 3.2 Non-goals

- A drop-in replacement for the AWS-hosted endpoint without changing client
  configuration.
- Identical AWS billing, RCU/WCU consumption, adaptive capacity, throttling,
  CloudWatch, IAM policy evaluation, encryption administration, or SLAs.
- DAX, global tables, multi-Region replication, PartiQL, backup/export jobs,
  resource policies, or the complete DynamoDB control plane in the first
  release.
- Preserving physical native DynamoDB row layout for logical items.
- Hiding the table-wide head contention inherent in exact table snapshots.
- Treating a content-derived state version as an audit event. No-op writes and
  reversions may point to an already-known `TableVersionId`.

### 3.3 Compatibility levels

Every advertised operation is assigned one level:

- **Exact:** observable request, result, ordering, error, and atomicity match
  the documented DynamoDB behavior, excluding capacity metadata.
- **Compatible stronger:** the service provides a stronger guarantee permitted
  by the DynamoDB request, such as returning the latest committed state for an
  eventually consistent base-table read.
- **Subset:** common request fields work, but named unsupported fields fail with
  `ValidationException`; fields are never silently ignored.
- **Extension:** behavior is outside the DynamoDB API and lives under the
  `_prolly` HTTP namespace or explicit client middleware.
- **Unsupported:** fail deterministically and document the missing capability.

The service exposes its operation/field matrix through
`GET /_prolly/v1/capabilities` and a versioned static document checked into the
repository.

Target v1 operation levels are:

| Operation family | Target level | Phase | Notes |
| --- | --- | --- | --- |
| `CreateTable`, `DescribeTable`, `ListTables`, `DeleteTable` | Subset | 2 | Logical schema/lifecycle; no AWS fleet/capacity control |
| `GetItem`, `PutItem`, `DeleteItem` | Exact or compatible stronger | 2 | Capacity fields are absent or explicitly estimated |
| `UpdateItem` | Subset promoted field-by-field | 2 | Expression matrix is normative |
| `Query`, `Scan` | Exact for declared fields | 3 | Serial Scan first; no Parallel Scan claim |
| `BatchGetItem`, `BatchWriteItem` | Exact for declared fields | 3 | Preserve partial batch behavior |
| `TransactGetItems`, `TransactWriteItems` | Exact within advertised internal limits | 4 | Backend root-action limit may be lower for many tables |
| LSI/GSI `Query` and `Scan` | Subset | 6 | No independent AWS capacity model |
| Streams and TTL APIs | Subset/extension | 7 | Native Prolly feed precedes Streams protocol subset |
| PartiQL, DAX, global tables, exports, backups, IAM control plane | Unsupported | — | Fail or remain outside the endpoint |
| Historical reads, diff, restore, retention | Extension | 1/3/5 | `_prolly` API and optional headers |

## 4. Core correctness invariants

These invariants apply in every phase:

1. A logical table name resolves through exactly one committed catalog
   snapshot to an immutable `table_id` and schema.
2. A table head names one complete tree. Readers never observe a root whose
   reachable nodes or offloaded values are missing.
3. A standard read without a version extension resolves the head once and pins
   it for the complete operation.
4. A paginated historical read continues against the same table version.
5. A conditional write evaluates against the same candidate base version it
   later attempts to replace. A head conflict causes full re-read and
   re-evaluation, not blind publication.
6. A write commits only while the catalog snapshot that resolved an `ACTIVE`
   table remains current. Concurrent delete/recreate cannot redirect a staged
   write to another table incarnation.
7. Attribute encoding is canonical. Logically equal items produce identical
   bytes and therefore identical table-state roots.
8. Key encoding is injective and preserves the documented DynamoDB sort order.
9. New immutable nodes and blobs may be prepared before head publication, but
   a mutable head never moves until all referenced content is durable.
10. A failed strict transaction advances none of its participating table heads.
11. Standard `BatchWriteItem` is not reported as all-or-nothing. Individual
    successful writes remain valid even when other requests are returned as
    unprocessed.
12. A `TransactWriteItems` request either advances every participating table
    state or none.
13. Restoring an old table version creates a head transition but does not
    mutate the historical version.
14. Retention removes catalog references before GC removes unreachable content.
15. Unknown, pruned, expired-cursor, or not-cataloged-for-target-table version
    IDs fail closed.
16. Unsupported request fields return a stable compatibility error rather than
    being accepted with different semantics.

## 5. Proposed repository and module boundaries

Create a transport-independent logical core plus a thin standalone service
crate. The companion Rust client layout and shared-core boundary are specified
in plan 019:

```text
extensions/dynamodb/core/
  Cargo.toml
  src/
    model/
    expression/
    catalog/
    engine/
    history/
    worker/

extensions/dynamodb/client/                 # owned and phased by plan 019
  Cargo.toml                     # package: prolly-dynamodb-client
  src/
    lib.rs
    fluent/
    conversion/
    executor/

services/prolly-dynamodb/
  Cargo.toml
  README.md
  src/
    main.rs
    config.rs
    error.rs
    protocol/
      mod.rs
      aws_json.rs
      routes.rs
      request.rs
      response.rs
      auth.rs
    admin/
      mod.rs
      routes.rs
    worker/
      runtime.rs
  tests/
    aws_sdk_contract.rs
    auth_contract.rs
    executor_parity.rs
```

`prolly-dynamodb-core` owns the model, canonical persisted encodings,
expressions, catalog/table semantics, transactions, commits, version operations,
indexes, and worker state machines. The service adapts AWS JSON/HTTP requests to
that core and must not keep transport-specific copies of those modules.

Required changes outside the shared core and service crates are narrowly scoped:

- `src/prolly/versioned_map.rs`: missing native async lifecycle and multi-map
  coordination needed by the service.
- `src/prolly/transaction.rs`: only capability-safe transaction publication
  improvements accepted by the core contract.
- `stores/prolly-store-dynamodb`: Dynamo-sized configuration helpers,
  immutable-node prepublication support if approved, and an async blob store.
- `docker-compose.store-services.yml`: optional service plus DynamoDB Local.
- `conformance/`: language-neutral DynamoDB item/key/expression fixtures.

The shared DynamoDB core and service must not decode raw Prolly nodes or
duplicate routing, mutation, diff, proof, or merge algorithms.

## 6. Persistent logical model

### 6.1 System catalog

Use one managed map with ID equivalent to the segment tuple:

```text
["system", "dynamodb", "catalog", "v1"]
```

Catalog keys are segment-safe tuples:

```text
["table-name", utf8_table_name] -> TableDescriptor
["table-id", 16-byte table_id]  -> TableNameRecord
```

The catalog is not updated for ordinary item writes. That would turn table-local
head serialization into a database-wide catalog-head bottleneck. Durable
commit order is maintained by a separate per-table commit-log map described
below.

Transaction idempotency records live outside the catalog in 256 sharded maps:

```text
["system", "dynamodb", "idempotency", "v1", token_hash[0]]
```

This avoids a global catalog write for every tokenized transaction while
keeping token/result publication in the same strict multi-map transaction.

`TableDescriptor` contains:

```rust
struct TableDescriptor {
    format_version: u16,
    table_id: [u8; 16],
    table_name: String,
    status: TableStatus,
    partition_key: KeyAttribute,
    sort_key: Option<KeyAttribute>,
    indexes: Vec<IndexDescriptor>,
    table_format_digest: [u8; 32],
    retention: RetentionPolicy,
    created_at_millis: u64,
    deleted_at_millis: Option<u64>,
}
```

Table names are mutable catalog labels; `table_id` is immutable. Reusing a
deleted name produces a new ID so old versions can never be mistaken for a new
table incarnation.

### 6.2 Table map IDs and root isolation

One logical table uses one map ID:

```text
["system", "dynamodb", "table", table_id, "items"]
```

Derived index maps use:

```text
["system", "dynamodb", "table", table_id, "index", index_id]
```

The optional durable commit log for a table uses:

```text
["system", "dynamodb", "table", table_id, "commits"]
```

Root names remain generated by `VersionedMap`; applications do not construct
or publish them directly. A table descriptor stores the tree format digest so
all service instances reject configuration drift before serving the table.

### 6.3 Primary-key encoding

The ordered Prolly key is:

```text
0x01 || escaped_segment(partition_component)
     || [escaped_segment(sort_component)]
```

`0x01` is the logical key-format version. The complete encoded partition
segment is a query prefix, so all items with one partition key are contiguous.

Component encodings depend on the schema-fixed DynamoDB key type:

- String: UTF-8 bytes, validated as a non-empty legal key value.
- Binary: raw bytes, compared unsigned.
- Number: the ordered decimal encoding below.

The numeric representation supports DynamoDB's bounded 38 significant digits:

```text
zero:     0x01
positive: 0x02 || biased_adjusted_exponent || 38 right-zero-padded digits
negative: 0x00 || inverted_biased_exponent || 9's-complement padded digits
```

The adjusted decimal exponent range `-130..=125` maps to `0..=255`. Two digits
may be packed per byte after ordering tests prove nibble packing equivalent to
the reference digit-vector representation. Input numbers are normalized by
removing leading zeros, removing insignificant trailing fractional zeros, and
canonicalizing every zero to `0`.

Required key-codec tests include:

- injectivity for unequal legal keys;
- equality of equivalent number spellings such as `1`, `1.0`, and `1E0`;
- complete ordering across negative, zero, and positive values;
- exponent boundaries and 38-digit coefficients;
- embedded zero bytes in binary and UTF-8 encodings;
- prefix bounds for every partition key;
- forward/reverse `Query` ordering identical to reference fixtures.

### 6.4 Canonical item encoding

Do not store request JSON or a generic serializer's incidental map order.
Define and version a `DDBI` binary envelope:

```text
magic "DDBI" | format_version | sorted attribute entries
```

Each value has a one-byte type tag and explicit big-endian lengths. Attribute
maps are sorted by UTF-8 attribute-name bytes. Lists preserve order. Sets are
validated as non-empty, deduplicated, normalized, and sorted by canonical
element bytes. Numbers use one normalized decimal spelling. Binary values are
raw bytes. Recursive documents enforce DynamoDB's depth bound.

The internal model is independent of AWS SDK types:

```rust
enum AttributeValue {
    String(String),
    Number(DynamoNumber),
    Binary(Vec<u8>),
    Bool(bool),
    Null,
    StringSet(Vec<String>),
    NumberSet(Vec<DynamoNumber>),
    BinarySet(Vec<Vec<u8>>),
    List(Vec<AttributeValue>),
    Map(BTreeMap<String, AttributeValue>),
}
```

The codec validates logical DynamoDB item size before internal serialization.
Logical size, API payload size, Prolly leaf size, and physical backend item size
are separate measurements and must never be conflated.

### 6.5 Large logical items and Dynamo-sized nodes

The public compatibility target remains DynamoDB's 400-KB logical item limit.
Because a near-limit item plus Prolly node overhead cannot fit in one physical
DynamoDB item, the service uses `ValueRef` for canonical items above a
conservative inline threshold, initially 48 KiB.

Add a content-addressed `DynamoDbBlobStore` using physical keys:

```text
prefix || "blob:manifest:" || blob_cid
prefix || "blob:chunk:"    || blob_cid || u32be(chunk_index)
```

Blob chunks are at most 192 KiB. A small manifest keyed by the whole-value blob
CID records total length, chunk count, and ordered chunk CIDs. Reads verify the
reassembled bytes against that whole-value CID. Blobs are written and verified
before a table head may reference them. Failed head CAS may leave unreachable
blobs; retention-aware blob GC reclaims them later.

The table uses logical-byte chunking with an explicitly validated physical
serialized-node hard limit no greater than 300 KiB. Tests serialize the maximum
permitted node and assert the complete physical DynamoDB item remains below
400 KiB, including `pk`, `value`, and SDK overhead assumptions.

## 7. Version and commit semantics

### 7.1 Table versions

`TableVersionId` is the existing timestamp-free, content-derived
`MapVersionId`. It identifies the complete canonical table state and tree
format. Consequences are intentional:

- an idempotent/no-op write does not create a new table version;
- returning to an earlier state returns the earlier state ID;
- timestamps, authors, request IDs, and parentage are not encoded in the state
  ID;
- historical reads remain stable while the table head advances.

The hash itself is not table-namespaced: two tables with identical state and
format may have the same `MapVersionId`. Every external durable reference is
therefore a `(table_id, TableVersionId)` pair, and historical lookup verifies
that the target table's version catalog contains the ID.

Every successful standard response includes the current table version when one
table is involved:

```text
x-prolly-table-version: <64 lowercase hex characters>
```

The header is an extension. Its absence from clients that discard unknown
headers does not affect normal operation semantics.

### 7.2 Durable commits

A later phase adds `CommitRecord` to represent an applied event:

```rust
struct CommitRecord {
    format_version: u16,
    commit_id: [u8; 32],
    operation_id: [u8; 16],
    committed_at_millis: u64,
    operation: CommitOperation,
    transitions: Vec<TableTransition>,
    request_fingerprint: [u8; 32],
    client_token: Option<String>,
}

struct TableCommitEntry {
    table_sequence: u64,
    previous_table_commit: Option<[u8; 32]>,
    commit: CommitRecord,
}

struct TableTransition {
    table_id: [u8; 16],
    before: Option<TableVersionId>,
    after: TableVersionId,
}
```

`CommitId` hashes the canonical commit body including timestamp and request
identity/operation ID, so distinct events may point to the same table state. Each
participating table appends the same logical commit under its own monotonically
increasing `table_sequence` and links it to that table's previous commit. There
is intentionally no global commit sequence or total order across unrelated
tables. This powers per-table resumable streams, idempotency, and audit-oriented
integrations without changing state identity or creating a global write
hotspot.

Once durable commit logging is enabled, every accepted content-changing API
operation records an event even when its before/after `TableVersionId` values
are equal. Such an event is useful for request audit/idempotency, but produces no
item-level stream record because its logical diff is empty.

### 7.3 Historical request extensions

Normal DynamoDB operations always target current heads. Historical access is
additive:

- AWS request middleware may add `x-prolly-at-version` to `GetItem`, `Query`,
  or `Scan`.
- Clients without middleware use the `_prolly` endpoints.
- Historical requests are read-only. Standard writes reject the historical
  header.

Initial admin endpoints:

```text
GET  /_prolly/v1/tables/{name}/head
GET  /_prolly/v1/tables/{name}/versions?limit=&cursor=
POST /_prolly/v1/tables/{name}/versions/{id}/get-item
POST /_prolly/v1/tables/{name}/versions/{id}/query
POST /_prolly/v1/tables/{name}/versions/{id}/scan
POST /_prolly/v1/tables/{name}/diff
POST /_prolly/v1/tables/{name}/restore
POST /_prolly/v1/tables/{name}/retention
```

Restore requires an `expectedHead` unless an explicit administrative `force`
mode is enabled. The default is CAS, never unconditional overwrite.

## 8. DynamoDB operation semantics

### 8.1 Read snapshot selection

At operation start:

1. Resolve table name through one catalog snapshot.
2. Load and pin the requested table version or current head.
3. Execute all point/range reads against that tree.
4. Return the pinned version in response metadata.

`BatchGetItem` and `TransactGetItems` pin every participating table before
reading any requested item. `TransactGetItems` is advertised for multiple
tables only after Phase 4 adds the atomic root-validation boundary described
below.

### 8.2 `GetItem`

- Validate the exact PK/SK fields and types.
- Encode the primary key and perform one snapshot point read.
- Resolve a `ValueRef` if present and verify blob CID/length.
- Apply `ProjectionExpression` after decoding.
- Omit `Item` for a missing key.
- Accept `ConsistentRead=false` but still return the pinned committed state.

### 8.3 `PutItem` and `DeleteItem`

- Pin current head and read the old item when conditions or return values need
  it.
- Validate the complete new item, key agreement, document depth, sets, numbers,
  and logical size.
- Evaluate `ConditionExpression` against the old item.
- Stage one upsert/delete and publish by expected head.
- On a known head conflict, restart from catalog/table resolution and
  re-evaluate the condition.
- Populate `ALL_OLD` or condition-failure return values from the exact pinned
  base item.

### 8.4 `UpdateItem`

Use a parsed typed AST. All right-hand operands resolve against the item state
before the update. Apply actions to a separate mutable result in DynamoDB's
defined clause/action semantics, then validate and replace the complete item.

Supported expression features are introduced behind an explicit matrix:

- document paths with map keys and list indexes;
- `SET`, `REMOVE`, `ADD`, and `DELETE` clauses;
- arithmetic `+`/`-` on exact numbers;
- `if_not_exists` and `list_append`;
- condition functions `attribute_exists`, `attribute_not_exists`,
  `attribute_type`, `begins_with`, `contains`, and `size`;
- `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`, `AND`, `OR`, and `NOT`;
- expression attribute names and values;
- `ALL_OLD`, `UPDATED_OLD`, `ALL_NEW`, and `UPDATED_NEW`.

Legacy `AttributeUpdates`, `Expected`, and `ConditionalOperator` fields are not
silently translated in the initial release; they return a documented subset
error until their conformance phase.

### 8.5 `Query`

The planner accepts exactly one partition-key equality and an optional legal
sort-key predicate. It converts them to a half-open Prolly range:

```text
partition equality                -> prefix range
sort = x                          -> exact encoded key
sort <, <=, >, >= x               -> adjusted half-open bounds
sort BETWEEN low AND high         -> inclusive logical bounds
begins_with(sort, prefix)         -> encoded partial-segment prefix range
```

Number sort keys do not support `begins_with`. Results are naturally ordered by
the encoded sort key. `ScanIndexForward=false` uses reverse traversal, not
collect-and-reverse.

The 1-MB evaluation limit is applied before filter/projection, matching
DynamoDB. `Count` is returned rows; `ScannedCount` is evaluated rows.
`LastEvaluatedKey` is the last evaluated logical primary key, even when filters
remove every returned row.

### 8.6 `Scan`

`Scan` traverses the pinned table snapshot in internal primary-key order.
DynamoDB does not promise global partition-key ordering, so this deterministic
order is compatible but not advertised as an API guarantee.

Phase-one scan support is serial. Parallel Scan requires a stable segment
assignment independent of tree shape; it is not implemented by naively slicing
tree pages. The later design uses `hash(canonical_partition_key) % TotalSegments`
and filters during traversal, then measures the cost before advertising it.

### 8.7 Pagination tokens

Standard responses return logical `LastEvaluatedKey`. Internally, the service
may carry a signed opaque cursor containing:

```rust
struct PageCursor {
    format_version: u16,
    table_id: [u8; 16],
    table_version: TableVersionId,
    index_id: Option<[u8; 16]>,
    direction: Direction,
    last_evaluated_primary_key: Vec<u8>,
    query_fingerprint: [u8; 32],
    expires_at_millis: u64,
}
```

For unmodified SDK compatibility, the next request still supplies
`ExclusiveStartKey`; current-head pagination therefore follows DynamoDB and may
observe a newer committed state. Clients that require repeatable pages use
`x-prolly-at-version` or the admin version endpoint. A cursor never silently
switches versions.

### 8.8 `BatchGetItem` and `BatchWriteItem`

`BatchGetItem` groups keys by table, pins table snapshots, uses ordered batch
reads, enforces request/result limits, and may return `UnprocessedKeys` under a
configured work budget.

`BatchWriteItem` preserves DynamoDB's non-atomic whole-request behavior:

- reject duplicate operations on the same logical item;
- execute each accepted put/delete atomically;
- permit a partially successful response with `UnprocessedItems`;
- do not claim one table version for the whole request;
- return the last observed table head in response metadata only as advisory.

Applications wanting one atomic version use `TransactWriteItems` or an explicit
Prolly extension, not `BatchWriteItem`.

### 8.9 Transactions

`TransactGetItems` begins an async Prolly transaction, loads the catalog and
every participating table head into the root-condition read set, reads
immutable items, and commits the read-only root conditions. A conflict restarts
the complete read. Successful atomic validation proves that all returned roots
were simultaneously current at the validation point; immutable nodes may then
be read safely from those roots.

`TransactWriteItems` processing is:

1. Validate at most 100 unique logical items and the request byte limit.
2. Begin one async Prolly transaction.
3. Load one catalog head into the transaction read set and resolve every table
   descriptor as `ACTIVE`.
4. Load each participating table head into the transaction read set.
5. Read all old items and evaluate every condition before applying mutations.
6. Build one candidate tree per table, including derived indexes when enabled.
7. Stage immutable version roots and new heads.
8. Stage the idempotency record and one ordered commit-log entry per
   participating table when supported by the phase.
9. Commit all root conditions and writes through the backend transaction.
10. On known root conflict, retry the whole transaction and re-evaluate all
    conditions; on semantic condition failure, return ordered cancellation
    reasons without retry.

`ClientRequestToken` stores a request fingerprint and result for the documented
idempotency window. Reusing a token with a different fingerprint fails. A
successful replay returns the original result without advancing table heads.

The physical DynamoDB limit is distinct from the logical 100-item limit.
Initially, a request may be rejected before execution when the number of
participating table roots plus internal writes exceeds the backend capability.
The capability endpoint reports both limits.

## 9. Expression architecture

The expression engine is a pure package with no storage access:

```text
request expression
  -> lexer with byte offsets
  -> parser
  -> name/value substitution and validation
  -> typed AST
  -> evaluation against immutable old item
  -> update plan or boolean/filter/projection result
```

The parser reports DynamoDB-shaped syntax and validation failures with stable
source spans. The evaluator owns exact type rules and never coerces strings,
numbers, or binaries. Update actions first resolve every source path/expression
against the old item and then apply the validated plan, which prevents
left-to-right mutation from changing later operands.

Property and differential tests are required:

- parser round-trip/AST snapshot tests;
- generated items and expressions against DynamoDB Local;
- update actions compared field-by-field;
- invalid type/path cases compared by error category;
- nesting, expression length, operator count, and substitution limits;
- deterministic behavior independent of map/hash iteration order.

## 10. Secondary-index architecture

Each GSI/LSI-like index is a separate versioned map. Its ordered key is:

```text
encoded index partition key
|| encoded optional index sort key
|| encoded base-table primary key tie-breaker
```

The tie-breaker preserves multiple base items with the same non-unique index
key. The value stores the projection (`KEYS_ONLY`, `INCLUDE`, or `ALL`) using the
canonical item codec.

Base table and active synchronous indexes advance in one transaction. Both the
Rust client and service reuse the existing `AsyncIndexedMap` coordinator; Phase
6 adds only the DynamoDB-specific descriptor, projection, historical-pairing,
and query-routing layer above it. A transport-specific coordinator is rejected
because it would let direct and server execution drift.

Compatibility rules:

- LSI descriptors can only be created with their table and share the base PK.
- GSI descriptors may use independent PK/SK definitions.
- Base-table writes never directly accept writes to index maps.
- `ConsistentRead=true` on a GSI is rejected, matching DynamoDB.
- Initial GSI updates may be synchronous even though AWS GSIs are eventually
  consistent. This is a compatible stronger state guarantee, but capacity and
  propagation metadata are not emulated.

## 11. Streams, TTL, retention, and GC

### 11.1 Change streams

Streams derive item changes from consecutive committed table transitions, not
from scans of the current table. `TableCommitEntry.table_sequence` is the
durable per-table resume position. A stream record contains table ID, commit ID,
table sequence, table before/after versions, key, event kind, selected old/new
images, and checksum.

Diff workers may checkpoint structural diff cursors. Records are idempotently
stored by `(stream_id, table_sequence, item_key)`. Delivery is at least once;
consumers deduplicate by record ID. DynamoDB shard topology and sequence-number
format are not emulated in the first stream phase.

### 11.2 TTL

TTL configuration lives in `TableDescriptor`. A background worker maintains an
expiry index keyed by `(expiration_second, base_primary_key)` and performs
conditional deletes against the version where it observed the TTL attribute.
If the item changed, the worker re-reads and re-evaluates rather than deleting a
new value. Expiry is asynchronous. Historical versions continue to contain the
item, as expected for state history.

### 11.3 Retention

Retention is per table and supports:

- keep last `N` distinct states;
- keep states newer than a duration;
- explicit protected version IDs;
- keep commits/stream records for a separate duration.

Pruning requires table maintenance authority. It removes immutable version root
names transactionally, records the retention decision, then schedules node and
blob GC. Store-wide GC retains every catalog, live table, protected historical
version, active index, commit, stream checkpoint, and prepared migration root.

## 12. Consistency, failures, and retries

### 12.0 Transaction publication modes

The core advertises one of two explicit transaction publication modes:

- `AtomicAll`: the backend atomically commits staged nodes and roots. Existing
  transactional stores keep this mode.
- `AtomicRootsWithImmutablePrepublication`: the engine verifies and publishes
  immutable node upserts before atomically validating/updating roots. This is
  the DynamoDB mode.

The second mode defines application visibility by reachability from committed
roots. Its required algorithm is:

1. Reject staged node deletes; logical version commits only create immutable
   nodes. Physical deletion belongs to separately authorized GC.
2. Verify every node key against its bytes and write node/blob content
   idempotently in provider-sized batches.
3. Confirm all writes completed; an error stops before any root movement.
4. Execute one backend transaction containing all root conditions and writes.
5. On conflict, report the conflict and leave prepared content unreachable.
6. On an ambiguous root-transaction outcome, reconcile the conditioned roots
   before deciding whether the logical commit applied.
7. Reclaim unreachable prepared content through normal retention-aware GC.

Raw store users who already know an orphan CID may observe its immutable bytes,
but no logical table/version catalog can reach it. The capability name and docs
must state this distinction; a store cannot advertise `AtomicAll` when it uses
prepublication.

### 12.1 Read guarantees

- Current base-table reads: committed, operation-pinned snapshot.
- Historical reads: immutable version snapshot.
- A single `Query`/`Scan`: one table version.
- Standard pagination without the version extension: may move between heads,
  matching DynamoDB's lack of whole-scan snapshot isolation.
- Multi-table nontransactional batch reads: one pinned head per table, not a
  global database snapshot.
- Transactional reads: one declared compatible commit boundary.

### 12.2 Write retries

Only known optimistic root conflicts are automatically retried. Conditions and
update expressions are re-evaluated on every retry. Validation errors,
condition failures, and resource limits are never retried.

An ambiguous transport/backend failure after commit submission is not blindly
replayed for non-idempotent updates. The service checks a durable request token
when available; otherwise it returns a retryable internal error with an
operation ID for reconciliation. The implementation must distinguish:

- request never submitted;
- known transaction canceled/conflicted;
- known committed;
- outcome unknown.

### 12.3 Hot-head behavior

Every content-changing write to one table competes on one head root. The
service records attempts, conflict retries, retry exhaustion, and time spent
rebuilding candidates. Admission control may serialize or microbatch writes for
one hot table, but it must preserve each operation's condition and return-value
semantics.

The MVP release gate is based on measured supported concurrency, not native
DynamoDB marketing targets. Sharded/table-manifest alternatives are evaluated
only after the single-head implementation has production-shaped data.

## 13. Protocol, authentication, and errors

### 13.1 AWS-compatible endpoint

The server accepts DynamoDB AWS JSON requests at `POST /`, dispatching by the
`X-Amz-Target` operation name. Protocol structs are service-owned `serde` types;
the server does not depend on private client serialization internals.

Official AWS SDK contract tests configure a custom endpoint and exercise the
service without custom request construction. The initial supported SDK matrix
is Rust, JavaScript v3, Python/boto3, and Go v2; one SDK is sufficient for the
first executable phase, and the matrix expands before production status.

### 13.2 Authentication modes

- `dev`: unsigned requests or any SigV4 test signature; loopback only by
  default.
- `static-sigv4`: verify SigV4 against configured access-key secrets.
- `proxy-auth`: trust identity headers only from an authenticated sidecar/mTLS
  peer.

Production startup fails if `dev` mode binds a non-loopback address without an
explicit unsafe override. Full IAM policy evaluation is not claimed.

### 13.3 Error mapping

Define one mapping table from internal categories to DynamoDB-shaped errors:

| Internal category | External error |
| --- | --- |
| Invalid schema/key/item/expression | `ValidationException` |
| Missing table | `ResourceNotFoundException` |
| Failed condition | `ConditionalCheckFailedException` |
| Transaction semantic failure | `TransactionCanceledException` |
| Known optimistic exhaustion | `TransactionConflictException` or throttling category by operation |
| Work/capacity admission rejection | `ThrottlingException` / `ProvisionedThroughputExceededException` according to configured mode |
| Internal backend failure | `InternalServerError` |
| Unsupported compatibility field | `ValidationException` with stable capability code |

Do not expose physical CIDs, root names, AWS credentials, or raw backend errors
in standard error messages. Correlation IDs connect client errors to structured
server logs.

## 14. Observability and operational controls

Required metrics, labeled by logical table ID rather than unbounded table name:

- requests, successes, failures, and latency by operation;
- logical bytes read/evaluated/returned/written;
- Prolly nodes and physical bytes read/written;
- head CAS conflicts and retries;
- transaction logical items, internal nodes, root actions, and backend actions;
- blob offloads, chunk reads, missing/corrupt blobs;
- versions created/reused/pruned;
- query/scan evaluated versus returned counts;
- stream lag and TTL lag;
- GC candidates, deleted nodes/blobs, and protected roots;
- per-table admission queue depth and wait time.

Operational endpoints:

```text
GET /health/live
GET /health/ready
GET /metrics
GET /_prolly/v1/capabilities
GET /_prolly/v1/operations/{operation_id}
```

Readiness verifies physical backend access, catalog readability, tree-format
compatibility, and transaction capability. It does not perform a destructive
write.

## 15. Phased implementation plan

| Phase | Dependency | Relative effort | Primary risk |
| --- | --- | --- | --- |
| 0. Core/backend readiness | existing engine and adapter | L | transaction visibility and provider limits |
| 1. Deterministic model | Phase 0 | L | key/value canonical correctness |
| 2. CRUD protocol | Phase 1 | XL | expression and SDK compatibility |
| 3. Query/Scan/batch | Phase 2 | L | pagination and evaluated-byte semantics |
| 4. Transactions/commits | Phases 0-3 | XL | cross-table atomicity and idempotency |
| 5. Version operations | Phase 4 | L | retention/GC and safe migration |
| 6. Secondary indexes | Phase 4 | XL | logical index mapping and atomic publication |
| 7. Streams/TTL | Phases 4 and 6 | L | durable resume and race-safe expiry |
| 8. Production/scale | all prior declared features | L | contention, security, and recovery |

### Phase 0: Core and backend readiness

#### Context and background

The existing DynamoDB adapter proves the remote backend contract, but normal
`AsyncVersionedMap` publication can place all rewritten nodes plus two root
writes in one DynamoDB transaction. Large mutations can hit the provider's
100-action ceiling before the logical DynamoDB API limit is considered. The
default node hard maximum also exceeds DynamoDB's physical item limit, and the
async facade lacks some lifecycle/multi-map conveniences needed by the service.

Building protocol behavior before resolving these boundaries would produce an
API that works only for toy items and fails unpredictably under batch or
transactional writes.

#### Scope and implementation

**Core files:** modify `src/prolly/versioned_map.rs`,
`src/prolly/transaction.rs`, `src/prolly/store/mod.rs`, documentation, and
focused tests.

**DynamoDB store files:** modify
`stores/prolly-store-dynamodb/src/lib.rs`, its README, examples, and tests.

- [ ] Add native async version listing, diff, rollback/CAS restore, and
  retention APIs backed by `AsyncManifestStoreScan`.
- [ ] Add `AsyncVersionedMapsTransaction` or an equivalent core-owned async
  multi-map coordinator; do not duplicate managed-root naming in the service.
- [ ] Define a transaction capability report containing maximum backend
  actions and the two publication modes above.
- [ ] Record the reachability-atomic decision in core transaction documentation,
  including orphan cleanup, failure outcomes, and the prohibition on staged
  node deletes.
- [ ] Implement verified immutable upsert prepublication for the DynamoDB mode;
  keep root conditions/writes in `TransactWriteItems` and reject staged node
  deletes before making any provider call.
- [ ] Add a DynamoDB-safe `Config` constructor using logical-byte chunking and a
  tested serialized-node hard limit.
- [ ] Implement `DynamoDbBlobStore` plus scan support and chunk manifests.
- [ ] Add a transaction-safe async large-value helper that durably writes and
  verifies the blob before staging its `ValueRef` in the transaction overlay.
- [ ] Add backend capability/limit errors before any provider request begins.
- [ ] Add DynamoDB Local tests for point writes, a tree taller than one level,
  large values, conflict rollback, orphan prepublication, and recovery.

#### Deliverable

A standalone engine example can create an async versioned map on DynamoDB
Local, write/read a legal near-limit logical value through blob offload, list
and diff versions, restore by CAS, and atomically update two maps without
violating advertised provider limits.

#### Verification

```text
cargo test
cargo test --manifest-path stores/prolly-store-dynamodb/Cargo.toml
cargo run --manifest-path stores/prolly-store-dynamodb/Cargo.toml \
  --example versioned_map_lifecycle
```

Run backend tests with the existing DynamoDB Local environment variables.

#### Exit gate

- No serialized physical item in the test matrix exceeds 400 KiB.
- A successful head never references a missing node/blob after injected
  failures at every publication boundary.
- Multi-map conflicts leave all heads unchanged.
- Backend action limits are discoverable before service construction.

#### Rollback boundary

All changes are additive capability paths. The existing atomic-all transaction
mode remains selectable and existing adapter users retain current behavior.

---

### Phase 1: Deterministic DynamoDB model and embedded versioned table

#### Context and background

Prolly accepts opaque bytes, while DynamoDB defines typed values, exact number
behavior, document rules, schema-fixed key types, and a specific sort order.
These semantics must be correct before HTTP compatibility: protocol tests built
on a noncanonical model would merely stabilize the wrong behavior.

#### Scope and implementation

**Files:** create `extensions/dynamodb/core` with `model`, `catalog`, and `engine` modules
plus the packaged canonical fixtures under `extensions/dynamodb/core/tests/fixtures` and
`extensions/dynamodb/client/src/fixtures`, as refined by plan 019.

- [ ] Implement `DynamoNumber` parsing, normalization, comparison, addition,
  subtraction, and bounded precision/range validation.
- [ ] Implement the ordered PK/SK codec and inverse decode.
- [ ] Implement `AttributeValue`, `Item`, item-size calculation, and `DDBI v1`.
- [ ] Add language-neutral golden fixtures for legal/illegal values, canonical
  bytes, sizes, and key order.
- [ ] Implement `TableDescriptor`, catalog storage, table-name resolution, and
  create/describe/list/delete lifecycle in the embedded API.
- [ ] Implement core `Table::{get, put, delete, head, at}` without
  expression strings; conditions may use a typed closure only in internal tests.
- [ ] Use large-value offload automatically above the inline threshold.
- [ ] Expose result metadata with before/after `TableVersionId`.

#### Deliverable

An embedded Rust example creates a table with string PK and numeric SK, writes
typed items, queries raw key ranges through the engine, reads an old version,
and proves canonical root equality for equivalent insertion order and number
spellings.

#### Verification

```text
cargo test --manifest-path extensions/dynamodb/core/Cargo.toml --test model_fixtures
cargo test --manifest-path extensions/dynamodb/core/Cargo.toml --test engine_contract
cargo run  --manifest-path extensions/dynamodb/core/Cargo.toml \
  --example embedded_versioned_table
```

#### Exit gate

- Golden codecs are stable and carry an explicit format version.
- Property tests prove encode/decode injectivity and order preservation.
- Equivalent logical items produce identical canonical bytes and table roots.
- Delete/recreate of a table name cannot access the old table ID or versions.

#### Rollback boundary

No network API exists. The new crate can be removed without changing core
storage formats beyond the separately approved Phase 0 capabilities.

---

### Phase 2: AWS-compatible CRUD server and expression engine

#### Context and background

This is the first phase in which existing SDK clients can use the service. CRUD
compatibility depends on expressions, especially `UpdateItem` and conditional
writes. Accepting request shapes before their conditions are correctly
implemented risks silent data corruption, so unsupported fields fail closed.

#### Scope and implementation

**Files:** add shared `extensions/dynamodb/core/expression`; add service `protocol`,
`main.rs`, HTTP configuration, SDK contract tests, and a Docker image
definition.

- [ ] Implement AWS JSON dispatch for `CreateTable`, `DescribeTable`,
  `ListTables`, `DeleteTable`, `GetItem`, `PutItem`, `DeleteItem`, and
  `UpdateItem`.
- [ ] Implement the lexer, parser, typed AST, condition evaluator, update
  planner, and projection evaluator described above.
- [ ] Implement `ReturnValues`, `ReturnValuesOnConditionCheckFailure`,
  expression names/values, stable errors, and request IDs.
- [ ] Implement bounded optimistic retries with full expression re-evaluation.
- [ ] Include the resolved catalog head in every write transaction read set and
  test races with `DeleteTable` plus same-name recreation.
- [ ] Accept `PAY_PER_REQUEST`; accept provisioned fields as descriptor metadata
  only when compatibility configuration allows it, never claim enforcement.
- [ ] Add `dev` authentication mode and safe bind defaults.
- [ ] Add response version headers and `GET /_prolly/v1/capabilities`.
- [ ] Run an official AWS SDK client against the service endpoint in CI.
- [ ] Differential-test expression fixtures against DynamoDB Local.

#### Deliverable

An unmodified supported AWS SDK can create a logical table and execute
conditional CRUD/update operations by changing only endpoint and credentials
configuration. The service runs against MemStore for fast tests and the
physical DynamoDB adapter for end-to-end tests.

#### Verification

```text
cargo test --manifest-path extensions/dynamodb/core/Cargo.toml --test expressions
cargo test --manifest-path services/prolly-dynamodb/Cargo.toml --test aws_sdk_contract
docker compose -f docker-compose.store-services.yml up -d dynamodb versioned-dynamodb
```

#### Exit gate

- Supported SDK CRUD tests pass without custom JSON construction.
- Every unsupported request field in the published matrix fails explicitly.
- Condition races demonstrate that stale candidates never publish.
- Update-expression results match DynamoDB Local fixtures, including old-item
  operand evaluation.

#### Rollback boundary

Deploy behind a separate endpoint and feature flag. No existing DynamoDB client
is redirected automatically.

---

### Phase 3: Query, Scan, batch operations, and pagination

#### Context and background

CRUD proves item semantics but not DynamoDB's main access patterns. Query and
Scan introduce ordering, post-read filtering, pre-filter byte limits, evaluated
counts, cursors, reverse traversal, and partial batch responses. These are easy
places to build a superficially compatible API with observably wrong behavior.

#### Scope and implementation

- [ ] Implement key-condition planning for all supported PK/SK forms.
- [ ] Implement forward/reverse `Query` on pinned snapshots.
- [ ] Implement serial `Scan`, projection/filter order, select/count modes, and
  the 1-MB evaluated-byte boundary.
- [ ] Implement `ExclusiveStartKey`/`LastEvaluatedKey` semantics.
- [ ] Implement `BatchGetItem` with per-table snapshot pinning and ordered
  reconstruction.
- [ ] Implement non-atomic `BatchWriteItem`, duplicate validation, bounded work,
  and `UnprocessedItems`.
- [ ] Add historical header support for `GetItem`, `Query`, and `Scan`.
- [ ] Add query fingerprints and signed cursors to `_prolly` repeatable-page
  endpoints.
- [ ] Add metrics for evaluated versus returned rows/bytes.

#### Deliverable

Existing SDK clients can use the common single-table DynamoDB data plane,
including pagination and batch operations. Version-aware clients can pin the
same operations to historical state.

#### Verification

- Golden Query tests cover every key type, operator, direction, empty page, and
  filtered-empty page with a continuation key.
- Scan/batch tests enforce 1-MB, 16-MB, 25-request, and 100-key boundaries.
- A concurrency test advances head between pages and proves standard versus
  version-pinned behavior is intentional.
- Differential tests compare logical items, ordering, counts, and continuation
  behavior with DynamoDB Local.

#### Exit gate

- Query never falls back to a full table scan for a valid key condition.
- Filters are never applied before evaluated-byte accounting.
- `BatchWriteItem` fault injection produces valid partial success rather than a
  false atomicity claim.

#### Rollback boundary

Advertise each operation independently through capability configuration.
Disabling Query/Scan/batch leaves Phase 2 CRUD intact.

---

### Phase 4: Strict transactions, durable commits, and idempotency

#### Context and background

Native DynamoDB transactions group up to 100 unique items across tables. Prolly
can atomically update multiple roots, but the service needs a high-level async
coordinator, ordered cancellation reasons, durable request tokens, and a clear
distinction between logical items and physical backend actions. Durable commits
also create the sequence needed for streams and audit integrations.

#### Scope and implementation

- [ ] Implement `TransactGetItems` and `TransactWriteItems` protocol models.
- [ ] Use the Phase 0 async multi-map coordinator; do not call separate
  per-table `apply` methods.
- [ ] Evaluate all conditions from one transaction read set.
- [ ] Implement ordered cancellation reasons and condition-failure old values.
- [ ] Add per-table commit-log maps, monotonic per-table sequence allocation,
  shared `CommitId`, previous-commit links, and table transition records.
- [ ] Route successful CRUD, individual `BatchWriteItem` actions, and
  transactions through the commit coordinator; preserve empty-diff commit
  events without inventing new state IDs.
- [ ] Implement `ClientRequestToken` fingerprinting, replay, mismatch failure,
  expiry, and cleanup.
- [ ] Report logical and physical transaction limits through capabilities and
  validate both before commit submission.
- [ ] Add ambiguous-outcome reconciliation tests.
- [ ] Return `x-prolly-commit-id` and per-table version metadata through the
  extension response surface.

#### Deliverable

Official SDK transaction calls atomically change one or more supported logical
tables. Replaying a token returns the original commit. The admin API resolves a
commit to exact per-table before/after states.

#### Verification

- Fault injection at every validation, node/blob preparation, root condition,
  root write, and response boundary.
- Concurrent transactions on same and disjoint tables.
- Token replay before/after simulated process restart.
- 100 logical items in one table, plus explicit tests for the smaller
  multi-table limit imposed by backend root actions.
- Cross-check transaction results and cancellation ordering with DynamoDB Local.

#### Exit gate

- No failed/canceled transaction advances any participating head.
- Every successful transaction has exactly one logical durable commit record,
  referenced by each participating table's ordered commit log.
- Idempotent replay never creates another table transition or commit.
- Unknown outcomes can be reconciled without blind mutation replay.

#### Rollback boundary

Transactions and commit recording are feature-gated. Existing nontransactional
operations continue to use table heads if the transaction endpoint is disabled.

---

### Phase 5: Version administration, diff, restore, retention, and migration

#### Context and background

Earlier phases create immutable table states but expose only minimal historical
reads. This phase turns versioning into an operable product: discovery,
comparison, safe restore, retention, GC, backup, and migration from an existing
native DynamoDB table.

#### Scope and implementation

- [ ] Implement the complete `_prolly/v1` head/version/read/diff/restore API.
- [ ] Add paginated version listing without full physical table scans where
  possible; document the v1 adapter cost when root scans remain necessary.
- [ ] Stream large diffs with resumable structural cursors and logical item
  decoding.
- [ ] Implement CAS restore and explicit privileged force restore.
- [ ] Implement per-table keep-last/keep-duration/protected-version policies.
- [ ] Integrate node/blob retention planning and safe store-wide GC.
- [ ] Export/import a table descriptor plus complete version snapshot bundle.
- [ ] Implement migration tooling:
  - consistent export or quiesced scan for base load;
  - canonical validation and sorted build;
  - optional DynamoDB Streams catch-up;
  - source/candidate counts and sampled/full hash verification;
  - cutover checkpoint and rollback record.
- [ ] Document that a plain long-running DynamoDB `Scan` is not a whole-table
  point-in-time snapshot and must not be marketed as one.

#### Deliverable

Operators can inspect history, compare versions, restore safely, set retention,
run verified GC, and migrate an existing table through a documented cutover
workflow.

#### Verification

- Restore under concurrent writers returns conflict unless expected head still
  matches.
- Pruned versions fail historical reads but retained/protected versions remain
  complete after GC.
- Backup/import preserves table state ID.
- Migration tests apply source mutations during catch-up and verify final
  logical equality before cutover.

#### Exit gate

- GC cannot delete content reachable from any live catalog/table/version/index/
  commit protection root.
- Migration has an explicit abort/cutback procedure and never requires dual
  writes without reconciliation.
- Large diffs and version lists are bounded and resumable.

#### Rollback boundary

Admin mutations require separate authorization. Version reads/diffs can remain
enabled while restore, prune, migration, or GC endpoints are disabled.

---

### Phase 6: GSI/LSI-compatible secondary indexes

#### Context and background

Many real DynamoDB schemas depend on alternate access paths. The engine already
has native `AsyncIndexedMap` coordination for strict derived indexes over async
stores. This phase must reuse that primitive while adding DynamoDB-specific
descriptors, key projections, historical base/index pairing, and query routing.
The service and direct Rust client must not create separate coordinators.

#### Scope and implementation

- [ ] Integrate the logical table engine with the existing `AsyncIndexedMap`
  path and document the exact base/index root set for each transition.
- [ ] Add catalog descriptors for LSI/GSI schemas and projections.
- [ ] Build index entries with base-PK tie-breakers and canonical projections.
- [ ] Maintain base and active index heads in one strict transaction.
- [ ] Implement `IndexName` Query/Scan routing and projected response behavior.
- [ ] Implement shadow build, verification, activation, retirement, and
  retention for indexes added after table creation where compatible.
- [ ] Enforce LSI creation-time and shared-partition-key restrictions.
- [ ] Reject strong reads on GSIs.
- [ ] Include exact index versions in commits and historical table snapshots.

#### Deliverable

Supported SDK clients can query declared LSIs/GSIs while Prolly version APIs can
reconstruct the exact base-plus-index state for each committed transition.

#### Verification

- Every base mutation is checked against a clean index rebuild oracle.
- Injected failures never publish a base state without its synchronous indexes.
- Non-unique keys, sparse indexes, every projection mode, and index-key type
  changes are covered.
- Historical index queries resolve the index version paired with the historical
  base version, never the current index head.

#### Exit gate

- Source/index atomicity passes remote DynamoDB fault and conflict tests.
- Index verification detects semantic drift before activation.
- The async path owns no duplicate ordered-tree algorithm.

#### Rollback boundary

Indexes are activated through catalog CAS. A failed new generation leaves the
previous generation selected; disabling index APIs does not affect base-table
CRUD.

---

### Phase 7: Change streams and TTL

#### Context and background

Streams and TTL are operational behaviors built on committed changes, not core
item CRUD. Implementing them before durable commit sequencing would either miss
changes or poll mutable heads without a reliable resume point.

#### Scope and implementation

- [ ] Materialize ordered logical diffs from commit transitions.
- [ ] Add stream descriptors, view types, retention, signed resume cursors, and
  at-least-once consumer APIs.
- [ ] Add a compatibility adapter for the supported subset of DynamoDB Streams
  only after the native Prolly stream is stable.
- [ ] Add TTL table configuration and the expiry secondary index.
- [ ] Implement conditional background deletes and distinguish service expiry
  events from user deletes.
- [ ] Add worker leases, checkpoints, backpressure, dead-letter handling, and
  lag metrics.

#### Deliverable

Consumers can resume a durable ordered change feed and operators can enable
asynchronous per-item TTL deletion without losing historical visibility.

#### Verification

- Worker crash/restart produces no missed committed transition.
- Duplicate delivery has stable record IDs.
- TTL races with item update never delete a renewed item.
- Stream/commit pruning respects active consumer checkpoints or fails them with
  an explicit expired-cursor error.

#### Exit gate

- Commit-to-stream lag and TTL lag meet declared SLOs under load.
- Retention pressure cannot silently invalidate an active checkpoint.

#### Rollback boundary

Workers are independently deployable. Disabling them stops new stream
materialization/TTL deletion without affecting synchronous table operations.

---

### Phase 8: Production hardening and scale architecture decision

#### Context and background

The single-head design prioritizes exact snapshots and simple correctness. It
must be measured under real contention before introducing sharding, commit
manifests, or write sequencers. Physical DynamoDB root/CID scans and hot root
items may also dominate operational cost even when item operations are fast.

#### Scope and implementation

- [ ] Build production-shaped benchmarks for point reads/writes, hot/cold
  partitions, Query, Scan, transactions, indexes, history, diff, and GC.
- [ ] Measure physical requests, bytes, cost estimates, p50/p95/p99 latency,
  head conflicts, and retry amplification.
- [ ] Add admission control, per-table concurrency limits, bounded queues,
  graceful shutdown, timeouts, and circuit breakers.
- [ ] Add SigV4 verification or production proxy authentication, TLS, secret
  rotation, and authorization for admin/version endpoints.
- [ ] Add corruption drills, backup/restore drills, GC pause/lease controls,
  and multi-instance deployment tests.
- [ ] Evaluate a physical schema v2 with efficient family/root/version listing
  instead of filter scans.
- [ ] Decide among:
  - retain one head per table;
  - single-writer or microbatch sequencer for hot tables;
  - partition-sharded maps with a table snapshot manifest;
  - relax global table versions in favor of partition versions.
- [ ] Record the decision with measured thresholds and compatibility impact.

#### Deliverable

A production-readiness report, reproducible benchmark harness, capacity guide,
security deployment profile, disaster-recovery runbook, and an approved scale
architecture decision.

#### Verification

- Multi-instance soak tests with injected throttling and network ambiguity.
- Restore plus replay drills from physical backup.
- Retention/GC during concurrent reads and writes under the approved lease
  model.
- SDK compatibility suite across the declared language/version matrix.
- Upgrade and downgrade test for every persisted service format.

#### Exit gate

- Published workload envelope states supported table size, item size, write
  concurrency, transaction shape, and latency targets.
- No production mode accepts unauthenticated remote traffic.
- Operators can recover from a lost service instance without losing committed
  heads or idempotency records.
- The scale decision is based on measured contention, not speculation.

#### Rollback boundary

Scale changes use a new table format/catalog generation and shadow migration.
The existing single-head generation stays readable until validation and CAS
cutover complete.

## 16. Capability progression

| Capability | P0 | P1 | P2 | P3 | P4 | P5 | P6 | P7 | P8 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Dynamo-safe remote engine | yes | yes | yes | yes | yes | yes | yes | yes | hardened |
| Embedded typed table | no | yes | yes | yes | yes | yes | yes | yes | yes |
| SDK CRUD | no | no | yes | yes | yes | yes | yes | yes | yes |
| Expressions | no | no | common/full declared subset | yes | yes | yes | yes | yes | hardened |
| Query/Scan | no | internal range only | no | yes | yes | yes | index-aware | yes | hardened |
| Batch APIs | no | no | no | yes | yes | yes | yes | yes | hardened |
| Cross-table transactions | engine only | no | no | no | yes | yes | index-integrated | yes | hardened |
| Historical point read | engine | yes | header metadata | full read header | yes | complete admin API | index-aware | yes | hardened |
| Diff/restore/retention | engine | minimal | minimal | minimal | commit lookup | yes | index-aware | yes | hardened |
| GSI/LSI subset | no | no | no | no | no | no | yes | yes | hardened |
| Streams/TTL | no | no | no | no | commit foundation | no | no | yes | hardened |

## 17. Testing strategy

### 17.1 Test layers

1. **Pure model tests:** numbers, attributes, canonical items, sizes, keys.
2. **Expression tests:** parser/evaluator without storage.
3. **Engine contract tests:** MemStore and fault-injecting stores.
4. **Remote adapter tests:** DynamoDB Local with real SDK calls.
5. **Protocol tests:** raw AWS JSON plus official SDK clients.
6. **Differential tests:** same requests against DynamoDB Local and the service.
7. **Concurrency/fault tests:** root races, throttling, ambiguous responses,
   restarts, and GC.
8. **Compatibility matrix tests:** every advertised operation/field and every
   explicitly unsupported field.
9. **Performance tests:** physical request and byte amplification as release
   gates, not merely wall-clock throughput.

### 17.2 Golden fixture policy

The following become versioned public fixtures under `conformance/`:

- Dynamo number canonical forms and ordered bytes;
- item canonical bytes and logical size;
- PK/SK tuple bytes and query bounds;
- expression AST/evaluation results;
- request/error compatibility cases;
- table descriptor, commit record, cursor, and stream record encodings.

Changing a golden persisted encoding requires a format-version bump, migration
reader, and compatibility note. Runtime protocol error wording may evolve, but
error type/category fixtures remain stable.

## 18. Deployment and migration topology

### 18.1 Development

```text
AWS SDK -> localhost service -> DynamoDB Local physical store
```

MemStore remains available for pure service tests, but no performance or
durability conclusion is drawn from it.

### 18.2 Production

```text
application
  -> internal load balancer / authenticated service
  -> N stateless compatibility-service instances
  -> physical DynamoDB table for nodes, roots, hints, blobs
```

Service instances are stateless except for bounded caches. Correctness depends
only on durable catalog/table roots and backend transactions, not sticky
sessions.

### 18.3 Existing-table cutover

Preferred workflow:

1. Create an isolated logical table ID in `IMPORTING` status.
2. Export a point-in-time source snapshot when available, or briefly quiesce
   writes for a consistent base scan.
3. Validate and canonicalize every source item; reject the import on any item
   the target service would not accept.
4. Build the candidate Prolly table through sorted bulk construction.
5. If using a stream catch-up, replay from the captured source position with
   idempotent source-event IDs.
6. Quiesce source writes briefly, drain catch-up, and verify logical equality.
7. CAS the logical catalog entry to `ACTIVE` and redirect client endpoint.
8. Keep the source table read-only for the rollback window.

Do not dual-write native DynamoDB and the Prolly service without a durable
outbox/reconciliation protocol. Two independent successful responses cannot be
made atomic by client convention.

## 19. Performance model and release gates

A logical `GetItem` may require a head read, multiple Prolly node reads, and a
blob read. A logical write may read/rewrite one path, prepare several immutable
nodes, and update version/head roots. Therefore native DynamoDB request count
and cost are not preserved.

Each benchmark reports:

```text
logical operation latency
logical bytes
physical GetItem/BatchGet/Put/BatchWrite/TransactWrite counts
physical bytes
tree height and nodes touched
cache/hint hit rate
head conflicts and retries
versions and new/reused nodes
estimated backend request cost
```

Initial release gates are declared after Phase 1 measurements. They must include
at least:

- 1-KB, 16-KB, 64-KB, and near-400-KB items;
- tables at 10K, 1M, and production-target item counts;
- cold and warm point reads;
- uniform and single-hot-table writes;
- queries returning 1, 10, 100, and 1-MB pages;
- 1-, 10-, and 100-item transactions;
- version retention of 10, 1K, and 100K transitions.

## 20. Risks and mitigations

| Risk | Consequence | Mitigation |
| --- | --- | --- |
| One table head becomes hot | Conflict retries and write ceiling | Measure, admission control, sequencer/sharding decision in Phase 8 |
| Physical transaction expansion | Legal logical transaction rejected | Prepublish verified immutable content; advertise root-action limits; optimize catalog roots later |
| Physical node/item exceeds 400 KB | Backend write failure | Dynamo-safe chunking, serialized-size validation, blob offload |
| Canonical codec bug | Wrong ordering or unstable versions | Golden fixtures, property tests, Dynamo differential tests, format versioning |
| Expression divergence | Silent application behavior change | Fail closed, field matrix, differential suite |
| Full scans for root/version listing | High cost and latency | Bounded admin APIs; evaluate physical schema v2 |
| Every write validates the shared catalog head | Extra hot-item reads and unrelated retries during catalog changes | Measure in Phase 8; replace with per-table lifecycle fences if material |
| GC races with readers | Historical/read corruption | Explicit live-root lease or operational serialization; mark before sweep |
| Ambiguous transaction response | Duplicate non-idempotent update | Durable tokens/operation reconciliation; no blind retry |
| SDK protocol drift | Clients stop interoperating | Multi-SDK contract CI and versioned capability matrix |
| Exact AWS expectations | Operational surprise | Explicit non-goals and compatibility levels in docs/responses |
| Client/server index mapping drifts | Historical or projected results differ by execution mode | One core mapping over `AsyncIndexedMap`, shared fixtures, executor parity tests |

## 21. Alternatives considered

### 21.1 Shadow Prolly history from DynamoDB Streams

Keep native DynamoDB authoritative and asynchronously build Prolly versions
from its stream. This preserves the endpoint and native scale, but versions lag,
cannot be atomically returned with writes, and historical reads require a
separate service anyway. It remains useful as a migration/capture mode, not the
authoritative architecture.

### 21.2 Store every item revision as another native DynamoDB item

This preserves native item access but duplicates full values, requires custom
key conventions, does not naturally produce whole-table snapshots/diffs, and
leaks version records into Query/Scan unless every access pattern changes.

### 21.3 One global Prolly map for every logical table

This simplifies cross-table atomicity and produces a database-wide version, but
every write in every table competes on one head. It is rejected for the MVP.

### 21.4 One Prolly map per DynamoDB partition key

This improves write concurrency and aligns with Query, but table-wide Scan,
transactions, versions, retention, and index consistency need a snapshot
manifest over many roots. It is a Phase 8 scale candidate, not the initial
correctness baseline.

### 21.5 Modify `prolly-store-dynamodb` into the compatibility server

Rejected because physical storage and logical DynamoDB semantics are different
layers. Combining them would make the store adapter unusable as a generic
backend and couple engine persistence to one application protocol.

## 22. Whole-program done criteria

The project is complete for its declared v1 scope when:

- official supported AWS SDKs use every advertised operation through endpoint
  configuration only;
- the capability matrix names every supported and rejected request field;
- key/value/expression semantics pass golden, property, and differential tests;
- current and historical reads are version-correct under concurrent writes;
- conditional and transactional writes never partially publish;
- large legal logical items work without oversized physical DynamoDB items;
- version diff, restore, retention, backup, migration, and GC have bounded,
  restartable operational workflows;
- secondary-index snapshots match their base versions;
- streams/TTL, when enabled, resume after process failure without missed
  commits or unsafe deletes;
- the workload envelope and physical request/cost amplification are published;
- production authentication, recovery, upgrade, and rollback paths are tested.

## 23. STOP conditions

Stop implementation and return to design review if any phase requires:

- decoding or routing stored Prolly nodes outside `ProllyEngine`;
- treating the physical DynamoDB row schema as the logical item schema;
- silently accepting an unsupported DynamoDB request field;
- moving a mutable head before all referenced nodes/blobs are durable;
- retrying an ambiguous non-idempotent write without reconciliation;
- claiming `BatchWriteItem` is all-or-nothing;
- publishing a base table without every declared synchronous index version;
- changing key/item persisted bytes without a format bump and migration path;
- running GC without proving the complete retained root/blob set;
- claiming native DynamoDB scale, cost, consistency, IAM, or global-table
  equivalence without evidence and an explicit compatibility contract.

## 24. External semantic references

- Amazon DynamoDB API actions:
  <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html>
- DynamoDB limits for items, expressions, transactions, batches, Query, and
  Scan:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html>
- Query key conditions and sort ordering:
  <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>
- Transaction semantics:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html>
- Read consistency:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html>
- BatchWriteItem partial atomicity:
  <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html>

Repository references:

- `plans/019-versioned-dynamodb-client-package.md`
- `docs/versioned-map.md`
- `docs/secondary-index-design.md`
- `docs/design-spec.md`
- `docs/async-first-api-inventory.md`
- `docs/language-store-adapters-design.md`
- `stores/prolly-store-dynamodb/README.md`
