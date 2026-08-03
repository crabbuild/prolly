# Build and operate secondary indexes with `IndexedMap`

`IndexedMap` maintains an authoritative byte-keyed collection and its ordered secondary indexes as one atomic state. This guide explains the complete application workflow: define deterministic extractors, activate indexes, mutate records, query current and historical snapshots, bound resources, observe health, verify correctness, replace definitions, transfer state, retain history, and operate garbage collection safely.

## Decide whether `IndexedMap` fits your data

Use `IndexedMap` when one source record can produce zero or more ordered lookup terms and every visible source version must agree with every visible index version.

Good fits include:

- Equality lookup, such as users by status or orders by customer
- Ordered range lookup, such as tasks by state and due time
- Sparse lookup, such as pending jobs or records with an optional field
- Multi-valued lookup, such as tags, categories, and access-control memberships
- Covering lookup with a deterministic summary projection
- Historical lookup against retained source and index snapshots
- Prefix candidate generation for paths, tokens, geohashes, and other ordered buckets

Choose another abstraction when you need:

- Enforced uniqueness: `IndexedMap` indexes are non-unique
- Full SQL planning, joins, foreign keys, or constraints
- Language-aware full-text ranking, stemming, typo tolerance, or relevance scoring
- Exact geospatial predicates without application-side filtering
- Approximate nearest-neighbor vector search: use `ProximityMap`
- An independently updated derived system whose consistency may lag the source

## Understand the collection model

One indexed collection contains a source map, runtime index definitions, immutable snapshots, and one canonical collection root.

Each visible snapshot selects:

- One exact source tree
- One exact tree for every active index
- The descriptor fingerprint for each selected index generation
- The parent snapshot used by retention and historical traversal

A write publishes new immutable source, index, snapshot, and state nodes, confirms the candidate roots are readable, and then transactionally validates and advances the collection root. The successful transaction makes the complete candidate visible. Readers observe the previous collection state or the next collection state, never a partially published mixture. Nodes left unreachable by a conflict are safe to reclaim during quiescent garbage collection.

The public constructor is:

```rust
let users = engine.indexed_map(b"users", index_registry()?)?;
```

There is no separate production constructor. The store and its deployment configuration determine durability, coordination, backup, and process-topology properties.

## Add the crate and choose a store

Add the package to `Cargo.toml`. Rust code imports the library as `prolly`.

```toml
[dependencies]
prolly-map = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

`indexed_map` requires a synchronous `IndexedStore`, which includes `Store`, `ManifestStore`, and strict `TransactionalStore`. The constructor fails closed if strict transactions are disabled. `MemStore` works for tests and examples. Use a durable adapter whose transaction implementation is linearizable across every process and handle sharing the store and configure durable acknowledgements for persistent deployments.

```rust
use prolly::{Config, MemStore, Prolly};
use std::sync::Arc;

let store = Arc::new(MemStore::new());
let engine = Prolly::new(store, Config::default());
# Ok::<(), prolly::Error>(())
```

You remain responsible for store credentials, durability mode, backups, multi-process coordination, and safe garbage-collection windows.

Remote-first adapters expose synchronous IndexedMap facades backed by their
existing native async transaction implementations:

- `SyncPostgresStore`
- `SyncMySqlStore`
- `SyncRedisStore`
- `SyncTursoStore`
- `SyncDynamoDbStore`
- `SyncCosmosDbStore`
- `SyncSpannerStore`

Use `build` when provider initialization is asynchronous. It creates the
backend on a runtime owned by the store, keeping driver background tasks alive
for the complete store lifetime:

```rust,no_run
use prolly::{Config, Prolly, SecondaryIndexRegistry};
use prolly_store_postgres::{PostgresBackend, SyncPostgresStore};

let database_url = std::env::var("DATABASE_URL")?;
let store = SyncPostgresStore::build(move || async move {
    let backend = PostgresBackend::connect(&database_url).await?;
    backend.initialize_schema().await?;
    Ok::<_, sqlx::Error>(backend)
})?;
let engine = Prolly::new(store, Config::default());
let users = engine.indexed_map(b"users", SecondaryIndexRegistry::new())?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`Sync*Store::new(existing_backend)` is also available for caller-configured
clients. Synchronous calls are safe when invoked from a Tokio context, but they
still block the calling task; latency-sensitive async services should execute
the synchronous IndexedMap workflow on their blocking-work pool.

## Define the source schema

`IndexedMap` stores raw byte keys and values. Typed applications should choose one deterministic encoding and reject malformed records in every extractor.

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct User {
    tenant_id: String,
    status: String,
    email: String,
    tags: Vec<String>,
}
```

An extractor receives the primary key and the exact stored source bytes. It returns zero or more `SecondaryIndexEntry` values, or it returns `SecondaryIndexError` and aborts the entire operation.

## Define deterministic extractors

Use `SecondaryIndex::non_unique` for a keys-only index. The callback returns one byte term for each logical membership.

```rust
use prolly::{SecondaryIndex, SecondaryIndexError};

let by_status = SecondaryIndex::non_unique(
    "by-status",
    1,
    "app.users.by-status/1",
    |_key, value| {
        let user: User = serde_json::from_slice(value)
            .map_err(|_| SecondaryIndexError::new("invalid user JSON"))?;
        Ok(vec![user.status.trim().to_lowercase().into_bytes()])
    },
)?;
# Ok::<(), prolly::Error>(())
```

The four definition inputs have distinct roles:

- **Name**: stable logical query name, such as `by-status`
- **Generation**: monotonically increasing integer for semantic replacement
- **Extractor ID**: application-controlled identity for extractor behavior
- **Extractor**: runtime callback that derives terms and projections

The descriptor fingerprint commits the source map ID, name, generation, extractor ID, projection, semantic limits, and physical layout. Callback machine code is not persisted.

Every extractor must satisfy these rules:

- Produce the same emissions for the same primary key and source bytes
- Avoid time, randomness, network calls, environment state, and mutable globals
- Avoid external side effects because conflict retries may run it again
- Normalize text with a versioned, application-defined policy
- Encode numeric fields so byte order matches application order
- Return an error for malformed data instead of guessing
- Stay within per-record term and projection limits

Duplicate emissions of the same term and projection for one record collapse into one physical entry. Different projections for the same term and primary key fail with `ConflictingIndexProjection`.

## Register definitions at open time

A registry supplies the runtime definitions needed to open, query, mutate, verify, and retain a collection.

```rust
use prolly::SecondaryIndexRegistry;

let registry = SecondaryIndexRegistry::new()
    .register(by_status)?
    .register(by_tag)?;
let users = engine.indexed_map(b"users", registry)?;
# Ok::<(), prolly::Error>(())
```

Register each logical name once. Reopening a persisted collection requires definitions whose fingerprints match its active descriptors. Historical snapshots also require the retained generation definitions they reference.

## Activate an index

Registering a definition makes its runtime behavior available. Call `ensure_index` to build and atomically activate the physical index.

```rust
let result = users.ensure_index(b"by-status")?;
println!(
    "generation={} entries={} attempts={}",
    result.generation,
    result.entries,
    result.attempts
);
# Ok::<(), prolly::Error>(())
```

`ensure_index` is idempotent for an already active matching definition. A successful build selects the source version used for construction and publishes the index only with a matching canonical snapshot.

Activate indexes before bulk loading an empty collection when you want every subsequent write to maintain them incrementally. You can also activate an index after loading source data; `ensure_index` then builds it from the selected source snapshot.

## Write only through `IndexedMap`

Once a map belongs to an indexed collection, route every head-changing mutation through its `IndexedMap`. Raw `VersionedMap` writes are fenced because they could bypass index maintenance.

Use `put` and `delete` for one record:

```rust
let bytes = serde_json::to_vec(&user).unwrap();
users.put(b"user-42", bytes)?;
users.delete(b"user-17")?;
# Ok::<(), prolly::Error>(())
```

Use `edit` or `apply` to amortize publication and update several records atomically:

```rust
users.edit(|edit| {
    edit.put(b"user-42", updated_user_bytes);
    edit.delete(b"user-17");
})?;
# Ok::<(), prolly::Error>(())
```

The coordinator normalizes repeated mutations for one primary key. It compares old and new extractor emissions, deletes stale physical entries, inserts changed entries, and skips unchanged emissions.

Use `apply_if` for application-level optimistic concurrency:

```rust
use prolly::{IndexedMapUpdate, Mutation};

let expected = users.snapshot()?.source_version().clone();
let update = users.apply_if(
    Some(&expected),
    vec![Mutation::Upsert { key, val }],
)?;
assert!(matches!(update, IndexedMapUpdate::Applied { .. }));
# Ok::<(), prolly::Error>(())
```

A conflict returns the current indexed version without publishing the candidate. Decide whether to reload, merge at the application layer, or retry with a fresh expected version.

## Choose a projection mode

Projection mode determines the value stored beside each physical `(term, primary_key)` key.

| Mode | Stored index value | Best use | Cost |
| --- | --- | --- | --- |
| `KeysOnly` | No application payload | Fetch keys or resolve complete source records | Smallest index |
| `Include` | Extractor-supplied bytes | Cover dashboards and list views | Duplicates selected fields |
| `All` | Complete source value | Serve matching values without source fetches | Largest index and write amplification |

`SecondaryIndex::non_unique` creates a `KeysOnly` definition. Use the builder for `Include` or `All`.

```rust
use prolly::{IndexProjection, SecondaryIndex, SecondaryIndexEntry};

let covering = SecondaryIndex::builder(
    "by-status-summary",
    1,
    "app.users.by-status-summary/1",
)
.projection(IndexProjection::Include)
.extract(|_, value| {
    let user = decode_user(value)?;
    let summary = encode_summary(&user)?;
    Ok(vec![SecondaryIndexEntry::included(user.status, summary)])
})?;
# Ok::<(), prolly::Error>(())
```

An `Include` extractor must emit `SecondaryIndexEntry::included`. `KeysOnly` and `All` extractors emit terms without custom projection bytes. `All` inserts the complete source value automatically.

## Encode terms for byte ordering

Secondary-index terms use raw lexicographic byte ordering. Your encoding determines equality, prefix grouping, and range order.

Use these conventions:

- UTF-8 normalized text for case-insensitive exact lookup
- Unsigned integers encoded with `to_be_bytes()`
- Signed integers and timestamps encoded with `KeyBuilder`
- Composite terms encoded as ordered `KeyBuilder` segments
- Canonical path segments without `.` or `..`
- Versioned tokenization and normalization for text terms

Do not concatenate variable-width fields with an ambiguous delimiter. `KeyBuilder` encodes segment boundaries and preserves numeric ordering.

```rust
use prolly::KeyBuilder;

fn tenant_status(tenant: &str, status: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str(tenant)
        .push_str(status)
        .finish()
}
```

Keep normalization in one shared function used by both extractor definitions and query construction. A query encoded with a different normalization policy will return no match even when the human-readable values look equal.

## Query one immutable snapshot

Call `snapshot()` once for a logical read operation. Every source and index read through that handle belongs to the same canonical collection snapshot.

```rust
let snapshot = users.snapshot()?;
let by_status = snapshot.index(b"by-status")?;
let keys = by_status.primary_keys(b"active")?;
# Ok::<(), prolly::Error>(())
```

Do not call `snapshot()` separately for related reads when they must agree. Concurrent writers may publish between calls.

## Run exact, prefix, and range queries

The three logical query shapes operate on terms before the primary-key tie-breaker.

```rust
let exact = index.exact(b"active")?;
let prefix = index.prefix(b"data")?;
let range = index.range(&start_term, Some(&end_term))?;
# Ok::<(), prolly::Error>(())
```

Range bounds are start-inclusive and end-exclusive. Pass `None` for an unbounded end. Matching entries sort by term and then primary key.

Use reverse methods when you need newest-first or highest-first results:

```rust
let page = index.range_reverse_page(
    &start_term,
    Some(&end_term),
    None,
    100,
)?;
# Ok::<(), prolly::Error>(())
```

Reverse traversal changes order, not logical bounds.

## Choose keys, projections, or source records

Select the lowest-cost result shape that contains the data you need.

```rust
let keys = index.primary_keys(b"active")?;
let summaries = index.projected(b"active")?;
let records = index.records(b"active")?;
# Ok::<(), prolly::Error>(())
```

The methods return:

- `primary_keys`: matching source keys only
- `projected`: primary keys plus optional `Include` or `All` bytes
- `records`: term, primary key, projection, and exact source value

`records` performs bounded source fetches from the source tree selected by the same snapshot. A missing referenced source key is reported as `IndexSnapshotMismatch`, not silently ignored.

## Stream results without collecting them

Use scan callbacks when the complete result set should not reside in memory.

```rust
let visited = index.scan_exact(b"active", |matched| {
    process(matched.primary_key, matched.projection);
})?;
println!("processed {visited} entries");
# Ok::<(), prolly::Error>(())
```

The `scan_*_until` variants accept `ControlFlow` and stop after an application condition. Borrowed `SecondaryIndexMatchRef` and `IndexedSourceRecordRef` values remain valid only during the callback.

## Paginate with snapshot-bound cursors

Page methods return a `SecondaryIndexPage` with `matches` and an optional `next_cursor`.

```rust
let query = index.query(query_budget)?;
let first = query.prefix_page(b"data", None, 100)?;
let second = query.prefix_page(
    b"data",
    first.next_cursor.as_ref(),
    100,
)?;
# Ok::<(), prolly::Error>(())
```

A cursor binds the snapshot, source version, state version, index name, index version, descriptor fingerprint, direction, bounds, and continuation key. Reusing it with another snapshot, direction, or bound returns `IndexCursorVersionMismatch`.

Serialize a cursor only when your service must send it across a process boundary:

```rust
use prolly::SecondaryIndexCursor;

let bytes = cursor.to_bytes()?;
let restored = SecondaryIndexCursor::from_bytes(&bytes)?;
# Ok::<(), prolly::Error>(())
```

Treat cursor bytes as opaque and untrusted. Decoding validates the envelope, while query execution validates its complete identity and physical continuation key.

## Apply explicit query budgets

Convenience queries use finite defaults. Create a query session when endpoint limits must differ from library defaults.

```rust
use prolly::QueryBudget;
use std::time::Duration;

let budget = QueryBudget {
    max_page_entries: 250,
    max_returned_entries: 1_000,
    max_returned_bytes: 4 * 1024 * 1024,
    max_scanned_entries: 10_000,
    max_source_fetches: 250,
    max_accounted_memory_bytes: 8 * 1024 * 1024,
    max_elapsed: Duration::from_secs(2),
};
let query = index.query(budget)?;
# Ok::<(), prolly::Error>(())
```

Reject page sizes above `max_page_entries` at your API boundary. A budget failure returns `IndexResourceLimitExceeded` and does not weaken snapshot consistency.

## Use real-world index patterns

The following patterns map common application access paths to deterministic term encodings. The runnable [`indexed_map_real_world.rs`](../examples/indexed_map_real_world.rs) example executes every pattern in this section.

| Scenario | Emitted term | Query |
| --- | --- | --- |
| Users by status | Normalized status | Exact |
| Orders by customer | Normalized customer ID | Exact |
| Multi-tenant status | `(tenant, status)` segments | Exact or tenant prefix |
| Tasks ordered by state and time | `(state, due_timestamp)` segments | Range or reverse range |
| Tags and categories | One normalized term per tag | Exact |
| Sparse pending jobs | Emit only for pending records | Exact pending state |
| Access-control reverse lookup | One term per group or member | Exact group or member |
| Email lookup | Normalized email | Exact, without uniqueness enforcement |
| Covering dashboard | Status plus summary projection | `projected()` |
| Expiration processing | Big-endian expiry timestamp | Range from minimum through cutoff |
| Hierarchical paths | Canonical path segments | Prefix |
| Geospatial buckets | Cell or geohash | Prefix or range, then application filtering |
| Basic inverted text | One normalized token per document | Exact or token prefix |
| Audit and history | Same index at a retained source version | `snapshot_at()` |

### Index users by status

Emit one normalized status term and query it exactly.

```rust
let by_status = SecondaryIndex::non_unique(
    "by-status",
    1,
    "app.users.by-status/1",
    |_, value| {
        let user = decode_user(value)?;
        Ok(vec![normalize(&user.status).into_bytes()])
    },
)?;
```

Status indexes work well for queues and administration filters. If one status has millions of records, paginate or stream instead of calling an unbounded convenience collector.

### Index orders by customer

Emit the customer ID from each order to support reverse lookup from a customer to their orders.

```rust
let by_customer = SecondaryIndex::non_unique(
    "by-customer",
    1,
    "app.orders.by-customer/1",
    |_, value| {
        let order = decode_order(value)?;
        Ok(vec![normalize(&order.customer_id).into_bytes()])
    },
)?;
```

Use a composite `(customer, created_at)` term when the application also needs chronological customer order history.

### Isolate tenants in composite terms

Place the tenant segment first. Exact lookup selects one tenant and status, while a tenant prefix returns every status for that tenant.

```rust
let term = KeyBuilder::new()
    .push_str(&user.tenant_id)
    .push_str(&user.status)
    .finish();
let tenant = KeyBuilder::new()
    .push_str("tenant-42")
    .finish();
let all_tenant_entries = index.prefix(&tenant)?;
# Ok::<(), prolly::Error>(())
```

The term order improves query locality but does not implement authorization. Validate tenant access before constructing the query and validate returned records when policy requires it.

### Order tasks by state and due time

Encode state first and an ordered timestamp second. Query a half-open time window within one state.

```rust
fn state_due(state: &str, due_ms: u64) -> Vec<u8> {
    KeyBuilder::new()
        .push_str(state)
        .push_u64(due_ms)
        .finish()
}

let start = state_due("pending", window_start_ms);
let end = state_due("pending", window_end_ms);
let due = index.range(&start, Some(&end))?;
# Ok::<(), prolly::Error>(())
```

Use `range_reverse_page` to process the same window from the greatest timestamp downward.

### Emit one term per tag or category

A multi-valued extractor maps one source record to several physical entries.

```rust
|_, value| {
    let user = decode_user(value)?;
    Ok(user
        .tags
        .iter()
        .map(|tag| normalize(tag).into_bytes())
        .collect())
}
```

Deduplicate normalized tags in the application when duplicate input should be rejected. The index canonicalizer collapses identical emissions, but preserving a validation error may better expose malformed source data.

### Build a sparse pending-work index

Return no term when a record should be absent from the index.

```rust
|_, value| {
    let task = decode_task(value)?;
    if task.state == "pending" {
        Ok(vec![b"pending".to_vec()])
    } else {
        Ok(Vec::new())
    }
}
```

Sparse indexes reduce storage and query scanning when a small subset needs attention. Updating a task to `done` atomically removes its previous pending entry.

### Reverse access-control memberships

Emit one term for each group or member that can access a resource.

```rust
|_, value| {
    let resource = decode_resource(value)?;
    Ok(resource
        .group_ids
        .iter()
        .map(|group| normalize(group).into_bytes())
        .collect())
}
```

An index accelerates candidate lookup but does not replace policy evaluation. Recheck deny rules, inheritance, expiry, and request context before granting access.

### Normalize email without assuming uniqueness

Normalize email identically during extraction and query construction.

```rust
let email_term = email.trim().to_lowercase().into_bytes();
let matching_users = by_email.primary_keys(&email_term)?;
# Ok::<(), prolly::Error>(())
```

`IndexedMap` does not reject a second primary key with the same term. Enforce uniqueness in an authoritative write protocol, or handle multiple matches explicitly.

### Cover a dashboard with `Include`

Store only the fields needed by the list view. This avoids source fetches and avoids duplicating the complete record.

```rust
let entry = SecondaryIndexEntry::included(
    normalize(&user.status),
    serde_json::to_vec(&UserSummary {
        display_name: user.display_name,
        plan: user.plan,
    }).map_err(|_| SecondaryIndexError::new("summary encode failed"))?,
);
```

Treat projection bytes as a schema with its own versioned extractor ID. Changing field encoding or meaning requires a greater generation and atomic replacement.

### Process records by expiration time

Encode an unsigned timestamp as eight big-endian bytes. Lexicographic order then matches numeric order.

```rust
let term = item.expires_at_ms.to_be_bytes().to_vec();
let minimum = 0_u64.to_be_bytes();
let cutoff_exclusive = cutoff_ms.saturating_add(1).to_be_bytes();
let expired = index.range(&minimum, Some(&cutoff_exclusive))?;
# Ok::<(), prolly::Error>(())
```

Use an end-exclusive cutoff directly when the application already models half-open time windows. Avoid `saturating_add(1)` if the maximum timestamp needs distinct handling.

### Query hierarchical paths by prefix

Split canonical paths into `KeyBuilder` segments. A parent term becomes the byte prefix for its descendants.

```rust
fn path_term(path: &str) -> Vec<u8> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .fold(KeyBuilder::new(), |key, segment| key.push_str(segment))
        .finish()
}

let subtree = index.prefix(&path_term("/acme/projects"))?;
# Ok::<(), prolly::Error>(())
```

Reject dot segments, ambiguous separators, invalid Unicode policy, and non-canonical aliases before storing the record.

### Use geospatial cells as candidates

Emit a geohash or another hierarchical cell ID. Query a cell prefix, decode projected records, then apply the exact spatial predicate.

```rust
let candidates = by_cell.prefix(b"c2b2")?;
let inside = candidates
    .iter()
    .filter_map(|row| row.projection.as_deref())
    .map(decode_place)
    .filter(|place| bounding_box.contains(place))
    .collect::<Vec<_>>();
```

Cell lookup can return false positives and miss neighbors if the application queries too few adjacent cells. Choose precision, neighbor expansion, and exact filtering from measured geographic workloads.

### Build a basic inverted text index

Tokenize, normalize, deduplicate, and emit each distinct token.

```rust
let terms = text
    .split(|character: char| !character.is_alphanumeric())
    .filter(|token| !token.is_empty())
    .map(normalize)
    .collect::<BTreeSet<_>>()
    .into_iter()
    .map(String::into_bytes)
    .collect();
```

Exact token lookup implements boolean membership. Prefix lookup implements byte-prefix token candidates. This pattern does not implement relevance, positions, phrase matching, language-aware analysis, or typo correction.

### Query retained history for audits

Capture the source version from a canonical snapshot, retain it, and reopen it later.

```rust
let historical_version = users.snapshot()?.source_version().clone();
let _pin = users.pin_snapshot(b"audit-export", &historical_version)?;

users.put(b"user-42", updated_bytes)?;
let historical = users.snapshot_at(&historical_version)?;
let old_active = historical
    .index(b"by-status")?
    .primary_keys(b"active")?;
# Ok::<(), prolly::Error>(())
```

The snapshot and every index it selects remain exact. Release the pin after the external audit no longer needs the version.

## Retain and pin historical snapshots

`keep_last(count)` retains the newest snapshot chain and always keeps at least the head. Durable pins add selected snapshots even when they fall outside that count.

```rust
let version = users.snapshot()?.source_version().clone();
let pin = users.pin_snapshot(b"month-end-export", &version)?;
users.keep_last(30)?;
run_export(users.snapshot_at(&version)?)?;
pin.release()?;
# Ok::<(), prolly::Error>(())
```

Dropping a `SnapshotPinGuard` also attempts to release its pin. Call `release()` when the application must observe and handle release failure.

Retaining a snapshot keeps its source tree, index trees, descriptors, and required state reachable. It does not keep arbitrary external resources referenced inside application values.

## Replace an index definition

Change extractor semantics by creating the same logical name with a strictly greater generation. Keep the extractor ID descriptive and versioned.

```rust
let generation_two = SecondaryIndex::non_unique(
    "by-status",
    2,
    "app.users.by-status/2-unicode-normalization",
    generation_two_extractor,
)?;
let result = users.replace_index(b"by-status", generation_two)?;
assert!(result.activated);
# Ok::<(), prolly::Error>(())
```

Replacement builds against one source snapshot and atomically publishes the new descriptor and tree. The previous generation becomes retired but remains available to retained snapshots.

When reopening after replacement, construct the registry with `SecondaryIndexRegistry::replace` so it retains the prior runtime definition for historical snapshots:

```rust
let registry = SecondaryIndexRegistry::new()
    .register(generation_one)?
    .replace(generation_two)?;
# Ok::<(), prolly::Error>(())
```

Do not reuse a generation or extractor ID after changing normalization, term encoding, projection encoding, limits, or meaning.

Call `deactivate_index(name)` when new snapshots should stop maintaining and exposing an index. Deactivation atomically removes the index from the new head and retires its descriptor. Retained snapshots still select their exact historical index tree, so keep the corresponding runtime definition registered until retention no longer references it.

## Use hard cutover for persisted format changes

The current indexed-collection layout has one supported persisted format. There is no compatibility reader and no suffix-named `V2` or `V3` collection.

For an incompatible persisted-format change:

1. Stop writes or establish a source snapshot boundary
2. Build or import into an empty destination using the new software
3. Verify the complete destination snapshot and every index
4. Switch readers and writers to the destination atomically at the deployment layer
5. Keep the old deployment available for rollback until the cutover is accepted
6. Retire the old deployment after the rollback window closes

`Error::IndexFormatUnsupported` means the deployment must perform this hard cutover. Do not rename the old format or append a format suffix to avoid the migration decision.

## Verify and repair index correctness

`health()` performs structural closure checks and reports selected versions, active indexes, retained snapshots, and durable pins.

```rust
let health = users.health()?;
assert!(health.closure_valid);
for index in &health.active_indexes {
    println!("{:?} generation={}", index.name, index.generation);
}
# Ok::<(), prolly::Error>(())
```

Health does not recompute extractor semantics. Use `verify_index` or `verify_all` for semantic qualification.

```rust
let source = users.snapshot()?.source_version().clone();
for verification in users.verify_all(&source)? {
    assert!(verification.is_valid());
    assert!(verification.is_canonical());
}
# Ok::<(), prolly::Error>(())
```

Verification rebuilds expected index content under a finite maintenance budget and compares it with the selected physical tree. `is_valid()` checks entries and semantic differences. `is_canonical()` also checks deterministic root identity.

`repair_index` can publish a rebuilt tree only for the current head snapshot. Investigate the cause before repair because deterministic extraction and atomic publication should prevent ordinary drift.

## Export and import one canonical collection closure

`export_current` creates a content-addressed bundle for the current canonical state and every reachable immutable node.

```rust
let bundle = users.export_current()?;
let expected_digest = bundle.digest()?;
let encoded = bundle.to_bytes()?;
let decoded = prolly::IndexedSnapshotBundle::from_bytes(&encoded)?;
assert_eq!(decoded.digest()?, expected_digest);
# Ok::<(), prolly::Error>(())
```

Inspect and verify untrusted bytes under a `TransferBudget` before allocating deployment-specific resources. Import verifies hashes, records, reachability, ownership, runtime definitions, and the selected snapshot before publishing one destination root.

```rust
let replica = replica_engine.indexed_map(b"users", registry)?;
replica.import_current(&decoded, None)?;
# Ok::<(), prolly::Error>(())
```

Pass `None` only when the destination is logically empty. For an existing destination, pass its current source version as the compare-and-set expectation. Import does not merge two independently changed heads, so establish an application-level source of truth before transfer.

## Bound mutation and maintenance work

All indexed operations have finite defaults. Override budgets from measured endpoint and maintenance objectives.

| Budget | Bounds |
| --- | --- |
| `MutationBudget` | Input records and bytes, derived entries and bytes, accounted memory, CAS attempts, elapsed time |
| `QueryBudget` | Page and result entries, returned bytes, scanned entries, source fetches, accounted memory, elapsed time |
| `MaintenanceBudget` | Source and derived entries, findings, memory, spill bytes and runs, merge fan-in, CAS attempts, elapsed time |
| `TransferBudget` | Encoded and decoded bytes, nodes, verification work, accounted memory, elapsed time |

Use `apply_with_budget`, `ensure_index_with_budget`, `verify_all_with_budget`, `repair_index_with_budget`, bundle budget methods, and `index.query` to supply explicit limits.

The default values are safety ceilings, not capacity promises. Lower them for request paths and raise them only after measuring memory, latency, spill capacity, and retry behavior.

## Observe indexed operations

`metrics()` returns counters accumulated by the current `IndexedMap` metrics handle.

```rust
let metrics = users.metrics();
println!("mutations={}", metrics.normalized_source_mutations);
println!("emitted_terms={}", metrics.terms_emitted);
println!("retries={}", metrics.retries);
println!("verification={}", metrics.verification_outcomes);
```

Export these measurements with operation latency and store-level input/output metrics. Alert on sustained retries, extraction failures, resource-limit errors, verification differences, unexpected projection growth, and retained-root growth.

The logical counters do not claim to measure physical backend writes. Store adapters should expose their own requests, bytes, throttling, transaction conflicts, and durability failures.

## Handle errors by code and retry advice

`Error::index_code()` classifies secondary-index failures without parsing redacted display strings. `Error::retry_advice()` distinguishes fresh-state retries, delayed store retries, and non-retryable failures.

| Error code | Meaning | Application response |
| --- | --- | --- |
| `FormatUnsupported` | Persisted indexed format is obsolete | Hard cut over to an empty destination |
| `Conflict` | Source moved during bounded publication | Reload and retry within policy |
| `DefinitionInvalid` | Runtime definition or fingerprint is wrong | Fix deployment configuration or code |
| `RuntimeDefinitionMissing` | A selected generation has no callback | Register active and retained definitions |
| `ManagedWriteRequired` | A raw write bypassed `IndexedMap` | Route the mutation through `IndexedMap` |
| `OperationUnsupported` | The requested operation lacks a safe indexed implementation | Use the supported lifecycle path or change the operation |
| `ExtractionFailed` | An extractor rejected source bytes | Reject or repair the source record |
| `ProjectionInvalid` | Emission does not match projection mode | Fix the extractor |
| `ResourceLimit` | Typed budget or semantic limit was exceeded | Reduce work or revise an explicit limit |
| `CursorInvalid` | Cursor identity does not match the query | Restart pagination from a fresh snapshot |
| `SnapshotUnavailable` | Retention removed or never created the snapshot | Return an expired-history response |
| `Corruption` | Selected source and index closure disagree | Stop unsafe writes and investigate |
| `BundleInvalid` | Transfer envelope or closure failed validation | Reject the bundle |

Display and debug messages redact application keys, terms, values, bounds, and extractor text. Log structured sensitive fields only through an explicitly access-controlled application path.

## Tune performance from workload shape

Index cost depends on source write rate, active index count, terms per record, projection bytes, and backend publication cost.

Use these rules before tuning low-level tree parameters:

- Batch related mutations with `edit` or `apply`
- Emit only queryable terms
- Prefer `KeysOnly` unless projection avoids measured source fetch cost
- Prefer `Include` over `All` when a bounded summary serves the endpoint
- Place the highest-value range prefix first in composite terms
- Use big-endian or `KeyBuilder` numeric encodings
- Paginate high-cardinality terms
- Stream scans when the consumer can process one entry at a time
- Set budgets from service-level objectives and observed distribution tails
- Build and verify large indexes in maintenance paths with bounded spill storage
- Monitor write amplification after adding each index

An index with one emitted term per record adds one derived upsert or delete when that term changes. Multi-valued and `All` indexes multiply derived bytes. Measure with production-shaped values and term distributions.

## Preserve correctness and security

Treat extractor code as part of the persisted schema. Review it for determinism, denial-of-service bounds, sensitive projections, and normalization ambiguity.

Apply this checklist:

- Reject invalid source encoding before publication
- Cap source value, term, term-count, and projection sizes
- Avoid secrets in terms because terms influence physical keys and diagnostics
- Avoid secrets in `Include` projections unless the index store has matching controls
- Keep authorization checks outside candidate lookup indexes
- Authenticate and integrity-check transport around snapshot bundles
- Register all retained extractor generations before opening historical snapshots
- Verify indexes after restore, adapter changes, and extractor replacement
- Pin snapshots during long-running exports or audits
- Require explicit quiescence before destructive indexed garbage collection

Content addressing detects changed node bytes. It does not encrypt application data or authenticate an untrusted caller.

## Operate retention and garbage collection safely

Retention removes canonical references to old snapshot records. Garbage collection (GC) later reclaims unreachable nodes from the shared store.

Use `plan_indexed_gc` to inspect candidates when the store implements node and manifest scans. `sweep_indexed_gc(true)` requires the caller to assert an external quiescence or lease-safety proof.

Do not infer reader safety from cache residency. Coordinate all processes, readers, backups, transfers, and other named roots before sweeping a shared store.

## Qualify a store deployment

An adapter type alone does not prove that every deployment configuration is safe. Qualify the exact store settings and process topology you will run.

Verify these properties:

- Immutable node writes are durable before manifest publication
- Transactional root validation and commit are linearizable under concurrency
- Candidate roots are readable before visibility changes
- Multiple processes agree on one manifest coordination domain
- Restart and forced-termination tests preserve the old or new complete state
- Backup and restore preserve the canonical state closure
- Timeouts and retries do not publish a partial collection
- Garbage collection coordinates with live readers and writers
- Credentials and encryption protect source values, projections, and terms
- Store-level observability exposes requests, bytes, conflicts, throttling, and errors

Run the adapter’s indexed-map contract test against the deployed configuration when possible.

## Test definitions and release gates

Extractor tests should prove semantics before integration tests exercise storage and publication.

Cover at least:

- Empty, malformed, minimum-size, and maximum-size source records
- Case, Unicode, whitespace, and normalization boundaries
- Zero, one, duplicate, and maximum term counts
- Stable output ordering and deduplication
- Projection encoding and size limits
- Insert, update, term removal, and delete maintenance
- Exact, prefix, range, reverse, page, and cursor behavior
- Concurrent writers, stale conditional writes, and bounded transaction conflicts
- Current and historical snapshot consistency
- Definition replacement with retained generations
- Verification, repair, export, import, retention, pins, and GC planning
- Restart and forced-termination qualification for durable adapters
- Benchmark baselines for representative cardinality and skew

Run the repository example and focused test suite:

```sh
cargo run --example indexed_map_real_world
cargo run --example secondary_index
cargo test --test secondary_index
```

Release only when API inventory, formatting, lint, correctness, adapter, binding, address-sanitizer, minimum-supported-Rust-version, and bounded benchmark gates pass on the exact release commit.

## Use the API surface by task

This table summarizes the primary application methods.

| Task | API |
| --- | --- |
| Open a collection | `engine.indexed_map(source_map_id, registry)` |
| Activate a registered definition | `ensure_index` or `ensure_index_with_budget` |
| Read one source value | `get` or `get_with` |
| Mutate source and indexes | `put`, `delete`, `edit`, `apply`, `apply_with_budget` |
| Compare-and-set a source version | `apply_if` |
| Open current state | `snapshot` |
| Open retained history | `snapshot_at` or `snapshot_by_id` |
| Query an index | `exact`, `prefix`, `range`, reverse variants, or page variants |
| Control query resources | `query(QueryBudget)` |
| Resolve complete source records | `records` or `scan_records` |
| Replace semantics | `replace_index` with a greater generation |
| Retire an index | `deactivate_index` |
| Inspect structural health | `health` |
| Inspect logical work | `metrics` |
| Verify semantics | `verify_index`, `verify_all`, or budget variants |
| Repair the head index | `repair_index` or `repair_index_with_budget` |
| Keep history | `keep_last`, `pin_snapshot`, `retain_snapshot_pin` |
| Transfer current state | `export_current`, bundle verification, `import_current` |
| Plan or sweep GC | `plan_indexed_gc`, `sweep_indexed_gc` |

## Continue into semantics and implementation details

This guide covers application use and operations. Read [IndexedMap secondary-index semantics](secondary-index-design.md) for the canonical state layout, cursor identity, physical publication, retention closure, transfer validation, and observability contract.

The repository includes two complementary executable examples:

- [`indexed_map_real_world.rs`](../examples/indexed_map_real_world.rs) runs all 14 application patterns from this guide
- [`secondary_index.rs`](../examples/secondary_index.rs) focuses on projections, atomic edits, verification, generation replacement, retention, export, and import
