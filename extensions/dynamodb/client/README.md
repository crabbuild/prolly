# prolly-dynamodb-client

An in-process Rust client that keeps the familiar AWS SDK DynamoDB fluent shape
while storing logical tables as immutable, versioned Prolly maps. It does not
run or require a compatibility service.

The caller owns and configures the underlying `aws_sdk_dynamodb::Client`. Use a
dedicated physical node table/root table namespace; native DynamoDB item clients
cannot interpret the physical representation.

Production deployment, authority separation, worker lifecycle, incident, and
rollback procedures are defined in [`OPERATIONS.md`](OPERATIONS.md).
The mandatory threat model and exact-table AWS policy templates are in
[`SECURITY.md`](SECURITY.md) and [`deploy/aws`](deploy/aws).
The exact Rust/format contract is in [`COMPATIBILITY.md`](COMPATIBILITY.md),
the measurement and scale contract is in [`PERFORMANCE.md`](PERFORMANCE.md),
publication failure qualification is in
[`FAULT_INJECTION.md`](FAULT_INJECTION.md), and recovery exercises are in
[`RECOVERY.md`](RECOVERY.md). Multi-process stress and process-death evidence is
defined in [`SOAK.md`](SOAK.md).
Current evidence and remaining production blockers are tracked in
[`RELEASE_STATUS.md`](RELEASE_STATUS.md).
The complete reviewed Rust signature/trait surface is frozen in
[`public-api.txt`](public-api.txt) and checked by
`scripts/verify_dynamodb_client_public_api.sh`.
The packaged `rolling_compatibility_probe` example and
`scripts/run_dynamodb_rolling_compatibility.py` qualify independently built
release pairs; an identical-binary smoke is explicitly diagnostic only.

```rust,no_run
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use prolly_dynamodb_client::Client;
use prolly_store_dynamodb::DynamoDbBackend;

# async fn example(physical: aws_sdk_dynamodb::Client) -> Result<(), Box<dyn std::error::Error>> {
let backend = DynamoDbBackend::new(physical, "prolly-versioned")
    .with_root_table_name("prolly-versioned-roots")
    .with_key_prefix(b"orders-prod:".to_vec());
backend.initialize_schema().await?;
let client = Client::open(backend).await?;

// Frozen, serializable deployment contract for the audited operation subset.
println!("{}", client.capabilities().to_json()?);

client.put_item()
    .table_name("Orders")
    .item("accountId", AttributeValue::S("acct-1".into()))
    .send().await?;

let updated = client.update_item()
    .table_name("Orders")
    .key("accountId", AttributeValue::S("acct-1".into()))
    .update_expression("SET #status = :closed")
    .expression_attribute_names("#status", "status")
    .expression_attribute_values(":closed", AttributeValue::S("CLOSED".into()))
    .return_values(ReturnValue::AllNew)
    .send_with_metadata().await?;
assert!(updated.output.attributes().is_some());

let head = client.table("Orders").head().await?;
let old = client.table("Orders").at(head.id.clone()).get_item()
    .key("accountId", AttributeValue::S("acct-1".into()))
    .send().await?;

// Whole-table optimistic control; a changed head fails with HeadConflict.
client.table("Orders").if_head(head.id).put_item()
    .item("accountId", AttributeValue::S("acct-2".into()))
    .send().await?;
# Ok(()) }
```

For explicit adapter policy, use the mutually exclusive construction builder:

```rust,no_run
# use prolly::RemoteStoreConfig;
# use prolly_dynamodb_client::Client;
# use prolly_store_dynamodb::{DynamoDbBackend, DynamoDbStore};
# async fn open(backend: DynamoDbBackend, store: DynamoDbStore) -> Result<(), prolly_dynamodb_client::Error> {
let from_backend = Client::builder()
    .backend(backend)
    .remote_store_config(RemoteStoreConfig { verify_node_cids: true })
    .logical_retry_limit(7)
    .node_cache_max_nodes(100_000)
    .node_cache_max_bytes(64 * 1024 * 1024)
    .open()
    .await?;

// An existing store already owns its RemoteStoreConfig and cannot be combined
// with either `.backend(...)` or `.remote_store_config(...)`.
let from_store = Client::builder().store(store).open().await?;
assert_eq!(
    from_backend.backend().table_name(),
    from_store.backend().table_name(),
);
# Ok(()) }
```

These are process-local resource controls, not persisted database settings.
`logical_retry_limit` counts optimistic retries after the first attempt: zero
means one attempt, the default seven means at most eight attempts, and values
above 63 are rejected before any provider request. Node caching is bounded by
64 MiB of retained serialized-node weight by default; the optional node-count
and byte limits are both enforced,
and zero disables caching. Correctness pins may temporarily exceed a cache cap
until they are unpinned. `client.cache_usage()` returns one internally
consistent snapshot of entry count, retained serialized-node weight, and the
pinned portions of both. This is the quantity governed by the cache ceiling;
it is deliberately distinct from process RSS, which also includes decoded
objects, AWS SDK buffers, allocator overhead, and unrelated application state.
The effective values are exported by `client.capabilities()`.
Changing any of these controls leaves the negotiated database-format record
unchanged.

Clones of one `Client` also share process-local admission for point and
transaction writes. Admission occurs before speculative Prolly tree work, so
concurrent tasks do not repeatedly build against the same stale table head.
Reuse one opened client per physical namespace in a process. Independently
opened clients and other processes remain safe through provider CAS and the
configured logical retry budget, but they do not share this optimization.

The client emits `tracing` spans at `debug` level for open, every supported
data-plane operation, and explicit worker/maintenance lifecycle calls. Stable
span fields identify only the logical operation (`db_system` and
`db_operation`); builders, keys, items, expressions, credentials, physical
table names, and results are skipped so ordinary telemetry does not copy
financial or legal record content. Applications choose and configure the
subscriber; the crate installs no global subscriber.

`Client` owns no runtime thread, HTTP server, or implicit worker. It is a cheap
`Arc`-backed handle over the caller-supplied DynamoDB adapter. Cloning or
dropping it does not shut down, reconfigure, or invalidate the caller's shared
`aws_sdk_dynamodb::Client`. There is therefore no general client shutdown call.
Explicit stream, TTL, and maintenance workers have their own idempotent,
durably reconciled `shutdown`/release workflows; dropping a worker never
guesses that an in-flight provider operation was rolled back.

Unsupported operation fields are intentionally absent or rejected. Physical
schema creation remains explicit through `DynamoDbBackend::initialize_schema`;
`Client::open` validates both configured tables without provisioning them.
Canonical items larger than 64 KiB are stored through the provider's verified,
chunked content-addressed blob path. That threshold is part of durable format
negotiation, so clients configured incompatibly fail closed during open.
The current database format is version 12; it binds canonical table/index
descriptors, table schema-version records, bounded active index state, compact
snapshot locators and their detached immutable manifest trees, current-only
commit roots, append-only successful-blob registries,
maintenance/import/index audit, fail-closed maintenance fences, durable GC execution, and worker
lease/checkpoint/release codecs. A namespace initialized with another format version
must go through an explicit verified migration and is never silently upgraded.

## Collection reads

`Query` and `Scan` expose explicit paginators. A current-head paginator may
observe a newer table version on a later page, matching ordinary moving-head
pagination. Construct it through `client.table(name).at(version)` when every
page must remain on one immutable historical version. `next_page` returns
`WithMetadata`, including the exact version used by that page.

```rust,no_run
# use aws_sdk_dynamodb::types::AttributeValue;
# use prolly_dynamodb_client::Client;
# async fn pages(client: &Client) -> Result<(), prolly_dynamodb_client::Error> {
let mut pages = client.query()
    .table_name("Orders")
    .key_condition_expression("#account = :account")
    .expression_attribute_names("#account", "accountId")
    .expression_attribute_values(":account", AttributeValue::S("acct-1".into()))
    .limit(100)
    .into_paginator();
while let Some(page) = pages.next_page().await? {
    let version = page.version_id.as_ref().expect("Query always resolves a version");
    println!("{} items from {version}", page.output.count());
}
# Ok(()) }
```

For stream combinators, consume the same paginator with `.into_stream()`; its
item type is `Result<WithMetadata<QueryOutput>, Error>` (and equivalently for
`Scan`). Pin the returned stream before calling `StreamExt::next`.

`BatchGetItem` accepts the official `KeysAndAttributes` model, validates the
100-key limit and canonical duplicate keys before item-tree reads, and performs
one ordered multi-get on one immutable snapshot per table. It enforces the
1-MiB-per-partition and 16-MiB response boundaries and returns remaining keys
through the official `UnprocessedKeys` shape. Use `.at(table_versions)` for a
repeatable multi-table historical read; `table_versions` in returned metadata
records every snapshot actually used. Response item order is deliberately not
an API contract, just as with DynamoDB.

`BatchWriteItem` accepts the official `WriteRequest` model and preserves
DynamoDB's non-atomic batch contract: every item is a separate conditional head
transition, and global validation (including the 25-operation limit and
canonical duplicate targets) finishes before the first write. Metadata lists
every transition the client knows was accepted. A safely retryable,
definitely-not-applied failure is returned through official
`UnprocessedItems`; a transport failure whose transaction outcome is unknown is
instead a structured `Error::BatchWrite`. That error keeps the uncertain
request separate from definitely unattempted work and exposes the provider's
idempotency token for reconciliation. Applications must not blindly replay an
outcome-unknown request.

Expression processing is resource-bounded before storage access: each
expression is at most 4 KiB, the shared name/value binding set is at most 2
MiB, placeholders are at most 255 bytes, and document paths are at most 32
elements. Parenthesized/`NOT` and recursive `list_append` syntax is limited to
64 levels; constructed core ASTs have a separate 512-level defensive ceiling.
Invalid deserialized paths, empty or overlapping projections/update plans, and
excessive recursion return typed validation errors rather than panicking.
These are compatibility limits when generating expressions programmatically.

## Transactions and durable history

`TransactGetItems` reads every requested slot from one validated root read set.
`TransactWriteItems` evaluates all conditions and updates against the original
table heads and publishes every participating table atomically. The client
matches the AWS Rust SDK's safety behavior by generating a random request token
when one is omitted. An explicit `client_request_token` is stored only as a
domain-separated hash and supports safe retry across process restart for ten
minutes. Reusing it with a changed canonical request is rejected.

Every accepted mutation has a durable `CommitId`, including identical puts,
deletes of absent items, condition-only transactions, restores to the current
state, and table lifecycle changes. `send()` continues to return the official
AWS output. Use `send_with_metadata()` to obtain commit and transition data:

```rust,no_run
# use aws_sdk_dynamodb::types::AttributeValue;
# use prolly_dynamodb_client::Client;
# async fn history(client: &Client) -> Result<(), prolly_dynamodb_client::Error> {
let written = client.put_item()
    .table_name("Orders")
    .item("accountId", AttributeValue::S("acct-1".into()))
    .send_with_metadata()
    .await?;
let commit_id = written.commit_id.expect("accepted writes have a commit ID");

let orders = client.table("Orders");
let commit = orders.commit(&commit_id).await?.expect("durable commit");
assert_eq!(commit.commit_id, commit_id);

let first_page = orders.commits(None, 100).await?;
for event in first_page.commits {
    println!("{} {}", event.sequence, event.commit_id);
}
# Ok(()) }
```

Table history is ordered by a monotonically increasing per-incarnation
sequence. Pages are bounded to 1,000 records and use the exclusive
`last_sequence` continuation. Each transition includes the immutable table
incarnation ID; a commit from a deleted and recreated table with the same name
cannot be mistaken for the current table.

`PutItem`, `DeleteItem`, and `UpdateItem` also expose the additive
`.request_token(...)` method. Supplying it routes the operation through the
same durable ten-minute replay protocol as transactions, including canonical
payload and expected-head fingerprinting. Replays return the original commit
and reconstruct the original old/new images from immutable versions—even from
a fresh process and after the logical table name has been deleted. Response-only
`ReturnValues` selection is intentionally not part of the mutation fingerprint;
the selected response is derived from those preserved images.

Logical `CreateTable` and `DeleteTable` builders accept the same
`.request_token(...)` extension. Restore is an explicit CAS builder, so callers
cannot accidentally restore over an unobserved concurrent write:

```rust,no_run
# use prolly_dynamodb_client::Client;
# async fn restore(client: &Client) -> Result<(), prolly_dynamodb_client::Error> {
let orders = client.table("Orders");
let current = orders.head().await?;
let target = orders.collect_versions().await?.into_iter()
    .find(|version| version.id != current.id)
    .expect("retained target");
let restored = orders
    .restore(target.id)
    .expected_head(current.id)
    .request_token("restore-orders-2026-08-08")
    .send_with_metadata()
    .await?;
assert!(restored.commit_id.is_some());
# Ok(()) }
```

Administrative replay resolves the original immutable table descriptor by its
incarnation ID. Replaying an old delete token therefore cannot delete a newer
table that reused the same logical name.

Version discovery and diff traversal are bounded by default. `versions()`
returns a paginator in stable version-ID order; its serializable cursor is
bound to the exact table incarnation. `diff(from, to)` returns a structural
diff paginator whose serializable cursor is bound to both immutable roots:

```rust,no_run
# use prolly_dynamodb_client::Client;
# async fn history(client: &Client, from: prolly::MapVersionId, to: prolly::MapVersionId) -> Result<(), prolly_dynamodb_client::Error> {
let orders = client.table("Orders");
let mut versions = orders.versions().page_size(100);
while let Some(page) = versions.next_page().await? {
    for version in page.versions {
        println!("{}", version.id);
    }
}

let mut changes = orders.diff(from, to).page_size(256);
while let Some(page) = changes.next_page().await? {
    for change in page.diffs {
        println!("{change:?}");
    }
}
# Ok(()) }
```

`collect_versions()` and `collect_diff()` are convenience methods only. They
fail closed above their advertised 10,000-entry collection ceilings instead of
allowing unbounded memory growth.

## Retention safety

Retention is always a two-step administrative operation. `plan()` performs a
read-only scan and returns at most 80 exact version IDs for one atomic provider
transaction. Applying the plan requires explicit actor and reason attribution:

```rust,no_run
# use prolly_dynamodb_client::{Client, MaintenanceContext, RetentionPolicy};
# async fn retention(client: &Client) -> Result<(), prolly_dynamodb_client::Error> {
let evidence = client.table("Evidence");
let plan = evidence
    .retention(RetentionPolicy::keep_last(365))
    .plan()
    .await?;

// Persist/review `plan` under the application's change-control process first.
let result = evidence
    .apply_retention(
        &plan,
        MaintenanceContext::new("records-officer", "approved annual schedule")
            .change_ticket("LEGAL-2026-0042"),
    )
    .await?;
assert_eq!(result.removed, plan.remove);
# Ok(()) }
```

The plan identity covers its complete canonical contents. Execution
revalidates the logical table incarnation, immutable head, and durable commit
sequence, then atomically deletes the exact roots and writes a durable audit
record. Concurrent writes, head ABA, missing candidates, plan tampering, or
wrong-table use fail closed. Reapplying the same plan and operator context
returns the original audit result; changing the context is an idempotency
mismatch. When `more_removable` is true, create and review another plan after
the first batch succeeds.

Retention deletes only version catalog roots. It does not reclaim immutable
nodes or blobs; GC remains a separate planned and authorized maintenance step.

## Bounded backup and audited import

Backups pin one immutable version and require an explicit memory/resource
envelope. The canonical archive contains the logical table descriptor, exact
`MapVersionId`, database/tree format record, every reachable Prolly node, and
every externally stored large-value blob. Verification rejects missing, extra,
duplicate, length-mismatched, noncanonical, or content-hash-mismatched objects.

```rust,no_run
# use prolly_dynamodb_client::{Client, MaintenanceContext, TableArchive, TableArchiveLimits};
# async fn backup_import(client: &Client) -> Result<(), prolly_dynamodb_client::Error> {
let limits = TableArchiveLimits::new(
    1_000_000,            // nodes
    512 * 1024 * 1024,    // node bytes
    100_000,              // blobs
    512 * 1024 * 1024,    // blob bytes
    1024 * 1024 * 1024,   // encoded archive bytes
);
let version = client.table("Evidence").head().await?.id;
let archive = client.table("Evidence").at(version.clone()).export(limits).await?;
let bytes = archive.to_bytes(limits)?;

// Decode performs complete verification before returning an archive.
let archive = TableArchive::from_bytes(&bytes, limits)?;
let import = client.import(archive, "EvidenceRecovered", limits);
let plan = import.plan().await?; // read-only: the target remains absent

// Persist/review `plan` through change control before applying it.
let result = import.apply(
    &plan,
    MaintenanceContext::new("recovery-officer", "approved evidence recovery")
        .change_ticket("LEGAL-2026-0088"),
).await?;
assert_eq!(result.version, version);
# Ok(()) }
```

The plan binds the canonical archive digest, source identity/version, fresh
target incarnation, target name, and exact destination format. Import first
verifies all content, then may prepublish immutable blobs and nodes. It exposes
the target catalog entry, descriptor, exact version root and head, lifecycle
commit, and operator audit together in one strict transaction. A failed or
conflicting transaction can leave only unreachable content-addressed objects,
never a partially visible logical table. Retrying the same plan and context,
including from a fresh process after an ambiguous provider response, resolves
the original durable audit and returns the original result.

Archives currently require an exact database-format match. Cross-format
migration is deliberately separate from restore and must use a versioned,
verified migration workflow rather than silently rewriting evidence during
import.

The separate [`prolly-dynamodb-admin`](../admin/README.md) package exposes the
same backup/import and retention workflows as JSON-emitting commands with
create-new output files. It keeps CLI and AWS configuration dependencies out of
normal application binaries.

## Explicit stream and TTL workers

`Client::open` never starts background work. A process must deliberately create
a worker through `client.workers()`, supply a stable logical subscription or TTL
configuration, and run it with a cancellation token. Job identity binds the
table incarnation, so deleting and recreating a table cannot accidentally
resume an old table's checkpoint.

```rust,no_run
use prolly_dynamodb_client::{
    CancellationToken, Client, StreamWorkerOptions, TtlWorkerOptions,
};

# async fn workers(client: Client) -> Result<(), prolly_dynamodb_client::Error> {
let cancellation = CancellationToken::new();
let mut stream = client.workers().stream(StreamWorkerOptions::new(
    "Orders",
    "legal-audit-ledger",
    "worker-host-a",
)).await?;

// Delivery is sequential and bounded by the configured page. Persist the
// stable commit_id in the destination's deduplication record.
let page = stream.run_once(&mut |commit| async move {
    println!("{} {}", commit.commit_id, commit.sequence);
    Ok::<_, std::io::Error>(())
}).await?;
println!("delivered={}", page.delivered);
// A daemon normally calls `stream.run(&cancellation, sink)`. Its shutdown
// controller calls `cancellation.cancel()` from another task.
cancellation.cancel();
let exit = stream.shutdown().await?;
assert!(!exit.release.replayed);

let mut ttl = client.workers().ttl(TtlWorkerOptions::new(
    "Orders",
    "expiresAt",
    "worker-host-a",
)).await?;
let page = ttl.run_once().await?;
println!("evaluated={}, deleted={}", page.evaluated, page.deleted);
ttl.shutdown().await?;
# Ok(()) }
```

Stream delivery is at-least-once. The worker checkpoints a sequence only after
the sink returns success while the exact fencing generation is still live. A
crash after the external effect but before its checkpoint can redeliver the
same stable `CommitId`; a sink that needs effectively-once effects must make
that ID unique in its own transaction. The worker renews its lease during slow
sink calls and idle waits. Cancellation is observed between records, allowing
an in-flight sink result to be checkpointed before the lease is durably
released.

The TTL worker accepts only integer Number attributes containing Unix epoch
seconds. It ignores future values, non-numbers, fractional/negative values, and
timestamps more than five 365-day years old, matching DynamoDB's documented
eligibility window. Every expiry uses a conditional delete that requires the
currently stored TTL attribute to equal the value observed by the scan. A
concurrent refresh, removal, or type change therefore prevents deletion. TTL
pages and cumulative acknowledged counters are durably checkpointed; a crash
may rescan a page but cannot make the conditional delete unsafe.

Worker leases are single-owner, renewable, and protected by monotonically
increasing fencing tokens. Takeover is allowed only after expiry. A stale
process cannot renew, checkpoint, or release a newer generation. Runtime tuning
such as page size, polling delay, and lease duration does not change job
identity, so operators can tune or relocate a worker without abandoning its
checkpoint.

Physical GC is also available as an explicit
`client.workers().maintenance(context, duration_millis)` session. The returned
`MaintenanceWorker` only binds the global fence to `plan_gc`, `apply_gc`, and
an explicit attributed `shutdown`; it never scans, deletes, or releases on
construction or drop. This preserves the reviewed dry-run/apply boundary and
the fail-closed recovery model described below.

## Fail-closed maintenance fence

Destructive physical maintenance must first acquire the namespace-wide writer
fence with an attributed `MaintenanceContext`. Every logical write transaction
reads the fence root, so a lease acquired after a writer begins invalidates that
writer's commit condition. Already-completed idempotent replays remain readable.

Lease expiry never automatically admits writers: a paused or crashed sweeper
could still be deleting objects. The holder must explicitly release the lease,
or an operator may force-break it only after expiry. Release/break changes the
control root and writes durable operator evidence atomically. This deliberately
prefers a recoverable write outage over a race that could delete reachable
financial or legal records.

The current public fence API is `maintenance_lease()`,
`acquire_maintenance_lease(...)`, `release_maintenance_lease(...)`, and
`break_expired_maintenance_lease(...)`. The administrative CLI provides the
equivalent `lease-status`, `lease-acquire`, `lease-release`, and
`lease-break-expired` commands.

`plan_gc(lease_id, cursor, limits)` is the read-only half of that workflow. It
enumerates every retained named root under an explicit cap, expands immutable
snapshot catalogs into their referenced base/source/index trees, merges the
per-table successful-blob registries, and computes bounded node and
large-value-blob reachability,
and evaluates at most 1,000 physical
DynamoDB items per candidate scan page. The plan binds the lease ID, complete
root-set digest, input/output cursors, exact reclaimable CIDs, and the exact
numbers of tree nodes and leaf values inspected by blob reachability. It samples
the lease and full root digest again after candidate scans and fails if either
moved. Empty pages with a continuation are valid because DynamoDB applies a
`Scan` limit before the namespace filter. A dry run never deletes the reported
candidates.

`apply_gc(plan, context, options)` rechecks the canonical plan identity, exact
lease, full root digest, bounded reachability summary (including
`protected_trees`, `scanned_blob_nodes`, and `scanned_values`), and that every
candidate is still
unreachable. It then durably records and pins that exact plan before
performing bounded idempotent node/blob deletes. Partial failure keeps the fence
pinned; retrying the same plan and context resumes safely. Completion is
durable and replayable, and lease release or expired-lease break is rejected
while any execution remains in progress. The client never releases the lease
implicitly. Each successful physical node-deletion chunk also invalidates the
engine's decoded-node, rightmost-path, recent-leaf, and branch-lineage caches
before later work can reuse a swept CID.

The blob registry is append-only in format 12. This deliberately favors
evidence safety: failed or prepublished orphan blobs are reclaimable, while a
blob introduced by any successful write/import remains protected even after
all referencing history is removed. Reclaiming that conservative residue
requires a future explicit, audited exact-registry compaction; GC never guesses.

## DynamoDB Local starter

The repository compose file exposes DynamoDB Local on port 8000. The example
initializes the physical schema explicitly and then opens the logical client:

```bash
docker compose -f docker-compose.store-services.yml up -d dynamodb
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
export PROLLY_STORE_DYNAMODB_ENDPOINT=http://127.0.0.1:8000
export PROLLY_STORE_DYNAMODB_TABLE=prolly-versioned-example
cargo run --manifest-path extensions/dynamodb/client/Cargo.toml --example direct_crud
```

Use a unique non-empty key prefix for every test or tenant. DynamoDB Local is a
contract-test environment, not evidence of AWS service latency or durability.
