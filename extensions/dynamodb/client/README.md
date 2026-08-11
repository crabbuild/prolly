# prolly-dynamodb-client

`prolly-dynamodb-client` is an in-process Rust client for logical DynamoDB tables stored as immutable, versioned Prolly maps. It follows the AWS SDK for Rust's fluent builder style and adds historical reads, diffs, durable commits, optimistic head checks, audited maintenance, and explicit workers.

Use the [DynamoDB extension overview](../README.md) first if you need the architecture, physical table model, use cases, or adoption limits.

## Decide whether the client fits your application

Choose this client when you need:

- AWS SDK-shaped Rust builders for logical DynamoDB operations
- immutable table versions and structural diffs
- atomic transitions across logical tables
- durable request replay after ambiguous responses
- explicit retention, backup, import, stream, time-to-live (TTL), and garbage-collection workflows

This client is not a DynamoDB-compatible network service. Other DynamoDB SDKs and native item tools cannot read its private physical representation.

Your application owns the underlying `aws_sdk_dynamodb::Client`, Tokio runtime, credentials, region, retry policy, and endpoint configuration. Use dedicated physical tables or a unique non-empty key prefix for each tenant or environment.

## Review the production contracts

Read these documents before deploying the client:

| Document | What it defines |
| --- | --- |
| [`COMPATIBILITY.md`](COMPATIBILITY.md) | Supported operations, fields, and durable format contract |
| [`OPERATIONS.md`](OPERATIONS.md) | Deployment, authority separation, incidents, and rollback |
| [`SECURITY.md`](SECURITY.md) | Threat model and exact-table AWS Identity and Access Management (IAM) policies |
| [`PERFORMANCE.md`](PERFORMANCE.md) | Measurement and scaling contract |
| [`FAULT_INJECTION.md`](FAULT_INJECTION.md) | Publication failure qualification |
| [`RECOVERY.md`](RECOVERY.md) | Recovery exercises |
| [`SOAK.md`](SOAK.md) | Multi-process and process-death qualification |
| [`RELEASE_STATUS.md`](RELEASE_STATUS.md) | Current evidence and production blockers |
| [`public-api.txt`](public-api.txt) | Reviewed Rust signature and trait surface |

The `rolling_compatibility_probe` example and `scripts/run_dynamodb_rolling_compatibility.py` test independently built release pairs. An identical-binary smoke test is diagnostic only.

## Open a client

Create the physical schema explicitly, then open the logical client. `Client::open` validates both physical tables and the database format without provisioning or upgrading them.

```rust
use prolly_dynamodb_client::Client;
use prolly_store_dynamodb::DynamoDbBackend;

async fn open_client(
    physical: aws_sdk_dynamodb::Client,
) -> Result<Client, Box<dyn std::error::Error>> {
    let backend = DynamoDbBackend::new(physical, "prolly-versioned")
        .with_root_table_name("prolly-versioned-roots")
        .with_key_prefix(b"orders-prod:".to_vec());

    backend.initialize_schema().await?;
    let client = Client::open(backend).await?;
    println!("{}", client.capabilities().to_json()?);
    Ok(client)
}
```

Provision physical tables outside the runtime process when production IAM policy separates control-plane and data-plane authority. In that setup, call `validate_initialized_schema` or run the admin CLI's `verify` command before serving traffic.

## Configure retries and caching

Use `Client::builder` when you need explicit retry or cache limits:

```rust
use prolly::RemoteStoreConfig;
use prolly_dynamodb_client::{Client, Error};
use prolly_store_dynamodb::DynamoDbBackend;

async fn open_with_limits(
    backend: DynamoDbBackend,
) -> Result<Client, Error> {
    Client::builder()
        .backend(backend)
        .remote_store_config(RemoteStoreConfig {
            verify_node_cids: true,
        })
        .logical_retry_limit(7)
        .node_cache_max_nodes(100_000)
        .node_cache_max_bytes(64 * 1024 * 1024)
        .open()
        .await
}
```

These controls apply to one process. They do not change the persistent database format.

Use `.store(existing_store)` instead of `.backend(...)` when you already own a `DynamoDbStore`. An existing store owns its `RemoteStoreConfig`, so you cannot combine `.store(...)` with `.backend(...)` or `.remote_store_config(...)`.

- `logical_retry_limit(0)` allows one attempt
- the default limit of `7` allows eight attempts
- values above `63` fail before any provider request
- the default cache retains up to 64 MiB of serialized node weight
- a zero node or byte limit disables that cache dimension

`client.cache_usage()` reports retained entries, serialized bytes, and pinned portions. Process resident set size also includes decoded objects, AWS SDK buffers, allocator overhead, and unrelated application memory.

Clone and reuse one open `Client` for each physical namespace in a process. Clones share write admission and avoid rebuilding against the same stale head. Separate clients and processes remain safe through provider compare-and-swap (CAS), but they do not share this optimization.

## Write and read items

The builders accept official AWS `AttributeValue` types and return official AWS output types. Call `send_with_metadata()` when you also need the accepted commit or resolved version.

```rust
use aws_sdk_dynamodb::types::AttributeValue;
use prolly_dynamodb_client::{Client, Error};
async fn close_order(client: &Client) -> Result<(), Error> {
    client.put_item()
        .table_name("Orders")
        .item("accountId", AttributeValue::S("acct-1".into()))
        .item("status", AttributeValue::S("OPEN".into()))
        .send()
        .await?;

    let updated = client.update_item()
        .table_name("Orders")
        .key("accountId", AttributeValue::S("acct-1".into()))
        .update_expression("SET #status = :closed")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(
            ":closed",
            AttributeValue::S("CLOSED".into()),
        )
        .send_with_metadata()
        .await?;

    println!("commit={:?}", updated.commit_id);
    Ok(())
}
```

Unsupported fields are absent or return a typed validation error. The client does not approximate behavior it cannot guarantee.

## Protect writes with an expected head

Use `if_head` when a write must fail after any unobserved table transition:

```rust
use aws_sdk_dynamodb::types::AttributeValue;
use prolly_dynamodb_client::{Client, Error};

async fn insert_if_unchanged(client: &Client) -> Result<(), Error> {
    let orders = client.table("Orders");
    let observed = orders.head().await?;

    orders.if_head(observed.id)
        .put_item()
        .item("accountId", AttributeValue::S("acct-2".into()))
        .send()
        .await?;
    Ok(())
}
```

A changed head returns `HeadConflict`. This check covers the whole logical table, not one item.

## Read collections

`Query` and `Scan` expose explicit paginators. Each page from `next_page()` includes the exact version used for that page.

```rust
use aws_sdk_dynamodb::types::AttributeValue;
use prolly_dynamodb_client::{Client, Error};

async fn query_orders(client: &Client) -> Result<(), Error> {
    let mut pages = client.query()
        .table_name("Orders")
        .key_condition_expression("#account = :account")
        .expression_attribute_names("#account", "accountId")
        .expression_attribute_values(
            ":account",
            AttributeValue::S("acct-1".into()),
        )
        .limit(100)
        .into_paginator();

    while let Some(page) = pages.next_page().await? {
        let version = page.version_id.expect("query resolves a version");
        println!("{} items from {version}", page.output.count());
    }
    Ok(())
}
```

A current-head paginator may use a newer version on a later page. Start the operation with `client.table(name).at(version)` when every page must use one immutable snapshot.

Use `.into_stream()` when stream combinators fit your application. Pin the returned stream before calling `StreamExt::next`.

### Batch reads

`BatchGetItem` accepts the official `KeysAndAttributes` model. It validates the 100-key limit and duplicate canonical keys before reading item trees.

Each logical table uses one immutable snapshot for the batch. Use `.at(table_versions)` for a repeatable historical read across tables. Returned metadata records every version used.

The response enforces these DynamoDB boundaries:

- 1 MiB per partition
- 16 MiB for the complete response
- no item-order guarantee

Remaining keys use the official `UnprocessedKeys` shape.

### Batch writes

`BatchWriteItem` validates the complete request before its first write. It preserves DynamoDB's non-atomic batch contract, so each item becomes a separate conditional head transition.

The client returns a definitely-not-applied request through `UnprocessedItems`. An outcome-unknown transport failure returns `Error::BatchWrite` with the uncertain request, unattempted work, and provider idempotency token. Do not blindly replay that uncertain request.

### Expression limits

The client validates expression resources before storage access:

| Resource | Limit |
| --- | ---: |
| One expression | 4 KiB |
| Shared name and value bindings | 2 MiB |
| One placeholder | 255 bytes |
| One document path | 32 elements |
| Parentheses, `NOT`, or recursive `list_append` | 64 levels |
| Constructed core abstract syntax tree | 512 levels |

Invalid paths, overlapping update plans, and excessive recursion return typed validation errors instead of panicking.

## Use transactions and durable request tokens

`TransactGetItems` reads one validated root set. `TransactWriteItems` evaluates every condition against the original heads and atomically advances all participating tables.

The client generates a random request token when you omit one. An explicit `client_request_token` supports safe retry across process restarts for ten minutes. Reusing the token with a different canonical request fails.

Point writes and logical table lifecycle builders also accept `.request_token(...)`. Replays return the original commit and reconstruct the requested old or new images from immutable history.

## Inspect commits and versions

Every accepted mutation receives a durable `CommitId`. This includes identical puts, deletion of an absent item, condition-only transactions, restores to the current state, and table lifecycle changes.

```rust
use aws_sdk_dynamodb::types::AttributeValue;
use prolly_dynamodb_client::{Client, Error};

async fn list_commits(client: &Client) -> Result<(), Error> {
    let written = client.put_item()
        .table_name("Orders")
        .item("accountId", AttributeValue::S("acct-1".into()))
        .send_with_metadata()
        .await?;
    let id = written.commit_id.expect("write has a commit ID");

    let orders = client.table("Orders");
    let commit = orders.commit(&id).await?.expect("commit exists");
    assert_eq!(commit.commit_id, id);

    for event in orders.commits(None, 100).await?.commits {
        println!("{} {}", event.sequence, event.commit_id);
    }
    Ok(())
}
```

Commit history uses a monotonically increasing sequence for each table incarnation. A commit from a deleted table cannot match a recreated table with the same logical name.

Use bounded paginators for large histories and diffs:

```rust
use prolly::MapVersionId;
use prolly_dynamodb_client::{Client, Error};

async fn inspect_diff(
    client: &Client,
    from: MapVersionId,
    to: MapVersionId,
) -> Result<(), Error> {
    let orders = client.table("Orders");
    let mut changes = orders.diff(from, to).page_size(256);

    while let Some(page) = changes.next_page().await? {
        for change in page.diffs {
            println!("{change:?}");
        }
    }
    Ok(())
}
```

`versions()` uses a stable version-ID order. Its cursor is bound to one table incarnation. `diff(from, to)` binds its cursor to both immutable roots.

`collect_versions()` and `collect_diff()` allocate the complete result. Both fail above 10,000 entries. Use paginators when the result can exceed that ceiling.

## Restore a retained version

Restore requires the head you observed. The operation fails instead of replacing a concurrent write.

```rust
use prolly_dynamodb_client::{Client, Error};

async fn restore_previous(client: &Client) -> Result<(), Error> {
    let orders = client.table("Orders");
    let current = orders.head().await?;
    let target = orders.collect_versions().await?
        .into_iter()
        .find(|version| version.id != current.id)
        .expect("a retained version exists");

    let restored = orders.restore(target.id)
        .expected_head(current.id)
        .request_token("restore-orders-2026-08-08")
        .send_with_metadata()
        .await?;
    assert!(restored.commit_id.is_some());
    Ok(())
}
```

Replay resolves the original table incarnation. An old delete or restore token cannot mutate a newer table that reused the same name.

## Retain versions with a reviewed plan

Retention separates discovery from mutation. `plan()` performs a read-only scan and returns at most 80 exact version IDs for one provider transaction.

```rust
use prolly_dynamodb_client::{
    Client, Error, MaintenanceContext, RetentionPolicy,
};

async fn retain_versions(client: &Client) -> Result<(), Error> {
    let table = client.table("Evidence");
    let plan = table.retention(RetentionPolicy::keep_last(365))
        .plan()
        .await?;

    let context = MaintenanceContext::new(
        "records-officer",
        "approved annual schedule",
    ).change_ticket("LEGAL-2026-0042");
    let result = table.apply_retention(&plan, context).await?;
    assert_eq!(result.removed, plan.remove);
    Ok(())
}
```

Persist and review the plan before applying it. Execution revalidates the table incarnation, head, commit sequence, plan identity, candidates, and operator context.

Retention removes version catalog roots. It does not reclaim immutable nodes or blobs. Run physical garbage collection as a separate maintenance workflow.

## Export and import a verified archive

An export pins one version and requires explicit resource limits. The archive includes the table descriptor, version, format record, reachable Prolly nodes, and referenced large-value blobs.

```rust
use prolly_dynamodb_client::{
    Client, Error, TableArchiveLimits,
};

async fn export_table(client: &Client) -> Result<Vec<u8>, Error> {
    let limits = TableArchiveLimits::new(
        1_000_000,
        512 * 1024 * 1024,
        100_000,
        512 * 1024 * 1024,
        1024 * 1024 * 1024,
    );
    let table = client.table("Evidence");
    let version = table.head().await?.id;
    let archive = table.at(version).export(limits).await?;
    Ok(archive.to_bytes(limits)?)
}
```

Decode verifies canonical encoding, object hashes, lengths, duplicates, and completeness before returning an archive. Import also separates a read-only plan from an attributed apply step:

```rust
use prolly_dynamodb_client::{
    Client, Error, MaintenanceContext, TableArchive,
    TableArchiveLimits,
};

async fn import_table(client: &Client, bytes: &[u8]) -> Result<(), Error> {
    let limits = TableArchiveLimits::new(
        1_000_000,
        512 * 1024 * 1024,
        100_000,
        512 * 1024 * 1024,
        1024 * 1024 * 1024,
    );
    let archive = TableArchive::from_bytes(bytes, limits)?;
    let import = client.import(archive, "EvidenceRecovered", limits);
    let plan = import.plan().await?;
    let context = MaintenanceContext::new(
        "recovery-officer",
        "approved evidence recovery",
    ).change_ticket("LEGAL-2026-0088");
    import.apply(&plan, context).await?;
    Ok(())
}
```

Import may prepublish immutable content before one strict transaction exposes the new table, root, commit, and audit record. A failed transaction cannot expose a partial logical table.

Archives require an exact database-format match. Cross-format migration is a separate verified workflow. Use the [`prolly-dynamodb-admin`](../admin/README.md) package when you need JSON plans and create-new output files for change control.

## Run explicit workers

`Client::open` starts no background work. Create each worker with a stable job identity, run it with a cancellation token, then call its explicit shutdown method.

### Consume the commit stream

The stream worker delivers commits sequentially and at least once. Deduplicate the stable `CommitId` in the destination transaction when you need effectively-once effects.

```rust
use prolly_dynamodb_client::{
    CancellationToken, Client, Error, StreamWorkerOptions,
};

async fn consume_commits(client: Client) -> Result<(), Error> {
    let cancel = CancellationToken::new();
    let options = StreamWorkerOptions::new(
        "Orders",
        "audit-ledger",
        "worker-host-a",
    );
    let mut worker = client.workers().stream(options).await?;
    let page = worker.run_once(&mut |commit| async move {
        println!("{} {}", commit.commit_id, commit.sequence);
        Ok::<_, std::io::Error>(())
    }).await?;
    println!("delivered={}", page.delivered);

    cancel.cancel();
    worker.shutdown().await?;
    Ok(())
}
```

The worker checkpoints only after the sink succeeds while the same fencing generation remains live. A crash after the external effect can redeliver the commit.

### Delete expired items

The TTL worker accepts integer Number attributes containing Unix epoch seconds:

```rust
use prolly_dynamodb_client::{
    Client, Error, TtlWorkerOptions,
};

async fn expire_items(client: Client) -> Result<(), Error> {
    let options = TtlWorkerOptions::new(
        "Orders",
        "expiresAt",
        "worker-host-a",
    );
    let mut worker = client.workers().ttl(options).await?;
    let page = worker.run_once().await?;
    println!("evaluated={}", page.evaluated);
    println!("deleted={}", page.deleted);
    worker.shutdown().await?;
    Ok(())
}
```

The worker ignores future, fractional, negative, non-number, and stale values outside DynamoDB's five-year eligibility window. Each delete requires the current TTL value to match the scanned value, so a concurrent refresh prevents deletion.

Worker leases use one owner and a monotonically increasing fencing token. Another process can take over only after expiry. Tuning page size, polling delay, or lease duration does not change the durable job identity.

## Run physical maintenance behind the writer fence

Garbage collection requires a namespace-wide writer fence. Every logical write checks that fence before commit.

The maintenance lifecycle is:

1. Acquire a lease with `MaintenanceContext`
2. Create and review a bounded `plan_gc` result
3. Apply that exact plan with operator attribution
4. Continue through provider pages until no cursor remains
5. Release the lease explicitly

Lease expiry does not admit writers. A paused sweeper could still delete objects, so the holder must release the lease. An operator can break it only after expiry and only when no garbage-collection execution remains in progress.

Each plan binds these inputs:

- lease ID
- complete named-root digest
- input and output provider cursors
- reclaimable content identifiers
- bounded reachability counts

`apply_gc` rechecks the plan, lease, root digest, reachability, and candidates. It records the execution before deletion, resumes after partial failure, and keeps the fence pinned until completion.

The format 12 blob registry is append-only. Garbage collection protects every blob introduced by a successful write or import, even after version retention removes its last visible reference. A future audited registry-compaction workflow must reclaim that conservative residue.

Use `client.workers().maintenance(context, duration_millis)` when a process needs a lease-bound maintenance session. Construction and drop never scan, delete, release, or infer the outcome of an in-flight request.

## Understand runtime and format behavior

`Client` is an `Arc`-backed handle over the caller-supplied adapter. Cloning or dropping it does not stop a runtime, close the AWS client, or shut down workers.

The client emits `tracing` spans at `debug` level for open, data-plane operations, and worker or maintenance lifecycle calls. Stable fields include only `db_system` and `db_operation`. The client excludes keys, items, expressions, credentials, physical table names, and results, and it does not install a global subscriber.

Canonical items larger than 64 KiB use the verified chunked blob path. That threshold forms part of format negotiation.

The current persistent database format is version 12. It includes:

- canonical table and index descriptors
- table schema versions and active index state
- immutable snapshot locators and manifests
- current commit roots and durable commit history
- successful-blob registries
- maintenance, import, and index audit records
- maintenance fences and garbage-collection executions
- worker leases, checkpoints, and releases

Opening a namespace with another format version fails. Run an explicit verified migration instead of mixing format versions.

## Run against DynamoDB Local

Start DynamoDB Local, set the client environment, and run the packaged example:

```bash
docker compose -f docker-compose.store-services.yml up -d dynamodb
export AWS_ACCESS_KEY_ID=local
export AWS_SECRET_ACCESS_KEY=local
export AWS_REGION=us-east-1
export PROLLY_STORE_DYNAMODB_ENDPOINT=http://127.0.0.1:8000
export PROLLY_STORE_DYNAMODB_TABLE=prolly-versioned-example
cargo run --manifest-path extensions/dynamodb/client/Cargo.toml \
  --example direct_crud
```

DynamoDB Local supports contract tests. Its latency, durability, and capacity do not represent AWS DynamoDB.
