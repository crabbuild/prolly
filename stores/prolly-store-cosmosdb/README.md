# prolly-store-cosmosdb

Azure Cosmos DB-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` using the Cosmos DB SQL API REST
surface with key authentication. Use it through `RemoteProllyStore` and
`AsyncProlly` when your deployment is Azure-native and needs globally available
Prolly tree storage.

## Installation

```toml
[dependencies]
prolly-map = "0.4"
prolly-store-cosmosdb = "0.2.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## When to use it

Use this adapter when Cosmos DB is already part of your platform, when you want
managed global distribution, or when tenant/session metadata should live near
other Cosmos DB application data. It is suitable for sync checkpoints, named
branch heads, and remote maps with Cosmos DB consistency and RU budgeting.

Use PostgreSQL/MySQL when SQL transactions and relational operations are the
primary requirement. Use Redis when the store is intentionally cache-like. Use
DynamoDB when you need the AWS-managed equivalent.

## Container model

The adapter expects a Cosmos DB SQL API container with partition key:

```text
/kind
```

Documents have these fields:

- `id`: deterministic document id derived from the logical key.
- `kind`: the shared partition-key value for one backend instance. The default
  is `prolly`.
- `family`: `node`, `root`, or `hint`.
- `key`: hex-encoded logical key bytes.
- `value`: base64-encoded payload bytes.

Cosmos DB transactional batch operations are scoped to one logical partition, so
this adapter stores nodes, roots, and hints for a backend instance in the same
`/kind` partition. Use `with_key_prefix` to separate tenants or test runs inside
that partition. Use `with_partition_key_value` only when you intentionally want
a separate logical partition; transactions cannot span two different values.

The adapter does not create the database or container. Create them through Azure
CLI, ARM/Bicep/Terraform, or the Azure portal before running the adapter.

## Setup

Required environment variables:

```bash
export PROLLY_STORE_COSMOS_ENDPOINT=https://<account>.documents.azure.com:443
export PROLLY_STORE_COSMOS_KEY=<base64-account-key>
export PROLLY_STORE_COSMOS_DATABASE=prolly
export PROLLY_STORE_COSMOS_CONTAINER=prolly_store
```

Example Azure CLI container creation:

```bash
az cosmosdb sql database create \
  --account-name <account> \
  --resource-group <group> \
  --name prolly

az cosmosdb sql container create \
  --account-name <account> \
  --resource-group <group> \
  --database-name prolly \
  --name prolly_store \
  --partition-key-path /kind
```

## Basic usage

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_cosmosdb::CosmosDbBackend;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let backend = CosmosDbBackend::with_key(
    "https://example.documents.azure.com:443",
    "<base64-account-key>",
    "prolly",
    "prolly_store",
)?
.with_key_prefix(b"tenant-a:".to_vec());

let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
let tree = prolly
    .batch(
        &prolly.create(),
        vec![Mutation::Upsert {
            key: b"item/1".to_vec(),
            val: b"active".to_vec(),
        }],
    )
    .await?;

prolly.publish_named_root(b"items/main", &tree).await?;
# Ok(())
# }
```

## Transactions

The backend advertises `supports_transactions() == true` and maps prolly
transactions to Cosmos DB transactional batch requests. A commit validates named
root conditions and applies staged node/root writes atomically; if a root changed
since the transaction read it, the commit returns a conflict and Cosmos rolls the
batch back.

Cosmos DB limits one transactional batch to a single partition key and up to 100
operations. Large transactions should be split by the caller.

## Diff and merge

```rust
# use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
# use prolly_store_cosmosdb::CosmosDbBackend;
# async fn run(backend: CosmosDbBackend) -> Result<(), Box<dyn std::error::Error>> {
# let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
# let base = prolly.batch(&prolly.create(), vec![
#     Mutation::Upsert { key: b"item/1".to_vec(), val: b"active".to_vec() },
#     Mutation::Upsert { key: b"item/2".to_vec(), val: b"active".to_vec() },
# ]).await?;
let left = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"item/1".to_vec(),
            val: b"paused".to_vec(),
        }],
    )
    .await?;
let right = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"item/2".to_vec(),
            val: b"deleted".to_vec(),
        }],
    )
    .await?;

let diffs = prolly.diff(&base, &left).await?;
assert_eq!(diffs.len(), 1);

let merged = prolly.merge(&base, &left, &right, None).await?;
assert_eq!(
    prolly.get(&merged, b"item/2").await?,
    Some(b"deleted".to_vec())
);
# Ok(())
# }
```

## Operational notes

- The account key passed to `with_key` must be base64 encoded.
- Use `with_key_prefix` for tenant, environment, and test isolation.
- `clear_namespace` queries each `kind` partition and deletes matching prefixed
  documents. It is intended for isolated tests, not broad production cleanup.
- Batch methods are implemented as ordered REST calls. Size RU/s and retry
  policy around your production workload.
- Root compare-and-swap uses document ETags.

## Running the example

From the standalone repository root:

```bash
export PROLLY_STORE_COSMOS_ENDPOINT=https://<account>.documents.azure.com:443
export PROLLY_STORE_COSMOS_KEY=<base64-account-key>
export PROLLY_STORE_COSMOS_DATABASE=prolly
export PROLLY_STORE_COSMOS_CONTAINER=prolly_store
cargo run --manifest-path stores/prolly-store-cosmosdb/Cargo.toml --example basic_usage
```

The example writes into a unique key prefix, publishes a named root, reloads it,
runs diff/merge, and resolves a conflict.

## Testing

Unit tests are credential-free. The integration test runs when all four Cosmos
DB variables from the setup section are present:

```bash
cargo test --manifest-path stores/prolly-store-cosmosdb/Cargo.toml
```

Use a dedicated container or key prefix for integration tests. The test suite
creates isolated records but still consumes provisioned request units.

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for the
async map, transaction, diff, and merge APIs used with this backend.
