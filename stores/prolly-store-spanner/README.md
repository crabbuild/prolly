# prolly-store-spanner

Cloud Spanner-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` using `gcloud-spanner`. Use it
through `RemoteProllyStore` and `AsyncProlly` when you need globally consistent
SQL-backed Prolly tree storage on Google Cloud Spanner.

## When To Use It

Use this adapter when your application already runs on Cloud Spanner or when
named roots need strongly consistent, horizontally scalable SQL storage. It is a
good fit for multi-region metadata, durable branch heads, and services where
Spanner availability and transaction semantics are more important than local
single-node latency.

Use PostgreSQL/MySQL for simpler single-region SQL deployments. Use Redis for
cache-like state. Use DynamoDB or Cosmos DB when your cloud platform is AWS or
Azure and you prefer their native NoSQL services.

## Table Model

The adapter expects these GoogleSQL tables:

```sql
CREATE TABLE ProllyNodes (
  Cid BYTES(32) NOT NULL,
  Node BYTES(MAX) NOT NULL
) PRIMARY KEY (Cid);

CREATE TABLE ProllyHints (
  Namespace BYTES(MAX) NOT NULL,
  HintKey BYTES(MAX) NOT NULL,
  Value BYTES(MAX) NOT NULL
) PRIMARY KEY (Namespace, HintKey);

CREATE TABLE ProllyRoots (
  Name BYTES(MAX) NOT NULL,
  Manifest BYTES(MAX) NOT NULL
) PRIMARY KEY (Name);
```

The same DDL is exposed as `SPANNER_SCHEMA`.

## Setup

Set the Spanner database resource name:

```bash
export PROLLY_STORE_SPANNER_DATABASE=projects/<project>/instances/<instance>/databases/<database>
```

Create the tables using `gcloud`:

```bash
gcloud spanner databases ddl update <database> \
  --instance=<instance> \
  --ddl='CREATE TABLE ProllyNodes (Cid BYTES(32) NOT NULL, Node BYTES(MAX) NOT NULL) PRIMARY KEY (Cid)'

gcloud spanner databases ddl update <database> \
  --instance=<instance> \
  --ddl='CREATE TABLE ProllyHints (Namespace BYTES(MAX) NOT NULL, HintKey BYTES(MAX) NOT NULL, Value BYTES(MAX) NOT NULL) PRIMARY KEY (Namespace, HintKey)'

gcloud spanner databases ddl update <database> \
  --instance=<instance> \
  --ddl='CREATE TABLE ProllyRoots (Name BYTES(MAX) NOT NULL, Manifest BYTES(MAX) NOT NULL) PRIMARY KEY (Name)'
```

Authentication depends on `gcloud-spanner` configuration. In the examples and
tests, set `PROLLY_STORE_SPANNER_AUTH=1` to call `ClientConfig::with_auth()`.

## Basic Usage

```rust
use google_cloud_spanner::client::ClientConfig;
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_spanner::SpannerBackend;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let database = "projects/my-project/instances/my-instance/databases/my-db";
let config = ClientConfig::default().with_auth().await?;
let backend = SpannerBackend::connect(database, config).await?;

let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
let tree = prolly
    .batch(
        &prolly.create(),
        vec![Mutation::Upsert {
            key: b"account/1".to_vec(),
            val: b"active".to_vec(),
        }],
    )
    .await?;

prolly.publish_named_root(b"accounts/main", &tree).await?;
# Ok(())
# }
```

## Diff And Merge

```rust
# use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
# use prolly_store_spanner::SpannerBackend;
# async fn run(backend: SpannerBackend) -> Result<(), Box<dyn std::error::Error>> {
# let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
# let base = prolly.batch(&prolly.create(), vec![
#     Mutation::Upsert { key: b"account/1".to_vec(), val: b"active".to_vec() },
#     Mutation::Upsert { key: b"account/2".to_vec(), val: b"active".to_vec() },
# ]).await?;
let left = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"account/1".to_vec(),
            val: b"suspended".to_vec(),
        }],
    )
    .await?;
let right = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"account/2".to_vec(),
            val: b"closed".to_vec(),
        }],
    )
    .await?;

let diffs = prolly.diff(&base, &left).await?;
assert_eq!(diffs.len(), 1);

let merged = prolly.merge(&base, &left, &right, None).await?;
assert_eq!(
    prolly.get(&merged, b"account/2").await?,
    Some(b"closed".to_vec())
);
# Ok(())
# }
```

## Operational Notes

- The adapter does not create tables. Apply DDL before startup.
- Named-root compare-and-swap uses a Spanner read-write transaction.
- `batch_put_nodes` is applied as Spanner mutations.
- There is no adapter-level key prefix. Use distinct named-root prefixes for
  tenants or environments, and isolate databases when you need full physical
  separation.
- Node garbage collection should be coordinated at the application layer after
  deciding which named roots to retain.

## Running The Example

From the CrabDB workspace root:

```bash
export PROLLY_STORE_SPANNER_DATABASE=projects/<project>/instances/<instance>/databases/<database>
export PROLLY_STORE_SPANNER_AUTH=1
cargo run -p prolly-store-spanner --example basic_usage
```

The example writes a base tree, diffs and merges branches, resolves a conflict,
publishes a unique named root, and loads it back.
