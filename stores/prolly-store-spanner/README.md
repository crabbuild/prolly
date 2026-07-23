# prolly-store-spanner

Cloud Spanner-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` using `gcloud-spanner`. Use it
through `RemoteProllyStore` and `AsyncProlly` when you need globally consistent
SQL-backed Prolly tree storage on Google Cloud Spanner.

## Installation

The client dependency is listed explicitly because applications construct the
`ClientConfig` passed to the adapter:

```toml
[dependencies]
prolly-map = "0.5.1"
prolly-store-spanner = "0.3.0"
google-cloud-spanner = { package = "gcloud-spanner", version = "=1.8.1" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## When to use it

Use this adapter when your application already runs on Cloud Spanner or when
named roots need strongly consistent, horizontally scalable SQL storage. It is a
good fit for multi-region metadata, durable branch heads, and services where
Spanner availability and transaction semantics are more important than local
single-node latency.

Use PostgreSQL/MySQL for simpler single-region SQL deployments. Use Redis for
cache-like state. Use DynamoDB or Cosmos DB when your cloud platform is AWS or
Azure and you prefer their native NoSQL services.

## Table model

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

## Basic usage

```rust
use google_cloud_spanner::client::ClientConfig;
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_spanner::SpannerBackend;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}
```

## Diff and merge

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_spanner::SpannerBackend;

async fn run(backend: SpannerBackend) -> Result<(), Box<dyn std::error::Error>> {
    let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
    let base = prolly.batch(&prolly.create(), vec![
        Mutation::Upsert { key: b"account/1".to_vec(), val: b"active".to_vec() },
        Mutation::Upsert { key: b"account/2".to_vec(), val: b"active".to_vec() },
    ]).await?;
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
    Ok(())
}
```

## Operational notes

- The adapter does not create tables. Apply DDL before startup.
- Strict commits validate named-root preconditions and apply node and root
  mutations in one Spanner read-write transaction.
- `batch_put_nodes` is applied as Spanner mutations.
- There is no adapter-level key prefix. Use distinct named-root prefixes for
  tenants or environments, and isolate databases when you need full physical
  separation.
- Node garbage collection should be coordinated at the application layer after
  deciding which named roots to retain.

## Running the example

From the standalone repository root:

```bash
export PROLLY_STORE_SPANNER_DATABASE=projects/<project>/instances/<instance>/databases/<database>
export PROLLY_STORE_SPANNER_AUTH=1
cargo run --manifest-path stores/prolly-store-spanner/Cargo.toml --example basic_usage
```

The example writes a base tree, diffs and merges branches, resolves a conflict,
publishes a unique named root, and loads it back.

## Testing

The integration test runs when `PROLLY_STORE_SPANNER_DATABASE` is set. Set
`PROLLY_STORE_SPANNER_AUTH=1` to use application-default authentication; omit it
when the client environment already supplies the intended emulator or channel
configuration:

```bash
cargo test --manifest-path stores/prolly-store-spanner/Cargo.toml
```

Use a dedicated test database or distinct named-root prefix. The adapter does
not provide a backend-wide key prefix or cleanup helper.

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for the
async map, transaction, diff, and merge APIs used with this backend.
