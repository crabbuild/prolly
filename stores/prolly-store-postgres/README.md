# prolly-store-postgres

PostgreSQL-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` using `sqlx::PgPool`. Use it through
`RemoteProllyStore` and `AsyncProlly` when you want Prolly tree nodes, traversal
hints, and named root manifests in PostgreSQL.

## Installation

```toml
[dependencies]
prolly-map = "0.4"
prolly-store-postgres = "0.2.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## When to use it

Use this adapter when your application already depends on PostgreSQL and you
want durable, transactional storage for versioned maps. It is a good fit for
server-side systems that need normal SQL backup/restore operations, managed
Postgres reliability, and atomic named-root updates.

Prefer this adapter over Redis when named roots are durable business state. Use
object-store or NoSQL adapters when the node volume is very large or when your
deployment is already centered on those services.

## Data model

`initialize_schema` creates three tables:

- `prolly_nodes(cid BYTEA PRIMARY KEY, node BYTEA NOT NULL)`
- `prolly_hints(namespace BYTEA, key BYTEA, value BYTEA)`
- `prolly_roots(name BYTEA PRIMARY KEY, manifest BYTEA NOT NULL)`

Nodes are content addressed by CID. Root manifests give stable names to immutable
tree handles, such as `main`, `tenant/42/head`, or `sync/job/abc/checkpoint`.

## Setup

Run PostgreSQL locally:

```bash
docker run --rm \
  -e POSTGRES_DB=prolly \
  -e POSTGRES_USER=prolly \
  -e POSTGRES_PASSWORD=prolly \
  -p 55432:5432 \
  postgres:16-alpine
```

Or use the Prolly service compose file from the Prolly repo root:

```bash
docker compose -p prolly-store-services -f docker-compose.store-services.yml up -d postgres
```

Set the connection URL:

```bash
export PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly
```

The adapter can create its own tables:

```rust
# async fn run() -> Result<(), sqlx::Error> {
let backend = prolly_store_postgres::PostgresBackend::connect(
    "postgres://prolly:prolly@127.0.0.1:55432/prolly",
)
.await?;
backend.initialize_schema().await?;
# Ok(())
# }
```

## Basic usage

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_postgres::PostgresBackend;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let backend = PostgresBackend::connect(
    "postgres://prolly:prolly@127.0.0.1:55432/prolly",
)
.await?;
backend.initialize_schema().await?;

let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
let tree = prolly
    .batch(
        &prolly.create(),
        vec![
            Mutation::Upsert {
                key: b"user/1".to_vec(),
                val: b"Ada".to_vec(),
            },
            Mutation::Upsert {
                key: b"user/2".to_vec(),
                val: b"Grace".to_vec(),
            },
        ],
    )
    .await?;

prolly.publish_named_root(b"main", &tree).await?;
let loaded = prolly.load_named_root(b"main").await?.expect("main root");
assert_eq!(
    prolly.get(&loaded, b"user/1").await?,
    Some(b"Ada".to_vec())
);
# Ok(())
# }
```

## Diff, merge, and conflict resolution

Each update returns a new immutable `Tree`. Old and new trees share unchanged
subtrees, so diffs and merges only need to inspect changed branches:

```rust
# use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
# use prolly_store_postgres::PostgresBackend;
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
# let backend = PostgresBackend::connect("postgres://prolly:prolly@127.0.0.1:55432/prolly").await?;
# backend.initialize_schema().await?;
# let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
# let base = prolly.batch(&prolly.create(), vec![
#     Mutation::Upsert { key: b"user/1".to_vec(), val: b"Ada".to_vec() },
#     Mutation::Upsert { key: b"user/2".to_vec(), val: b"Grace".to_vec() },
# ]).await?;
let left = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"user/1".to_vec(),
            val: b"Ada Lovelace".to_vec(),
        }],
    )
    .await?;
let right = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"user/2".to_vec(),
            val: b"Grace Hopper".to_vec(),
        }],
    )
    .await?;

let diffs = prolly.diff(&base, &left).await?;
assert_eq!(diffs.len(), 1);

let merged = prolly.merge(&base, &left, &right, None).await?;
assert_eq!(
    prolly.get(&merged, b"user/2").await?,
    Some(b"Grace Hopper".to_vec())
);
# Ok(())
# }
```

## Operational notes

- `initialize_schema` is idempotent and safe to run during startup.
- Strict commits lock the roots table, validate named-root preconditions, and
  apply node and root writes in one PostgreSQL transaction.
- Node rows are content-addressed and can be shared by many named roots.
- Removing a named root does not immediately delete unreachable nodes. Use a
  higher-level retention/GC flow before pruning nodes.
- Keep PostgreSQL connection pooling aligned with your app concurrency. The
  adapter uses the `PgPool` you provide.

## Running the example

From the standalone repository root:

```bash
export PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly
cargo run --manifest-path stores/prolly-store-postgres/Cargo.toml --example basic_usage
```

The example initializes schema, writes a base tree, computes diffs, merges
branches, resolves a conflict, publishes a named root, and loads it back.

## Testing

The integration test runs when `PROLLY_STORE_POSTGRES_URL` is set and returns
without connecting otherwise:

```bash
export PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly
cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml
```

Run it against a disposable database or schema. The adapter tables are shared by
every client using that database.

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for the
async map, transaction, diff, and merge APIs used with this backend.
