# prolly-store-turso

Native [Turso Database](https://github.com/tursodatabase/turso) storage adapter
for [`prolly-map`](https://crates.io/crates/prolly-map).

The default build embeds Turso locally. Optional Turso Cloud synchronization is
local-first and explicit: prolly reads and writes never perform network I/O;
your application decides when to call `push()` or `pull()`.

## Requirements

- Rust 1.88 or newer. Turso 0.7's required dependency graph sets this floor;
  the core `prolly-map` crate itself continues to support Rust 1.81.
- Tokio or another executor capable of polling Turso and prolly's async APIs.
- The `turso-cloud-sync` feature only when Turso Cloud push/pull is needed.

Turso Database currently describes its native engine as beta. Keep tested
backups for production data and follow Turso's release notes when upgrading.

## Local database

```toml
[dependencies]
prolly-map = "0.3"
prolly-store-turso = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use prolly::{AsyncProlly, Config};
use prolly_store_turso::{TursoBackend, TursoStore};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let backend = TursoBackend::open("app.prolly.turso").await?;
let prolly = AsyncProlly::new(TursoStore::new(backend), Config::default());
let tree = prolly
    .put(&prolly.create(), b"name".to_vec(), b"Ada".to_vec())
    .await?;
prolly.publish_named_root(b"main", &tree).await?;
# Ok(())
# }
```

`open` creates the schema when necessary. Reopening the same path preserves
both immutable nodes and named roots.

Run the checked-in example from the repository root:

```sh
cargo run --manifest-path stores/prolly-store-turso/Cargo.toml \
  --example basic_usage -- ./target/example.prolly.turso
```

## Turso Cloud sync

Enable the adapter feature, which enables `turso/sync`:

```toml
prolly-store-turso = { version = "0.1", features = ["turso-cloud-sync"] }
```

```rust
use prolly::{AsyncProlly, Config};
use prolly_store_turso::{TursoBackend, TursoStore};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let backend = TursoBackend::open_synced(
    "app-replica.db",
    std::env::var("TURSO_DATABASE_URL")?,
    std::env::var("TURSO_AUTH_TOKEN")?,
)
.await?;
let cloud_sync = backend.clone();
let prolly = AsyncProlly::new(TursoStore::new(backend), Config::default());

let tree = prolly
    .put(&prolly.create(), b"name".to_vec(), b"Grace".to_vec())
    .await?;
prolly.publish_named_root(b"main", &tree).await?;

// Network synchronization happens only at these explicit calls.
cloud_sync.push().await?;
let remote_changes_applied = cloud_sync.pull().await?;
println!("remote changes applied: {remote_changes_applied}");
# Ok(())
# }
```

Calling `push()` or `pull()` on a backend created with `open` returns
`TursoStoreError::NotSynced`. Coordinate explicit sync with application writes
and handle Turso `Busy` or conflict errors according to the application's retry
policy.

For dynamic authentication, remote encryption, partial sync, or other Turso
builder settings, construct the database yourself and retain those settings:

```rust
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let database = turso::sync::Builder::new_remote("app-replica.db")
    .with_remote_url(std::env::var("TURSO_DATABASE_URL")?)
    .with_auth_token(std::env::var("TURSO_AUTH_TOKEN")?)
    .build()
    .await?;
let backend = prolly_store_turso::TursoBackend::from_synced_database(database).await?;
# let _ = backend;
# Ok(())
# }
```

For caller-configured local databases, use `TursoBackend::from_local_database`.

## Storage and transaction model

The adapter creates three binary tables:

- `prolly_nodes(cid BLOB PRIMARY KEY, node BLOB NOT NULL)`
- `prolly_hints(namespace BLOB, key BLOB, value BLOB)`
- `prolly_roots(name BLOB PRIMARY KEY, manifest BLOB NOT NULL)`

Each operation uses an independent native Turso connection. Node batches,
node-plus-hint writes, named-root compare-and-swap, and coordinated prolly
transactions use native SQL transactions. A measured local fast path begins
explicit point-upsert publications as deferred transactions, so Turso acquires
the write lock with the first node statement instead of at transaction start.
The SQL statements, atomic node-plus-hint boundary, commit acknowledgement,
durability, and error behavior are unchanged.

Every other current or future publication origin uses the conservative
immediate transaction path. Root compare-and-swap and coordinated commits also
remain immediate so the checked root cannot change before its update is
committed. Unknown publication origins automatically receive this general
fallback. The generic `TursoStore` layer validates node bytes against content
IDs and decodes named-root manifests before the backend fast path runs.

The local-only paired evidence for this override and the universal fallback is
recorded in the repository's
[`node-publication findings`](../../performance-results/node-publication-local-adapters-2026-07-19/findings.md).
Turso Cloud synchronization was disabled for every reported measurement.

These transaction and compare-and-swap guarantees are scoped to one local
database or synced replica. Turso Cloud synchronization does not turn them into
a distributed compare-and-swap across replicas: two offline writers can each
satisfy the same local root condition. Applications with multiple writers must
serialize ownership or detect, rebase, and resolve divergent roots around
explicit `push()` and `pull()` calls.

## Verification

Local tests do not need a service:

```sh
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml
cargo clippy --manifest-path stores/prolly-store-turso/Cargo.toml \
  --all-targets -- -D warnings
```

Compile and test the optional Turso Cloud synchronization surface:

```sh
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml \
  --features turso-cloud-sync
cargo clippy --manifest-path stores/prolly-store-turso/Cargo.toml \
  --all-targets --features turso-cloud-sync -- -D warnings
```

The cloud-sync integration test makes network calls only when both
`TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are set. Otherwise it exits without
contacting Turso Cloud.
