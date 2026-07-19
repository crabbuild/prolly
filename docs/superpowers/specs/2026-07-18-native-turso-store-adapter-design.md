# Native Turso Store Adapter Design

## Goal

Add a native Rust Turso Database adapter for `prolly-map`. The adapter supports
local embedded databases by default and explicit Turso Cloud push/pull sync
behind an optional Cargo feature.

## Scope

The adapter is a standalone crate at `stores/prolly-store-turso`. It targets the
native async `turso` Rust crate and prolly's asynchronous store interfaces. It
does not hide an async runtime, implement a synchronous `Store`, automatically
synchronize, or provide direct stateless access to a remote libSQL database.

The initial adapter uses Turso `0.7` with default features disabled. Its own
`sync` feature enables `turso/sync`. Turso 0.7's required dependency graph has
a Rust 1.88 floor, so the adapter declares Rust 1.88 even though the core
`prolly-map` crate retains Rust 1.81. The adapter must compile in both default
and sync-enabled configurations.

## Architecture

`TursoBackend` implements prolly's `RemoteStoreBackend`. `TursoStore` is a type
alias for `RemoteProllyStore<TursoBackend>`, reusing the core adapter's async
store, manifest, scan, transaction, manifest serialization, and CID-validation
logic.

The backend owns a cloneable database handle rather than a long-lived SQL
connection. Every operation opens its own connection. This isolates SQL
transactions, permits independent concurrent operations, and avoids sharing an
active transaction with unrelated calls.

The backend exposes:

- `open(path)` for a local native Turso database;
- `from_local_database(database)` for caller-configured local builders;
- `open_synced(path, remote_url, auth_token)` behind `sync`;
- `from_synced_database(database)` behind `sync` for advanced Turso sync
  configuration such as dynamic authentication or encryption;
- `is_synced()`, plus explicit `push()` and `pull()` behind `sync`.

`pull()` returns Turso's `bool` indicating whether remote changes were applied.
Calling `push()` or `pull()` on a local backend returns a typed `NotSynced`
error. Normal store reads and writes never perform network synchronization.

## Data Model

Schema initialization is idempotent and occurs in every constructor:

```sql
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid BLOB PRIMARY KEY,
  node BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace BLOB NOT NULL,
  key BLOB NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name BLOB PRIMARY KEY,
  manifest BLOB NOT NULL
);
```

Node keys and values, hint components, root names, and serialized manifests stay
binary. Node scans and root scans use raw byte ordering for deterministic
results.

## Transactions and Concurrency

Single node, hint, and root writes use upserts. Batch node mutations and
node-plus-hint writes use one SQL transaction. Root compare-and-swap and the
combined prolly transaction use an immediate transaction so no competing writer
can change a root between the comparison and update.

A failed root condition rolls the transaction back before returning a conflict;
no node or root writes from that transaction become visible. The backend reports
transaction support to prolly.

Transaction and compare-and-swap guarantees are local to one database or synced
replica. Cloud synchronization is not a distributed compare-and-swap: separate
offline replicas can each satisfy the same local root condition. Multi-writer
applications must serialize ownership or detect, rebase, and resolve divergent
roots around explicit pushes and pulls.

The adapter does not opt applications into experimental `BEGIN CONCURRENT` or a
journal mode. Callers that need experimental Turso settings can supply a
pre-built database through the `from_*_database` constructors.

## Error Handling

`TursoStoreError` distinguishes invalid non-UTF-8 database paths, native
`turso::Error` failures, and use of sync operations on a local database. It
implements `Error + Send + Sync + 'static`. Paths that Turso's string-based
builder cannot represent are rejected rather than lossily rewritten.
The outer `RemoteProllyStore` continues to distinguish backend failures from
invalid manifests and CID mismatches.

Query helpers reject missing result rows only where a row is required. Point
reads return `None` for missing keys. Row conversion failures propagate as
native Turso errors.

## Testing

Default-feature tests use temporary local database files and cover:

- the shared remote backend conformance contract;
- the shared transactional backend contract;
- persistence after dropping and reopening a database;
- idempotent schema creation;
- local sync misuse when the `sync` feature is compiled.

The sync feature has an environment-gated integration test using
`TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`. When credentials are absent it
exits without network access; when present it creates a unique local replica,
writes through one adapter, explicitly pushes, pulls into a second replica, and
verifies the named root and value crossed the replica boundary.

Verification includes formatting, tests and Clippy for both the default and
`sync` feature sets, plus documentation tests through the full crate tests.

## Documentation

The adapter README includes local and synced usage, advanced-builder usage,
feature flags, explicit sync semantics, schema details, verification commands,
and Turso's current beta warning with a recommendation to keep backups for
production data.

