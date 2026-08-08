# redb Store Adapter Design

## Summary

Add a `prolly-store-redb` crate under `stores/` that provides a synchronous,
persistent, full-featured `prolly-map` backend using `redb` 4.1.0. The adapter
will target Rust 1.89 independently of the root crate, whose minimum supported
Rust version remains 1.81.

The adapter will implement `Store`, `ManifestStore`, `ManifestStoreScan`,
`NodeStoreScan`, and `TransactionalStore`. It will also persist performance
hints and atomically publish nodes together with a hint.

## Crate and Public API

The new crate will live at `stores/prolly-store-redb` and use the package name
`prolly-store-redb` and library name `prolly_store_redb`.

Its primary API will be:

- `RedbStore::open(path)` to open or create a database with default settings.
- `RedbStore::open_with_config(path, config)` to open or create a configured
  database.
- `RedbStoreConfig` with public `cache_size_bytes` and `durability` fields.
- `RedbStoreError` as the contextual adapter error type.
- A public re-export of `redb::Durability` so callers do not need a direct
  `redb` dependency to configure commits.

The default configuration will use redb's 1 GiB cache default and
`Durability::Immediate`. Every write transaction, including initialization,
will use the selected durability.

Opening the store will initialize all tables in a single write transaction.
This ensures subsequent read transactions never fail because a table has not
yet been created.

## Storage Layout

The database will contain three typed redb tables:

- `prolly_nodes`: raw CID bytes to serialized node bytes.
- `prolly_roots`: arbitrary root names to encoded `RootManifest` bytes.
- `prolly_hints`: `(namespace, key)` byte pairs to hint bytes.

Separate tables keep node and root scans isolated and avoid prefix encoding,
namespace collisions, and metadata filtering. All three tables remain writable
inside one redb transaction, allowing atomic publication and strict
transactions.

## Store Operations

Point reads will open a redb read transaction and copy the returned guarded
value into an owned `Vec<u8>`. Point writes and deletes will open one write
transaction, apply the configured durability, perform the operation, and
commit.

Batch reads will use one read transaction for all requested keys. The ordered
methods will preserve input order, duplicate keys, and missing entries. The
map-returning method will deduplicate found keys according to the `Store`
contract. The adapter will report that it prefers batch reads because one redb
snapshot serves the full batch.

`batch`, `batch_put`, and `batch_put_with_hint` will each use one write
transaction so their changes are atomic. The adapter will report hint support
and implement `get_hint` and `put_hint`. It will not opt into rightmost-path
hints initially; that preference requires workload measurements rather than
only a correctness-capable implementation.

## Manifests and Transactions

Manifest encoding and decoding will use `RootManifest::to_bytes` and
`RootManifest::from_bytes`.

Root compare-and-swap will run entirely in one redb write transaction:

1. Read and decode the current root.
2. Compare it with the expected manifest.
3. Return `ManifestUpdate::Conflict` without committing if it differs.
4. Insert or delete the root and commit when it matches.

Redb permits only one active writer, so concurrent compare-and-swap operations
are serialized by the database without an adapter-level mutex.

`commit_transaction` will open the node and root tables inside one write
transaction. It will first validate all root conditions. A failed condition
will return `TransactionUpdate::Conflict` and drop the uncommitted transaction.
When all conditions match, it will apply every node and root write and commit
once. This gives strict atomicity across immutable nodes and mutable roots.

## Scanning and Validation

`NodeStoreScan::list_node_cids` will iterate the node table only. Every key must
be exactly 32 bytes; malformed keys will return an adapter error. Results will
be sorted by raw CID bytes.

`ManifestStoreScan::list_roots` will iterate the root table only, decode every
manifest, and return roots sorted by raw name bytes. Corrupt manifests will
produce an error rather than being skipped.

## Error Handling

`RedbStoreError` will contain an operation-specific message and retain a redb
source error when the failure originates in redb. Manifest serialization,
manifest decoding, poisoned data, and invalid CID lengths will be represented
with contextual messages even when there is no redb source error.

`TransactionalStore::commit_transaction` will wrap adapter failures in
`prolly::Error::Store`, matching the existing full adapters.

## Tests and Documentation

Integration tests will use isolated temporary database files and cover:

- The shared `Store` contract.
- The shared manifest and manifest-scan contract.
- The shared node-scan and garbage-collection contract.
- The strict indexed-map transaction contract.
- Named-root and node persistence across reopen.
- Hint persistence and atomic node-plus-hint publication.
- Configured non-durable transactions at the adapter API boundary.

The crate will include a README and a basic usage example. Documentation will
explain the single-file storage model, Rust 1.89 requirement, durability and
cache settings, transaction behavior, and the command for running the adapter's
test suite.

## Non-Goals

- Changing the root crate's Rust 1.81 minimum.
- Adding asynchronous traits around redb's synchronous API.
- Exposing redb savepoints, compaction, repair callbacks, or cache metrics in
  the first version.
- Adding benchmarks before a correctness-complete adapter exists.
- Opting into rightmost-path hints without workload measurements.
