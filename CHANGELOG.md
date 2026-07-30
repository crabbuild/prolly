# Changelog

## 0.6.0 — 2026-07-29

This is a hard-cutover release. It does not provide compatibility readers,
dual publication, migration shims, or suffix-versioned secondary-index APIs.

### Added

- A canonical `IndexedCollectionState` root with one CAS visibility boundary
  for source data, secondary indexes, descriptors, retention, and pins.
- Finite mutation, query, maintenance, spill, and transfer budgets.
- Snapshot-bound forward and reverse index pages, bounded source-record joins,
  durable pins, retention, health checks, bundle transfer, and verification.
- Stable redacted index error codes and retry advice across portable bindings.
- Production indexed-store qualification for the SQLite adapter.

### Changed

- Secondary indexes now use the canonical hard-cutover state architecture.
- Indexed writes update cardinalities and canonical state through local deltas.
- The minimum supported Rust version is now 1.89 for the core and most
  adapters, 1.90 for Turso, and 1.91.1 for DynamoDB's pinned AWS graph.
- `FileNodeStore`, memory, PGlite, redb, RocksDB, and SlateDB are explicitly
  verification-profile stores for indexed collections.
- Store adapters depend on `prolly-map 0.6.0`.

### Fixed

- Releasing a durable snapshot pin no longer leaks its pin identifier.
- Bounded benchmark queries use finite pages at large configured scales.
- Release CI respects available worker width, uses valid benchmark sample
  counts, and builds UniFFI where JVM parity tests load it.

### Release qualification

The release gates cover formatting, Clippy, all Rust targets, documentation,
secondary-index fault and concurrency suites, portable API inventory, binding
parity, SQLite production-store conformance, AddressSanitizer, packaged-crate
verification, and bounded benchmark smoke.
