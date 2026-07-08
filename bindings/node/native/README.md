# Prolly Node Native Binding

This crate is the first Node-API binding for the Rust `prolly-bindings`
facade. It exposes the memory-engine CRUD/range surface through
`NativeProllyEngine`.

It includes:

- file, SQLite, and SQLite-in-memory engine constructors;
- fixture-backed wire helpers and batch/get-many coverage;
- bulk-build, append-batch, range cursor, range page, and diff page APIs;
- merge policies, JavaScript resolver callbacks, and custom host stores;
- named-root publish/load/CAS/retention flows;
- stats/debug views, cache controls, metrics, hints, structural diffs, and GC;
- portable proof bundles and HMAC-authenticated proof envelopes.

It also exposes Rust-backed `NativeProllyBlobStore` memory/file stores,
large-value helpers, value refs, blob reachability, blob GC, CRDT helpers, and
tombstone helpers.

The Node package ships `AsyncProllyEngine`, `AsyncMergePolicyRegistry`, and
`AsyncProllyBlobStore` as Promise wrappers over the native engine, store, and
policy APIs.

Build from the Node package:

```sh
npm run build:native
```

The checked-in TypeScript fixture harness remains useful for conformance
inspection. Production Node packages should load the `.node` artifact built
from this crate, and browser packages should use the sibling WASM binding.
