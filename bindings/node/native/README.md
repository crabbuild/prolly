# Prolly Node Native Binding

This crate is the first Node-API binding for the Rust `prolly-bindings`
facade. It exposes the memory-engine CRUD/range surface through
`NativeProllyEngine`, plus file, SQLite, and SQLite-in-memory engine
constructors, fixture-backed wire helpers, and P2/P3 smoke coverage for
batch/get-many, Rust bulk-build, sorted bulk-build, append-batch,
range-after/cursor resumption, range and diff pages, three-way merge with
built-in resolvers,
JavaScript custom resolver callbacks for full-tree, range-limited, and
prefix-limited merges, merge policy registries with named and JavaScript
callback rules, JavaScript custom host stores, merge explanations,
paged three-way conflict inspection, store-independent single-key, multi-key,
range, cursor-page, diff-page, and prefix proofs with canonical bundle bytes,
proof-bundle introspection/routing summaries, one-shot proof-bundle verification,
HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification, named-root
publish/load/CAS/retention, and P4 operational APIs for stats/debug views,
cache stats/pinning, engine metrics, optional performance hints, structural
diff pages, node GC plan/sweep, and missing-node sync between memory engines.
It also exposes Rust-backed
`NativeProllyBlobStore` memory/file stores, large-value helpers, value-ref
inspection, blob reachability, explicit blob GC, blob-store GC, CRDT merge
presets, JavaScript custom CRDT resolver callbacks, timestamped value
envelopes, multi-value set helpers, tombstone
envelopes, tombstone upsert, and tombstone compaction. The Node package also
ships `AsyncProllyEngine`, `AsyncMergePolicyRegistry`, and
`AsyncProllyBlobStore`, Promise wrappers over the native engine/store/policy for
create/read/write/range/diff, merge, named-root, stats/cache, hint, GC/sync,
large-value, and blob-store methods.

Build from the Node package:

```sh
npm run build:native
```

The checked-in TypeScript fixture harness remains useful for conformance
inspection. Production Node packages should load the `.node` artifact built
from this crate, and browser packages should use the sibling WASM binding.
