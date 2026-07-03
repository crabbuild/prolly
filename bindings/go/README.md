# Prolly Go Binding

This package is the first Go binding for the Rust `prolly-bindings` facade.
It uses the design's cgo fallback path and calls the UniFFI-exported Rust ABI.

See `COOKBOOK.md` for Go application patterns covering SQLite-backed indexes,
prefix queries, paging, context-aware calls, merge callbacks, large values, and
custom stores.

Current surface:

- memory engine;
- file, SQLite, and SQLite-in-memory Rust-backed engines;
- create, put, get, delete, range, batch, batch-with-stats, parallel batch,
  Rust bulk-build, sorted bulk-build, append-batch, append-batch-with-stats,
  and `get_many`;
- eager range, range-after/cursor resumption with cursor constructors,
  cursor-resumed diffs, paged range/diff, and paged three-way conflict
  inspection;
- three-way merge, merge explanations, named roots, named-root manifest
  metadata listing, CAS, retention policy constructors, retention selection,
  and retained named-root store GC;
- built-in and Go callback merge resolvers, including full-tree, range-limited,
  and prefix-limited merge APIs;
- merge policy registries with named built-in rules and Go callback rules;
- Go `HostStore` callbacks for Rust-backed engines over host-owned node bytes,
  hints, node scans, named-root manifests, CAS, GC, and sync;
- stats/debug JSON and text views, cache stats/pinning, metrics reset, and
  changed-span constructors plus optional performance hint smoke paths;
- key helpers for prefix ends/ranges, numeric keys, segment encoding/decoding,
  composite key construction, debug rendering, and boundary checks;
- structural diff pages, node reachability/GC plan/sweep, store GC, retained
  named-root GC, and missing-node sync between memory engines;
- memory/file blob stores, large-value helpers, value-ref inspection, blob
  reachability, blob GC, blob-store GC, value-ref stored-byte helpers, and
  blob-ref byte validation;
- store-independent single-key, shared multi-key, range, cursor-page,
  diff-page, and prefix proof generation, compact path-node export/import,
  canonical proof bundle bytes, proof-bundle introspection/routing summaries,
  one-shot proof-bundle verification, HMAC-authenticated proof envelopes,
  one-shot authenticated proof-bundle verification, and proof verification;
- CRDT config presets, Go callback CRDT resolvers, timestamped value envelopes,
  multi-value set helpers, tombstone envelopes, tombstone upsert, and tombstone
  compaction;
- versioned value byte round trips plus schema match/require guard helpers;
- `context.Context` wrappers for create/read/write/range/diff, merge,
  named-root, stats/cache, hint, GC/sync, large-value, and blob-store methods;
- opaque config and tree handles backed by UniFFI record bytes;
- `[]byte` keys and values.

Local smoke test:

```sh
cargo build -p prolly-bindings
(cd crates/prolly/bindings/go && go test ./...)
```

The cgo wrapper links against `target/debug/libprolly_bindings.*` for local
tests. Release packages should replace this with CI-built native artifacts.
