# Prolly Go Binding

This package is the first Go binding for the Rust `prolly-bindings` facade.
It uses the design's cgo fallback path and calls the UniFFI-exported Rust ABI.

See `COOKBOOK.md` for Go application patterns covering SQLite-backed indexes,
prefix queries, paging, context-aware calls, merge callbacks, large values, and
custom stores.

Current surface:

- memory engine;
- file, SQLite, and SQLite-in-memory Rust-backed engines;
- create, put, get, delete, range, prefix, batch, batch-with-stats, parallel batch,
  parallel-batch-with-stats, Rust bulk-build, sorted bulk-build, append-batch, append-batch-with-stats,
  and `get_many`;
- eager range, prefix scans/pages, range-after/cursor resumption with cursor constructors, reverse and prefix-reverse pages,
  ordered boundary helpers, cursor windows, cursor-resumed diffs, paged range/diff, and paged three-way conflict
  inspection;
- three-way merge, merge explanations with typed trace events plus JSON trace
  compatibility, named roots, named-root manifest
  metadata listing, CAS, retention policy constructors, retention selection,
  retained named-root store GC, and mutation helper constructors;
- built-in and Go callback merge resolvers, including full-tree, range-limited,
  and prefix-limited merge APIs, plus merge/CRDT resolution constructors and
  built-in resolver helper functions;
- merge policy registries with named built-in rules and Go callback rules;
- Go `HostStore` callbacks for Rust-backed engines over host-owned node bytes,
  hints, node scans, named-root manifests, CAS, GC, and sync;
- typed stats/debug records, stats/debug JSON and text views, cache stats/pinning, metrics reset, and
  changed-span constructors plus optional performance hint smoke paths;
- key helpers for prefix ends/ranges, numeric keys, segment encoding/decoding,
  composite key construction, debug rendering, and boundary checks;
- structural diff pages with typed cursor resume plus JSON cursor compatibility,
  node reachability/GC plan/sweep, store GC, retained named-root GC, and
  missing-node sync plus portable snapshot bundle export/import between
  engines with canonical snapshot bundle bytes, digests, summaries, and self-contained
  verification;
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
- `context.Context` wrappers for create/read/write/range/cursor-window/diff, merge,
  named-root, stats/cache, hint, GC/sync, large-value, and blob-store methods;
- opaque config and tree handles backed by UniFFI record bytes, with encoding
  helpers plus tree, large-value, and parallel config constructors;
- `[]byte` keys and values.

Local smoke test:

```sh
cargo build -p prolly-bindings
(cd crates/prolly/bindings/go && go test ./...)
```

The cgo wrapper links against `target/debug/libprolly_bindings.*` for local
tests. Release packages should replace this with CI-built native artifacts.
