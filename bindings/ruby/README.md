# Prolly Ruby Binding

This gem contains the Ruby UniFFI binding for the Rust `prolly-bindings`
facade. The generated module is `Prolly` and the public loader is
`require "prolly"`.

See `COOKBOOK.md` for Ruby application patterns covering SQLite-backed indexes,
prefix queries, futures, merge callbacks, large values, and custom stores.

The smoke test covers memory, file, SQLite, SQLite-in-memory, generated wire
helpers, paged range/diff/conflict inspection, merge/named-root flows, Rust
bulk-build, sorted bulk-build, append-batch, parallel batch, batch execution statistics, range-after/cursor resumption with cursor helpers, cursor-resumed diffs,
host `Prolly::MergeResolverCallback` custom resolvers for full-tree,
range-limited, and prefix-limited merges, merge policy registries with named
and Ruby callback rules, `Prolly::HostStoreCallback` custom stores,
operational APIs, sync/GC, retained named-root GC with retention policy helpers, blob
stores, large-value helpers, key helpers for prefix bounds and segment
encoding/decoding plus composite key construction, value-ref stored-byte helpers, blob-ref byte validation, blob GC, CRDT
merge presets, single-key, multi-key, range, cursor-page, diff-page, and prefix proofs with compact path-node export/import, canonical bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification,
`Prolly::CrdtResolverCallback` custom resolvers, timestamped
value envelopes, multi-value set helpers, tombstone
envelopes, tombstone upsert, tombstone compaction, changed-span constructors, and versioned-value schema
guards through the generated
Ruby API. `Prolly::AsyncEngine` provides dependency-free `Future` wrappers for
the generated engine surface, and `Prolly::AsyncBlobStore` wraps blob-store
methods. Named-root flows include manifest metadata listing. The async wrappers cover create/read/write, range/diff, merge,
named-root, stats/cache, hint, GC/sync, large-value, and blob-store flows.

Local smoke test:

```sh
cargo build -p prolly-bindings
bundle install
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  bundle exec ruby -Icrates/prolly/bindings/ruby/lib \
  crates/prolly/bindings/ruby/test/prolly_smoke_test.rb
```

Use `.so` on Linux and `.dll` on Windows. Compiled native libraries are built
by release CI and are not checked in.
