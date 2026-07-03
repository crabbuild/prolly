# Prolly Kotlin Binding

This package contains the Kotlin/JVM UniFFI binding for the Rust
`prolly-bindings` facade.

See `COOKBOOK.md` for Kotlin application patterns covering SQLite-backed
indexes, prefix queries, coroutine wrappers, merge callbacks, large values, and
custom stores.

The generated source lives in
`src/main/kotlin/build/crab/prolly/generated/prolly.kt` and uses package
`build.crab.prolly`. Compiled native libraries are built by Cargo or release
CI and are not checked in. The generated surface includes range-after/cursor
resumption with cursor helpers, cursor-resumed diffs, range/diff pages, paged three-way conflict inspection, Rust
bulk-build, sorted bulk-build, append-batch, parallel batch, batch execution statistics, `MergeResolverCallback` custom
merge resolvers, merge policy registries with named and callback rules,
named-root manifest metadata listing, named-root retention policy helpers and GC,
`ProllyBlobStore`, large-value helpers, value-ref inspection,
value-ref stored-byte helpers, blob-ref byte validation, blob reachability, explicit blob GC, blob-store GC, CRDT
merge presets, store-independent single-key, multi-key, range, cursor-page, diff-page, and prefix proofs with compact path-node
export/import, canonical bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification, `CrdtResolverCallback` custom resolvers, timestamped value
envelopes, multi-value set helpers, tombstone
envelopes, tombstone upsert, tombstone compaction, versioned-value schema
match/require guards, prefix bounds, segment
encoding/decoding, composite key construction, numeric key helpers, boundary checks, and `HostStoreCallback`
custom stores. Hint helpers include exact-key, prefix, and range changed-span
constructors. Tests cover memory, file, SQLite, SQLite-in-memory, and
callback-backed host-store engine paths through the generated Kotlin API.
`AsyncProllyEngine` and `AsyncProllyBlobStore` expose suspend wrappers for
create/read/write, range/diff, merge, named-root, stats/cache, hint, GC/sync,
large-value, and blob-store methods.

Local smoke test:

```sh
cargo build -p prolly-bindings
mvn -f crates/prolly/bindings/kotlin/pom.xml test
```

The tests call `ProllyNative.useLocalDebugLibrary()` to point UniFFI/JNA at the
local Cargo debug library.
