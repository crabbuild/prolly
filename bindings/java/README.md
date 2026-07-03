# Prolly Java Binding

This package is a Java-friendly facade over the generated Kotlin/JVM UniFFI
binding in `crates/prolly/bindings/kotlin`.

See `COOKBOOK.md` for Java application patterns covering SQLite-backed indexes,
prefix queries, `CompletableFuture` wrappers, merge callbacks, large values,
and custom stores.

The public Java package is `build.crab.prolly`. It exposes `byte[]`,
`Optional<byte[]>`, and Java collections while delegating all tree behavior to
the Rust `prolly-bindings` native library through the Kotlin/JVM artifact. The
facade includes range-after/cursor resumption with cursor helpers, cursor-resumed diffs, range/diff pages, paged
three-way conflict inspection, Rust bulk-build, sorted bulk-build,
append-batch, parallel batch, batch execution statistics, `MergeResolverCallback` custom merge resolvers, merge policy
registries with named and callback rules, named-root manifest metadata listing,
named-root retention policy helpers and GC, memory/file blob
stores, large-value helpers, value-ref inspection and stored-byte helpers, blob-ref
byte validation, blob GC
wrappers, store-independent single-key, multi-key, range, cursor-page, diff-page, and prefix proofs with compact path-node export/import, canonical bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification,
CRDT merge presets, timestamped value envelopes, multi-value set
helpers, `CrdtResolverCallback` custom resolvers, tombstone envelopes,
tombstone upsert, and tombstone compaction without exposing Kotlin unsigned
types, plus versioned-value byte schema match/require guards. Key helpers include prefix ends/ranges, numeric key encoders, segment
encoding/decoding, composite key construction, debug rendering, and boundary checks. It also exposes Java
`HostStore` custom stores over the generated
Kotlin/JVM callback surface. The shared JVM tests cover memory, file, SQLite,
SQLite-in-memory, and callback-backed host-store engine paths. `AsyncProlly` and
`AsyncBlobStore` expose `CompletableFuture` wrappers for create/read/write,
range/diff, merge, named-root, stats/cache, hint, GC/sync, large-value, and
blob-store methods. Hint helpers include exact-key, prefix, and range
changed-span constructors.

Local smoke test:

```sh
cargo build -p prolly-bindings
mvn -f crates/prolly/bindings/pom.xml test
```
