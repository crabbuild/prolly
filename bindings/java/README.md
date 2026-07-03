# Prolly Java Binding

This package is a Java-friendly facade over the generated Kotlin/JVM UniFFI
binding in `crates/prolly/bindings/kotlin`.

See `COOKBOOK.md` for Java application patterns covering SQLite-backed indexes,
prefix queries, `CompletableFuture` wrappers, merge callbacks, large values,
and custom stores.

The public Java package is `build.crab.prolly`. It exposes `byte[]`,
`Optional<byte[]>`, and Java collections while delegating all tree behavior to
the Rust `prolly-bindings` native library through the Kotlin/JVM artifact. The
facade includes ordered boundary helpers, range-after/cursor resumption with cursor helpers, reverse and prefix-reverse pages,
cursor windows, cursor-resumed diffs, range/diff pages, typed structural diff cursor resume,
paged three-way conflict inspection, Rust bulk-build, sorted bulk-build,
append-batch, parallel batch, batch/append/parallel batch execution statistics, `MergeResolverCallback` custom merge resolvers, merge policy
registries with named and callback rules, typed merge explanation traces with
JSON trace compatibility, named-root manifest metadata listing,
named-root retention policy helpers and GC, memory/file blob
stores, large-value helpers, value-ref inspection and stored-byte helpers, blob-ref
byte validation, blob GC
wrappers, store-independent single-key, multi-key, range, cursor-page, diff-page, and prefix proofs with compact path-node export/import, canonical bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification,
portable snapshot bundle export/import with canonical bundle bytes, digests, summaries,
and self-contained verification,
CRDT merge presets, timestamped value envelopes, multi-value set
helpers, `CrdtResolverCallback` custom resolvers, tombstone envelopes,
tombstone upsert, and tombstone compaction without exposing Kotlin unsigned
types, mutation constructors, encoding helpers, tree/large-value/parallel
config constructors, plus merge/CRDT resolution helpers, built-in resolver
helper functions, and versioned-value byte schema match/require guards. Key helpers include prefix ends/ranges, numeric key encoders, segment
encoding/decoding, composite key construction, debug rendering, and boundary checks. It also exposes Java
`HostStore` custom stores over the generated
Kotlin/JVM callback surface. The shared JVM tests cover memory, file, SQLite,
SQLite-in-memory, and callback-backed host-store engine paths. `AsyncProlly` and
`AsyncBlobStore` expose `CompletableFuture` wrappers for create/read/write,
range/diff, merge, named-root, typed stats/debug/cache, hint, GC/sync, snapshot bundles, large-value, and
blob-store methods. Hint helpers include exact-key, prefix, and range
changed-span constructors.

Local smoke test:

```sh
cargo build -p prolly-bindings
mvn -f crates/prolly/bindings/pom.xml test
```

## Source Tree Layout

The Java binding is the object-oriented JVM facade for applications that prefer
Java records, `Optional`, `byte[]`, `AutoCloseable`, and `CompletableFuture`
integration. The implementation is layered on top of the Kotlin/JVM generated
binding, but Java users should treat `build.crab.prolly.Prolly` as the primary
entrypoint.

Important files:

- `src/main/java/build/crab/prolly/Prolly.java` is the synchronous engine facade.
- `AsyncProlly.java` wraps engine calls in `CompletableFuture`.
- `BlobStore.java` and `AsyncBlobStore.java` cover large-value storage.
- `HostStoreAdapter.java` adapts Java-owned node stores to the native engine.
- `src/main/java/build/crab/prolly/examples/*.java` contains standalone
  executable scenarios. Each scenario owns its setup and helper code instead
  of delegating to one large scenario module.

## Running Examples

Build the Rust facade and JVM modules first:

```sh
cargo build -p prolly-bindings
mvn -f crates/prolly/bindings/pom.xml -DskipTests compile
```

Run one scenario:

```sh
mvn -f crates/prolly/bindings/java/pom.xml \
  -Dexec.mainClass=build.crab.prolly.examples.LocalFirstState \
  exec:java
```

Run all scenarios:

```sh
mvn -f crates/prolly/bindings/java/pom.xml \
  -Dexec.mainClass=build.crab.prolly.examples.CookbookScenarios \
  exec:java
```

`CookbookScenarios` is only a launcher; the runnable scenario code lives in the
individual example classes.

If Maven resolves an older local `prolly-kotlin` artifact, install the freshly
compiled Kotlin module from the parent reactor:

```sh
mvn -f crates/prolly/bindings/pom.xml -pl kotlin -am -DskipTests install
```

## API Style

The Java API deliberately stays byte-first. Application code should own its key
and value codecs and keep those codecs deterministic. Use small helper methods
for domain keys, such as `userKey(tenant, id)` or
`statusIndexKey(tenant, status, id)`, instead of scattering string formatting
through business logic.

`Prolly.memory()` is ideal for tests and short-lived computations. `Prolly.file`
and `Prolly.sqlite` are better when roots and nodes must survive process
restarts. Every engine and blob store is `AutoCloseable`; use try-with-resources
in production code and in examples.

## Async Usage

`AsyncProlly` does not change the underlying native execution model. It provides
Java-friendly `CompletableFuture` composition for services that already use
future pipelines. Use it to keep application orchestration non-blocking, but
still design tree updates as explicit, deterministic operations. Avoid hiding
large transactional workflows inside unbounded asynchronous chains.

## Merge And Callback Guidance

Use built-in resolver names when they match the value semantics. Use
`MergeResolverCallback` when Java owns a domain-specific conflict rule. Callback
resolvers should be deterministic, quick, and side-effect free. If a resolver
needs to parse JSON, protobuf, or another envelope, validate malformed values and
return a clear failure rather than silently choosing one side.

For host stores, `HostStoreAdapter` is the boundary between Java persistence and
the Rust engine. Implement CAS and named-root methods carefully; weak CAS
semantics can make local-first examples appear to work while losing concurrent
updates under load.

## Large Values And Retention

Use blob stores for document bodies, file contents, long prompt contexts, model
outputs, and any value that should not inflate prolly leaves. Publish roots
before considering old blobs reclaimable. Named-root retention helpers let an
application retain current heads, checkpoints, or audit roots while collecting
unreachable data.

## Testing Strategy

Use the parent Maven test command for full JVM parity:

```sh
mvn -f crates/prolly/bindings/pom.xml test
```

Add Java-only tests when validating Java ergonomics, callback adapters, or
`CompletableFuture` behavior. Keep low-level codec and fixture parity in the
shared generated binding tests so Java and Kotlin stay aligned.

## Packaging Notes

Release artifacts must declare which native library versions they support.
Source-tree examples load `target/debug/libprolly_bindings.*`, but published JVM
packages should ship or resolve platform artifacts through the release process.
Document the supported operating systems, CPU architectures, and library search
rules for downstream users.

## Troubleshooting

- `UnsatisfiedLinkError` means the native library cannot be found or was built
  for a different platform.
- `NoSuchMethodError` between Java and Kotlin classes usually means Maven is
  using a stale local Kotlin artifact. Rebuild and install the Kotlin module.
- Empty `Optional` results are normal for missing keys. Native exceptions,
  validation failures, and callback failures should be treated as operational
  errors.
- If merge output surprises you, run the resolver scenario with a tiny base,
  left, and right tree before debugging the full application state.
