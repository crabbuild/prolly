# Prolly Ruby Binding

This gem contains the Ruby UniFFI binding for the Rust `prolly-bindings`
facade. The generated module is `Prolly` and the public loader is
`require "prolly"`.

See `COOKBOOK.md` for Ruby application patterns covering SQLite-backed indexes,
prefix queries, futures, merge callbacks, large values, and custom stores.

The smoke test covers memory, file, SQLite, SQLite-in-memory, generated wire
helpers, paged range/diff/conflict inspection, typed structural diff cursor
resume, merge/named-root flows, Rust bulk-build, sorted bulk-build,
append-batch, parallel batch, batch/append/parallel batch execution statistics, ordered boundary helpers, range-after/cursor resumption with cursor helpers, reverse and prefix-reverse pages, cursor-resumed diffs,
cursor windows,
host `Prolly::MergeResolverCallback` custom resolvers for full-tree,
range-limited, and prefix-limited merges, merge policy registries with named
and Ruby callback rules, typed merge explanation traces with JSON trace
compatibility, `Prolly::HostStoreCallback` custom stores,
operational APIs, sync/GC, portable snapshot bundle export/import with
canonical bytes, digests, summaries, and self-contained verification, retained named-root GC with retention policy helpers, blob
stores, large-value helpers, key helpers for prefix bounds and segment
encoding/decoding plus composite key construction, value-ref stored-byte helpers, blob-ref byte validation, blob GC, CRDT
merge presets, single-key, multi-key, range, cursor-page, diff-page, and prefix proofs with compact path-node export/import, canonical bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification,
`Prolly::CrdtResolverCallback` custom resolvers, timestamped
value envelopes, multi-value set helpers, tombstone
envelopes, tombstone upsert, tombstone compaction, mutation constructors, changed-span constructors, merge/CRDT resolution helpers, built-in resolver helper functions, versioned-value schema
guards, encoding helpers, and tree/large-value/parallel config constructors
through the generated Ruby API. `Prolly::AsyncEngine` provides dependency-free `Future` wrappers for
the generated engine surface, and `Prolly::AsyncBlobStore` wraps blob-store
methods. Named-root flows include manifest metadata listing. The async wrappers cover create/read/write, range/diff, merge,
named-root, typed stats/debug/cache, hint, GC/sync, large-value, and blob-store flows.

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

## Source Tree Layout

The Ruby binding wraps the generated FFI surface with Ruby-friendly entrypoints,
small async helpers, and executable examples.

Important files:

- `lib/prolly.rb` is the public require path.
- `lib/prolly/generated/prolly.rb` is the generated FFI layer.
- `examples/*.rb` contains standalone scenario programs. Each scenario sets its
  local load path and includes the helper code it needs.
- `test/prolly_smoke_test.rb` covers the generated API and wrapper behavior.
- `prolly.gemspec`, `Gemfile`, and `Rakefile` define local gem development.

## Running Examples

Install dependencies and build the native Rust facade:

```sh
bundle install
cargo build -p prolly-bindings
```

Run one scenario:

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  bundle exec ruby crates/prolly/bindings/ruby/examples/local_first_state.rb
```

Run all scenarios:

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  bundle exec ruby crates/prolly/bindings/ruby/examples/cookbook_scenarios.rb
```

Use `.so` on Linux and `.dll` on Windows. The run-all file launches each
scenario separately so the scenario files remain self-contained and readable.

## API Style

Ruby callers should pass byte strings for keys and values. The examples use
`"value".b` to make encoding explicit. Keep application codecs near the domain
model and avoid implicit encoding conversions in tree operations.

Use memory engines for tests and scripts. Use SQLite or file-backed stores when
roots must survive process restarts. Use blob stores for large documents, prompt
transcripts, files, retrieval chunks, and generated artifacts.

## Futures And Concurrency

`Prolly::AsyncEngine` and `Prolly::AsyncBlobStore` provide lightweight Future
wrappers without imposing a framework. They are useful when an application wants
to schedule work while keeping a familiar Ruby API. They do not change root
consistency, merge behavior, or CAS semantics. Keep publication and merge steps
visible in application code.

## Merge And Callback Guidance

Built-in resolver names cover common policies. Ruby callback resolvers are best
for value formats with application semantics, such as timestamp envelopes,
tombstones, counters, or append-only log records. Keep callbacks deterministic
and fast. Avoid network calls, wall-clock reads, and global mutable state inside
resolver callbacks.

Host store callbacks are powerful but should be treated as a storage boundary.
CAS methods must be implemented with real compare-and-swap behavior if multiple
workers can publish the same root name.

## Large Values And GC

Large-value helpers separate small indexable keys from large payloads. Publish
roots before considering old blobs unreachable. Use named-root retention helpers
to retain current heads, checkpoints, or audit roots before sweeping nodes or
blobs.

The filesystem and document chunk scenarios show common Ruby use cases:
application metadata stays in prolly leaves, while large text or file content is
stored as blob-backed values.

## Testing Strategy

Run the smoke test after rebuilding the native library:

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  bundle exec ruby -Icrates/prolly/bindings/ruby/lib \
  crates/prolly/bindings/ruby/test/prolly_smoke_test.rb
```

Add focused tests for wrapper behavior and Ruby callback semantics. Keep
cross-language byte compatibility in generated fixture tests.

## Packaging Notes

The gem should declare the `ffi` dependency and document how native libraries
are found. Source-tree development uses `PROLLY_BINDINGS_LIBRARY`; released gems
should rely on packaged native artifacts or a documented install-time build
process. Keep generated FFI code and native exports in lockstep.

## Troubleshooting

- `cannot load such file -- ffi` means Bundler dependencies are not installed.
- `cannot load such file -- prolly` means the gem is not installed and the local
  `lib` path is not on `$LOAD_PATH`.
- Native load errors usually mean `PROLLY_BINDINGS_LIBRARY` points to the wrong
  file for the current platform.
- Ruby string encoding surprises usually come from missing `.b` on keys or
  values. Treat prolly keys and values as binary strings.
