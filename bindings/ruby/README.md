# Prolly Ruby Binding

This gem contains the Ruby UniFFI binding for the Rust `prolly-bindings`
facade. The generated module is `Prolly` and the public loader is
`require "prolly"`.

See `COOKBOOK.md` for Ruby application patterns covering SQLite-backed indexes,
prefix queries, futures, merge callbacks, large values, and custom stores.

The smoke test covers memory, file, SQLite, SQLite-in-memory, generated wire
helpers, and core tree operations.

It also covers:

- root-bound `read_session` point, multi-get, range, diff, and conflict reads;
- paged range, diff, conflict, reverse-page, and cursor-resume flows;
- bulk-build, append-batch, parallel-batch, and execution-stat APIs;
- merge policies, Ruby callbacks, CRDT helpers, and explanation traces;
- named roots, retention policies, sync, node GC, blob stores, and large values;
- key encoders, mutation constructors, versioned values, and hints;
- portable snapshot bundles and store-independent proof bundles.

`Prolly::AsyncEngine` and `Prolly::AsyncBlobStore` provide dependency-free
`Future` wrappers for create/read/write, range/diff, merge, named-root,
stats/debug/cache, hint, GC/sync, large-value, and blob-store flows.

Local smoke test:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
BUNDLE_GEMFILE=bindings/ruby/Gemfile bundle install
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=bindings/ruby/Gemfile \
  bundle exec ruby -Ibindings/ruby/lib \
  bindings/ruby/test/prolly_smoke_test.rb
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
BUNDLE_GEMFILE=bindings/ruby/Gemfile bundle install
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
```

Run one scenario:

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=bindings/ruby/Gemfile \
  bundle exec ruby bindings/ruby/examples/local_first_state.rb
```

Run all scenarios:

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=bindings/ruby/Gemfile \
  bundle exec ruby bindings/ruby/examples/cookbook_scenarios.rb
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

Version-1 remote-store provider gems live under `stores/` and accept
caller-owned SDK clients. The SQLite provider borrows an
`SQLite3::Database`, serializes operations, and leaves connection lifetime to
the application. Verify it from the repository root:

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=bindings/ruby/stores/sqlite/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-sqlite-bundle \
  bundle exec ruby -Ibindings/ruby/stores/sqlite/lib \
  bindings/ruby/stores/sqlite/test/sqlite_store_test.rb
```

The PostgreSQL provider similarly borrows a `PG::Connection` and uses
transaction advisory locks for missing-root CAS safety. Its check is:

```sh
PROLLY_POSTGRES_URL=postgresql://prolly:prolly@127.0.0.1:55432/prolly?sslmode=disable \
  PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=bindings/ruby/stores/postgres/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-postgres-bundle \
  bundle exec ruby -Ibindings/ruby/stores/postgres/lib \
  bindings/ruby/stores/postgres/test/postgres_store_test.rb
```

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
  BUNDLE_GEMFILE=bindings/ruby/Gemfile \
  bundle exec ruby -Ibindings/ruby/lib \
  bindings/ruby/test/prolly_smoke_test.rb
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
