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

## Source Tree Layout

The Go binding is intentionally small at the package boundary. The public
module lives at `build.crab/prolly-go`, while the example programs live under
`examples/<scenario>/main.go`. Each example is self-contained: opening a single
scenario file shows the imports, setup code, mutations, validation, and output
for that workflow. The run-all launcher in `examples/cookbook_scenarios` starts
those scenario programs as separate `go run` targets so it stays an
orchestrator, not another combined implementation.

Important files:

- `prolly.go` contains the synchronous binding wrapper and value types.
- `context.go` contains context-aware variants for common operations.
- `callbacks.go` exposes Go callback adapters for host stores and resolvers.
- `prolly_test.go` covers parity and fixture behavior for the Go facade.
- `COOKBOOK.md` explains when to use each scenario.

## Running Examples

Run one scenario while iterating:

```sh
cargo build -p prolly-bindings
(cd crates/prolly/bindings/go && go run ./examples/local_first_state)
```

Run every scenario:

```sh
(cd crates/prolly/bindings/go && go run ./examples/cookbook_scenarios)
```

The scenarios cover basic maps, batch build, local-first state, resolver
policies, CRDT helpers, conversation memory, agent event logs, background
compaction, deterministic RAG snapshots, document chunk indexes, vector sidecar
filtering, provenance values, materialized views, filesystem snapshots, and
durable SQLite roots.

## Development Workflow

Use the Go binding when the application wants explicit resource lifetimes,
native performance, and simple deployment into a Go service. Engines and blob
stores implement `Close`, so production code should normally `defer Close()`
right after construction. Memory engines are best for tests and request-local
work. File and SQLite engines are better for process restarts, CLI tools, and
local-first state that must survive crashes.

The binding accepts and returns `[]byte` for keys and values. Keep key encoders
small and deterministic. Prefer prefix layouts such as
`tenant/<tenant>/entity/<id>` or `index/<name>/<term>/<id>` so range scans,
prefix pages, and prefix-limited merges stay cheap and predictable.

## Error Handling

Most methods return `(value, error)` or `(value, ok, error)`. Treat `ok == false`
as an absent key, not an error. Treat an error as a storage, validation, callback,
or native boundary failure. The examples use small `must` helpers only because
they are executable documentation; application code should usually return errors
with domain context.

Context-aware methods are available when a request can be canceled or timed out.
Use those wrappers around HTTP handlers, background jobs, and agent workflows
that may be interrupted. A canceled context should stop new work at the Go layer;
it is not a substitute for carefully choosing transaction and merge boundaries.

## Merge And Conflict Patterns

Built-in resolver names are convenient for predictable data classes:
`prefer_left`, `prefer_right`, `delete_wins`, and `update_wins`. Custom Go
callback resolvers are better when a value format has domain-specific rules,
such as timestamps, counters, tombstones, append-only logs, or typed JSON
records. Keep resolver callbacks deterministic. They should not read clocks,
randomness, remote services, or mutable process state unless that state is part
of the input value being resolved.

For larger trees, prefer range-limited or prefix-limited merge APIs when the
application knows the changed namespace. That keeps conflict inspection focused
and makes merge explanations easier to present in logs or user interfaces.

## Large Values And Blobs

Large values should be stored through a `BlobStore` with a deliberate inline
threshold. Small values can remain inline in prolly leaves; large documents,
file contents, chunk text, and model artifacts should move to blob storage.
After replacing or deleting large values, run blob GC from a known root set. The
file and filesystem snapshot examples show the intended lifecycle: put the large
value, inspect the value ref, publish the root, and sweep unreachable blobs only
after the retained roots are known.

## Testing And CI

Use `go test ./...` for the binding wrapper and examples that compile as Go
packages. Add scenario-specific checks as ordinary `go test` tests when a bug fix
needs a durable assertion. For CI, build the Rust native library first and set the
library search path consistently for the runner. Keep tests deterministic by
using memory stores unless the behavior under test specifically requires file or
SQLite persistence.

## Packaging Notes

The source tree is wired for local development against `target/debug`. A release
package should ship platform-specific native artifacts built by CI and loaded by
the Go wrapper without relying on a developer checkout. Document the supported
triples, the expected library filename, and the minimum Rust facade version for
each release.

## Troubleshooting

- `dlopen` or missing-library errors usually mean `cargo build -p prolly-bindings`
  has not run for the current platform or the library path does not include
  `target/debug`.
- Unexpected merge results usually mean the resolver name does not match the
  value semantics. Reproduce with a small base, left, and right tree before
  debugging a full application tree.
- Empty range scans usually mean the end bound is wrong. For prefix-style keys,
  use the prefix helper APIs where available instead of inventing byte math.
- SQLite examples should use temporary directories in tests and explicit paths
  in applications. Avoid sharing one SQLite file between unrelated processes
  unless the application owns the concurrency model.
