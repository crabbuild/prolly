# Swift Cookbook

Build the Rust facade once before running examples:

```sh
cargo build -p prolly-bindings
```

Run each scenario from the Swift package directory:

```sh
cd crates/prolly/bindings/swift
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-basic-map
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-cookbook-scenarios
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-diff-merge
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-file-blob-store
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-secondary-index
```

Set `PROLLY_BINDINGS_LIBRARY_DIR` if `libprolly_bindings.dylib` is not in the
workspace `target/debug` directory.

Cursor-resumed diffs use the same `RangeCursorRecord` shape as range pages:
`engine.diffFromCursor(base: oldTree, other: newTree, cursor: cursor, end: nil)`.

## Scenarios

- `prolly-basic-map`: immutable snapshots, range scans, pages, batch
  last-write-wins, named roots, and stats.
- `prolly-diff-merge`: three-way merge, merge explanations, range/prefix
  merge, and a host-language custom resolver.
- `prolly-file-blob-store`: file-backed node storage, file-backed blob storage,
  large-value indirection, value-ref inspection, and blob GC planning.
- `prolly-secondary-index`: a realistic user-record index maintained alongside
  a primary tree, with deterministic rebuild parity.
- Application-style scenario executables also include
  `prolly-batch-build`, `prolly-local-first-state`, `prolly-resolver`,
  `prolly-crdt-merge`, `prolly-conversation-memory`,
  `prolly-agent-event-log`, `prolly-background-compaction`,
  `prolly-deterministic-rag-snapshot`, `prolly-document-chunk-index`,
  `prolly-vector-sidecar`, `prolly-provenance-values`,
  `prolly-materialized-view`, `prolly-filesystem-snapshot`, and
  `prolly-durable-sqlite`.
