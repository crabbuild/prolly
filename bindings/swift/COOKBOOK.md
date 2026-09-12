# Swift Cookbook

Build the Rust facade once before running examples:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
```

Run each scenario from the Swift package directory:

```sh
cd bindings/swift
DYLD_LIBRARY_PATH="$PWD/../../target/debug" swift run prolly-basic-map
DYLD_LIBRARY_PATH="$PWD/../../target/debug" swift run prolly-cookbook-scenarios
DYLD_LIBRARY_PATH="$PWD/../../target/debug" swift run prolly-diff-merge
DYLD_LIBRARY_PATH="$PWD/../../target/debug" swift run prolly-file-blob-store
DYLD_LIBRARY_PATH="$PWD/../../target/debug" swift run prolly-secondary-index
```

Set `PROLLY_BINDINGS_LIBRARY_DIR` if `libprolly_bindings.dylib` is not in this
repository's `target/debug` directory.

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

## Build And Force A TurboQuant RAG Sidecar

TurboQuant is a disposable routing sidecar. The proximity map remains
authoritative and every retained candidate is reranked from its full-precision
vector. Keep the backend explicit until qualification enables `Auto`.

```swift
import Foundation

try Engine.withMemory { engine in
    let records = (0..<32).map { index in
        ProximityRecord(
            key: Data(String(format: "chunk/%02d", index).utf8),
            vector: [Float(index), Float(index % 3), 0, 1, 2, 3, 4, 5],
            value: Data(String(format: "document-%02d", index).utf8)
        )
    }
    let proximity = try engine.buildProximity(dimensions: 8, records: records)
    defer { proximity.close() }

    let built = try proximity.buildTurboquant(workerThreads: 2)
    var request = exactProximitySearchRequest(
        query: [0, 0, 0, 1, 2, 3, 4, 5], k: 3
    )
    request.policy = .fixedBudget
    request.backend = .turboQuantized

    let index = built.index
    let verification = try index.verify(proximity)
    precondition(verification.encodedVectors == UInt64(records.count))
    let result = try index.search(proximity, request: request)
    precondition(result.backend == .turboQuantized)
    let manifest = index.manifest
    index.close()

    let reopened = try proximity.loadTurboquant(manifest)
    reopened.close()
}
```
