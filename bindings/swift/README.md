# Prolly Swift Binding

This package exposes the Rust `prolly-map` engine through UniFFI-generated
Swift sources. The public API is byte-first and uses `Data` for keys, values,
CIDs, node bytes, and envelopes.

The generated API includes single-key, multi-key, range, prefix scans/pages, ordered boundary helpers, reverse and prefix-reverse pages, cursor-page, range cursor helpers, cursor windows, cursor-resumed diff, typed structural diff cursor resume, diff-page, and prefix proof generation,
store-independent proof verification, named-root manifest metadata listing, retained named-root GC with retention policy helpers, compact path-node byte export/import,
canonical proof bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification for portable inclusion and absence checks. It also exposes `parallelBatch`, `parallelBatchWithStats`, `batchWithStats`, and
`appendBatchWithStats` for parallel mutation application plus route and write-count telemetry, upsert/delete mutation constructors, along with versioned-value schema guard helpers, value-ref stored-byte helpers, blob-ref byte validation, prefix bounds, segment encoding/decoding, composite key construction, numeric key helpers, and boundary checks.
It also exposes portable snapshot bundle export/import with canonical bundle
bytes, digests, summaries, and self-contained verification, encoding helpers, tree/large-value/parallel config
constructors, changed-span constructors for exact-key, prefix, and half-open
range performance hints, typed stats/debug records, plus merge/CRDT resolution helpers and built-in
resolver helper functions for callback resolvers. Merge explanations expose a
typed trace event list while retaining the JSON trace string for compatibility.

Build the Rust facade before running Swift examples from the source tree:

```sh
cargo build -p prolly-bindings
cd crates/prolly/bindings/swift
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-basic-map
```

The package links against `libprolly_bindings` from
`../../../../target/debug` by default. Set `PROLLY_BINDINGS_LIBRARY_DIR` when
the native library is somewhere else.

Generated UniFFI sources are checked in under `Sources/Prolly` and
`Sources/prollyFFI` for offline builds. Compiled native libraries and SwiftPM
`.build` output are intentionally not checked in.

## Source Tree Layout

The Swift binding is a SwiftPM package with generated UniFFI sources and a set
of executable examples.

Important files:

- `Package.swift` declares the `Prolly` library and example executables.
- `Sources/Prolly/prolly.swift` is the generated Swift API.
- `Sources/prollyFFI` contains the C shim and headers for the native library.
- `Examples/<Scenario>/main.swift` contains standalone scenario programs.
- `Examples/CookbookScenarios/main.swift` launches the scenario executables.

Each scenario target depends directly on `Prolly`. There is no shared cookbook
support target, so opening a scenario file shows the complete example code for
that workflow.

## Running Examples

Run one scenario:

```sh
cargo build -p prolly-bindings
cd crates/prolly/bindings/swift
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-local-first-state
```

Run all cookbook scenarios:

```sh
DYLD_LIBRARY_PATH="$PWD/../../../../target/debug" swift run prolly-cookbook-scenarios
```

On Linux, use the appropriate dynamic library path variable for the platform.
If the library is not in `target/debug`, set `PROLLY_BINDINGS_LIBRARY_DIR` before
building the Swift package.

## API Style

The Swift API uses `Data` for keys and values. Keep domain-specific codecs in
small Swift functions or types so tree operations remain byte-oriented and
deterministic. Avoid relying on string sorting unless the key format is
explicitly UTF-8 and documented.

Use memory engines for tests, previews, and examples. Use file or SQLite engines
for command-line tools and local applications that need durable roots. Blob
stores should hold large documents, file contents, transcript bodies, and
retrieval chunks.

## SwiftPM Targets

The package exposes one reusable library target and many executable targets.
Executable targets are intentionally repetitive because they are cookbook
material: the point is that a user can copy one scenario into an application and
understand all required setup. The run-all executable is only an orchestrator.

When adding a scenario, add a new `Examples/<Name>/main.swift`, add an executable
product and target in `Package.swift`, and update `COOKBOOK.md` with the command
and application pattern.

## Merge And Callback Guidance

Use built-in resolver names for simple policies. Custom Swift callback resolvers
should be deterministic and should not depend on clocks, random numbers, network
calls, or mutable global state. If a value format has timestamps or tombstones,
prefer the typed CRDT and envelope helpers where they fit.

Merge explanations are useful for UI and logging because they retain structured
trace events. Preserve both the typed records and JSON strings when building
debugging tools.

## Large Values, Proofs, And Snapshots

Use large-value helpers when payload size would make prolly leaves inefficient.
Use proof helpers when exposing inclusion, absence, range, cursor-page, or
diff-page claims outside the process. Use snapshot bundles for moving a complete
reachable tree between stores, devices, tests, or offline tools.

Named roots define retention. Publish roots before GC and retain any checkpoint
or audit root that must remain available after compaction.

## Testing Strategy

SwiftPM can build individual scenarios quickly:

```sh
swift run prolly-batch-build
```

Run the scenario set after generated bindings change. Keep application tests
focused on key codecs, merge policies, root publication, and blob lifecycle.
Use memory engines unless the test specifically needs file, SQLite, or blob
storage behavior.

## Packaging Notes

The source package links to the local debug native library. Release packaging
should provide platform-specific native artifacts and document how SwiftPM finds
them. Generated Swift and C shim sources should be regenerated whenever the
UniFFI facade changes.

## Troubleshooting

- Linker errors usually mean `PROLLY_BINDINGS_LIBRARY_DIR` or the default
  `target/debug` path does not contain `libprolly_bindings`.
- `dyld` runtime errors usually mean the executable built but cannot locate the
  dynamic library at run time.
- Byte comparison bugs usually come from converting `Data` through `String`
  accidentally. Keep keys and roots as `Data`.
- The local `xcrun` warning about XCTest paths can appear with incomplete
  Command Line Tools installs. The examples can still build and run if SwiftPM
  completes successfully.
