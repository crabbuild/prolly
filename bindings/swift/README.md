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
