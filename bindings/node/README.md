# Prolly Node/TypeScript Binding

This package exposes the Rust `prolly-bindings` facade through a Node-API
module, typed TypeScript declarations, and Promise-based async wrappers.

See `COOKBOOK.md` for Node and TypeScript application patterns covering
SQLite-backed indexes, Promise wrappers, prefix queries, paging with range cursor constructors, cursor-resumed
diffs, named-root manifest metadata listing, merge callbacks, verifiable single-key, multi-key, range, cursor-page, diff-page, and prefix proofs with
portable bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated envelopes, and one-shot authenticated proof-bundle verification, retained named-root GC with retention policy constructors, large values, blob GC, and
JavaScript-owned custom stores.

The native and async engines expose `parallelBatch`, `batchWithStats`, and
`appendBatchWithStats` for parallel mutation application plus route/write
telemetry. `defaultParallelConfig()` returns the Rust default parallel-batch
configuration for Node callers.

Key helpers include `prefixEnd`, `prefixRange`, numeric key encoders,
`encodeSegment`, `keyFromSegments`, `keyFromPrefixedSegments`,
`decodeSegments`, `debugKey`, and Rust boundary checks.
Native codec helpers include versioned-value byte round trips plus schema
match/require guards, and value-ref stored-byte decode plus inline-escape
checks. Blob helpers include direct blob-ref byte validation for content
integrity checks outside the store. Hint helpers include exact-key, prefix, and
range changed-span constructors.

Local smoke test:

```sh
npm --prefix crates/prolly/bindings/node ci
npm --prefix crates/prolly/bindings/node run build:native
npm --prefix crates/prolly/bindings/node test
```
