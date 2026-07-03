# Prolly UniFFI Facade

This directory contains the shared Rust binding crate for Prolly language
bindings.

Current contents:

- `Cargo.toml` for the `prolly-bindings` crate;
- `src/lib.rs` with the FFI-safe, proc-macro UniFFI facade over `prolly-map`;
- `uniffi.toml` with language generator settings.

The first facade exposes Rust-backed memory, file, and SQLite engines plus
byte-first records and helpers for config, tree handles, nodes, CIDs, key
helpers, boundary decisions, eager and paged range/diff, range-after/cursor
resumption with range cursor constructors, cursor-resumed diffs, merge with built-in resolver names, merge explanations, parallel
batch writes, Rust bulk-build and sorted bulk-build, append-batch, merge policy
registries with named and callback resolver rules, CRDT merge
presets, named roots/CAS, named-root manifest listing, snapshot namespaces,
root manifests, versioned values with schema match/require guards, memory/file blob stores,
value/blob envelopes, large-value offload/resolution, value-ref inspection
with stored-byte decode and inline-escape helpers, blob-ref byte validation,
store-independent single-key, multi-key, range, cursor-page, diff-page, and prefix proof generation/verification with compact path-node
export/import, canonical proof bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification,
tombstone envelopes, range-limited diffs, structural diff cursor pages,
stats/debug JSON, node and blob GC plans/sweeps including named-root retention
policy constructors, store-to-store missing-node sync, cache/metrics inspection, and
optional performance hints. Key helpers include prefix bounds, numeric encoders,
single-segment escaping, decoded segment inspection, and composite key
construction from byte segments or an existing encoded prefix. Performance-hint
helpers include changed-span constructors for exact keys, prefixes, and
half-open ranges.

Language packages live in sibling directories such as
`crates/prolly/bindings/python`, `crates/prolly/bindings/node`,
`crates/prolly/bindings/go`, and `crates/prolly/bindings/java`.
