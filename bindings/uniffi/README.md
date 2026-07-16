# Prolly UniFFI Facade

This directory contains the shared Rust binding crate for Prolly language
bindings.

Current contents:

- `Cargo.toml` for the `prolly-bindings` crate;
- `src/lib.rs` with the FFI-safe, proc-macro UniFFI facade over `prolly-map`;
- `uniffi.toml` with language generator settings.

The first facade exposes Rust-backed memory, file, and SQLite engines plus
byte-first records and helpers for config, tree handles, nodes, CIDs, and
encoding.

It also exports:

- prefix, boundary, range-page, diff-page, cursor, and reverse-page helpers;
- bulk-build, append-batch, parallel-batch, and execution-stat APIs;
- merge policies, resolver callbacks, CRDT helpers, and explanation traces;
- named roots, root manifests, CAS, sync, node GC, blob GC, and retention;
- blob stores, large-value offload, value refs, blob refs, and versioned values;
- portable snapshot bundles and store-independent proof bundles;
- performance hints for exact keys, prefixes, and half-open ranges.
- streaming visitors for forward/reverse range and prefix scans, structural
  diffs, ranged diffs, and three-way conflicts.

Language packages live in sibling directories such as
`bindings/python`, `bindings/node`, `bindings/go`, and `bindings/java`.

## Role In The Binding Stack

This crate is the shared Rust facade that language bindings consume. It should
stay boring, explicit, and stable. Language packages may add ergonomic wrappers,
async adapters, or packaging logic, but the records, callbacks, exported
functions, and symbol names originate here.

When changing this crate, think in terms of downstream blast radius:

- Python generated glue must match the exported dynamic library symbols.
- Kotlin and Java consume the generated JVM records and helper functions.
- Swift consumes generated Swift and C shim sources.
- Ruby and other FFI consumers depend on stable function names and record shapes.
- Node and WASM may mirror helper names for parity even when they do not use
  UniFFI directly.

## Source Tree Layout

Important files:

- `src/lib.rs` defines the exported facade.
- `uniffi.toml` configures UniFFI generation.
- `Cargo.toml` declares crate type, dependencies, and build metadata.
- Sibling language directories contain generated outputs and wrappers.

Keep this crate focused on cross-language primitives. Do not put language-only
convenience behavior here unless every binding should expose it.

## Development Workflow

Build the native library first:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
```

Run facade tests and generated binding tests from the relevant language package
after changing exported records or functions. A change that compiles in Rust can
still break generated Python, Kotlin, Swift, or Ruby if the exported signature is
not representable or if generated code was not refreshed.

## API Design Rules

Prefer explicit record types over loosely structured strings. Prefer byte arrays
for keys, values, roots, CIDs, proof bytes, and bundle bytes. Keep enum variants
stable and document any new variant in each language README.

Avoid language-specific naming in exported functions. The same facade should
feel natural enough in Python, Kotlin, Swift, Ruby, and any future binding after
thin wrapper adaptation.

## Callback Boundaries

Host stores, merge resolvers, and CRDT resolvers cross from Rust into a host
language. Callback APIs should be small, deterministic, and explicit about
failure. Do not design callback flows that require chatty round trips for common
operations when a batched API can carry the same data.

Read visitors return `true` to continue and `false` to stop. The returned
`ScanOutcomeRecord` includes the stopping record in `visited`. Native traversal
borrows packed node bytes, while the FFI facade creates one owned record for
each callback because Rust references cannot safely cross the boundary. The
visitor prevents eager collection of the entire result; it does not make the
host-language byte array itself zero-copy.

When adding callbacks, update language tests for at least one happy path and one
failure path. Callback failures are where many binding bugs appear first.

## Versioning And Compatibility

Generated language glue and the native library must be built from the same
facade version. Adding a function is usually safe for source compatibility but
still requires regeneration. Removing or renaming a function breaks generated
glue. Changing field order, nullability, integer width, or enum variants must be
treated as a compatibility event for every language package.

Use snapshot bundle and proof byte helpers as compatibility anchors. They are
good cross-language fixtures because the same bytes should decode and verify in
every binding.

## Testing Strategy

Use a layered test plan:

- Rust tests validate core behavior and facade invariants.
- Generated language tests validate type conversion and symbol availability.
- Fixture tests validate byte compatibility across languages.
- Cookbook scenarios validate realistic application flows.

When a new facade function is added, update at least one generated binding test
or scenario so the export is exercised outside Rust.

## Packaging Notes

Release CI should produce native libraries for the supported platform matrix and
regenerate language glue from the same commit. Do not mix a generated Python,
Swift, Kotlin, or Ruby file from one facade version with a dynamic library from
another version.

## Troubleshooting

- Symbol lookup errors in generated languages mean the glue and native library
  are out of sync.
- Integer conversion bugs usually involve unsigned Rust values crossing into a
  language with fewer unsigned integer conveniences.
- Callback panics or exceptions should be reproduced with the smallest possible
  host callback before debugging storage or merge logic.
- If one binding passes and another fails, compare generated record shapes and
  optional fields before assuming the Rust core is wrong.
