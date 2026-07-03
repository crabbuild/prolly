# Prolly WASM Binding

This package is the browser-oriented WebAssembly binding for the Rust
`prolly-map` engine. It uses `wasm-bindgen` directly over the Rust memory
engine and exposes `Uint8Array` keys and values.

See `COOKBOOK.md` for browser patterns covering memory snapshots, UI paging,
diffs, built-in merges, stats, and persistence handoff guidance.

Current surface:

- memory engine only;
- create, get, get-many, put, delete, batch, batch-with-stats, parallel batch, parallel-batch-with-stats,
  bulk-build, sorted bulk-build, append-batch, append-batch-with-stats, eager
  range, prefix scans/pages, ordered boundary helpers, reverse and prefix-reverse pages, range-after/cursor resumption, cursor windows, range pages, and
  eager/ranged/cursor-resumed/paged/structural diff with typed cursor resume
  plus JSON cursor compatibility;
- three-way merge helpers for the built-in resolver names (`prefer_left`,
  `prefer_right`, `delete_wins`, `update_wins`), merge explanations with typed
  trace events plus JSON trace compatibility, conflict pages, and scoped
  range/prefix merges;
- single-key, multi-key, range, cursor-page, diff-page, and prefix proof generation, store-independent
  verification from root/bounds/path node bytes, and canonical proof bundle
  bytes with proof-bundle introspection/routing summaries, one-shot
  proof-bundle verification, HMAC-authenticated proof envelopes, and
  one-shot authenticated proof-bundle verification;
- typed stats/debug objects plus stats/debug inspection as JSON or text;
- `WasmSnapshotBundle` export/import plus `toBytes()`/`fromBytes()`,
  `digest()`/`digestBytes()`, `summary()`/`summaryFromBytes()`, and `verify()`/`verifyBytes()` for complete
  portable memory-engine tree bundles with pre-import verification;
- node bytes/CID helpers, prefix bounds, segment encoding/decoding, numeric
  key helpers, and boundary checks from Rust.

Filesystem, SQLite, native store constructors, and host callback stores are
intentionally absent in browser builds. IndexedDB/OPFS stores belong in a later
host-integration pass.

Local checks:

```sh
rustup target add wasm32-unknown-unknown
cargo check -p prolly-wasm --target wasm32-unknown-unknown
npm --prefix crates/prolly/bindings/wasm test
```

To produce browser artifacts, install a matching `wasm-bindgen` CLI and run:

```sh
npm --prefix crates/prolly/bindings/wasm run build:wasm
```

The generated `pkg/` directory and compiled `.wasm` output are release
artifacts and should not be checked in.

## Source Tree Layout

The WASM binding targets browser and worker environments. It exposes a memory
engine and browser-safe helpers while intentionally excluding native filesystem,
SQLite, and host-store callbacks.

Important files:

- `src/lib.rs` is the Rust wasm-bindgen facade.
- `src/index.ts` loads and types the generated WASM package.
- `examples/*.ts` contains standalone browser/worker scenarios.
- `test/wasm.test.ts` covers the TypeScript loader and WASM behavior.
- `package.json` defines build, test, and example commands.

## Running Examples

Build browser artifacts first:

```sh
rustup target add wasm32-unknown-unknown
npm --prefix crates/prolly/bindings/wasm run build:wasm
```

Run one scenario under Node using the generated WASM package:

```sh
node crates/prolly/bindings/wasm/examples/local-first-state.ts
```

Run all browser-oriented scenarios:

```sh
node crates/prolly/bindings/wasm/examples/browser-scenarios.ts
```

The run-all file launches each scenario as its own process. Scenario files stay
standalone and readable.

## API Style

WASM keys and values are `Uint8Array`. Keep encoding explicit and deterministic.
Browser applications should keep key layouts prefix-friendly so UI pagination,
sync windows, and worker jobs can operate on bounded key ranges.

The WASM engine is memory-only. Persistent browser storage should be layered
outside this crate through IndexedDB, OPFS, Cache Storage, or application-owned
sync protocols. The browser storage scenario demonstrates root handoff with a
small in-memory map, not a production storage adapter.

## Browser Boundaries

WASM cannot expose every native feature safely. File stores, SQLite stores,
native host stores, and direct OS handles are intentionally absent. Browser
applications should decide how roots and node bytes move to durable browser
storage, and that integration should be explicit about quotas, transactions,
worker ownership, and recovery after tab termination.

For UI applications, keep large merges and proof generation in a worker when
possible. Avoid blocking the main thread with large range scans or snapshot
imports.

## Merge, Proofs, And Snapshots

Built-in resolver names cover common browser state policies. Use domain-specific
value envelopes when synchronizing local changes with remote state. Proof helpers
are useful when a browser must verify inclusion or absence claims from a server.
Snapshot bundles are useful for import/export, offline cache seeding, and tests.

Because browser storage is external, verify bundle bytes before trusting them and
publish roots only after the application has stored all required side data.

## Large Values

The WASM binding does not include native blob stores. For large documents,
chunks, and generated assets, store the payload in browser storage and keep
metadata, content IDs, or vector IDs in prolly keys and values. The document
chunk and vector sidecar scenarios show this split.

## Testing Strategy

Run Rust and TypeScript checks:

```sh
cargo check -p prolly-wasm --target wasm32-unknown-unknown
npm --prefix crates/prolly/bindings/wasm test
```

Add browser integration tests around application-owned storage, worker startup,
quota behavior, and recovery. Keep core prolly tests deterministic and
memory-only.

## Packaging Notes

The `pkg/` output must be produced with a compatible `wasm-bindgen` CLI. Release
packages should document the expected bundler mode, whether the package is ESM
only, how the `.wasm` asset is loaded, and which browsers are supported.

## Troubleshooting

- Missing `pkg/prolly_wasm.js` or `.wasm` files means `npm run build:wasm` has
  not run.
- Loader failures often come from bundlers treating `.wasm` assets differently.
  Verify asset URLs and MIME types in the final application.
- Browser memory spikes usually mean a large range, diff, proof, or snapshot
  import is running on the main thread.
- Persistent-state bugs should first be debugged in the host storage layer,
  because the WASM engine itself is memory-only.
