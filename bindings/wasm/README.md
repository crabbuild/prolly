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
