# Prolly Python Binding

This package builds the Rust-backed Prolly Python binding with UniFFI and
maturin.

See `COOKBOOK.md` for Python application patterns covering SQLite-backed
indexes, prefix queries, paging, merge callbacks, large values, sync, and custom
stores.

## Develop

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install "maturin>=1.10,<2.0" "uniffi-bindgen==0.31.0"
maturin develop
python -m unittest discover -s tests
```

`maturin develop` builds `crates/prolly/bindings/uniffi` and installs the
generated module as `prolly.uniffi`.

For source-tree checks without a maturin install, build the Rust library and
point the generated loader at it:

```sh
cargo build -p prolly-bindings
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=crates/prolly/bindings/python \
  python -c "import prolly.uniffi"
```

The current Rust-backed surface includes memory, file, and SQLite engines;
CRUD, batch, batch-with-stats, Rust bulk-build, sorted bulk-build,
append-batch, append-batch-with-stats, parallel batch, and parallel-batch-with-stats operations; eager
range, prefix scans/pages, ordered boundary helpers, reverse and prefix-reverse pages, range-after/cursor resumption with cursor constructors, cursor windows, cursor-resumed diffs, and paged
range/diff; paged
three-way conflict inspection; merge with built-in resolver names and merge
explanations with typed trace events plus JSON trace compatibility; Python
`MergeResolverCallback` custom resolvers for full-tree,
range-limited, and prefix-limited merges; merge policy registries with named
and Python callback resolvers; mutation constructors; merge/CRDT resolution constructors and built-in resolver helper functions; Python `HostStoreCallback` custom stores;
named root
publish/load/list/manifest-list/delete/CAS; root manifest bytes; node/CID helpers; key
helpers, including prefix bounds, segment encoding/decoding, and composite key
construction; encoding helpers and tree/large-value/parallel config
constructors; boundary
checks; range-limited diffs; structural diff cursor pages with typed resume;
typed stats/debug records plus stats/debug JSON; GC planning and sweeping, including named-root retention
policy constructors; store-to-store missing-node sync; portable snapshot bundle
export/import plus canonical bundle bytes, digests, summaries, and self-contained
verification; cache and metrics inspection;
changed-span constructors for performance hints; optional performance hints; CRDT merge presets and `CrdtResolverCallback`
custom resolvers; tombstone envelopes; versioned values with schema
match/require guards; memory/file blob
stores; large-value offload/resolution;
value-ref inspection and stored-byte helpers; blob-ref byte validation; store-independent single-key, multi-key, range, cursor-page, diff-page, and prefix proofs
with compact path-node export/import, canonical bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated proof envelopes, and one-shot authenticated proof-bundle verification; blob GC; and
value/blob envelopes.

The source tree keeps the generated Python glue under
`prolly/uniffi` for offline review. Native libraries produced by
maturin are ignored and should be rebuilt locally or in release CI.

When the generated native module is not built, `prolly` falls back to the
temporary pure-Python fixture harness in `src`. That fallback exists only to
keep source-tree conformance tests useful while the Rust-backed API expands.
