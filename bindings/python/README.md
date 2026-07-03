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

## Source Tree Layout

The Python binding is a UniFFI-generated package wrapped for normal Python
development workflows. It supports both installed-package usage through
`maturin develop` and source-tree example execution.

Important files:

- `pyproject.toml` configures the Python package and maturin build.
- `prolly/__init__.py` selects the generated Rust-backed module or fallback.
- `prolly/uniffi/prolly.py` is generated glue for the native library.
- `examples/*.py` contains standalone scenarios with local import setup.
- `tests/` covers fixture compatibility and generated binding behavior.

## Running Examples

For installed development:

```sh
python -m pip install "maturin>=1.10,<2.0" "uniffi-bindgen==0.31.0"
python -m maturin develop --manifest-path crates/prolly/bindings/uniffi/Cargo.toml
python crates/prolly/bindings/python/examples/local_first_state.py
```

For source-tree checks, build the Rust library first and run the scenario from
the repository root:

```sh
cargo build -p prolly-bindings
python crates/prolly/bindings/python/examples/cookbook_scenarios.py
```

Each scenario inserts the binding directory into `sys.path` before importing
`prolly`, so the examples do not rely on a central helper file or an installed
wheel just to be readable.

## API Style

The generated API is byte-first. Keys and values should be `bytes`, not Python
strings, unless a helper explicitly encodes text for a scenario. Keep codecs
small and deterministic. Prefix-oriented key layouts make range scans, cursor
pages, and prefix-limited merges easier to reason about.

Use memory engines for unit tests and examples. Use file or SQLite engines when
state must survive process restarts. Use blob stores for large values such as
documents, chunks, transcript bodies, and generated artifacts.

## Callbacks And Async Boundaries

Python callback resolvers and host stores are convenient for integrating with
application-owned persistence, but they execute across a native boundary. Keep
callbacks short, deterministic, and explicit about exceptions. A callback should
not depend on global mutable process state unless that state is part of the
application contract.

The binding surface is synchronous. If an application uses `asyncio`, call the
binding from a deliberate executor boundary and keep root publication, CAS, and
merge steps visible in the async workflow.

## Merge, CRDT, And Proof Usage

Use built-in resolver names for simple policies. Use CRDT helpers, timestamped
value envelopes, tombstone helpers, or custom resolvers when the value format has
domain-specific semantics. Proof bundle and authenticated envelope helpers are
intended for data that crosses process, machine, or trust boundaries.

Snapshot bundle helpers are useful for tests, offline transfer, and migration
tools. Verify a bundle before importing it into a durable store.

## Testing Strategy

Run Python tests after rebuilding the native library:

```sh
python -m unittest discover -s crates/prolly/bindings/python/tests
```

Keep fixture tests focused on byte compatibility and record conversion. Add
scenario tests when a user-facing workflow regresses, such as named-root CAS,
blob GC, or prefix paging.

## Packaging Notes

Release wheels should be built by CI for supported Python versions and
platforms. The generated glue must match the native library exports exactly. If
`prolly/uniffi/prolly.py` expects a symbol that the loaded dynamic library does
not export, rebuild the Rust facade and regenerate or reinstall the wheel.

## Troubleshooting

- `ModuleNotFoundError: prolly` means the package is neither installed nor on
  `PYTHONPATH`. The examples handle this for source-tree execution.
- `AttributeError: dlsym ... symbol not found` means generated Python glue and
  the loaded native library are out of sync.
- Byte/string bugs usually come from mixing `str` and `bytes`. Encode at the
  boundary and keep tree operations byte-only.
- SQLite examples should use temporary directories in tests and explicit paths
  in applications.
