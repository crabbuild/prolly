# Prolly Python Binding

This package builds the Rust-backed Prolly Python binding with UniFFI and
maturin.

See `COOKBOOK.md` for Python application patterns covering SQLite-backed
indexes, prefix queries, paging, merge callbacks, large values, sync, and custom
stores.

## Develop

```sh
cd bindings/python
python3 -m venv .venv
. .venv/bin/activate
python -m pip install "maturin>=1.10,<2.0" "uniffi-bindgen==0.31.0"
maturin develop
python -m unittest discover -s tests
```

`maturin develop` builds `bindings/uniffi` and installs the generated module as
`prolly.uniffi`.

For source-tree checks without a maturin install, build the Rust library and
point the generated loader at it:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=bindings/python \
  python -c "import prolly.uniffi"
```

The current Rust-backed surface includes memory, file, and SQLite engines;
CRUD, batch, append, parallel batch, and bulk-build operations.

It also exposes:

- root-bound `read_session` point, multi-get, range, diff, and conflict reads;
- prefix, range, cursor, reverse-page, diff-page, and boundary helpers;
- conflict inspection, merge policies, merge traces, and Python callbacks;
- named roots, root manifests, node/CID helpers, GC, sync, cache, and metrics;
- key encoders, mutation constructors, CRDT helpers, and versioned values;
- blob stores, large-value offload, value refs, blob refs, and blob GC;
- portable snapshot bundles and store-independent proof bundles.

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
cd bindings/python
python -m maturin develop
python examples/local_first_state.py
```

For source-tree checks, build the Rust library first and run the scenario from
the repository root:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=bindings/python \
  python3 bindings/python/examples/cookbook_scenarios.py
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

The binding also exposes the version-1 asynchronous foreign-store protocol.
Provider packages live under `stores/` and accept caller-owned SDK clients.
The SQLite package accepts a caller-owned `sqlite3.Connection` and optional
executor, so it never blocks the event loop or assumes ownership of application
resources. Keep root publication, CAS, and merge steps visible in the async
workflow.

Verify the SQLite provider from the repository root:

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=bindings/python:bindings/python/stores/sqlite \
  python3 -m unittest discover -s bindings/python/stores/sqlite/tests -v
```

The PostgreSQL package borrows a running Psycopg 3 async pool. Its live-service
check uses `PROLLY_POSTGRES_URL` and verifies contention, rollback,
cancellation, and Rust-engine interoperability.

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
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=bindings/python \
  python3 -m unittest discover -s bindings/python/tests
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
