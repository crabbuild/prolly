# Generated UniFFI Python Sources

Generated from `crates/prolly/bindings/uniffi` with:

```sh
VIRTUAL_ENV=/tmp/prolly-uniffi-venv \
PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
maturin develop
```

Tool versions used for this snapshot:

- `uniffi` Rust crate: `0.31.0`
- `uniffi-bindgen` Python package: `0.31.0`
- `maturin`: `1.14.1`
- `prolly-bindings` ABI version: `0.1.0`

The generated Python glue is checked in for offline review and vendored builds.
Compiled native libraries are intentionally not checked in.

Local adaptation:

- `_uniffi_load_indirect` first honors `PROLLY_BINDINGS_LIBRARY` so source-tree
  tests can run against `target/debug/libprolly_bindings.*` without checking in
  a native library.
