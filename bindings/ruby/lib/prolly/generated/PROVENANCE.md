# Generated Ruby UniFFI Sources

Generated from `crates/prolly/bindings/uniffi` with:

```sh
cargo build -p prolly-bindings
VIRTUAL_ENV=/tmp/prolly-uniffi-venv \
PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
uniffi-bindgen generate target/debug/libprolly_bindings.dylib \
  --language ruby \
  --out-dir crates/prolly/bindings/ruby/lib/prolly/generated \
  --config crates/prolly/bindings/uniffi/uniffi.toml
```

Tool versions used for this snapshot:

- `uniffi` Rust crate: `0.31.0`
- `uniffi-bindgen` Python package: `0.31.0`
- `prolly-bindings` ABI version: `0.1.0`

Local adaptation:

- the generated `ffi_lib` line accepts `ENV["PROLLY_BINDINGS_LIBRARY"]` so
  tests can use a locally built Cargo debug library.
- generated local references to Ruby keyword `end` in methods with `_end`
  parameters were rewritten to `_end`.

Compiled native libraries are intentionally not checked in.
