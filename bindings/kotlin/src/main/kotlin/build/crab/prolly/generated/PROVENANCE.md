# Generated Kotlin UniFFI Sources

Generated from `crates/prolly/bindings/uniffi` with:

```sh
cargo build -p prolly-bindings
VIRTUAL_ENV=/tmp/prolly-uniffi-venv \
PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
uniffi-bindgen generate target/debug/libprolly_bindings.dylib \
  --language kotlin \
  --out-dir crates/prolly/bindings/kotlin/src/main/kotlin/build/crab/prolly/generated \
  --config crates/prolly/bindings/uniffi/uniffi.toml
```

Tool versions used for this snapshot:

- `uniffi` Rust crate: `0.31.0`
- `uniffi-bindgen` Python package: `0.31.0`
- `prolly-bindings` ABI version: `0.1.0`

Compiled native libraries are intentionally not checked in.

Local adaptation:

- generated `ProllyBindingException` subclass fields named `message` were
  changed to `override val message` for Kotlin/JVM compilers that reject
  hiding `Throwable.message`.
