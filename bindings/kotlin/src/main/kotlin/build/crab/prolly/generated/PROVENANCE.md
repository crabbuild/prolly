# Generated Kotlin UniFFI Sources

Generated from `bindings/uniffi` with:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
VIRTUAL_ENV=/tmp/prolly-uniffi-venv \
PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
uniffi-bindgen generate target/debug/libprolly_bindings.dylib \
  --language kotlin \
  --out-dir bindings/kotlin/src/main/kotlin/build/crab/prolly/generated \
  --config bindings/uniffi/uniffi.toml
```

Tool versions used for this snapshot:

- `uniffi` Rust crate: `0.31.0`
- `uniffi-bindgen` Python package: `0.31.0`
- `prolly-bindings` ABI version: `0.1.0`

Compiled native libraries are intentionally not checked in.

Local adaptation:

- binding error payload fields are exported from Rust as `reason` so generated
  Kotlin does not hide `Throwable.message`.
