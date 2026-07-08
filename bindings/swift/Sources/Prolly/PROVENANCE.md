# Generated Swift UniFFI Sources

Generated from `bindings/uniffi` with:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
VIRTUAL_ENV=/tmp/prolly-uniffi-venv \
PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
uniffi-bindgen generate target/debug/libprolly_bindings.dylib \
  --language swift \
  --out-dir bindings/swift/Sources/Prolly \
  --config bindings/uniffi/uniffi.toml
```

The generated `prollyFFI.h` and module map are stored under
`Sources/prollyFFI/include` so Swift Package Manager can compile the FFI module
without a separate system module install.

Tool versions used for this snapshot:

- `uniffi` Rust crate: `0.31.0`
- `uniffi-bindgen` Python package: `0.31.0`
- `prolly-bindings` ABI version: `0.1.0`

Compiled native libraries are intentionally not checked in.
