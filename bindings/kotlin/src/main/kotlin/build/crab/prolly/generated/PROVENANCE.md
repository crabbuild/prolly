# Generated Kotlin UniFFI Sources

Generated from `bindings/uniffi` with the repository script:

```sh
scripts/regenerate-kotlin-bindings.sh
```

Tool versions used for this snapshot:

- `uniffi` Rust crate: `0.31.0`
- `uniffi-bindgen` Python package: `0.31.0`
- `prolly-bindings` ABI version: `0.1.0`

Compiled native libraries are intentionally not checked in.

Local adaptation:

- binding error payload fields are exported from Rust as `reason` so generated
  Kotlin does not hide `Throwable.message`.
- generated async engine and transaction Kotlin type names are prefixed with
  `RemoteNative` to keep the public coroutine facade unambiguous. The script
  verifies that this post-processing cannot change Rust FFI symbol names.
