# Prolly WASM Binding Provenance

- Binding path: direct `wasm-bindgen` wrapper over `prolly-map`
- Rust crate: `bindings/wasm`
- Package: `@crabdb/prolly-wasm`
- Generated artifacts: `pkg/` from `wasm-bindgen --target web --typescript`
- Compiled artifacts checked in: none

Reference build command:

```sh
cargo build --manifest-path bindings/wasm/Cargo.toml --release --target wasm32-unknown-unknown --target-dir target
wasm-bindgen target/wasm32-unknown-unknown/release/prolly_wasm.wasm \
  --target web \
  --typescript \
  --out-dir bindings/wasm/pkg
```
