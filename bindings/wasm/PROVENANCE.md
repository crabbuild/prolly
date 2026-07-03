# Prolly WASM Binding Provenance

- Binding path: direct `wasm-bindgen` wrapper over `prolly-map`
- Rust crate: `crates/prolly/bindings/wasm`
- Package: `@crabdb/prolly-wasm`
- Generated artifacts: `pkg/` from `wasm-bindgen --target web --typescript`
- Compiled artifacts checked in: none

Reference build command:

```sh
cargo build -p prolly-wasm --release --target wasm32-unknown-unknown
wasm-bindgen ../../../../target/wasm32-unknown-unknown/release/prolly_wasm.wasm \
  --target web \
  --typescript \
  --out-dir pkg
```
