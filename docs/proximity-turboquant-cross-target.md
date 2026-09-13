# TurboQuant Cross-Target Conformance

TurboQuant persisted construction is checked against one shared canonical
fixture rather than against target-specific expected values. The fixture is
`conformance/proximity-fixtures.json` and fixes the SplitMix stream,
transform-plan/input/output hashes for dimensions 8, 24, 128, 200, 768, 1536,
and 3072, all codebook bits, 2/3/4-bit packing, and a complete three-record
persisted closure.

The persisted wire fixture has these target-independent identities:

- authoritative descriptor CID:
  `aa33d78f3fa8e8df48bb18b4f03756dba1a5ca4b78c6f09512c81e2bf07927dd`
- TurboQuant manifest CID:
  `f927847b6f9d119d728e1145d835d11b2720f5171248b308a38f3ae72ddac338`

## Evidence

| Target | Evidence | Result |
| --- | --- | --- |
| x86_64 Linux | [`core` CI job at fixture commit `0d1b3a9a`](https://github.com/crabbuild/prolly/actions/runs/34686866431/job/103535258271), `cargo test --all-targets` | 534 unit tests passed, including every TurboQuant algorithm hash; the complete TurboQuant wire-closure test also passed |
| aarch64 macOS | Rust 1.97.0 on `aarch64-apple-darwin`, `cargo test --all-features turboquant::tests::` and focused wire suite | algorithm hashes, grouped decode parity, and complete wire closure passed |
| browser WASM | `bindings/wasm/test/portable-parity.test.ts`, `npm --prefix bindings/wasm run build`, `typecheck`, and `test` on `wasm32-unknown-unknown` | 37/37 package tests passed; the browser build reproduced both canonical CIDs above |

The x86 fixture commit replaced host-libm-generated input vectors with direct
SplitMix-derived IEEE-754 bits. This makes the input itself part of the frozen
contract, so matching output hashes cannot be invalidated by platform `sin`
implementations. The later grouped decode optimization changes only query-time
unpacking and is separately required to match the canonical decoder and
scalar/SIMD result bits.

Any future target is conformant only when it consumes the same fixture and
reproduces the same hashes and CIDs. A successful compile alone is not
cross-target conformance evidence.
