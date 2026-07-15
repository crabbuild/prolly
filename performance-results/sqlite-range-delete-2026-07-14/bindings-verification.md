# Binding verification — range deletion

## Final library build

The final UniFFI facade was rebuilt from `e1505427bc3feaf4aa018c68d7507f5aa48371c2` with:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
```

It completed with exit status zero. Every verification command below used that final library build (or rebuilt its documented native/WASM package from the same revision).

## Verification matrix

| Surface | Command result | Exact result |
|---|---|---|
| Rust async store | `cargo test --features async-store` | 931 passed, 3 explicitly ignored (934 listed test cases); doctests: 97 passed, 2 ignored |
| Rust UniFFI facade | `cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target` | 27 passed, 0 failed |
| Python | documented `unittest discover` command | 17 passed, 0 failed |
| Go | `(cd bindings/go && go test ./...)` | 16 passed, 0 failed; 19 example packages report no test files |
| Node/TypeScript | `npm --prefix bindings/node run build:native && npm --prefix bindings/node test` | 21 passed, 0 failed |
| Browser WASM | documented Cargo check, WASM build, and npm test commands | 4 passed, 0 failed |
| Kotlin/JVM | `mvn -f bindings/kotlin/pom.xml test` | 17 passed, 0 failures, 0 errors, 0 skipped |
| Java | `mvn -f bindings/pom.xml -pl java -am test` | Kotlin 17 passed and Java 17 passed; 34 total, 0 failures/errors/skips |
| JVM aggregate | `mvn -f bindings/pom.xml test` | Kotlin 17 passed and Java 17 passed; 34 total, 0 failures/errors/skips |
| Swift | `DYLD_LIBRARY_PATH="$PWD/target/debug" swift run --package-path bindings/swift prolly-fixture-check` | fixture-check executable passed |
| Ruby | documented smoke command | **blocked before test load; 0 tests executed** |

The WASM Cargo check/build emits five target-specific unused-code/variable warnings from the SIMD fallback configuration, but every documented command exits zero. Swift emits a non-failing `xcrun` XCTest-platform discovery warning from the local command-line-tools installation; both fixture and cookbook executables complete successfully.

## Ruby external blocker

Both the smoke and cookbook commands stop in Bundler dependency resolution, before Ruby loads the generated binding or runs any test:

```text
Could not find compatible versions
Because every version of trail-prolly depends on ffi >= 1.15, < 1.17
and ffi >= 1.15, < 1.17 could not be found in locally installed gems
```

This host lacks a compatible local `ffi` gem. It is not a passing Ruby result and was not masked by a fallback; after installing a compatible dependency, rerun the two documented Ruby commands.

## Runnable cookbook scenarios

The supplementary cookbook commands from `bindings/VERIFICATION.md` also completed successfully for Python (14 scenarios), Go (14), Node (14), browser WASM (12 browser-safe scenarios), Kotlin (18 reported scenario checks), Java (18), and Swift (14). Ruby cookbook execution is blocked by the same Bundler/`ffi` condition above, before application code loads.

No generated lockfile, native binary, `node_modules`, Maven target, Swift `.build`, or WASM `pkg` artifact is included in the evidence commit.
