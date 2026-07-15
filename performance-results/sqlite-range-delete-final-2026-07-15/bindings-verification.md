# Final binding verification — range deletion

## Scope

This verification was run against final candidate revision
`79b7f5811bf687b7803822431027fd09cef75ebd` on 2026-07-15. It rechecks
every maintained binding after the final internal canonical-delete repair.
No public binding API, generated binding, source implementation, or benchmark
harness was changed during this task.

The first command rebuilt the native UniFFI facade used by the FFI-backed
surfaces:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
```

It finished with exit status zero. Python, Go, Kotlin, Java, Ruby, and Swift
used the documented local library adaptation
`target/debug/libprolly_bindings.dylib`; Node and browser WASM rebuilt their
documented native packages from this revision.

## Range-delete contract exercised

The binding suites exercise `delete_range` / `deleteRange` and
`delete_range_with_stats` / `deleteRangeWithStats` with raw byte bounds
`[b, e)`, confirming that `b`, `c`, and `d` are removed while `a`, `e`, and
`f` remain. The UniFFI facade test is
`memory_engine_delete_range_is_half_open`; native async tests additionally
exercise the stats return path and metric deltas. The Node native and async,
browser WASM, Python, Go, Kotlin, Java, and Swift fixture suites contain the
same half-open-plus-stats smoke behavior.

## Maintained binding matrix

| Surface | Fresh command(s) | Result |
| --- | --- | --- |
| Rust async store | `cargo test --features async-store` | 933 passed, 3 explicitly ignored (936 native test cases); async range-delete, no-op, equivalence, and stats tests passed. |
| Rust async doctests | `cargo test --doc --features async-store --quiet` | 97 passed, 2 explicitly ignored (99 total). |
| Rust UniFFI facade | `cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target` | 27 passed, 0 failed; includes the half-open range-delete test. |
| Python | `PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" PYTHONPATH=bindings/python python3 -m unittest discover -s bindings/python/tests` | 17 passed, 0 failed. |
| Go | `(cd bindings/go && go test ./...)`; additionally forced fresh with `go test -count=1 -v ./...` | 16 passed, 0 failed; the forced run includes `TestMemoryEngineDeleteRangeUsesHalfOpenBounds`. |
| Node/TypeScript native | `npm --prefix bindings/node run build:native && npm --prefix bindings/node test` | Native package rebuilt; 21 passed, 0 failed, including synchronous and async half-open range-delete/stat tests. |
| Browser WASM | `cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown --target-dir target && npm --prefix bindings/wasm run build:wasm && npm --prefix bindings/wasm test` | WASM package rebuilt; 4 passed, 0 failed, including raw-byte half-open range deletion and stats. |
| Kotlin/JVM | `mvn -f bindings/kotlin/pom.xml test` | 17 passed, 0 failures/errors/skips. |
| Java | `mvn -f bindings/pom.xml -pl java -am test` | Kotlin prerequisite: 17 passed; Java: 17 passed; 34 total, 0 failures/errors/skips. |
| JVM aggregate | `mvn -f bindings/pom.xml test` | Kotlin 17 plus Java 17: 34 total, 0 failures/errors/skips. |
| Swift fixture | `DYLD_LIBRARY_PATH="$PWD/target/debug" swift run --package-path bindings/swift prolly-fixture-check` | Passed; fixture explicitly checks half-open deletion and the stats-returning call. |
| Ruby smoke | documented Bundler command | **Blocked before binding/test loading; 0 tests executed.** See the external blocker below. |

## Cookbook scenarios

The supplementary documented cookbook commands also completed successfully:

| Binding | Command result |
| --- | --- |
| Python | 14 scenarios passed. |
| Go | 14 scenarios passed. |
| Node/TypeScript | 14 scenarios passed. |
| Browser WASM | 12 browser-safe scenarios passed. |
| Kotlin | 18 scenarios passed. |
| Java | 18 scenarios passed. |
| Swift | 14 scenarios passed. |
| Ruby | **Blocked before loading; 0 scenarios executed.** |

## Ruby external blocker

Both documented Ruby commands stopped during Bundler dependency resolution,
before Ruby loaded the generated binding or entered either test program:

```text
Could not find compatible versions

Because every version of trail-prolly depends on ffi >= 1.15, < 1.17
  and ffi >= 1.15, < 1.17 could not be found in locally installed gems,
  trail-prolly cannot be used.
```

No host dependency was installed, downgraded, or otherwise changed. Ruby is
therefore an external verification prerequisite, not a passing surface. Once
a compatible local `ffi` is present, rerun both documented Ruby commands.

## Non-failing host/tool warnings

- The WASM Cargo check and release build emitted five target-specific
  unused-variable/dead-code warnings from the SIMD fallback configuration;
  all three documented WASM commands exited zero.
- Kotlin/JVM emitted local Kotlin-daemon registry retry messages and the Java
  runtime emitted the JNA restricted-native-access warning. Both Maven runs
  completed successfully with zero failures.
- Swift emitted the local command-line-tools `xcrun` XCTest platform-path
  warning, but both the fixture and all cookbook scenarios completed.

## Cleanup and commit scope

After verification, removed generated root/binding `Cargo.lock` files, Node
`node_modules` and native binary, WASM `pkg`, Kotlin/Java Maven targets,
Swift `.build`, Python `__pycache__`, and the temporary Ruby bundle path.
The evidence commit contains only this report; no build output, lockfile,
generated binding, source, public API, or benchmark-harness change is
included.
