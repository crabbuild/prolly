# Language-binding verification

The clustered-delete change is internal to the Rust canonical writer and does not alter the public binding interface. The documented binding matrix was nevertheless exercised against the modified tree implementation.

| Surface | Result |
| --- | --- |
| Rust async store | Pass: complete `async-store` test run |
| Rust UniFFI facade | Pass: 26 tests |
| Python 3.14.6 | Pass: 16 tests |
| Go 1.26.0 | Pass: package tests and example-package compilation |
| Node 25.6.1 | Pass: native build and 19 tests |
| Browser WASM | Pass: Rust wasm32 check, release build, and 3 tests |
| Kotlin/JVM | Pass: 15 tests |
| Java/JVM | Pass: 15 tests |
| Swift 5.10 | Pass: fixture executable built and completed |
| Ruby 4.0.6 | Environment blocked before project code loaded |

The Ruby blocker is the pinned `ffi` 1.16.3 native extension. Bundler resolves the dependency after installation is attempted, but its compiler probe cannot link an executable against the local macOS command-line-tools SDK. This is a host Ruby/SDK dependency failure, not a failure observed in the binding or tree implementation. It must still be cleared on a supported Ruby build host before a binding release.

Additional release gates passed: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.
