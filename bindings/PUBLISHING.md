# Publishing language bindings

Binding releases use one synchronized version and one protected release tag.
The current package set is recorded in `release-manifest.json` and validated by
`scripts/check-binding-release.py`.

## Published artifacts

| Runtime | Registry artifact | Delivery |
| --- | --- | --- |
| Node.js / TypeScript | `prollydb` | npm package with prebuilt Node-API addons |
| Browser WebAssembly | `prollydb-wasm` | npm package with generated JS, declarations, and WASM |
| Python | `prollydb` | PyPI wheels containing the UniFFI native library |
| Go | `github.com/crabbuild/prolly/bindings/go` plus store submodules | Go module tags; native libraries are attached to the GitHub release |

The release also attaches versioned native UniFFI libraries for macOS arm64,
macOS x86-64, Linux x86-64, and Windows x86-64. Go consumers need the matching
archive because the Go module proxy distributes source rather than native
release assets. The native archives can also be used with the checked-in JVM,
Ruby, and Swift bindings.

Java/Kotlin, Ruby, and Swift are compile- and runtime-gated by
`bindings-required.yml`, but they are not uploaded to Maven Central, RubyGems,
or a Swift package registry yet. Their current package definitions require an
external `prolly_bindings` library, so publishing those source packages alone
would create installable but non-working artifacts.

## One-time registry setup

Create protected GitHub environments named `npm` and `pypi`. Configure the
`prollydb` and `prollydb-wasm` npm packages to trust the `crabbuild/prolly`
GitHub repository, workflow
`bindings-release.yml`, and environment `npm`. npm may require the package name
to be bootstrapped once by an owner before a trusted publisher can be attached.

On PyPI, create a pending or existing-project trusted publisher for owner
`crabbuild`, repository `prolly`, workflow `bindings-release.yml`, and
environment `pypi`. No long-lived PyPI token is used.

Protect `bindings-v*` and `bindings/go/**/v*` tags so only release maintainers
can create them. The workflow receives `contents: write` only in the jobs that
create Go module tags and upload GitHub release assets.

## Release procedure

1. Update every binding package version and `release-manifest.json` in one
   change. Provider packages must depend on the matching core binding version.
2. Run the local metadata and package gates:

   ```sh
   python3 scripts/check-binding-release.py
   cargo test --locked --manifest-path bindings/uniffi/Cargo.toml --target-dir target
   npm --prefix bindings/node ci
   npm --prefix bindings/node run build:native:release
   npm --prefix bindings/node run typecheck
   npm --prefix bindings/node test
   npm --prefix bindings/node run test:package
   cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
   (cd bindings/go && go test -tags prolly_dev ./...)
   ```

3. Merge only after `Language bindings required` passes. That workflow checks
   metadata, native Rust/UniFFI, Node, packed Node installation, Python wheels,
   Go, JVM, Ruby, Swift, and browser WASM.
4. Create and push the manifest tag, for example:

   ```sh
   git tag -a bindings-v0.1.0 -m "Release Prolly bindings 0.1.0"
   git push origin bindings-v0.1.0
   ```

The release workflow validates that the tag exactly matches the manifest,
builds every supported native target, install-tests package artifacts, publishes
npm and PyPI through OIDC, creates the required subdirectory tags for all Go
modules, and uploads checksummed artifacts to the matching GitHub release.

Never reuse or move a published release tag. Registry versions and Go module
versions are immutable; fix a failed or bad release with a new version.
