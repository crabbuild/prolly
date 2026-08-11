# Versioned DynamoDB Client Compatibility

This reference defines the compatibility contract for the in-process Rust
client. It does not describe or require a DynamoDB-compatible HTTP service.

## Release line

| Component | Initial supported version | Policy |
| --- | --- | --- |
| `prolly-dynamodb-client` | `0.1.0` | pre-1.0 API; SemVer minor releases may change source API |
| `prolly-dynamodb-core` | `0.1.0` | never bypass through application code |
| `prolly-store-dynamodb` | `0.6.0` | exact provider behavior is part of qualification |
| `prolly-map` | `0.7.0` | canonical tree format is negotiated durably |
| `aws-sdk-dynamodb` | `1.73.0` exactly | official public input/output types are re-used |
| `aws-lc-rs` | `1.17.3` exactly | qualified rustls provider; target/TLS matrix required before upgrade |
| Rust | `1.91.1` | declared MSRV for client, core, provider, and admin crates |

Rust 1.91.1 compilation is proven for the declared target set:

- `aarch64-apple-darwin` on the native host;
- `aarch64-unknown-linux-gnu` with GCC 15.2.0 cross-linking;
- `x86_64-unknown-linux-gnu` with GCC 15.2.0 cross-linking.

Every target passes locked root minimal/default/Tokio checks plus provider,
core, client, and admin all-target/default/no-default-feature checks. This is
compile/link evidence, not execution evidence for cross-built Linux binaries.
Linux runtime tests and hosted AWS smoke remain release gates. Other Rust
targets are unsupported until added to this list and qualified by the same
matrix.

The only supported TLS configuration is the AWS SDK's rustls path with exact
`aws-lc-rs 1.17.3` and `aws-lc-sys 0.43.0`. There is no advertised alternate
TLS feature set; introducing one requires its own compile, link, provider, and
hosted-service qualification.

The local qualification command is:

```bash
./scripts/verify_dynamodb_client_matrix.sh --toolchain 1.91.1
```

It checks root-library minimal/default/Tokio configurations,
provider/core/client default and no-default-feature all-target configurations,
the admin binary, and the exact SDK/TLS dependency inputs. Passing this host
matrix does not substitute for cross-target linking or hosted AWS smoke tests.
All checks use the existing lockfiles. The verifier fails early when the exact
Rust target is not installed, and records the Rust/Cargo and configured linker
versions.

Cross-target invocations use the matching compiler variables, for example:

```bash
rustup target add --toolchain 1.91.1 aarch64-unknown-linux-gnu
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar \
./scripts/verify_dynamodb_client_matrix.sh \
  --toolchain 1.91.1 --target aarch64-unknown-linux-gnu
```

The client currently has one feature set. There is no reduced semantic build:
default and minimal features are identical. Tokio is caller-owned. The crate
does not create a runtime, install a global tracing subscriber, or own the AWS
client's transport lifecycle. Full create/put/get/history/drop lifecycles are
proven against DynamoDB Local on both a caller-owned current-thread runtime and
a caller-owned two-worker multi-thread runtime.

## Durable namespace format

The only writable database format in this release is format `12`, logical
protocol `1.0`. Open compares the complete fixed-width format record, including
canonical item/key/catalog/commit codec digests, tree-format digest, provider
publication mode, blob inline threshold, and reader/writer versions.

Compatibility is deliberately fail-closed:

- format-12 binaries may share a namespace only when their complete negotiated
  records are equal;
- a publication-mode, tree-config, codec, or blob-policy difference rejects
  `Client::open` before logical operations;
- formats 1 through 11 are not supported migration sources or downgrade
  targets by this release;
- no automatic format upgrade, downgrade, repair, or archive conversion exists;
- an older package may be rolled back only when it produces the exact same
  format-12 record and passes the same fixtures.

The `minimum_reader_version` and `minimum_writer_version` fields are persisted,
but this release does not use them to weaken exact equality. A future
backward-compatible reader or migration must add frozen fixtures, mixed-version
tests, and an explicit state transition before advertising a wider range.

The writable format-12 fixture lives in the repository at
`extensions/dynamodb/core/tests/fixtures/database-format-12.json` and in the published
core source archive at `tests/fixtures/database-format-12.json`. The retained
format-10 and format-11 fixtures are historical decode guards, not supported
writable or migration formats. Conformance tests require
the current codec identities to reproduce its exact bytes and require those
bytes to decode to the same semantic record. Contract tests replace each
durable field independently—including both older and newer format versions—and
prove that open fails before logical operations. Envelope tests also reject a
truncated record, trailing data, invalid magic, unknown record-envelope
version, and unknown publication mode. These tests prove exact-format-12
negotiation for this source revision; they do not substitute for running an
independently built historical package in a rolling deployment.

`Client::capabilities()` publishes the complete negotiated record as
`database_format_record_hex`. Rolling qualification compares this exact value
rather than reconstructing compatibility from a subset of fields.

## Dependency policy

The AWS SDK dependency is initially exact because its generated public model
types are part of this crate's source-facing API. Upgrade it only in a reviewed
release that runs fluent compile fixtures, official-input tests, DynamoDB Local
differential tests, and a clean downstream package build.

The provider also pins `aws-lc-rs = 1.17.3`. Resolution to 1.18.0 selected
`aws-lc-sys 0.44.0`, which produced an invalid empty object under the supported
Apple clang toolchain. This pin is part of the qualified TLS dependency set;
change it only with the complete host/target/TLS compile and link matrix.

The core and store dependencies carry both path and version requirements for
repository development and Cargo packaging. A registry release must publish in
dependency order: `prolly-map`, `prolly-store-dynamodb`,
`prolly-dynamodb-core`, then `prolly-dynamodb-client` and the optional admin
binary. Consumers must not patch only one member to an unqualified revision.

Before publication, run the clean package-content consumer check from a clean
worktree:

```bash
./scripts/verify_dynamodb_client_packages.sh
```

For local pre-commit evidence only, pass `--allow-dirty` (or set
`PROLLY_PACKAGE_ALLOW_DIRTY=1`) to run the same
archive/extract/downstream-compile flow from an intentionally dirty worktree.
Archive SHA-256 values are emitted for later provenance attestation; the script
itself does not publish or sign artifacts. It also compiles every core/client
target from the extracted archives and verifies that the packaged core and
client canonical fixtures are byte-identical.

## API stability

The concise versioned extensions are the intended names:

- `client.table(name).at(version)` for a pinned snapshot;
- `table.versions()`, `table.diff(from, to)`, and `table.restore(target)`;
- `table.indexes(desired)` for index reconfiguration;
- `client.workers()` for explicit stream, TTL, and maintenance workers.

There are no `at_version`, `at_versions`, or service-wrapper APIs. Lower-level
core method names are implementation primitives and are not a substitute for
the client compatibility surface.

The source-compatibility policy covers error categories, builder methods,
metadata and capability JSON shapes, worker configuration identities, MSRV,
and dependency alignment. Persisted bytes and durable identities are separately
compatibility-sensitive and require format negotiation to change.

The reviewed Rust surface is frozen in `public-api.txt`. It contains 1,216
signature and trait-implementation lines, including `Send`, `Sync`, and derived
trait contracts; only generic blanket implementations are omitted. The
baseline was produced by `cargo-public-api 0.52.0` with
`nightly-2026-06-19` and has SHA-256
`04dc620779525275fb5c9c7ab381819a7ff97e2e8210b130dae909ce1b6ac648`.
CI runs `scripts/verify_dynamodb_client_public_api.sh`, and package verification
requires the exact baseline to be present in the client archive. Any API diff
fails until its SemVer and migration impact is reviewed and the baseline is
explicitly regenerated with `--update`; updating it is not itself approval.

For the `0.1.x` line, patch releases must be source-compatible. Before 1.0, a
reviewed minor release may make a breaking source change only with explicit
migration notes and a new baseline. Durable format compatibility remains an
independent fail-closed contract and cannot be weakened by a SemVer increment.

The pre-release GC plan gained canonical `protected_trees`,
`scanned_blob_nodes`, and `scanned_values` work counters during scale
qualification. This is an additive API change but a source break for exhaustive
`GcPlan` struct literals and a canonical-plan identity change. Regenerate any
unexecuted draft GC plan with the current client. The bounded snapshot catalog,
current-only commit roots, and append-only blob registry are part of persisted
database format 12; they are not readable through format-11 clients.

Runtime controls are deliberately outside persisted format identity.
`ClientBuilder::logical_retry_limit` counts retries after the first logical
attempt, defaults to seven, accepts zero through 63, and rejects larger values
before provider access. `node_cache_max_nodes` and `node_cache_max_bytes`
configure the process-local decoded-node cache; zero disables it and the
default retained serialized-node weight is 64 MiB. Both cache ceilings apply when configured,
although temporary correctness pins may exceed them until unpinned. The
effective controls are serialized in `Client::capabilities()` so deployment
evidence can record them without treating them as durable compatibility fields.
Clones of one client share runtime-only write admission before speculative tree
work. Admission is neither persisted nor shared by independently opened clients
or processes; those writers continue to coordinate through provider CAS and
bounded logical retries.

## Numeric representation

Numbers preserve exact base-10 value and never pass through binary
floating-point. Equivalent input spellings are normalized to DynamoDB's
documented variable-length form with leading/trailing zeroes removed. The
client therefore returns canonical `AttributeValue::N` strings. DynamoDB Local
can preserve arithmetic scale in an immediate `UpdateItem` return image (for
example `1.00` or a scale-bearing zero); differential tests compare those
emulator spellings by exact decimal value and separately require the client's
canonical output. Applications must compare numeric values as numbers, not use
number-string scale as a financial-significance field; store explicit scale or
currency metadata when it is legally meaningful.
