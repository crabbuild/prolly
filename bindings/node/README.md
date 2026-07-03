# Prolly Node/TypeScript Binding

This package exposes the Rust `prolly-bindings` facade through a Node-API
module, typed TypeScript declarations, and Promise-based async wrappers.

See `COOKBOOK.md` for Node and TypeScript application patterns covering
SQLite-backed indexes, Promise wrappers, prefix queries, ordered boundary helpers, paging with range cursor constructors, reverse and prefix-reverse pages, cursor windows, cursor-resumed
diffs, typed structural diff cursors, named-root manifest metadata listing, merge callbacks, verifiable single-key, multi-key, range, cursor-page, diff-page, and prefix proofs with
portable bundle bytes, proof-bundle introspection/routing summaries, one-shot proof-bundle verification, HMAC-authenticated envelopes, and one-shot authenticated proof-bundle verification, retained named-root GC with retention policy constructors, large values, blob GC, and
JavaScript-owned custom stores.

The native and async engines expose `parallelBatch`, `parallelBatchWithStats`,
`batchWithStats`, and `appendBatchWithStats` for parallel mutation application plus route/write
telemetry. `defaultParallelConfig()` returns the Rust default parallel-batch
configuration for Node callers. Native helpers also expose `defaultConfig`,
encoding constructors, `treeConfig`, `largeValueConfig`, `parallelConfig`, and
`parallelConfigSequential`; the TypeScript facade mirrors those helper shapes
for non-native callers.
Native and async engines expose typed `collectStats`/`statsDiff` and
`debugTree`/`debugCompareTrees` objects alongside the existing JSON strings.
Merge explanations expose typed trace events via `trace.events` while retaining
`traceJson` for compatibility.
Native and async engines also expose `exportSnapshot`/`importSnapshot`, plus
`snapshotBundleToBytes`/`snapshotBundleFromBytes`,
`snapshotBundleDigest*`, `snapshotBundleSummary*`, and `verifySnapshotBundle*`, for complete portable
tree bundles with reachable node bytes and pre-import verification.

Key helpers include `prefixEnd`, `prefixRange`, numeric key encoders,
`encodeSegment`, `keyFromSegments`, `keyFromPrefixedSegments`,
`decodeSegments`, `debugKey`, and Rust boundary checks.
Native codec helpers include versioned-value byte round trips plus schema
match/require guards, and value-ref stored-byte decode plus inline-escape
checks. Blob helpers include direct blob-ref byte validation for content
integrity checks outside the store. Hint helpers include exact-key, prefix, and
range changed-span constructors. Batch helpers include upsert/delete mutation
constructors. Merge helpers include normal and CRDT
resolution constructors plus built-in resolver helper functions for callback
resolvers.

Local smoke test:

```sh
npm --prefix crates/prolly/bindings/node ci
npm --prefix crates/prolly/bindings/node run build:native
npm --prefix crates/prolly/bindings/node test
```

## Source Tree Layout

The Node binding has two layers. The TypeScript facade in `src/` provides the
public API and native loading helpers. The Rust Node-API crate in `native/`
builds the platform-specific `.node` addon. Example programs live under
`examples/` and are written as direct executable TypeScript files. Each scenario
contains its own imports, helper functions, setup, assertions, and output.

Important files:

- `src/native.ts` loads the native addon and defines native-facing types.
- `src/async.ts` exposes Promise-based wrappers.
- `src/index.ts` is the package entrypoint.
- `examples/*.ts` contains standalone scenarios.
- `test/*.test.ts` covers native parity, fixtures, and async wrappers.

## Running Examples

Install dependencies and build the native addon:

```sh
npm --prefix crates/prolly/bindings/node ci
npm --prefix crates/prolly/bindings/node run build:native
```

Run one scenario:

```sh
node crates/prolly/bindings/node/examples/local-first-state.ts
```

Run all scenarios:

```sh
node crates/prolly/bindings/node/examples/cookbook-scenarios.ts
```

The run-all launcher starts each scenario as a separate Node process. This keeps
the individual files independently readable and prevents the launcher from
becoming another combined implementation.

## API Style

Keys and values use `Uint8Array`. Application code should centralize text,
binary, and structured codecs rather than mixing `TextEncoder` calls throughout
the codebase. Keep key layouts prefix-friendly so range scans and cursor pages
map naturally to application screens and background jobs.

Use the native engine for server-side code, CLIs, and tests that need the full
Rust-backed surface. Use the async wrapper when orchestration is already
Promise-based. The async wrapper does not change data consistency semantics; it
only gives JavaScript code a familiar scheduling model.

## Native Loading

The source tree expects a locally built addon. The release package should ship
prebuilt `.node` artifacts for supported platforms or document exactly how
callers build them. When debugging load failures, check the Node version, CPU
architecture, operating system, and whether the addon was built against the same
package sources.

## Merge, Proofs, And Snapshots

The Node binding is useful for agent services, local indexing tools, and
developer automation because it exposes merge traces, cursor-resumed diffs,
proof bundles, HMAC-authenticated envelopes, and snapshot bundles in a
TypeScript-friendly shape. Use proof helpers when data crosses a trust boundary.
Use snapshot helpers when moving complete trees between stores or test fixtures.

For conflict handling, use built-in resolver names for simple policies and
callback resolvers for domain values. Callback resolvers must be deterministic.
Do not call remote services or read wall-clock time from inside a resolver.

## Large Values And Storage

Large document text, file contents, transcript bodies, and generated artifacts
should be stored through blob helpers rather than inline leaves. Pick an inline
threshold deliberately. The examples show how to put large values, read them
back, inspect value refs, and combine named roots with durable stores.

SQLite examples are intended for local development and embedded applications.
Server applications should decide explicitly whether SQLite files are per
workspace, per tenant, per user, or per background job.

## Testing Strategy

Run the full package tests before changing native bindings:

```sh
npm --prefix crates/prolly/bindings/node test
```

Add small scenario-style tests when an application pattern regresses. Keep
fixture tests focused on cross-language byte compatibility and keep async tests
focused on Promise behavior, cancellation boundaries, and error propagation.

## Packaging Notes

The package should not assume a developer checkout in production. Release
artifacts should include platform-specific native addons, package metadata that
states the supported Node versions, and a fallback story for unsupported
platforms. If a consumer builds from source, document the Rust toolchain and
Node-API requirements.

## Troubleshooting

- `ERR_MODULE_NOT_FOUND` usually means the example was run from an unexpected
  package state or dependencies were not installed.
- Native addon load failures usually mean `npm run build:native` has not run for
  the current Node version and platform.
- `Uint8Array` comparison bugs usually come from comparing object identity.
  Convert to text, hex, or use byte-by-byte comparisons in assertions.
- Empty cursor pages usually indicate an incorrect start cursor or end bound.
  Reproduce with a short sorted key list before debugging the full tree.
