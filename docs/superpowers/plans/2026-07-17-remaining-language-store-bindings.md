# Remaining Language Store Bindings Implementation Plan

**Goal:** Complete the version-1 asynchronous store protocol across Python,
Ruby, Swift, and browser/WASM using idiomatic provider libraries, while keeping
Ruby/Cosmos DB and Swift/Cosmos DB or Spanner explicitly unsupported.

**Architecture:** Regenerate the UniFFI async foreign-store bridge for Python,
Ruby, and Swift. Keep every provider SDK in a separate language-native package
or product, inject caller-owned clients/resources, reuse the normative Rust
physical layout, and run the shared conformance behavior. Browser stores use
the logical protocol directly with IndexedDB, OPFS, and PGlite; no HTTP gateway
is introduced.

## Task 1: Regenerate and verify async bridge glue

- Regenerate checked-in Python, Ruby, and Swift UniFFI sources from the current
  `prolly-bindings` library with the provenance-pinned toolchain.
- Restore documented local-library lookup adaptations.
- Add one in-memory foreign async store smoke test per language and verify that
  it can open an `AsyncProllyEngine`.

## Tasks 2-8: Provider-first vertical slices

Implement and verify one provider across every supported remaining language
before moving to the next provider:

1. SQLite for Python, Ruby, and Swift.
2. PostgreSQL for Python, Ruby, and Swift.
3. MySQL for Python, Ruby, and Swift.
4. Redis for Python, Ruby, and Swift.
5. DynamoDB for Python, Ruby, and Swift.
6. Cosmos DB for Python; keep Ruby and Swift explicitly unsupported because
   Microsoft publishes no supported SDK for either language.
7. Spanner for Python and Ruby; keep Swift explicitly unsupported because
   Google publishes no supported Swift SDK.

Prefer native async SDKs. Offload unavoidable synchronous SDK operations to a
caller-selected executor without blocking an event loop. Each slice includes
conformance, contention, cancellation, ownership, limits, and exact-layout
tests before its provider-first commit.

## Task 9: Browser-native stores

- Add IndexedDB, OPFS, and PGlite implementations under `bindings/wasm/stores`.
- Preserve binary keys, transaction semantics, ordered batch reads, and
  persistence across reopen without exposing privileged cloud credentials.

## Task 10: Matrix and aggregate release gate

- Extend `compatibility.json` to every supported and explicitly unsupported
  cell.
- Generalize the compatibility verifier to check package metadata and core
  dependency isolation for every language.
- Add aggregate service runners and update operator documentation.
- Run Rust, UniFFI, Go, Node, JVM, Python, Ruby, Swift, and browser verification;
  commit and push each provider-first vertical slice to PR #12.
