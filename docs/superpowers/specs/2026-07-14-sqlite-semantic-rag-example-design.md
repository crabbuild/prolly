# SQLite Semantic RAG Example Design

**Date:** 2026-07-14

## Objective

Add a production-shaped, fully offline semantic document retrieval example that
uses `ProximityMap` for exact cosine search and `SqliteStore` for durable local
storage. The example must build and publish a corpus on first use, reopen the
same content-addressed index on later process runs, return ranked citations,
and assemble an LLM-ready context block without calling a network service.

## Location and invocation

The runnable example belongs to the SQLite adapter package because
`prolly-store-sqlite` depends on the core `prolly-map` crate. Adding the adapter
as a root-package development dependency would create a dependency cycle.

Files:

- `stores/prolly-store-sqlite/examples/semantic_rag.rs`
- `stores/prolly-store-sqlite/examples/data/semantic_rag_embeddings.json`
- `stores/prolly-store-sqlite/Cargo.toml`
- `stores/prolly-store-sqlite/README.md`
- `README.md`

Representative invocation:

```sh
cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- \
  ./target/semantic-rag.sqlite password-reset
```

The database path and offline query name are required positional arguments.
Usage errors list all supported query names.

## Corpus and embeddings

The fixture contains a small support-documentation corpus covering account
recovery, password reset, two-factor-authentication recovery, API-key rotation,
and billing. Each chunk has a stable key with this shape:

```text
tenant/acme/docs/<document>/<chunk>
```

Each `ProximityRecord` contains:

- a 1,536-dimensional finite, approximately unit-normalized vector;
- a JSON value with `title`, `section`, `source`, and `text` fields;
- a stable key used for tenant-scoped prefix filtering and citation identity.

The checked-in fixture records an explicit demo embedding-model identifier,
`dimensions: 1536`, document-chunk vectors, and vectors for the supported
offline queries. The identifier describes the fixture contract and does not
claim that a live hosted embedding request occurs. A clearly named query
embedding function is the integration seam applications replace with their
embedding provider.

Startup validates fixture uniqueness, dimensions, finite values, and unit norm
before construction or search. It rejects inconsistent document and query
vectors rather than silently correcting them.

## Durable lifecycle

The example opens `SqliteStore` with WAL enabled, a five-second busy timeout,
and the adapter's durable synchronous configuration. It uses
`rag/corpus/main` as the named content-root manifest.

If the named root is absent, the example:

1. validates the fixture;
2. builds a cosine `ProximityMap` from the document chunks;
3. runs the full structural verifier;
4. publishes the descriptor as a `TypedContentRoot::proximity_descriptor`;
5. stores corpus version, embedding identifier, and dimensions in manifest
   metadata.

If the root exists, the example:

1. loads the manifest;
2. checks the object kind and embedding metadata;
3. reopens the persisted `ProximityMap` from its descriptor CID;
4. verifies the reopened index before serving the query;
5. reports that the existing corpus was reopened rather than rebuilt.

Corrupt or incompatible persisted state is never rebuilt automatically. The
operator must remove the demo database explicitly before replacing it.

## Retrieval and output

The selected offline query resolves to its checked-in 1,536-dimensional
embedding. Retrieval uses:

- `DistanceMetric::Cosine`;
- exact search with `k = 3`;
- `ProximityFilter::Prefix(b"tenant/acme/docs/")`;
- deterministic `(distance, key)` result ordering.

For each neighbor, the example decodes the JSON value and prints rank, cosine
distance, title, section, source, and a short excerpt. It then renders a final
`<context>...</context>` block containing numbered source citations and chunk
text. Answer generation is intentionally omitted so the example remains
offline and does not imply that retrieval alone generated an answer.

## Errors

The CLI returns actionable errors for:

- missing arguments or an unknown query name;
- malformed fixture JSON or duplicate identifiers;
- wrong vector dimensions, non-finite values, or non-unit vectors;
- a named root with the wrong content kind;
- corpus-version, model-identifier, or dimension mismatch;
- missing or corrupt persisted content;
- failed ProximityMap structural verification;
- malformed chunk metadata stored in a result value.

## Testing and verification

Implementation follows a red-green-refactor sequence. The example keeps its
pure fixture validation, query lookup, and context rendering functions
testable. Tests cover:

- rejection of invalid vector dimensions and normalization;
- rejection of unknown query names;
- deterministic top-three retrieval for a known query;
- citation and context formatting;
- first-run build/publication followed by drop, SQLite reopen, manifest load,
  ProximityMap load, verification, and identical retrieval.

Required verification commands:

```sh
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml --example semantic_rag
cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- ./target/semantic-rag.sqlite password-reset
cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- ./target/semantic-rag.sqlite lost-2fa
cargo clippy --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- -D warnings
cargo fmt --all -- --check
```

The two process runs must show initial construction followed by durable reopen.
The existing ProximityMap and SQLite adapter test suites remain green.

## Non-goals

- Calling an embedding or generation API.
- Claiming the fixture vectors came from a specific hosted provider.
- Implementing tokenization, prompt truncation, or an LLM client.
- Adding approximate sidecars such as PQ or HNSW.
- Adding corpus refresh, migrations, or concurrent publication to this focused
  example.
