# prolly-store-sqlite

SQLite storage adapter for [`prolly-map`](https://crates.io/crates/prolly-map).

```toml
[dependencies]
prolly-map = "0.2"
prolly-store-sqlite = "0.1"
```

```rust
use prolly::{Config, Prolly};
use prolly_store_sqlite::SqliteStore;

let store = SqliteStore::open("app.prolly.sqlite")?;
let map = Prolly::new(store, Config::default());
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Durable semantic RAG example

[`examples/semantic_rag.rs`](examples/semantic_rag.rs) builds a native
`ProximityMap` over six support-documentation chunks, publishes its descriptor
as the SQLite named root `rag/corpus/main`, and performs exact cosine retrieval.
Run it twice against the same database to see initial construction followed by
a process-independent reopen:

```sh
rm -f ./target/semantic-rag.sqlite*

cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- \
  ./target/semantic-rag.sqlite password-reset

cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- \
  ./target/semantic-rag.sqlite lost-2fa
```

The example prints three ranked chunks with stable keys and source citations,
then renders an offline `<context>` block ready to pass to an LLM. It does not
call an embedding or generation service.

The checked-in fixture uses synthetic, precomputed, normalized vectors with
1,536 dimensions so it has the storage shape of a hosted embedding while
remaining deterministic, credential-free, and fully offline. Replace
`embedding_for_query` and corpus ingestion with the embedding provider used by
your application; keep the model identifier and dimensions in the named-root
metadata so incompatible indexes fail closed on reopen.
