# prolly-store-sqlite

SQLite storage adapter for [`prolly-map`](https://crates.io/crates/prolly-map).

```toml
[dependencies]
prolly-map = "0.1"
prolly-store-sqlite = "0.1"
```

```rust
use prolly::{Config, Prolly};
use prolly_store_sqlite::SqliteStore;

let store = SqliteStore::open("app.prolly.sqlite")?;
let map = Prolly::new(store, Config::default());
# Ok::<(), Box<dyn std::error::Error>>(())
```
