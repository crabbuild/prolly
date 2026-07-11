# prolly-store-rocksdb

RocksDB storage adapter for [`prolly-map`](https://crates.io/crates/prolly-map).

```toml
[dependencies]
prolly-map = "0.2"
prolly-store-rocksdb = "0.1"
```

```rust
use prolly::{Config, Prolly};
use prolly_store_rocksdb::RocksDBStore;

let store = RocksDBStore::open("app.prolly.rocksdb")?;
let map = Prolly::new(store, Config::default());
# Ok::<(), Box<dyn std::error::Error>>(())
```
