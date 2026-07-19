# Node SQLite remote store

`@trail/prolly-store-sqlite` implements store protocol version 2 with an
application-owned `better-sqlite3` database.

```ts
import Database from "better-sqlite3";
import { RemoteAsyncProllyEngine } from "@trail/prolly-node/remote-async";
import { SqliteStore } from "@trail/prolly-store-sqlite";

const database = new Database("prolly.db");
const store = new SqliteStore(database);
await store.initializeSchema();
const engine = await RemoteAsyncProllyEngine.open(store);
```

Schema initialization is explicit and idempotent. The adapter uses the exact
`prolly_nodes`, `prolly_hints`, and `prolly_roots` `WITHOUT ROWID` tables used
by the Rust SQLite store. Batch writes, node-plus-hint writes, root CAS, and
strict commits use immediate SQLite transactions.

The injected database remains owned by the application. `store.close()` stops
new adapter work and drains queued work, but never closes the database.
`better-sqlite3` database objects cannot be transferred to a worker thread, so
this borrowed-client constructor serializes synchronous calls on the owning
JavaScript thread. Use a dedicated Node worker for the whole application when
SQLite latency must be isolated from an event loop.
