# @trail/prolly-store-mysql

MySQL implementation of the Prolly version 1 asynchronous store protocol.

```ts
import { createPool } from "mysql2/promise";
import { MysqlStore } from "@trail/prolly-store-mysql";

const pool = createPool(process.env.DATABASE_URL!);
const store = new MysqlStore(pool);
await store.initializeSchema();
```

The injected `mysql2/promise` pool remains owned by the caller. `close()` drains
accepted adapter operations but never calls `pool.end()`. Abort signals destroy
only the checked-out connection, which cancels the query and rolls back its
transaction without closing the pool.

The adapter uses the exact Rust `VARBINARY`/`LONGBLOB` schema. Node CIDs longer
than 32 bytes and hint/root keys longer than 255 bytes are rejected before pool
access. InnoDB deadlock victims are retried at the whole-transaction boundary,
so batch writes, node-plus-hint, CAS, and strict commits never partially replay.
