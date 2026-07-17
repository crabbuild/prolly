# @trail/prolly-store-postgres

PostgreSQL implementation of the Prolly version 1 asynchronous store protocol.

```ts
import { Pool } from "pg";
import { PostgresStore } from "@trail/prolly-store-postgres";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const store = new PostgresStore(pool);
await store.initializeSchema();
```

The adapter uses the caller's `pg.Pool` and never calls `pool.end()`. `close()`
stops new adapter operations and drains operations already accepted. Abort signals
cancel active PostgreSQL queries with a separate server cancellation request.

The schema is identical to `prolly-store-postgres`: `prolly_nodes(cid BYTEA,
node BYTEA)`, `prolly_hints(namespace BYTEA, key BYTEA, value BYTEA)`, and
`prolly_roots(name BYTEA, manifest BYTEA)`. Schema creation is explicit and
idempotent. Batch writes, node-plus-hint writes, root CAS, and strict commits are
transactional. Root updates use row locks plus transaction-scoped advisory locks
so missing-root CAS is linearizable without serializing unrelated root names.
