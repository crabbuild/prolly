# `@trail/prolly-store-redis`

Redis implementation of the shared async store protocol for Node.js. It accepts an already-connected official `redis` client and never closes or destroys that caller-owned client.

```ts
import { createClient } from "redis";
import { RedisStore } from "@trail/prolly-store-redis";

const client = createClient({ url: process.env.REDIS_URL });
await client.connect();
const store = new RedisStore(client, { keyPrefix: Buffer.from("prolly:") });
```

Keys and values remain binary-safe. Atomic batches, root compare-and-swap, and strict transactions execute in Lua. Redis Cluster users must choose a hash-tagged prefix such as `{prolly}:` so every key used by one Lua transaction occupies the same slot.

## Durability

Redis defaults are not sufficient for primary storage. Enable AOF persistence and select an `appendfsync` policy whose acknowledged-write loss window matches your requirements (`always` is the strictest option). Monitor persistence health, test restart recovery, and maintain independently restorable backups; replication is not a backup. Snapshot and AOF rewrite settings should be validated under the application's production write load.
