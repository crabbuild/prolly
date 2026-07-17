# Python Redis store

`trail-prolly-store-redis` adapts a caller-owned `redis.asyncio.Redis` client
to Prolly's version-1 asynchronous store protocol. Closing the adapter does not
close the injected client.

Keys use the interoperable binary families `node:`, `root:`, and `hint:` under
the configured prefix. CAS and conditional transactions execute atomically in
Redis Lua scripts. For Redis Cluster, choose a prefix containing a shared hash
tag (for example `b"prolly:{my-store}:"`) so multi-key scripts use one slot.

For durable primary storage, enable AOF persistence with an appropriate
`appendfsync` policy and maintain tested backups. Without that deployment
configuration Redis should be treated as a cache or edge store.
