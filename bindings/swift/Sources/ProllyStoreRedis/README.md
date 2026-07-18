# Swift Redis store

`ProllyStoreRedis` adapts a caller-owned RediStack client to Prolly's shared
asynchronous store protocol. The adapter uses binary-safe raw commands and Lua
scripts for atomic CAS and transactions; closing it does not close the client.

Redis Cluster callers should choose a prefix with a shared hash tag. Durable
primary deployments require AOF, an appropriate `appendfsync` policy, and
tested backups.
