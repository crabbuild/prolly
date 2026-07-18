# Ruby Redis store

`trail-prolly-store-redis` adapts a caller-owned `Redis` client to Prolly's
version-1 asynchronous store bridge. Closing the adapter does not close the
client.

Keys use interoperable binary `node:`, `root:`, and `hint:` families under the
configured prefix. Lua scripts provide atomic CAS and transactions. Redis
Cluster users must choose a prefix with a shared hash tag, such as
`"prolly:{my-store}:"`, so multi-key scripts use one slot.

For durable primary storage, enable AOF with an appropriate `appendfsync`
policy and maintain tested backups. Otherwise treat Redis as a cache.
