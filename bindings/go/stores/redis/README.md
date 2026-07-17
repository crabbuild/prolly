# Go Redis store

This module uses `go-redis/v9` and the Rust adapter's binary key layout:
`prolly:node:`, `prolly:root:`, and length-delimited `prolly:hint:` keys.
Atomic batches use `MULTI/EXEC`; root CAS and strict transactions use Lua.
