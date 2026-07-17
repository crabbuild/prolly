# Kotlin Redis store

This module implements the shared async store protocol over caller-owned Lettuce `RedisAsyncCommands<ByteArray, ByteArray>` created with `ByteArrayCodec`.

```kotlin
val connection = client.connect(ByteArrayCodec())
val store = RedisStore(connection.async(), RedisStoreOptions(keyPrefix = "{prolly}:".encodeToByteArray()))
```

Closing the store does not close the Lettuce connection or client. Atomic batches, CAS, and strict transactions use Lua; Redis Cluster prefixes must include a hash tag so all transaction keys occupy one slot.

For primary storage, enable AOF, choose and validate an `appendfsync` policy, monitor persistence health, test recovery, and maintain independently restorable backups.
