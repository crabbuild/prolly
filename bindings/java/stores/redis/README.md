# Java Redis store

Java factory for the shared Lettuce Redis adapter:

```java
var connection = client.connect(ByteArrayCodec.INSTANCE);
var store = RedisStores.from(connection.async(), "{prolly}:".getBytes());
```

The store borrows the binary Lettuce commands and never closes the connection or client. For primary storage, configure and monitor AOF/`appendfsync`, test recovery, and keep independently restorable backups. Use a hash-tagged prefix with Redis Cluster.
