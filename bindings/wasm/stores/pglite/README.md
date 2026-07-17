# Browser PGlite store

This browser package adapts a caller-owned PGlite database to the shared async
store protocol. Use an `idb://` PGlite data directory for durable browser
storage. Closing the adapter does not close the database.
