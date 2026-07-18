# Prolly PostgreSQL store for Swift

`PostgresRemoteStore` implements the shared asynchronous store protocol with
PostgresNIO 1.27, the newest release compatible with the package's Swift 5.10
toolchain. It borrows a running `PostgresClient`; the application owns the
client's `run()` task and shutdown.
