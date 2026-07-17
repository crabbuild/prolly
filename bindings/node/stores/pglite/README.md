# `@trail/prolly-store-pglite`

Browser-native PostgreSQL/WASM implementation of the shared async store
protocol using a caller-owned `PGlite` database. It uses the same three-table
physical layout as the PostgreSQL and Rust stores and never closes the injected
database.

Call `initializeSchema()` before serving requests.
