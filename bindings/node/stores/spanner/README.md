# `@trail/prolly-store-spanner`

Cloud Spanner implementation of the shared async store protocol using the
official `@google-cloud/spanner` SDK. Construct it with a caller-owned
`Database`; closing the adapter never closes the database or client.

Create the three tables in `SPANNER_DDL` before serving requests. Their names,
columns, primary keys, and raw byte values exactly match the Rust and Go stores.
