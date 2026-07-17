# Prolly MySQL store for Python

This package implements the shared asynchronous store protocol using aiomysql
0.3.2. It borrows a caller-owned `aiomysql.Pool` and preserves the Rust MySQL
adapter's `VARBINARY`/`LONGBLOB` layout.
