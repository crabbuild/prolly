# Prolly PostgreSQL store for Python

This package implements the shared asynchronous store protocol with Psycopg 3.
It borrows an open `psycopg_pool.AsyncConnectionPool`; the application opens
and closes the pool.

```python
from psycopg_pool import AsyncConnectionPool
from prolly_store_postgres import PostgresStore

pool = AsyncConnectionPool(connection_string, open=False)
await pool.open()
store = PostgresStore(pool)
await store.initialize_schema()
```

Run the provider check from the repository root with `PROLLY_POSTGRES_URL` set.
