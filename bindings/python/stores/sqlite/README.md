# Python SQLite store

`trail-prolly-store-sqlite` implements async store protocol major 1 over a
caller-owned `sqlite3.Connection`. Open the connection with
`check_same_thread=False`, inject a bounded executor if desired, and call
`await store.initialize_schema()` explicitly. Closing the adapter never closes
the connection or executor.
