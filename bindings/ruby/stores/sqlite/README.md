# Ruby SQLite store

`trail-prolly-store-sqlite` implements protocol major 1 over a caller-owned
`SQLite3::Database`. Call `initialize_schema` explicitly. The adapter serializes
operations and uses immediate transactions, but never closes the database.
