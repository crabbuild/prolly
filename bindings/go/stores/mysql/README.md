# Go MySQL store

This adapter uses `database/sql` with `go-sql-driver/mysql` and the exact
`VARBINARY`/`LONGBLOB` tables used by the Rust `prolly-store-mysql` crate.
Root CAS and strict commits use one InnoDB transaction with locking reads.
