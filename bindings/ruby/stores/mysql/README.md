# Prolly MySQL store for Ruby

This provider borrows a caller-owned `Mysql2::Client`, uses prepared binary
parameters, and preserves the Rust adapter's `VARBINARY`/`LONGBLOB` tables.
Root CAS and conditional transactions use MySQL named locks.
