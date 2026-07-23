# redb versus SQLite at one million records

This benchmark runs the same deterministic prolly workload against redb and
SQLite:

- append 1,000,000 records in 50,000-record batches and publish the root;
- close and reopen the database;
- perform 10,000 deterministic random reads;
- scan all 1,000,000 records;
- apply and publish 10,000 deterministic updates;
- verify the root, length, selected values, and checksums throughout.

Run an interleaved three-repetition matrix:

```bash
benchmarks/redb-sqlite-1m/run-matrix.sh 3
```

Run one adapter directly:

```bash
cargo +1.89.0 run --release \
  --manifest-path benchmarks/redb-sqlite-1m/Cargo.toml -- redb 1

cargo +1.89.0 run --release \
  --manifest-path benchmarks/redb-sqlite-1m/Cargo.toml -- sqlite 1
```

Each successful run prints one `RESULT` CSV record containing the adapter,
repetition, build time and throughput, build file size, random-read time and
throughput, scan time and throughput, update time and throughput, final file
size, and validation checksums.

Databases are created beneath the operating system's temporary directory and
deleted after each run. Set `PROLLY_BENCH_DIR` to choose another directory or
`KEEP_DB=1` to retain the generated databases for inspection.

The redb profile uses immediate durability, a 192 MiB page cache, LZ4 node
encoding, and no decoded-node cache. SQLite enables WAL and retains its
platform-default synchronous setting.
