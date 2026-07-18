# Python Cloud Spanner store

This package adapts a caller-owned `google.cloud.spanner_v1.Database` to the
shared asynchronous store protocol. Blocking SDK calls run in worker threads;
the adapter uses atomic mutation batches and serializable read/write
transactions. Closing it does not close the database or its owning client.

Create the three tables with the exported `SPANNER_DDL` statements before
constructing the adapter.
