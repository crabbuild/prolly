# Ruby Cloud Spanner store

This gem adapts a caller-owned `Google::Cloud::Spanner::Client` to the shared
store protocol. It uses atomic commits and serializable read/write
transactions. Closing the adapter does not close the client.

Create the three tables using `Prolly::SpannerRemoteStore::DDL` before opening
the adapter.
