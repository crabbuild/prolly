# Java Cloud Spanner store

Java entry points for the shared Kotlin Cloud Spanner adapter. Supply a
caller-owned `DatabaseClient` and `Executor`; closing the store cancels its own
work without closing either injected resource.
