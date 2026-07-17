# Browser IndexedDB store

This package stores protocol nodes, hints, and roots in native IndexedDB object
stores. The adapter borrows its `IDBDatabase`; callers retain connection and
database lifecycle ownership.
