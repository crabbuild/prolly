# IndexedMap Secondary Indexes

`IndexedMap` is the synchronous secondary-index coordinator for one
authoritative ordered collection. It supports sparse, non-unique and
multi-valued indexes with `KeysOnly`, `Include`, and `All` projections.

This document describes the only supported persisted layout. There is no
compatibility reader or suffix-named replacement format. A deployment using an
older indexed layout must rebuild or import into an empty destination and cut
over.

## Atomic state model

One named root owns the complete visible collection state:

```text
\0prolly/indexed-collection/<hex-source-map-id>/state
```

The immutable tree selected by that root contains the source ID, policy,
current snapshot record, retained snapshot records, descriptors, active and
retired generations, and durable pins. Each snapshot record names one exact
source tree and the exact index trees derived from it.

A mutation or maintenance operation:

1. loads that one state root;
2. derives immutable source, index, snapshot, and state objects;
3. writes every immutable node;
4. confirms that every candidate root is readable;
5. performs one compare-and-swap of the collection root.

Only the last step changes visibility. Readers therefore observe the complete
old state or the complete new state, never a mix. Conflicts reload the entire
state and retry under a finite operation budget.

Raw head-changing `VersionedMap` operations are fenced once a canonical
indexed-collection root exists. Reads remain possible, while mutations must go
through `IndexedMap`.

## Store profiles

`IndexedStoreProfile::Verification` is for correctness tests and local
experiments. `MemStore`, `FileNodeStore`, PGlite, redb, RocksDB, and SlateDB
deliberately have this profile; it is not a production durability claim.

`IndexedStoreProfile::Production` is accepted only when an adapter declares
and validates cross-handle coordination, immutable-write visibility, durable
acknowledgement, and a GC-safety mechanism. The file-backed SQLite adapter is
the currently qualified production adapter when durable synchronous writes are
enabled. Opening a production coordinator on a verification store fails
closed.

## Runtime definitions

An extractor receives the primary key and exact stored source bytes and emits
zero or more entries:

```rust
use prolly::{SecondaryIndex, SecondaryIndexRegistry};

let by_tag = SecondaryIndex::non_unique(
    "by-tag",
    1,
    "app.users.by-tag/1",
    |_primary_key, value| {
        Ok(value
            .split(|byte| *byte == b',')
            .filter(|tag| !tag.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    },
)?;
let registry = SecondaryIndexRegistry::new().register(by_tag)?;
# Ok::<(), prolly::Error>(())
```

The descriptor fingerprint commits the source ID, index name, generation,
extractor identity, projection, per-record semantic limits, and physical
layout. Callback code is not serialized. Every process must register the exact
definition needed by the snapshot it opens, including retained generations.
Extractors must be deterministic, side-effect free, and retry safe.

## Bounded operation model

All production paths have finite typed budgets:

- `MutationBudget` bounds admitted input, derived entries and bytes, accounted
  memory, CAS attempts, and elapsed time.
- `QueryBudget` bounds page size, returned and scanned entries, returned bytes,
  source fetches, accounted memory, and elapsed time.
- `MaintenanceBudget` bounds source and derived work, findings, memory, spills,
  merge fan-in, CAS attempts, and elapsed time.
- `TransferBudget` bounds encoded and decoded bytes, nodes, verification work,
  memory, and elapsed time.

Convenience query methods use finite defaults. Call `query(budget)` for an
explicit query session. Oversized page requests are rejected before
allocation. Build, replacement, repair, and verification share a spillable
sorted-run engine; exceeding memory, run, spill-byte, fan-in, work, or time
limits publishes nothing.

## Query identity

`IndexedSnapshotId` is the content ID of the canonical immutable snapshot
record. This distinguishes multiple generations derived from the same source
tree without coupling historical lookup to a later collection-state version.

Serialized cursors bind all of:

- snapshot-record, source, collection-state, and index versions;
- index name and descriptor fingerprint;
- direction and logical bounds;
- a validated physical continuation key.

Changing any field or moving the key outside the physical bounds returns a
typed cursor error. Forward scans resume strictly after the continuation key;
reverse scans resume strictly before it.

## Retention, transfer, and GC

Retention is a new canonical state followed by the same one-root CAS. Durable
snapshot pins prevent retained records from being removed. A pin guard releases
its pin explicitly or on drop.

Bundles contain one canonical state closure and a globally deduplicated,
content-address-ordered node set. Export and import are bounded; import checks
the encoded envelope before decode, verifies hashes, canonical records,
reachability, ownership, definitions, and the selected snapshot, stages
immutable nodes, then publishes one root.

GC marks from one pinned canonical state closure. Sweeping is refused unless
the caller supplies the required quiescence proof. Cache residency is not a
reader lease.

## Health and observability

`health()` reports store profile, selected source/state versions, active
descriptors and roots, structural closure validity, retained snapshot count,
and durable pin count. Complete semantic verification remains an explicit
bounded operation.

Metrics report measured logical work—admitted source mutations, extracted
records, emitted terms, projected bytes, physical entry upserts/deletes,
skipped emissions, retries, builds, verification outcomes, and retained roots.
They do not label estimates as physical node writes. Error codes and retry
advice are structured; default messages redact application keys, terms,
values, bounds, and extractor text.

Run `cargo run --example secondary_index` for an end-to-end verification-store
example. Production services should use a qualified production adapter and
must keep the release-gate workflows green.
