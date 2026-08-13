# Cache and scale design

## Goals

- make repeated and historical lookups independent of bucket listings;
- keep memory and background work bounded;
- permit millions or billions of logical files and long commit histories;
- preserve correctness when every cache and advisory index is empty.

![Cache and scale architecture](diagram/prolly-s3-cache-scale-architecture.svg)

## Immutable node cache

The primary cache key is:

```text
(repository identity, tree format identity, content ID)
```

No protocol-version field is required because the repository has one format.
Values contain canonical immutable node bytes. Every cache hit is decoded and
content-hash verified before use.

The client supports:

- bounded in-process memory caching for hot nodes and pack locations;
- optional Foyer memory/disk caching behind the `foyer-cache` feature;
- safe cache persistence across restarts;
- provider reads as the authoritative fallback.

Cache corruption becomes a miss or a validation error; it cannot silently
change repository history.

## Lookup path

```text
branch ref
  → publication event
  → commit/root manifest
  → journal-derived node locator
  → ranged node-pack read
  → payload binding
  → immutable payload
```

The journal-derived index is a Prolly tree of content ID to node-pack range.
Its small mutable head advances behind the publication journal. Normal point
lookups never list the repository namespace.

## Cold start

A process should:

1. open the repository and validate provider attestation;
2. load the branch ref and index heads;
3. open the persistent Foyer cache, if configured;
4. catch indexes up to the current journal generation;
5. optionally prewarm upper tree levels for known hot branches.

Persisting immutable nodes removes the 421+ request cold-traversal pattern seen
in early prototypes. A fully cold cache still performs bounded tree-depth and
index reads, so production qualification must measure actual depth, range-read
latency, hit ratio, and provider throttling.

## Bounded metadata

- **Journal index** maps node and commit graph identities from linked events.
- **Operation index** stores recent retry identities in immutable segments and
  bounds the unindexed journal tail.
- **Ref catalog** uses fixed shards and paginated Prolly trees.
- **Commit sessions** checkpoint mutations in bounded pages.
- **Merge cursors** persist bounded frontier and plan state.
- **Mutable controls** retain a configured number of provider versions.

All foreground structures grow by immutable pages, not by rewriting one
repository-wide document.

## Concurrency and sharding

The natural writer shard is a branch. Each branch has independent authority,
ref state, operation index, and journal progress. This removes the
repository-wide publication mutex while retaining a linear history per branch.

More writers on one hot branch cannot remove its serialization point. Use:

- larger commit-session batches;
- multiple ingestion branches followed by structural merge;
- stable operation IDs and bounded retry/backoff;
- separate repositories when histories do not need atomic relationship.

## Cardinality

Prolly trees do not impose a configured maximum file, commit, or ref count.
Tree depth grows logarithmically, immutable pages are reusable, and cursors
bound each operation.

Practical capacity is still finite. Qualify:

- provider object-count and request-rate quotas;
- storage and retained-unreachable-data growth;
- node-cache size and hit ratio;
- index lag and rebuild time;
- ref-catalog and merge page latency;
- hot-branch CAS conflict rate;
- cost at expected read/write mix.

“Unlimited” therefore means no architecture-wide scalar manifest or in-memory
full scan, not infinite provider resources.

## Eviction and prewarming

Prefer retaining upper tree levels, current branch roots, recent commit graph
nodes, and hot payload bindings. Leaf pages and old branch histories may use
frequency/recency eviction. Foyer cache admission rejects objects larger than
the usable entry space in its configured disk block, including Foyer's
block-index and entry overhead.

Prewarming is advisory and cancellable. A reader remains correct if prewarming
never runs or the cache directory is deleted.

## Remaining production gaps

- production garbage collection for unreachable immutable data;
- published AWS qualification at customer-specific scale and traffic;
- automatic cache sizing from observed working set;
- operational SLOs for index lag and rebuild completion;
- cross-region and disaster-recovery workflows.
