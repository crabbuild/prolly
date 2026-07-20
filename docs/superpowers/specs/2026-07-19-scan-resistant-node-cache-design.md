# Scan-Resistant Node Cache Design

## Problem

The bounded node cache is shared by point reads and range scans. A full scan
currently admits every missed node and can evict the point-read working set.
Conversely, preventing every scan admission also harms cold-start behavior: an
initial validation or application scan no longer populates reusable read nodes.

At five million records, the default 256 MiB cache cannot retain the complete
decoded tree. The fresh/random benchmark therefore records heavy eviction and
miss traffic even after its warm-up phase. Increasing the global default would
hide the symptom while imposing a large memory cost on every application.

## Considered Approaches

1. Increase the default cache to 768 MiB. This restores the measured five
   million-record warm workload but is rejected because it raises the memory
   floor for small and embedded deployments.
2. Add adaptive scan-resistant admission. This is the selected approach because
   cold scans can still populate an empty read cache while scans opened over an
   existing read working set cannot displace it.
3. Replace LRU with TinyLFU or a segmented cache. This may improve mixed
   workloads, but it is deferred until trace-driven evidence justifies the
   additional policy and synchronization complexity.

## Design

The canonical async engine will distinguish two internal cache access modes:

- `Admit`: normal reads retain the existing low-overhead lookup and admit a
  validated miss.
- `ObserveOnly`: scan continuation may reuse an existing cached node without
  promoting it; a validated miss is returned to the cursor but not admitted.

Each read session records whether decoded read nodes existed before it loaded
its root. A cold session uses normal admission, allowing its first scan to warm
the cache. A session opened over a warm read cache, or one that already
performed a point read, uses observe-only access for forward and reverse cursor
advancement. Range seek still uses normal admission for its initial
root-to-leaf path. Point reads, bulk reads, writes, proofs, diffs, and
maintenance operations retain normal cache behavior.

All store reads still perform CID and node-format validation before a node is
returned. Metrics continue to count observed hits, misses, nodes, and bytes;
evictions count only real cache removals.

## Correctness and Performance Requirements

- Sync and async scans must return the same ordered entries as before.
- A scan larger than the bounded cache must not evict a previously hot point
  path after the initial seek path is resident.
- A cold scan must continue admitting nodes for later reuse.
- Cache size and byte limits remain enforced.
- Disabled and unbounded cache modes preserve their current semantics.
- No public API, storage trait, node encoding, or persisted tree identity
  changes.
- The five-million-record fresh/random comparison must be rerun before and
  after the change. Writes must not materially regress.

## Testing

Integration tests build deliberately multi-node trees with small cache limits,
prime the leftmost point path, perform a complete scan, and verify that the
same point path remains cache-resident. The scenario is tested through both
the native async engine and the ready-only sync facade. Existing cache-limit,
pinning, scan, and conformance suites remain regression gates. An unbounded
cache test verifies that cold scans still admit nodes and make a second scan
fully cache-resident.
