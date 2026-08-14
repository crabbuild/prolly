# Prolly S3 measured performance envelope

Date: 2026-08-13

Provider: local RustFS 1.0.0-beta.10, versioned bucket, Apple Silicon Docker
Desktop, 32-way client concurrency. These numbers are regression evidence for
this machine, not AWS SLO evidence.

## Results

| Workload | Result |
|---|---:|
| 4K first-parent commits plus indexed merge-base selection | 11.56 s; exact base, zero fallback commit visits |
| 10K one-file commits on one branch | 743.9 s, 13.44 commits/s, 339,427 S3 calls, 33.94 calls/commit |
| 10K files, one grouped commit | 2.252 s, 4,441 files/s, 65 S3 calls |
| 10K warm random reads (1K samples) | 1,699 reads/s, p99 28.0 ms, 1.159 MB downloaded |
| 10K full list | 38,994 entries/s, page p99 30.8 ms, 19.2 KB downloaded |
| 10K branch / 100-key sparse diff / merge | 230 ms / 20.7 ms / 329 ms |
| 100K files, ten grouped commits | 25.65 s, 3,899 files/s, 910 S3 calls |
| 100K warm random reads (1K samples) | 1,738 reads/s, p99 25.4 ms, 1.701 MB downloaded |
| 100K full list | 13,876 entries/s, page p99 113 ms, 40.7 MB downloaded |
| 100K branch creation | 98.9 ms, 8.7 KB downloaded |
| 100K snapshot, 100-key sparse diff | 214 ms, 2.52 MB downloaded |
| 100K snapshot, 100-key merge | 1.148 s, 48.3 MB downloaded |
| 500K files, 50 grouped commits | 142.0 s, 3,521 files/s, 7,416 S3 calls |
| 500K full list, opaque snapshot cursor (clean runs) | 26.3–29.1 s, 17.2K–19.0K entries/s, page p99 97–178 ms, 343.8 MB downloaded |
| 500K full list, opaque snapshot cursor (contended runs) | 61.8–75.5 s, 6.6K–8.1K entries/s, page p99 423–631 ms |
| 500K random reads (100 samples, restart-cold process) | 441–545 reads/s, p99 89–105 ms, 15.4–15.7 MB downloaded |
| 500K full list, 512 MiB in-process warm cache | 9.21 s, 54.3K entries/s, page p99 28.4 ms, 607 KB downloaded, 391 MiB RSS |
| 500K random reads, 512 MiB in-process warm cache (1K samples) | 1,469 reads/s, p99 38.6 ms, 1.23 MB downloaded |
| 500K full list, reopened 128 MiB + 2 GiB Foyer cache | 10.59 s, 47.2K entries/s, page p99 31.4 ms, 602 KB downloaded, 168 MiB RSS after list |
| 500K random reads, reopened Foyer cache (1K samples) | 1,467 reads/s, p99 37.2 ms, 1.21 MB downloaded, 192 MiB RSS |
| 500K branch creation | 72–120 ms in clean runs |
| 500K snapshot, 100-key direct-child diff, before / after | 7.42 s, 659 MB, 7,284 calls / 9–12 ms, 22 KB, 4 calls |
| 500K snapshot, 100-key merge, before / after | 17.28 s, 1.37 GB / 474 ms, 88.9 KB |
| 500K snapshot, 10K-key external-delta diff | 83–208 ms, 12.9 KB, 22 calls |
| 500K snapshot, 10K-key external-delta merge, before / after promotion | 29.6 s, 60.4 MB, 879 calls / 8.38 s, 3.53 MB, 232 calls |
| 500K → 1M extension, 50 grouped commits | 220.3 s, 2,270 files/s, 16,075 calls, 2.68 GB uploaded, 763 MB downloaded, 480 MiB peak observed RSS |
| 1M full list, indexed-head barrier + persistent-warm Foyer | 20.92 s, 47.8K entries/s, page p99 33.2 ms, 1.19 MB downloaded, 2,027 GETs, zero PUTs |
| 1M random reads, persistent-warm Foyer (1K samples) | 936 reads/s, p99 76.7 ms, 1.21 MB downloaded |
| 1M branch / 100-key direct diff / merge | 87.7 ms / 8.24 ms / 582 ms |
| 1.01M-object exact payload-pack inventory | 298.4 s, 1,011 restartable pages, 1,004 physical packs, 99.99% utilization |
| 1.01M current / 1.02M version deep fsck | 38.79 s, 2.03M logical references, 1,004 physical packs, 39.36 MB downloaded, 23.25 MB uploaded, 245 MiB RSS |
| 1M resumed pack-aware GC, final scan/sweep segment | 126.1 s, 1.047M reachable logical versions, 16,542 nodes, 91.2K provider versions scanned, 186 exact versions / 4.05 MB deleted, 44 MiB RSS |
| 65 MiB streamed chunk-manifest object | 5.74 s test wall time; nine 8 MiB-or-smaller chunks, zero multipart/spool operations, tail and cross-chunk ranges passed |

The original 500K list failure used the compatibility API that rebuilt a
key-based traversal on every page. The benchmark now uses one immutable,
opaque snapshot cursor and completes all 500 pages without retries. Clean runs
clear the 10K entries/s throughput goal; Docker/host contention can still cut
throughput below that goal and materially widen p99. The traversal downloads
343.8 MB and performs about 8.6K GETs, so request and byte amplification remain
the next listing/cache target.

A restartable branch-index rebuild over 56 publications and 12,404 indexed
nodes completed in 1.42 seconds. The 512 MiB memory-cache pass had 5,231 node
hits and zero misses on its second full traversal. A separately reopened Foyer
process retained the same 5,231 hits and zero misses, reducing metadata download
from 340.3 MB cold to 0.60 MB persistent-warm. Point reads still perform about
three provider GETs each for mutable/ref and payload metadata even when every
Prolly node hits cache; that is now the dominant warm-read request cost.

The 1M extension completed every foreground functional phase. Its first list
overlapped background index catch-up (150 PUTs), so the benchmark now executes
and reports an explicit indexed-head barrier before resetting foreground list
metrics. The clean rerun found zero lag in 15.3 ms, then listed 1M entries with
2,027 GETs and zero PUTs. Peak observed RSS was 213 MiB during the read/branch
phases of that reopened 128 MiB-memory Foyer process.

The pack inventory covered 1,010,100 live logical objects (the 1M file set plus
10,100 accumulated branch-probe objects), 35,190,680 logical bytes, and 1,004
physical immutable packs totaling 35,192,387 bytes. All objects were packed;
the 1,010,100 unique extents accounted for 35,190,680 live bytes, yielding
9,999 basis points utilization. The first exact implementation used a durable
seen-tree entry per extent and did not reach 100K objects in several minutes.
The revised cursor stores one sorted extent summary per physical pack and
completed in 298.4 seconds. That is bounded and restartable but still an offline
maintenance operation; pack manifests should eventually carry pre-aggregated
live-range summaries so inventory scales with packs without repeatedly decoding
all logical objects.

Deep fsck now binds the snapshot's object/version roots into its durable cursor,
uses range-readable commit metadata during DAG discovery, checkpoints every
10K logical records, and stores only compact per-physical-pack identities in
its maintenance tree. Logical extent checksums are verified page-by-page using
bounded concurrent full-pack reads. The final 1M run checked 1,010,100 current
objects, 1,020,801 logical versions, and 70,582,197 referenced logical bytes in
38.79 seconds. It reduced 2,030,901 logical payload references to 1,004 unique
physical packs; deep content traffic was 39,070,167 bytes and total provider
download was 39,355,355 bytes. The process ended at 250,752 KiB RSS. An earlier
prototype that durably rewrote every extent summary uploaded 3.53 GB and grew
to 1.1 GB RSS; it was rejected and replaced by the compact manifest design.

GC now checkpoints its cursor under the repository-wide maintenance epoch at
start and after every returned page. A new process resumed the measured 1M run
after two forced terminations; an explicit recovery operation can abandon only
legacy/crashed epochs that never published a cursor. Node marking dequeues a
bounded wave, batch-checks marks, fetches nodes with concurrency 32, deduplicates
physical pack marks within version leaves, and publishes one work-tree mutation
per wave. Candidate scanning batch-probes reachability and publishes candidates
once per provider page. The resumed final segment completed in 126.1 seconds,
deleted 186 exact unreachable versions totaling 4,046,261 bytes, downloaded
888 KB, uploaded 437 KB, and ended at 44,000 KiB RSS. This proves restart and
pack-safe sweep locally; a clean uninterrupted end-to-end timing and deliberate
mid-sweep restart remain release gates.

The 65 MiB streaming gate now uses nine independently content-addressed chunks
plus one immutable manifest rather than a whole-file disk spool followed by a
single multipart object. Upload buffers at most eight 8 MiB chunks, chunk puts
run concurrently, full reads validate every chunk and the final logical hash,
and range reads fetch only overlapping chunk ranges. The native RustFS gate
completed in 5.74 seconds and verified both the final 32 bytes and a 32-byte
range crossing the first chunk boundary. Content-addressed retries deduplicate
already uploaded chunks, but durable source-offset resume and client-side
encryption remain open release work.

The 500K grouped ingest uploaded 2.56 GB for 17.5 MB of logical content, about
146x byte amplification. Direct-child diff now pages the exact commit delta
rather than allowing tree boundary shifts to trigger a collected structural
fallback. Merge uses the same restartable delta frontier and promotes an
external direct-child state/delta without rebuilding identical object and
version trees.

## Supported envelope

- Reliability semantics, bounded-memory grouped ingestion, complete listing,
  restart-cold reads, branch creation, and 100/10K-key direct-child diff/merge
  are exercised at 500K files. This is a qualified local functional envelope,
  not yet a 500K production SLO: cache-cold reads and listings remain sensitive
  to host contention and transfer hundreds of megabytes.
- Do not claim 1M production support until the remaining cold-cache, interrupted
  fsck resume, clean full-GC timing, lifecycle/fencing, and real-provider cost/throttling matrix
  passes. Warm/persistent traversal, sparse direct-child history operations,
  rebuild, bounded RSS, pack inventory, and deep fsck now have local 1M evidence.
- One-file-per-commit history is reliable through 10K and repeated authority
  renewal, but 13.44 commits/s and 33.94 calls/commit make it unsuitable for
  bulk ingest.
- Multiple ingestion branches remove same-ref serialization when independent
  histories are acceptable, but final structural-merge cost must be included.

## Remaining release gates

1. Repeat 100K, 500K, and 1M on AWS with cold/warm/Foyer-cache matrices and
   provider cost/throttling data.
2. Reduce listing GET/byte amplification, stabilize page p99 under contention,
   remove foreground index-maintenance PUTs during traversal, and avoid the
   remaining two mutable/control GETs per warm page.
3. Keep delta-driven diff/merge for direct children; add an equivalent bounded
   change index for arbitrary non-parent snapshot pairs and divergent merges.
4. Add resumable multipart manifests, streaming chunk encryption, parallel
   ranged download, and chunk-level deduplication. Multipart currently uses a
   bounded disk spool.
5. Fault-inject cross-process GC ticket expiry, process death, lifecycle,
   replication, retention, and Object Lock behavior on real providers.
