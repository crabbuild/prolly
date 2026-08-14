# Prolly S3 measured performance envelope

Date: 2026-08-13

Provider: local RustFS 1.0.0-beta.10, versioned bucket, Apple Silicon Docker
Desktop, 32-way client concurrency. These numbers are regression evidence for
this machine, not AWS SLO evidence.

> Architecture notice: these measurements were collected with an experimental
> payload-packing and Prolly-owned multipart implementation that has since been
> removed. They remain useful metadata-tree baselines, but they do not qualify
> the current whole-object payload architecture. Ingest, point-read, fsck, GC,
> request-cost, and byte-amplification results must be rerun before release.

## Corrected whole-object baseline

After removing payload packs and Prolly-owned multipart state, the 10K
tiny-file RustFS gate stored every distinct 16-byte body as one complete
provider object. Eliminating a redundant authority read per staged object and
replacing cumulative checkpoints with append-only mutation windows reduced the
run from 35.19 seconds, 10,097 calls, and 44.1 MB uploaded to 20.64 seconds,
10,079 calls, and 19.43 MB. With 128 bounded uploads it reached 484.6 files/s.
The 64-way and 256-way probes reached 461.8 and 477.1 files/s respectively, so
128 is the measured local configuration. It remains just below the unchanged
500 files/s release gate. Remaining amplification is complete logical/version
metadata plus bounded checkpoint windows, not payload chunks or packs.

The revised 10K same-branch gate now records per-commit p50/p95/p99 as well as
throughput. A debug-build diagnostic at concurrency 32 was deliberately stopped
after 1,000 commits took 247.35 seconds (4.04 commits/s); completing 10K would
have consumed roughly 41 minutes before allowing for history growth. This is a
failed scalability signal, not a completed release measurement. It confirms
that independent one-object commits must not be the bulk-ingestion model:
ordered group commit or multiple ingestion branches followed by structural
merge is required. The earlier 13.44 commits/s result remains historical and
must not be used as evidence for the current whole-object build.

The metadata-only deep-history gate was rerun in release mode with 4,096
first-parent commits. Indexed merge-base selection completed in 1.578 ms using
27 object-plane calls and zero fallback commit visits, confirming that graph
skip pointers avoid linear history traversal in this case.

An ordered publication queue now coalesces independently submitted unique keys
into one commit, splits repeated keys across commits, returns per-object staging
errors, and uses constant-size acknowledgements. The first implementation
revalidated the session per object and copied the full batch receipt to every
caller; those defects produced 2.057 calls/file and O(N²) acknowledgement work.
After correction, a fresh-bucket release run published 10K submissions as one
commit with ten durable checkpoint windows, 10,083 S3 calls (1.008/file), and a
24.84-second p99. Best throughput was 402.6 files/s at concurrency 512; 256 and
1,024 reached 376.6 and 335.4 files/s. The unchanged 500 files/s gate therefore
still fails. The p99 is cohort latency: every caller is acknowledged after the
single grouped ref CAS. A same-host bulk run reached 342.0 files/s, showing the
queue is now near the provider's current whole-object PUT envelope rather than
limited by publication or acknowledgement copying.

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

Deep fsck now binds the snapshot's object/version roots into its durable cursor,
uses range-readable commit metadata during DAG discovery, checkpoints every
10K logical records, and deduplicates checks by complete physical-object
identity. The former packed-payload numbers are not evidence for this revised
whole-object fsck path; the 100K, 500K, and 1M gates must be rerun.

GC now checkpoints its cursor under the repository-wide maintenance epoch at
start and after every returned page. A new process resumed the measured 1M run
after two forced terminations; an explicit recovery operation can abandon only
legacy/crashed epochs that never published a cursor. Node marking dequeues a
bounded wave, batch-checks marks, fetches nodes with concurrency 32, deduplicates
physical payload marks within version leaves, and publishes one work-tree mutation
per wave. Candidate scanning batch-probes reachability and publishes candidates
once per provider page. The former packed-object GC timing is not evidence for
the whole-object layout. Cross-process fencing and restart semantics remain,
but clean end-to-end 100K–1M timing and deliberate mid-sweep restart must be
rerun.

The 500K grouped ingest uploaded 2.56 GB for 17.5 MB of logical content, about
146x byte amplification. Direct-child diff now pages the exact commit delta
rather than allowing tree boundary shifts to trigger a collected structural
fallback. Merge uses the same restartable delta frontier and promotes an
external direct-child state/delta without rebuilding identical object and
version trees.

## Supported envelope

- Metadata listing, branch creation, and 100/10K-key direct-child diff/merge
  have historical 500K–1M local evidence. The current whole-object payload
  architecture does not yet have a qualified 500K or 1M end-to-end envelope.
- Do not claim 1M production support until cold/warm/persistent reads, grouped
  whole-object ingest, interrupted fsck, clean full GC, lifecycle/fencing, and
  the real-provider cost/throttling matrix pass on the revised format.
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
4. Keep multipart and resumable-transfer state outside Prolly. A provider
   transfer manager uploads the final content-addressed object, then hands its
   whole-object identity back for verification and publication. Prolly must
   not own upload IDs, part geometry, part ETags, chunk manifests, or chunk
   garbage collection. Use native ranged GETs and provider encryption controls.
5. Fault-inject cross-process GC ticket expiry, process death, lifecycle,
   replication, retention, and Object Lock behavior on real providers.
