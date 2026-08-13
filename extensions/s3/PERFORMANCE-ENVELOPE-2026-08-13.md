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
| 500K full list | failed with a RustFS `GetObject` body streaming error |
| 65 MiB streamed multipart object | 3.78 s; multipart upload and historical range read passed |

The 500K grouped ingest uploaded 2.56 GB for 17.5 MB of logical content, about
146x byte amplification. The 100K list meets the 10K entries/s throughput goal
but misses the 100 ms page-p99 goal. Sparse diff and especially merge exceed
their byte-amplification targets at 100K.

## Supported envelope

- Reliability semantics and bounded-memory grouped ingestion are exercised at
  500K files, but complete 500K traversal did not pass. Do not claim 500K or 1M
  production support from this run.
- The measured efficient local envelope is 100K current objects with grouped
  commits. Reads, ingestion, branching, and list throughput remain useful;
  list p99 and sparse diff/merge amplification require more work.
- One-file-per-commit history is reliable through 10K and repeated authority
  renewal, but 13.44 commits/s and 33.94 calls/commit make it unsuitable for
  bulk ingest.
- Multiple ingestion branches remove same-ref serialization when independent
  histories are acceptable, but final structural-merge cost must be included.

## Remaining release gates

1. Repeat 100K, 500K, and 1M on AWS with cold/warm/Foyer-cache matrices and
   provider cost/throttling data.
2. Reduce listing page p99 and remove foreground index-maintenance PUTs during
   traversal.
3. Reduce 100K sparse diff below 1 MiB and merge planning below 2 MiB through
   smaller range-addressable node packs or a sparse commit-state index.
4. Add resumable multipart manifests, streaming chunk encryption, parallel
   ranged download, and chunk-level deduplication. Multipart currently uses a
   bounded disk spool.
5. Fault-inject cross-process GC ticket expiry, process death, lifecycle,
   replication, retention, and Object Lock behavior on real providers.
