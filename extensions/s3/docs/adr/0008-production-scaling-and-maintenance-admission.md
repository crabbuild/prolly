# ADR 0008: Production scaling and maintenance admission

Status: accepted

## Context

The 20K RustFS qualification demonstrated efficient snapshot reads, listing,
branching, sparse diff, and merge after the node-index and traversal-cursor
work. It did not establish a production envelope for deep hot-branch history,
large streamed files, payload-pack lifecycle, or garbage collection while
other operating-system processes were publishing.

Profiling the 4K first-parent test found that every ordinary write walked the
complete unindexed operation-journal suffix to prove that a freshly allocated
operation ID was absent. That made history construction quadratic. Large
streamed files still used one `PutObject`, and GC's publication barrier and
dirty-root sequence were process-local.

## Decision

1. Immutable payload hashing/upload happens before the branch publication
   lane. The lane orders tree mutation and the ref CAS only. Commit sessions
   remain the group-commit mechanism for bulk ingestion.
2. Internally allocated operation IDs skip retry lookup. Caller-stable IDs use
   an incremental in-memory view of the unindexed journal suffix; the durable
   segmented index remains authoritative across restart.
3. Journal-derived node and commit-graph indexes load commit descriptors with
   bounded concurrency and apply one deterministic tree batch per journal
   page. First-parent graph entries retain binary-lifting skip pointers.
4. Streamed immutable files at or above 64 MiB use S3 multipart upload with a
   dynamically sized part layout, eight uploads in flight, exact returned
   version binding, and abort-on-error/cancellation cleanup. Smaller spools
   retain conditional single-put behavior.
5. Payload packs expose a restartable current-snapshot inventory. It counts
   unique physical packs and unique referenced extents, reports utilization,
   and feeds bounded repack pages for direct payloads no larger than 4 KiB.
   Fsck reports packed logical references and bytes separately.
6. GC closes durable repository-wide publication admission before discovering
   roots. Every branch/tag CAS owns an expiring, instance-scoped ticket from
   immediately before provider CAS through outcome resolution. GC drains or
   expires all tickets before scanning refs. This favors correctness over
   write availability during destructive maintenance and works across
   processes that do not share memory.

## Consequences

- Same-branch publication remains intentionally serialized. Use commit-session
  group commits for bulk loads and independent branches when independent
  histories are acceptable; merge them structurally afterward.
- Maintenance admission adds one immutable ticket create, one coordinator
  read, and one exact ticket delete to branch/tag publication. This cost must be
  included in AWS request budgets. Independent branches avoid ref-CAS
  contention but not provider account/prefix quotas.
- Repacking improves the current serving layout but does not erase historical
  payloads. Old packs remain reachable while commits, tags, or retention pins
  reference them; only exact-version GC may reclaim unreachable packs.
- Multipart completion is immutable by repository convention and exact version
  binding. Repository IAM must deny writes by other principals under the
  reserved prefix.
- RustFS is a compatibility and regression substrate, not evidence of AWS
  latency, throttling, cost, lifecycle, replication, or regional behavior.

## Release evidence

- The release-mode 4K first-parent merge-base gate completes in 11.56 seconds
  after the operation-journal fix; the pre-fix release run remained CPU-bound
  after five minutes.
- Deterministic core tests cover concurrent immutable preparation, bounded
  operation-suffix lookup, batched journal indexing, pack inventory/repacking,
  cross-handle GC fencing, and multipart layout through the 5 TiB repository
  limit.
- Live multipart, 10K hot-commit, and 100K–1M multi-operation provider results
  remain release evidence requirements; they are not inferred from unit tests.
