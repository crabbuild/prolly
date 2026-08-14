# ADR 0008: Production scaling and maintenance admission

Status: accepted

## Context

The 20K RustFS qualification demonstrated efficient snapshot reads, listing,
branching, sparse diff, and merge after the node-index and traversal-cursor
work. It did not establish a production envelope for deep hot-branch history,
large streamed files, whole-object lifecycle, or garbage collection while
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
4. Every distinct logical payload is represented by one complete immutable S3
   object. Built-in uploads use one conditional `PutObject`; multipart,
   resumable parts, retry, buffering, and abort belong to an external provider
   transfer manager. Prolly verifies and publishes only the completed object.
5. Prolly never packs multiple user bodies together and never splits one body
   into repository-managed chunks. Payload bindings contain a whole-object
   path, provider version, ETag, and checksum. Metadata node packs remain an
   internal encoding for Prolly index nodes and never contain user payloads.
6. GC closes durable repository-wide publication admission before discovering
   roots. Every branch/tag CAS owns an expiring, instance-scoped ticket from
   immediately before provider CAS through outcome resolution. GC drains or
   expires all tickets before scanning refs. This favors correctness over
   write availability during destructive maintenance and works across
   processes that do not share memory.
7. Repair and history import delegate complete-object copying to the object
   provider boundary. The repository supplies source identity, destination,
   size, and whole-object checksum only; transfer-part state is never part of
   repository state.

## Consequences

- Same-branch publication remains intentionally serialized. Use commit-session
  group commits for bulk loads and independent branches when independent
  histories are acceptable; merge them structurally afterward.
- Maintenance admission adds one immutable ticket create, one coordinator
  read, and one exact ticket delete to branch/tag publication. This cost must be
  included in AWS request budgets. Independent branches avoid ref-CAS
  contention but not provider account/prefix quotas.
- Small-object request and object-count costs are not hidden by a Prolly-owned
  blob layer. Throughput comes from bounded whole-object concurrency and
  grouped metadata publication; exact duplicate bodies may reuse one complete
  content-addressed object.
- Externally uploaded objects are immutable by repository convention and exact
  version binding. Repository IAM must deny writes by other principals under
  the reserved prefix, while the transfer manager owns incomplete uploads.
- RustFS is a compatibility and regression substrate, not evidence of AWS
  latency, throttling, cost, lifecycle, replication, or regional behavior.

## Release evidence

- The release-mode 4K first-parent merge-base gate completes in 11.56 seconds
  after the operation-journal fix; the pre-fix release run remained CPU-bound
  after five minutes.
- Deterministic core tests cover concurrent whole-object preparation,
  whole-object deduplication, bounded operation-suffix lookup, batched journal
  indexing, and cross-handle GC fencing. They also assert that payload
  bindings never contain byte extents.
- External transfer-manager handoff, 10K hot-commit, and 100K–1M
  multi-operation provider results remain release evidence requirements; they
  are not inferred from unit tests.
