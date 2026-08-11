# Versioned DynamoDB Client Operations

This runbook covers the in-process Rust client. There is no compatibility
service to deploy. Application and worker processes link the same crate and
coordinate exclusively through the configured physical DynamoDB namespace.
The non-negotiable threat model and least-privilege policy examples are in
[`SECURITY.md`](SECURITY.md); this runbook does not replace them.

## Trust and authority boundaries

Use a dedicated physical node table, roots table, and non-empty key prefix for
each security/retention domain. Never allow native item applications to write
the physical representation.

Recommended AWS identities are separate:

| Identity | Required authority | Must not have |
| --- | --- | --- |
| Provisioner | Create/describe/update the two physical tables | Application data-plane credentials after bootstrap |
| Runtime application | Read/write the configured node/root namespace and transactions | Physical table deletion, global scans for GC |
| Stream/TTL worker | Runtime authority for its namespace; durable worker lease/checkpoint roots | Maintenance break authority |
| Maintenance operator | Root/node/blob scans and deletes under the global maintenance fence | Unreviewed automated startup in request processes |
| Recovery operator | Verified export/import, restore, retention, and lease-break commands | Routine application credentials |

Provider IAM cannot distinguish a correct logical write from a direct physical
write made with the same credentials. Keep physical credentials in trusted
server-side Rust processes and enforce crate usage through deployment controls,
code review, and audit logs.

## Runtime applications

1. Build the caller-owned `aws_sdk_dynamodb::Client` with the approved region,
   endpoint, TLS roots, timeout, and retry policy.
2. Build `DynamoDbBackend` with the exact physical table, roots table, and
   non-empty key prefix assigned to the deployment.
3. Provision explicitly; normal `Client::open` is read-only with respect to
   physical schema.
4. Record and compare `client.capabilities()` at startup. A format or provider
   capability mismatch is a deployment failure, not a warning.
5. Construct one logical `Client` per physical namespace in each process and
   clone it for request tasks. Clones share process-local write admission and
   avoid speculative work against the same stale head. Independently opening a
   client per request defeats that optimization; such clients and other
   processes remain correct through provider CAS and bounded retries but can
   amplify physical traffic sharply under contention.
6. Do not start workers from request-process initialization. Worker ownership
   is a separate explicit deployment decision.

Dropping `Client` never shuts down the caller's AWS client and starts no cleanup
operation. Dropping an in-flight write future cannot retract a request already
accepted by DynamoDB. Writes that need safe application-level retries across
process restart must use the supported durable request token.

## Stream worker deployment

Deploy stream workers as replaceable processes with a stable subscription ID
and a unique owner ID per process instance. Runtime tuning (page size, idle
delay, lease duration) may change without changing subscription identity.

- One fencing generation owns a subscription at a time. Another process may
  take over only after lease expiry or an explicit release.
- Delivery is sequential and bounded. Backpressure is the sink future itself;
  the worker does not accumulate an unbounded queue.
- Delivery is at-least-once. The sink must make `CommitId` unique in the same
  transaction as its external effect when effectively-once behavior is needed.
- A checkpoint is written only after the sink acknowledges the commit under a
  live exact fence. A crash between effect and checkpoint redelivers the same
  `CommitId`.
- Normal termination calls `CancellationToken::cancel()`, waits for `run` to
  finish the in-flight record and checkpoint it, and allows `shutdown` to write
  durable release evidence. The orchestration grace period must exceed the AWS
  request timeout plus one checkpoint transaction.
- Lease loss is terminal for that worker object. Do not silently reacquire and
  continue an in-flight sink call; construct a new worker and resume its durable
  checkpoint.

History retention does not remove the per-incarnation commit log, so an active
stream checkpoint is not silently invalidated. Monitor commit-log growth; no
commit-log compaction policy is currently advertised.

## TTL worker deployment

Use one stable job per table incarnation and case-sensitive TTL attribute.
Numbers must contain integer Unix epoch seconds. Missing, non-number,
fractional, negative, future, and more-than-five-365-day-old values are ignored.

Each candidate records the exact observed TTL number. The delete transaction
checks both the table incarnation and equality of that number. A concurrent
refresh, removal, type change, table deletion, or same-name recreation cannot
be deleted by a stale candidate.

TTL is asynchronous deletion, not access control. Readers can observe expired
items until a worker deletes them. Applications requiring immediate invisibility
must add an explicit read condition based on trusted time.

## Maintenance worker and change control

`client.workers().maintenance(...)` acquires the namespace-wide fail-closed
writer fence but performs no scan or deletion. Use this sequence:

1. Acquire with a named actor, reason, change ticket, and reviewed duration.
2. Confirm application writes are fenced.
3. Produce a bounded `plan_gc` page and persist its exact plan ID, root digest,
   cursor, counts, and candidates in change evidence. The planner expands
   indexed-collection state roots into every retained source/index snapshot;
   do not substitute a raw named-root-only sweep.
4. Review the plan before `apply_gc`; applying recomputes lease/root/candidate
   safety and rejects stale input.
5. Retry the same plan/context after partial or ambiguous failure. Never create
   a replacement plan while an execution is recorded in progress.
6. Release with attributed `shutdown` only after every reviewed page completes.

Expiry never re-admits writers automatically. A paused process may still be
deleting. Only an authorized operator may break the exact expired lease, and an
active GC execution prevents release/break.

## Rollback

Worker rollback is operationally independent of synchronous CRUD:

- Stop scheduling a stream or TTL worker and cancel it gracefully. Current
  table reads/writes and version history remain valid.
- Deploying an older worker binary is allowed only when its database format,
  worker configuration encoding, and minimum reader/writer versions match the
  namespace format record.
- Never delete or edit worker lease/checkpoint/audit roots manually. Allow lease
  expiry for process death, then construct the approved replacement owner.

Index rollback uses the reviewed index reconfiguration/restore workflow. A
generation is shadow-built and verified before catalog activation; historical
base versions retain exact generation pairings. Do not physically collect a
retired generation until the rollback/history retention window has closed.

Package rollback is allowed only inside the format record's declared reader and
writer compatibility. A format mismatch must fail `Client::open`. Do not bypass
it, overwrite the record, or point an older writer at the namespace. Restore a
verified backup into an isolated prefix when compatibility is uncertain.
Before approving a rolling pair, run the independently built binaries through
`scripts/run_dynamodb_rolling_compatibility.py` as specified in `SOAK.md` and
retain its complete report. The diagnostic identical-binary override is not
rollback evidence.

## Incident checklist

- **Unknown write outcome:** use the durable request token/commit lookup. Do not
  blindly replay a non-idempotent update.
- **Stream duplicates:** verify sink-side `CommitId` uniqueness, then inspect
  lease releases and checkpoint revision/fence. Duplicates are permitted;
  missing acknowledged sequences are not.
- **Worker lease held:** identify the recorded owner and expiry. Do not steal a
  live lease or reuse an owner ID across concurrent processes.
- **Maintenance fence stuck:** inspect active GC execution first. Break only the
  exact expired lease with recovery authority and a change ticket.
- **Corrupt or missing content:** stop writers for the namespace, preserve
  physical tables and logs, run read-only verification, and restore a verified
  archive into an isolated prefix. Do not repair CIDs or roots by hand.
- **Same-name table recreation:** workers bind `TableId`; an old worker must fail
  with an incarnation error. Construct a separately approved job for the new
  table.

## Minimum monitoring

Alert on format negotiation failure, corruption, outcome-unknown writes,
conflict exhaustion, lease loss/takeover, checkpoint age, stream delivery lag,
TTL cycle age, maintenance fence duration, GC partial execution, and provider
throttling. Export tracing spans through the application's subscriber; normal
spans deliberately omit keys, items, expressions, credentials, physical table
names, and result bodies.
