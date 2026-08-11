# Versioned DynamoDB Recovery Drills

Use this runbook with `OPERATIONS.md`. Recovery commands run from the explicit
Rust administrative package or client APIs; there is no service control plane.

## Evidence required before a drill

Record the physical table/root table, non-empty namespace prefix, capability
JSON, database format, application release, AWS region/account, archive limits,
operator identity, change ticket, and start time. Never include credentials,
logical item bodies, or unredacted expressions in ordinary logs.

## Backup and isolated restore

1. Pin and record the source table incarnation and immutable head.
2. Export with explicit node/blob/encoded-byte limits.
3. Decode and verify the archive independently; retain its canonical digest.
4. Import into a new logical name and preferably an isolated physical prefix.
5. Review the dry-run plan, then apply it with recovery attribution.
6. Compare descriptor, exact source version, item/query fixtures, indexes, and
   commit/audit evidence before directing any reader to the recovered copy.

The archive requires an exact format match. Import never overwrites an existing
table and never serves as an implicit migration.

## Lost writer or unknown outcome

For a tokenized operation, retry only the exact canonical request with the same
token inside its advertised window. A different request must produce an
idempotent-parameter mismatch. Resolve the returned `CommitId` and table-local
commit sequence before issuing compensating work.

For an outcome-unknown provider error without application-level replay
evidence, preserve the provider transaction token, stop automatic retries, and
inspect durable roots/commits. Never convert it to `UnprocessedItems` or assume
the mutation was rejected.

## Lost worker process

Do not reuse an owner ID concurrently. Wait for the recorded lease to expire or
perform the authorized exact release workflow. Start a new owner for the same
stable job configuration. A stream resumes its durable checkpoint and may
redeliver the last external effect; deduplicate by `CommitId`. A worker bound to
an old `TableId` must fail after same-name recreation.

## Retention and GC recovery

Retention replay uses the exact plan and operator context. Live idempotency
tokens automatically protect the transition versions needed to reconstruct
their return images. A stale plan is discarded and replanned; it is never
edited.

GC requires the global fail-closed maintenance fence. Persist every plan page,
root digest, cursor, and execution record. After a crash, retry the same plan
and context so partial physical deletion is reconciled. Do not release or break
the fence while an execution is in progress. Expiry by itself never admits
writers.

## Corruption drill

1. Stop logical writers and workers for the namespace.
2. Preserve physical tables, CloudTrail/provider logs, binaries, and capability
   records; do not rewrite a CID, root, or format record.
3. Run read-only archive/format/tree verification with bounded scans.
4. Classify the fault as unreachable orphan content, missing reachable content,
   malformed canonical data, or root/catalog disagreement.
5. Restore the last independently verified archive into an isolated prefix.
6. Replay only externally recorded operations whose commit IDs are proven
   absent and whose business policy permits replay.
7. Compare logical and audit state, then perform a separately approved cutover.

Missing reachable content is not repaired by GC. Keep the affected namespace
frozen for investigation.

## Drill acceptance

A release drill records achieved RPO/RTO, all commands and immutable digests,
the point at which writes were fenced, proof that the recovered namespace was
isolated, logical/index comparison results, and the exact cutover or rollback
decision. Local tests prove mechanics; production RPO/RTO is not declared until
a deployment-shaped drill supplies measured evidence.
