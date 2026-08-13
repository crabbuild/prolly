# ADR 0007: Persist merge work as structural Prolly state

Status: accepted

## Context

Materializing complete ancestor sets and snapshots makes memory and restart
cost grow with total history even when two branches changed few keys.

## Decision

Persist merge-base frontier, changes, conflicts, and output plan as job-scoped
Prolly trees. Advance every phase with bounded pages. Prune equal
content-addressed subtrees. Publish one two-parent commit only after target ref
revalidation and reconcile an ambiguous CAS by operation ID.

## Invariants

- cursors stay constant-size;
- every best base is reported;
- target movement is a conflict, not an implicit rebase;
- missing node locations fall back to deterministic CID reads, not listing;
- cleanup can address only job-scoped administration state.

## Consequences

Sparse merge cost follows changed structural paths. Work survives process
restart, but operators must bound concurrency, page sizes, cache, deadlines,
and provider request rates.
