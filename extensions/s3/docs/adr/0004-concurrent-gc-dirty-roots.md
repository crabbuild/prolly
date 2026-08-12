# ADR 0004: Mark stable roots concurrently and journal root changes

Status: accepted for partitioned GC v2

## Context

The previous partitioned collector took the repository-wide publication write
barrier for every advance step. It also compared a process-local publication
counter and restarted complete ref discovery after any intervening write. A
busy repository could therefore delay writers during marking and prevent GC
from ever reaching sweep.

## Decision

Starting an epoch publishes one bounded mutable `GcCoordinatorV2` and records
the current dirty-root sequence in the epoch checkpoint. While that coordinator
is active, the common mutable-control module observes every branch, tag, and
retention-pin CAS, including tombstones. Before the control CAS, it stores an immutable
`GcDirtyRootV2` under a fixed-width monotonic sequence. Writing before CAS is
conservative: a losing CAS may retain an extra root but cannot cause deletion
of live data. Each event retains both the old and new target when the control
record supplies them, preserving the epoch-start root even when a ref moves
before the initial namespace scan reaches it.

Root discovery, commit/node/version marking, candidate scanning, and dirty-root
marking run without the global publication barrier. GC takes the barrier only
long enough to capture a completed dirty sequence watermark, then releases it
before object IO. Because journal paths are ordered by a dedicated sequence,
catch-up advances from its durable sequence instead of rescanning refs or old
journal events. Sweep still holds the barrier for each configured deletion
batch so a root CAS cannot race an exact delete.

On process restart, opening the repository restores the active coordinator,
recovers the maximum durable dirty sequence, and resumes the epoch. When sweep
exhausts candidates it clears the coordinator while fenced, then exact-deletes
the immutable journal in bounded restartable batches.

## Consequences

- Ordinary marking does not block independent branch publication.
- A write schedules dirty-root catch-up rather than restarting root discovery.
- GC progress is proportional to new root events, not total repository refs or
  prior epoch events.
- Only one partitioned epoch may be active per repository.
- Writes during an active epoch pay one additional immutable journal PUT.
- The short watermark and sweep barriers remain fair, bounded synchronization
  points required for safe exact deletion.
