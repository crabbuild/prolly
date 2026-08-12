# ADR 0003: Bound idempotency with branch-local immutable segments

Status: accepted for protocol v2

## Context

Protocol v1 keeps every operation ID in one persistent repository tree. That
tree grows with total history even though callers need idempotent replay only
for a declared retry window. Searching the publication journal alone would
bound storage but could require one object GET per commit.

## Decision

Protocol v2 defines idempotency as the intersection of a maximum branch-ref
generation distance and a maximum wall-clock age. The defaults are one million
generations and seven days; both limits are configurable within enforced
bounds.

Each branch has an advisory `OperationIndexHeadV2`. It checkpoints a durable
publication event and references strictly sorted, content-addressed operation
segments. New leaf segments merge geometrically with a fixed fanout. The
number of levels is derived from the configured generation window and capped;
the top level compacts in place rather than growing with total repository
history. The mutable head uses the common exact-version compactor.

Lookup first checks the current ref, then the explicitly bounded journal tail
after the checkpoint, then at most `fanout - 1` segment objects per bounded
level. Segment entries outside either retention boundary are ignored, and
fully expired segment references are removed from the head.

## Consequences

- Index size and lookup work depend on the declared retry window, not total
  commits.
- Branches can advance and index independently.
- The index is initialized atomically with branch generation zero. Late
  initialization fails closed and requires resumable rebuild.
- Index lag beyond the configured tail bound returns
  `HistoryLimitExceeded`; it never falls back to an unbounded scan.
- Immutable expired and superseded segments remain candidates for protocol-v2
  GC.
