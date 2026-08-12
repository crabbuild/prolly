# ADR 0005: Drive node and graph indexes from branch journals

Status: accepted for protocol v2

## Context

The legacy node locator and commit graph discover work by repeatedly listing
the complete commit namespace. Maintenance cost therefore grows with total
history even when only one branch publication is new. Independent branch
writers also cannot safely append to one repository-global mutable index head
without recreating a publication bottleneck.

## Decision

Maintain one `JournalDerivedIndexHeadV2` per branch. It checkpoints an immutable
`PublicationEventV2` and atomically names two immutable Prolly roots: node
locations and commit-graph entries. Advancement opens a stable journal snapshot,
collects only events newer than the checkpoint, applies them oldest-to-newest,
and CAS-publishes both roots together.

The consumer is initialized at branch generation zero. Normal catch-up has an
explicit event bound and fails closed when it is exceeded. A missing late index
also fails closed and delegates to a separately resumable rebuild; it never
falls back to an implicit namespace scan. Authority barriers whose target is
unchanged advance the checkpoint without reindexing the same commit.

## Consequences

- Steady-state node and graph maintenance performs no commit/ref namespace scan.
- Work and reads are proportional to new branch publications.
- Independent branches have independent index heads and never contend on an
  index-wide CAS.
- Node and graph freshness is atomic at one publication checkpoint.
- Global branch enumeration is not inferred from branch-local journals; it is
  handled by the resumable whole-history administration design.
- Immutable obsolete index nodes require reachability cleanup from current
  index heads.
