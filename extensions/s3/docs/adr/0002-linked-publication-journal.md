# ADR 0002: Link immutable publication events from branch refs

Status: accepted for protocol v2

## Context

Listing ref or commit namespaces to discover new publications makes index
maintenance proportional to total repository size. A second mutable journal
head would also require a distributed transaction with the branch ref.

## Decision

Write one content-addressed `PublicationEventV2` before each branch-ref CAS.
The proposed ref contains that event ID, and the event contains the previous
event ID, ref generation, old and new commit IDs, operation ID, reflog ID, and
authority stamp. The ref CAS is therefore the only commit point. A losing CAS
may leave an unreachable immutable event but cannot append it to the journal.

Readers open the ref once and persist a cursor containing the snapshot event,
generation, and target. Every page follows and validates immutable links. A
concurrent publication cannot alter an already-open traversal.

## Consequences

- Indexers and replication consumers resume without namespace scans.
- Independent branches have independent journal chains.
- Publication costs one additional immutable object write.
- Whole-history traversal remains proportional to emitted events but is
  bounded per call and resumable.
- Protocol-v2 GC must trace the event chain from every live ref and reclaim
  unreachable events left by failed competing CAS operations.
