# ADR 0002: Link immutable publication events from branch refs

Status: accepted

## Context

Namespace listing makes discovery proportional to total repository size. A
second mutable journal head would require a distributed transaction with the
branch ref.

## Decision

Write one content-addressed publication event before each branch-ref CAS. The
event records its predecessor, generations, commits, operation identity,
reflog identity, and authority stamp. The proposed ref names the event.

The ref CAS is the only commit point. Traversal opens a stable event cursor and
follows immutable predecessor links in bounded pages.

## Consequences

- indexers resume without namespace scans;
- branches have independent journals and publication lanes;
- publication adds one immutable object;
- a losing CAS can leave an unreachable event but cannot append it to history.
