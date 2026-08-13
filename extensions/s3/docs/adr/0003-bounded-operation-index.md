# ADR 0003: Bound idempotency with branch-local immutable segments

Status: accepted

## Context

Operation IDs are needed only for a declared retry window. Keeping every ID in
one repository-wide tree makes lookup and storage grow with total history.

## Decision

Give each branch an advisory operation-index head over sorted immutable
segments. Define retention by generation distance and wall-clock age. Search
the current ref, the bounded journal tail, then a bounded number of segment
levels. Fail closed if index lag exceeds the configured tail.

## Consequences

- work depends on the retry window, not total commits;
- branches index independently;
- stale segments remain immutable storage until a production reclamation
  facility exists;
- an operation ID reused with unequal input is rejected.
