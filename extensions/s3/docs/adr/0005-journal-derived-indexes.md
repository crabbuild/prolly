# ADR 0005: Drive node and graph indexes from branch journals

Status: accepted

## Context

Repeatedly listing the commit namespace makes maintenance grow with complete
history and creates a repository-wide coordination point.

## Decision

Maintain one journal-derived index head per branch. It checkpoints a
publication event and atomically names immutable Prolly roots for node
locations and commit graph entries. Catch-up consumes only newer linked events
within an explicit bound. Missing or excessive lag requires a resumable rebuild.

## Consequences

- steady-state lookup and maintenance do not list namespaces;
- work is proportional to new branch publications;
- independent branches never contend on one global index head;
- obsolete immutable index pages remain reclaimable storage candidates.
