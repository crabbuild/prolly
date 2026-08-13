# ADR 0001: Store payload history under immutable derived keys

Status: accepted

## Context

Reusing an application key for physical payload versions creates hot-key
version pressure and couples logical history to provider retention behavior.
Chunking whole files adds manifests, request amplification, and management
complexity that this repository does not need.

## Decision

Store each file as one immutable payload under a repository-scoped SHA-256
derived key. Bind logical object versions to that payload. Represent deletion
only in the logical version tree.

## Consequences

- identical bytes are safely reusable;
- one write needs one payload upload rather than per-chunk requests;
- logical history does not consume versions of the original user key;
- each file must fit the repository and provider single-PUT limit;
- failed publication can leave unreachable immutable payload candidates.
