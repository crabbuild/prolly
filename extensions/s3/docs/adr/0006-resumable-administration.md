# ADR 0006: Persist whole-history traversal state in immutable Prolly trees

Status: accepted

## Context

Clone, repair, fsck, and export cannot assume that all refs, commits, or a DAG
visited set fit in one process. A cursor containing the pending stack merely
moves the unbounded memory problem into a large continuation token. Rewalking
from every root on each page creates quadratic work.

## Decision

Page every root namespace. A `CommitClosureCursor` names a job-specific
immutable Prolly tree whose keys hold the DFS stack and visited state. Callers
may attach up to 1,000 roots at a time. Advancement has independent work-step
and output limits, and emits a commit only after its parents, making each page
directly consumable by transfer pipelines.

The cursor contains only repository identity, job ID, tree root, and the next
stack sequence. It is canonical-serializable and process-independent. Job state
is not automatically collected because an external workflow may retain a live
cursor; successful and abandoned workflows exact-delete it with bounded
cleanup calls.

Pins and tag/branch reflogs also expose bounded page APIs. A branch reflog
cursor anchors its original head and remains stable if the live branch moves.

## Consequences

- Traversal memory and cursor size remain bounded at arbitrary DAG size.
- Pages do not list the commit namespace and do not rediscover prior work.
- Parent-before-child output removes whole-DAG buffering from clone and repair.
- Workflow engines must durably save side effects/mappings before advancing the
  cursor and must schedule cleanup for abandoned jobs.
- Legacy convenience APIs retain configured traversal limits but are not the
  production path for repository-wide jobs.
