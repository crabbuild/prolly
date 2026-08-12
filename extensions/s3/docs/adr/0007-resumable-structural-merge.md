# ADR 0007: Persist native-v2 merge work as structural Prolly state

Status: accepted

## Context

The legacy merge path constructs complete ancestor sets and materializes the
base, target, and source object maps. Its memory and restart cost therefore
grow with total history and snapshot cardinality even when the branches change
only a few keys. A cursor containing those collections would only move the
unbounded state into a continuation token.

Native-v2 branch publication is independently fenced and its journal-derived
commit graph records generation plus first-parent binary-lifting pointers. The
state and version maps already support structural diff cursors that prune equal
content-addressed subtrees.

## Decision

Represent merge as a caller-driven durable job:

1. Snapshot the target and source commit IDs at start.
2. Use first-parent skip pointers for the ancestry fast path. Otherwise persist
   a generation-priority, bidirectional paint-down frontier, seen flags, stale
   candidates, and best-base results in a job-scoped Prolly tree.
3. Require explicit selection when criss-cross history yields several best
   bases.
4. Structurally diff base-to-target and base-to-source with constant-size
   cursors and at most one pending record from each stream.
5. Persist selected changes and conflicts under separate plan prefixes. Batch
   every requested work page into one new plan root.
6. Union immutable version trees and build the output object tree in bounded
   pages. Store intermediate output nodes at deterministic repository CID paths
   so another process can resume before a final commit envelope exists.
7. Store non-empty merge deltas as a Prolly root plus exact record count instead
   of an unbounded inline vector.
8. Publish one two-parent commit only after revalidating the target ref. Resolve
   an ambiguous CAS by operation ID before fencing the target branch.
9. Require explicit, bounded cleanup of the job-scoped plan prefix after
   success or abandonment.

Every returned `MergeCursorV2` is sealed into the plan tree. Validation compares
the caller's canonical cursor state with that durable record, excluding only
the self-referential root CID. A modified phase, count, selected root, branch,
or operation ID is therefore rejected.

## Invariants

- Work and output per call are bounded by the caller's record limit.
- Cursor size does not grow with history, snapshot size, changes, or conflicts.
- Best-base discovery returns every best common ancestor; it does not silently
  choose one in a criss-cross graph.
- Equal Prolly subtrees are never expanded during merge planning.
- A source-selected deletion creates a new logical delete-marker version in
  the merge commit.
- Version-tree entries are immutable. The same key with unequal bytes is
  corruption.
- The first parent is the target observed at planning time. Commit generation
  is `max(parent generations) + 1`.
- Target movement after planning is a ref conflict, never an implicit rebase.
- Plan cleanup cannot address repository state/output nodes.
- Missing packed-node locations fall back to deterministic CID point reads,
  never namespace scans.

## Consequences

Sparse merge cost is proportional to changed structural paths rather than the
complete object count. Deep and criss-cross histories can be processed across
process restarts without rebuilding prior frontier state. Conflict and change
reporting remain pageable at arbitrary result cardinality.

The workflow owner must durably save each returned cursor and schedule cleanup
for completed, conflicted, or abandoned jobs. Intermediate plan and output
nodes add immutable storage until cleanup and repository GC. Cold readers may
issue CID point reads for merge-built nodes until the verified cache is warm.

This ADR does not make an individual unbounded job instantaneous. Deployments
must still bound concurrent merge workers, page sizes, deadlines, cache space,
and object-store request rates.
