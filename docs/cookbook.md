# Cookbook: build applications with prolly trees

This cookbook shows how to use `prolly-map` as a storage layer for local-first apps, AI memory, retrieval metadata, derived indexes, and durable embedded state. Each recipe explains the problem, the prolly-tree design, the reference example, and future enhancements you can add in your own application.

The examples use Rust today. The storage patterns are byte-oriented and portable, so future Python or TypeScript ports should keep the same key ordering, value envelopes, root naming, and conflict semantics.

## Recipe index

- [Shared conventions](#shared-conventions)
- [Basic ordered map](#basic-ordered-map)
- [Bulk import and tree statistics](#bulk-import-and-tree-statistics)
- [Local-first application state](#local-first-application-state)
- [Diff and merge for branches](#diff-and-merge-for-branches)
- [Delete-aware merge resolvers](#delete-aware-merge-resolvers)
- [Conflict-free custom merge](#conflict-free-custom-merge)
- [Conversation memory](#conversation-memory)
- [Agent event logs](#agent-event-logs)
- [Background compaction and retention-aware GC](#background-compaction-and-retention-aware-gc)
- [Deterministic RAG snapshots](#deterministic-rag-snapshots)
- [Document chunk indexes](#document-chunk-indexes)
- [Vector sidecars](#vector-sidecars)
- [Provenance-rich values](#provenance-rich-values)
- [Secondary indexes from diffs](#secondary-indexes-from-diffs)
- [Materialized views from diffs](#materialized-views-from-diffs)
- [Large values and file blob storage](#large-values-and-file-blob-storage)
- [Filesystem snapshots with named roots](#filesystem-snapshots-with-named-roots)
- [Durable SQLite setup](#durable-sqlite-setup)
- [Object-store backed snapshots](#object-store-backed-snapshots)
- [Browser and WASM storage](#browser-and-wasm-storage)

## Shared conventions

Use these conventions before you choose a recipe. They keep roots, keys, and values understandable across features and future language ports.

### Build byte-stable keys

`prolly-map` orders keys with raw byte lexicographic ordering. Use `KeyBuilder` for tuple-like keys so numeric and timestamp segments sort correctly.

```rust
use prolly::KeyBuilder;

let key = KeyBuilder::new()
    .push_str("tenant")
    .push_str("t1")
    .push_str("conversation")
    .push_str("c42")
    .push_str("event")
    .push_timestamp_millis(1_783_036_805_000)
    .push_u64(5)
    .finish();
```

Use these key conventions:

- Put a stable domain first: `app`, `conversation`, `agent-log`, `rag`, `doc-index`, or `materialized-view`
- Put the tenant, workspace, corpus, conversation, or run ID near the front when you scan by it
- Use `push_u64`, `push_i64`, and timestamp helpers instead of decimal strings for ordered numbers
- Use `prefix_range(prefix)` for prefix scans instead of hand-writing an end key
- Add a unique suffix when a prefix can contain more than one record with the same logical time

### Store typed values

The tree stores bytes. Use `VersionedValue` when the value can outlive one release.

```rust
use prolly::{Error, VersionedValue};
use serde::{Deserialize, Serialize};

const MEMORY_SCHEMA: &str = "ai.memory.record";
const MEMORY_VERSION: u64 = 1;

#[derive(Serialize, Deserialize)]
struct MemoryRecord {
    subject: String,
    fact: String,
}

fn encode_memory(record: &MemoryRecord) -> Result<Vec<u8>, Error> {
    VersionedValue::json(MEMORY_SCHEMA, MEMORY_VERSION, record)?.to_bytes()
}
```

Use value envelopes to record:

- schema name and version
- parser or model versions
- source URI and source content IDs
- timestamps from the application domain
- references to large values or sidecar systems

### Publish roots by name

Raw `Tree` values are immutable snapshots. Named roots are mutable application pointers.

Recommended root patterns:

```text
app/<app-id>/root/main
app/<app-id>/device/<device-id>/head
conversation/<conversation-id>/root/main
agent-log/<run-id>/root/events/current
rag/corpus/<corpus-id>/root/index/current
materialized-view/root/source/current
```

Use `compare_and_swap_named_root` when more than one writer can publish the same root name.

```rust
let current = prolly.load_named_root(b"main")?;
let base = current.clone().unwrap_or_else(|| prolly.create());
let next = prolly.put(&base, b"key".to_vec(), b"value".to_vec())?;

let update = prolly.compare_and_swap_named_root(
    b"main",
    current.as_ref(),
    Some(&next),
)?;
```

If the update is not applied, another writer published first. Reload the named root, merge or reapply your change, then retry.

### Prefer structural APIs

The tree is designed to skip unchanged content-addressed subtrees. Prefer:

- `range` or `range_page` over full-tree reads
- `diff` over comparing decoded application objects
- `batch` over repeated single-key edits
- `merge` over last-writer-wins at the root pointer
- `plan_missing_nodes` and `copy_missing_nodes` over exporting full snapshots
- retention-aware garbage collection (GC) over deleting files directly

## Basic ordered map

Use this recipe when you need a versioned ordered map with point lookups, deletes, and prefix scans. It maps to [`basic_map.rs`](../examples/basic_map.rs).

### Background

Application state usually starts as a map: users, documents, settings, tasks, or records. A mutable hash map works inside one process, but it cannot give you immutable snapshots, branchable state, or structural diffs.

### How prolly helps

`Prolly::put` and `Prolly::delete` return a new `Tree`. Old tree handles remain valid while their nodes remain in the store. `range` scans byte-ordered keys, so key prefixes become application namespaces.

### Reference design

Use a prefix per collection:

```text
user:<id>      -> user record
team:<id>      -> team record
setting:<name> -> setting value
```

For production schemas, prefer `KeyBuilder` segments over raw separators.

### Implementation sketch

The example creates a tree, inserts three users, deletes one user, then scans the `user:` range.

```rust
let tree = prolly.create();
let tree = prolly.put(&tree, b"user:001".to_vec(), b"Ada".to_vec())?;
let tree = prolly.put(&tree, b"user:002".to_vec(), b"Grace".to_vec())?;
let tree = prolly.delete(&tree, b"user:003")?;

let users = prolly
    .range(&tree, b"user:", Some(b"user;"))?
    .collect::<Result<Vec<_>, _>>()?;
```

### Future enhancements

- Add `VersionedValue` envelopes for typed records
- Publish the root as `main`
- Add secondary indexes with `diff`
- Switch from raw prefix strings to `KeyBuilder`
- Add range pagination for API responses

## Bulk import and tree statistics

Use this recipe when you need to load a large initial dataset. It maps to [`batch_build.rs`](../examples/batch_build.rs).

### Background

Repeated `put` calls work for small data, but imports and index rebuilds benefit from building leaves and internal nodes in bulk. Bulk building also gives you a clean place to inspect tree shape before publishing.

### How prolly helps

`BatchBuilder` accepts unsorted entries and builds a tree with the configured chunking rules. `SortedBatchBuilder` fits already sorted exports. `collect_stats` reports node count, entries, height, fill factor, and serialized size.

### Reference design

Use bulk builders for:

- first import from another database
- rebuilding a materialized view
- rebuilding a retrieval index
- compacting append-only events into a smaller tree
- loading fixtures for conformance tests

### Implementation sketch

```rust
let mut builder = BatchBuilder::new(store.clone(), config.clone());
for index in (0..1_000).rev() {
    builder.add(
        format!("event:{index:04}").into_bytes(),
        format!("payload-{index}").into_bytes(),
    );
}

let tree = builder.build()?;
let stats = prolly.collect_stats(&tree)?;
```

### Future enhancements

- Use `SortedBatchBuilder` for sorted source data
- Write import progress by named root checkpoints
- Compare stats across configs before choosing production chunk sizes
- Add a conformance fixture that records expected root CIDs

## Local-first application state

Use this recipe when local devices or background workers need independent branches that merge into a canonical root.

### Background

Local-first apps need optimistic writes, offline branches, and sync. A single mutable "current state" row makes branch reconciliation hard because you cannot diff, replay, or copy only missing state.

### How prolly helps

Each replica can publish its own named root. The canonical root advances with compare-and-swap. Merge compares `base`, `left`, and `right`, so a sync job can preserve both local and remote changes when they touch different keys.

### Reference design

Root names:

```text
app/<app-id>/root/main
app/<app-id>/device/<device-id>/head
app/<app-id>/sync/<peer-id>/last-seen
app/<app-id>/checkpoint/<timestamp>
```

Key layout:

```text
entity/<collection>/<id>                 -> entity value
index/<collection>/<field>/<value>/<id>  -> empty marker or pointer
meta/migration                           -> schema version
```

### Implementation sketch

Use one batch for source and index changes:

```rust
let next = prolly.batch(
    &base,
    vec![
        Mutation::Upsert {
            key: b"entity/user/001".to_vec(),
            val: br#"{"name":"Ada"}"#.to_vec(),
        },
        Mutation::Upsert {
            key: b"index/user/name/Ada/001".to_vec(),
            val: Vec::new(),
        },
    ],
)?;
```

Publish with CAS:

```rust
let update = prolly.compare_and_swap_named_root(
    b"app/demo/root/main",
    current.as_ref(),
    Some(&next),
)?;
```

### Operations

- Keep device roots until peers confirm sync
- Use `plan_missing_nodes` to copy only unknown CIDs
- Use `range_page` for background indexing work
- Use `debug_compare_trees` when a sync job publishes an unexpected root
- Run GC after retention roots are loaded and complete

### Future enhancements

- Add per-key merge policy rules
- Store tombstone values for deletes that must replicate before compaction
- Add remote object storage with `AsyncStore`
- Add root manifests for every materialized view derived from main

## Diff and merge for branches

Use this recipe when two branches edit the same base snapshot. It maps to [`diff_merge.rs`](../examples/diff_merge.rs).

### Background

Branching is useful only if you can understand and combine the changes. Prolly trees make this cheap because unchanged subtrees keep the same CID.

### How prolly helps

`diff(base, other)` returns added, removed, and changed keys. `merge(base, left, right, None)` applies disjoint changes without a resolver.

### Reference design

Use this flow for branch reconciliation:

1. Record the base root when a branch starts
2. Let each branch write a new tree
3. Preview `diff(base, branch)` for review or audit
4. Merge `base`, `left`, and `right`
5. CAS-publish the merged root

### Implementation sketch

```rust
let base = prolly.put(&prolly.create(), b"doc:title".to_vec(), b"Draft".to_vec())?;
let left = prolly.put(&base, b"doc:body".to_vec(), b"Hello".to_vec())?;
let right = prolly.put(&base, b"doc:tags".to_vec(), b"example".to_vec())?;

let changes = prolly.diff(&base, &left)?;
let merged = prolly.merge(&base, &left, &right, None)?;
```

### Future enhancements

- Add `merge_explain` output to audit logs
- Use `stream_conflicts` before applying a merge
- Add range-limited merge for partitioned keyspaces
- Add UI copy for conflict summaries

## Delete-aware merge resolvers

Use this recipe when concurrent edits can conflict. It maps to [`resolver.rs`](../examples/resolver.rs).

### Background

Delete/update conflicts need explicit absence. If a resolver treats missing values as empty bytes, it can resurrect deleted records or delete valid empty values.

### How prolly helps

Merge conflicts preserve all sides as optional values:

```rust
pub struct Conflict {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub left: Option<Vec<u8>>,
    pub right: Option<Vec<u8>>,
}
```

`None` means absent. `Some(Vec::new())` means the key is present with an empty value.

### Reference design

Choose policies by key family:

```text
settings/          update-wins
permissions/       delete-wins
documents/body/    domain merge
billing/           unresolved
```

### Implementation sketch

```rust
let update_wins = prolly.merge(
    &base,
    &left,
    &right,
    Some(Box::new(resolver::update_wins)),
)?;

let delete_wins = prolly.merge(
    &base,
    &left,
    &right,
    Some(Box::new(resolver::delete_wins)),
)?;
```

Use a policy registry when one resolver needs multiple rules:

```rust
let policies = MergePolicyRegistry::with_default(|_| Resolution::unresolved())
    .add_prefix(b"settings/".to_vec(), resolver::update_wins)
    .add_prefix(b"permissions/".to_vec(), resolver::delete_wins);

let merged = prolly.merge(&base, &left, &right, Some(policies.into_resolver()))?;
```

### Future enhancements

- Decode structured values inside resolver callbacks
- Persist unresolved conflicts as records for review
- Add key-family policy tests
- Use `merge_explain` to show which policy resolved each conflict

## Conflict-free custom merge

Use this recipe when your application requires merge to always produce a tree. It maps to [`crdt_merge.rs`](../examples/crdt_merge.rs).

### Background

Some systems cannot pause for manual conflict review. Counters, presence records, tag sets, and generated alternatives often need deterministic conflict-free merge behavior.

### How prolly helps

`crdt_merge` uses `CrdtConfig`. Built-in strategies include last-writer-wins and multi-value. Custom strategies receive the same `Conflict` shape and return `CrdtResolution::Value` or `CrdtResolution::Delete`.

### Reference design

Use CRDT-style merge for:

- additive counters
- multi-value alternatives
- timestamped records
- generated artifacts where all alternatives should be retained
- delete/update policies that should never return `Error::Conflict`

### Implementation sketch

```rust
let sum_values = CrdtConfig::custom(|conflict| {
    let left = conflict.left.as_deref().and_then(parse_u64).unwrap_or_default();
    let right = conflict.right.as_deref().and_then(parse_u64).unwrap_or_default();
    CrdtResolution::value((left + right).to_string().into_bytes())
});

let merged = prolly.crdt_merge(&base, &left, &right, &sum_values)?;
```

### Future enhancements

- Add schema-aware CRDT value types
- Add a bounded multi-value strategy for generated candidates
- Store causal metadata in values when your domain needs it
- Add property tests for commutativity and determinism

## Conversation memory

Use this recipe when agents extract durable memories from conversations. It maps to [`conversation_memory.rs`](../examples/conversation_memory.rs).

### Background

Agent memory needs reviewable branches. An extraction agent may propose memories, while the canonical conversation state can change at the same time.

### How prolly helps

Attempt roots let agents write speculative memories without publishing them. The canonical memory root advances through CAS. Merge accepts an attempt branch against the base it started from.

### Reference design

Root names:

```text
conversation/<conversation-id>/root/main
conversation/<conversation-id>/attempt/<actor>/<attempt>
```

Key layout:

```text
conversation/<conversation-id>/memory/<memory-id> -> MemoryRecord
```

Value fields:

- `subject`
- `fact`
- `source`
- `confidence`

### Implementation sketch

The example writes an initial memory, publishes `main`, writes an agent attempt, handles a concurrent canonical update, then merges the attempt back.

```rust
let canonical_before_accept = prolly
    .load_named_root(&main_root)?
    .expect("main exists");

let merged = prolly.merge(
    &base,
    &canonical_before_accept,
    &agent_attempt,
    None,
)?;
```

### Operations

- Store the base root for every attempt
- Use `VersionedValue` for memory schemas
- Keep attempt roots until accepted, rejected, or expired
- Use delete-aware resolvers for user-deleted memories
- Record accepted root CIDs in audit logs

### Future enhancements

- Add confidence thresholds before accepting attempt roots
- Add user review queues for unresolved memory conflicts
- Add retention rules for stale attempts
- Add provenance links to the source event log

## Agent event logs

Use this recipe when you need an ordered, replayable record of agent activity. It maps to [`agent_event_log.rs`](../examples/agent_event_log.rs).

### Background

Agents produce more than messages. Tool calls, tool results, memory writes, checkpoints, and summaries all matter for audit and recovery. A plain append log can record events, but it cannot branch or attach immutable memory roots cleanly.

### How prolly helps

Prolly keys give ordered event ranges. Values can contain typed event variants. Checkpoint events can embed a `Tree` handle, so the log records the exact memory root accepted at that step.

### Reference design

Root names:

```text
agent-log/<run-id>/root/events/current
agent-log/<run-id>/root/memory/current
```

Event keys:

```text
agent-log/<run-id>/event/<timestamp>/<sequence>
```

Event kinds:

- user message
- assistant message
- tool call
- tool result
- memory write
- checkpoint
- summary compaction

### Implementation sketch

Use a batch append for a group of events:

```rust
let mutations = events
    .iter()
    .map(|event| {
        Ok(Mutation::Upsert {
            key: event_key(event),
            val: encode_event(event)?,
        })
    })
    .collect::<Result<Vec<_>, Error>>()?;

let log = prolly.batch(&base, mutations)?;
```

### Operations

- Include the event sequence in the key to break timestamp ties
- Store tool arguments and results as values, not logs outside the tree
- Publish `events/current` after successful batch writes
- Page long logs with `range_page`
- Use summaries to reduce long-context replay cost

### Future enhancements

- Use `append_batch` for append-heavy runs
- Add checksum fields for large tool outputs stored in blobs
- Add signed event envelopes for audit
- Add automatic compaction windows by token count or event count

## Background compaction and retention-aware GC

Use this recipe when logs or memory trees need compaction without losing auditability. It maps to [`background_compaction.rs`](../examples/background_compaction.rs).

### Background

Long-running agents can accumulate large event histories. You may want to replace old event ranges with summaries while retaining enough roots for rollback and audit.

### How prolly helps

Because every root is immutable, compaction creates a new tree instead of rewriting the old log. Named root retention decides which historical roots survive GC. `plan_store_gc_for_retention` reports reclaimable nodes before sweep.

### Reference design

Root names:

```text
compaction/run/<run-id>/root/events/0001
compaction/run/<run-id>/root/events/current
compaction/run/<run-id>/root/summary-index/current
```

Compaction flow:

1. Load the current event log
2. Delete the compacted source event keys
3. Insert one `SummaryCompaction` event
4. Rebuild a summary index
5. Publish `events/current` and `summary-index/current`
6. Plan GC using retained named roots
7. Sweep only after the plan looks correct

### Implementation sketch

The example deletes an event window and inserts a summary event in one batch:

```rust
let mut mutations = (first_sequence..=last_sequence)
    .map(|sequence| Mutation::Delete {
        key: event_key(run_id, sequence),
    })
    .collect::<Vec<_>>();

mutations.push(Mutation::Upsert {
    key: event_key(run_id, compacted_sequence),
    val: encode_event(&summary_event)?,
});
```

It then retains selected named roots:

```rust
let retention = NamedRootRetention::exact(vec![
    root_name(run_id, "events/0001"),
    root_name(run_id, "events/current"),
    root_name(run_id, "summary-index/current"),
]);

let plan = prolly.plan_store_gc_for_retention(&retention)?;
```

### Future enhancements

- Add retention classes for legal hold, audit, and cache-only roots
- Compact by token budget instead of sequence range
- Store summaries with source root CIDs and model metadata
- Run compaction in an async worker against remote storage

## Deterministic RAG snapshots

Use this recipe when generated answers must be replayable against the exact retrieval metadata used at answer time. It maps to [`deterministic_rag_snapshot.rs`](../examples/deterministic_rag_snapshot.rs).

### Background

Retrieval-augmented generation (RAG) systems change over time. Documents are re-parsed, chunks are re-embedded, and indexes are republished. If you store only answer text and citations, you cannot prove which index state produced them.

### How prolly helps

A `Tree` snapshot pins the chunk metadata used for retrieval. Answer records can store `index_snapshot: Tree`, so replay uses the old root even after the current index changes.

### Reference design

Root names:

```text
rag/corpus/<corpus-id>/root/index/current
rag/corpus/<corpus-id>/root/answers
```

Keys:

```text
rag/corpus/<corpus-id>/chunk/<doc-id>/<chunk-id> -> DocumentChunk
rag/answer/<query-id>                           -> AnswerRecord
```

Answer values should include:

- query text or query ID
- answer text
- index snapshot
- citations
- parser version
- source URI

### Implementation sketch

Store the answer with the root used for retrieval:

```rust
let index_snapshot = prolly
    .load_named_root(&current_index_name)?
    .expect("current index exists");

let answer = answer_from_snapshot(&prolly, &index_snapshot, corpus_id, query)?;
```

Replay later:

```rust
let replayed = answer_from_snapshot(
    &prolly,
    &stored_answer.index_snapshot,
    corpus_id,
    &stored_answer.query,
)?;
```

### Future enhancements

- Store vector model and embedding dimensions with each answer
- Store prompt and model IDs for answer generation
- Add answer-root retention policies
- Add fixtures that prove replay returns identical citations

## Document chunk indexes

Use this recipe when you need ordered document and chunk metadata, with large text kept outside leaf nodes. It maps to [`document_chunk_index.rs`](../examples/document_chunk_index.rs).

### Background

Document indexes need metadata lookups, ordered chunk scans, and access to large chunk text. Storing long text in every leaf can make nodes heavy and reduce cache efficiency.

### How prolly helps

The tree stores small metadata records and `ValueRef` entries for large text. `BlobStore` holds chunk text. A vector sidecar can hold embeddings keyed by a stable vector ID.

### Reference design

Keys:

```text
doc-index/<corpus>/document/<doc-id>/meta
doc-index/<corpus>/parser/<parser>/document/<doc-id>/chunk/<start-byte>
doc-index/<corpus>/text/<parser>/<doc-id>/<chunk-id>
```

Chunk metadata fields:

- document ID
- chunk ID
- parser version
- byte range
- text key
- vector ID

### Implementation sketch

Store chunk text through `put_large_value`:

```rust
let policy = LargeValueConfig::new(32);
tree = prolly.put_large_value(
    blobs,
    &tree,
    text_key.clone(),
    chunk.text.into_bytes(),
    policy,
)?;
```

Store metadata inline:

```rust
tree = prolly.put(
    &tree,
    chunk_metadata_key(corpus_id, parser_version, doc_id, start_byte),
    encode_chunk(&metadata)?,
)?;
```

### Future enhancements

- Add parser-version roots for side-by-side re-ingestion
- Use file or object blob stores for chunk text
- Add diff-based re-embedding for changed chunks
- Store checksums for blob payload validation

## Vector sidecars

Use this recipe when a vector database scores embeddings while prolly stores reproducible metadata. It maps to [`vector_sidecar.rs`](../examples/vector_sidecar.rs).

### Background

Approximate nearest neighbor (ANN) systems are optimized for vector search, not immutable application snapshots. If a vector index changes, old answers can become hard to replay.

### How prolly helps

Prolly stores the metadata set and vector IDs for a snapshot. The sidecar stores embeddings. Retrieval filters sidecar hits to vector IDs present in the chosen prolly root.

### Reference design

Keys:

```text
vector-sidecar/corpus/<corpus-id>/chunk/<doc-id>/<chunk-id>
vector-sidecar/answer/<answer-id>
```

Chunk metadata should include:

- corpus ID
- document ID
- chunk ID
- source URI
- parser version
- vector ID
- embedding model
- embedding dimensions

### Implementation sketch

Build the allowed vector set from the snapshot:

```rust
let metadata = metadata_by_vector_id(prolly, index_snapshot, corpus_id)?;
let allowed = metadata.keys().cloned().collect::<HashSet<_>>();
let hits = sidecar.search_filtered(query_embedding, &allowed, limit);
```

Store answers with the metadata root:

```rust
let answer = AnswerRecord {
    query: query.to_string(),
    embedding_model: EMBEDDING_MODEL.to_string(),
    index_snapshot: index_snapshot.clone(),
    citations,
    answer,
};
```

### Future enhancements

- Add sidecar garbage collection from retained prolly roots
- Store vector checksums or version IDs
- Support multiple embedding models per corpus
- Add replay tests for old answers after sidecar updates

## Provenance-rich values

Use this recipe when every generated claim needs source, parser, model, and parent-record provenance. It maps to [`provenance_values.rs`](../examples/provenance_values.rs).

### Background

AI applications need to answer "where did this come from?" Claims derived from documents should carry enough context to audit the source and the pipeline.

### How prolly helps

Values can store source CIDs, chunk CIDs, parent keys, and pipeline metadata. The tree root pins the set of claims. Range scans by source file retrieve all derived claims for review.

### Reference design

Keys:

```text
provenance/chunk/<file-id>/<chunk-id>
provenance/claim/<file-id>/<claim-id>
```

Value records:

- `SourceRef`: source URI, file ID, source CID
- `PipelineRef`: parser, embedding model, dimensions, summarizer model, prompt version
- `ChunkRecord`: byte range, chunk text, chunk CID, pipeline
- `DerivedClaim`: claim text, confidence, parent chunk key, source CIDs, pipeline

### Implementation sketch

Calculate content IDs for source bytes and chunks:

```rust
let source = SourceRef {
    source_uri,
    file_id,
    source_cid: Cid::from_bytes(source_bytes),
};

let chunk_cid = Cid::from_bytes(chunk_text.as_bytes());
```

Scan claims for one source:

```rust
let (start, end) = prefix_range(claim_prefix(&source.file_id));
let claims = prolly
    .range(&tree, &start, end.as_deref())?
    .collect::<Result<Vec<_>, _>>()?;
```

### Future enhancements

- Add signatures for pipeline outputs
- Store prompt templates as content-addressed records
- Link claims to answer records
- Add provenance graph export for audits

## Secondary indexes from diffs

Use this recipe when a source tree needs a derived lookup structure. It maps to [`secondary_index.rs`](../examples/secondary_index.rs).

### Background

An ordered map gives one primary key order. Applications need alternate access paths: users by status, documents by tag, jobs by priority, or memories by subject.

### How prolly helps

Build the secondary index as another tree. Use `diff(source_v1, source_v2)` to update the index incrementally, then compare the result to a full rebuild in tests.

### Reference design

Source key:

```text
source/tenant/<tenant-id>/user/<user-id>
```

Index key:

```text
index/user-by-status/tenant/<tenant-id>/status/<status>/<user-id>
```

Index values can be empty marker bytes or compact pointers to source keys.

### Implementation sketch

Map source diffs to index mutations:

```rust
match diff {
    Diff::Added { val, .. } => upsert_index(decode_user(val)?),
    Diff::Removed { val, .. } => delete_index(decode_user(val)?),
    Diff::Changed { old, new, .. } => update_if_index_key_changed(old, new)?,
}
```

Apply the derived mutations in one batch:

```rust
let index_v2 = prolly.batch(&index_v1, mutations)?;
```

### Operations

- Publish source and index roots together through a view manifest or adjacent named roots
- Store the source root used to build each index root
- Rebuild from source periodically to detect index drift
- Treat index roots as disposable if source roots are retained

### Future enhancements

- Add helper APIs for source/index manifests
- Add drift-check tooling
- Add multi-index update batches
- Add async background indexing with `AsyncStore`

## Materialized views from diffs

Use this recipe when a derived tree stores aggregates rather than alternate keys. It maps to [`materialized_view.rs`](../examples/materialized_view.rs).

### Background

Dashboards and repeated queries need precomputed aggregates. Recomputing from source for every request wastes work, but mutable aggregate tables can drift from source if updates fail halfway.

### How prolly helps

A materialized view is another immutable tree. Diff the source, translate changes into aggregate deltas, build a new view root, and publish a manifest that records the source and view snapshots together.

### Reference design

Source key:

```text
orders/source/tenant/<tenant-id>/order/<order-id>
```

View key:

```text
orders/view/by-status/tenant/<tenant-id>/status/<status>
```

Manifest value:

```text
view_name
source_snapshot
view_snapshot
source_diff_count
```

### Implementation sketch

The example computes per-status revenue. Each source diff updates aggregate deltas:

```rust
match diff {
    Diff::Added { val, .. } => record_delta(decode_order(val)?, 1),
    Diff::Removed { val, .. } => record_delta(decode_order(val)?, -1),
    Diff::Changed { old, new, .. } => {
        record_delta(decode_order(old)?, -1);
        record_delta(decode_order(new)?, 1);
    }
}
```

After applying deltas, publish the view and manifest roots.

### Future enhancements

- Add transaction-like publish helpers for source, view, and manifest roots
- Add incremental rebuild checkpoints
- Add view invalidation when schema versions change
- Add range-limited views for tenant partitions

## Large values and file blob storage

Use this recipe when values are too large to keep inside tree nodes. It maps to [`file_blob_store.rs`](../examples/file_blob_store.rs).

### Background

Large documents, tool outputs, media, and chunk text can make leaf nodes expensive to read and cache. The ordered tree should keep metadata and references; payload bytes can live elsewhere.

### How prolly helps

`put_large_value` stores small values inline and large values in a `BlobStore` according to `LargeValueConfig`. The tree stores a `ValueRef`, so GC can trace retained roots and remove unreferenced blobs.

### Reference design

Store:

```text
node store: content-addressed tree nodes
blob store: large payload bytes
tree value: ValueRef::Inline or ValueRef::Blob
```

Use this for:

- document text
- large tool outputs
- transcript chunks
- generated artifacts
- binary attachments

### Implementation sketch

```rust
let blobs = FileBlobStore::open(&blob_dir)?;
let policy = LargeValueConfig::new(1024);

let tree = prolly.put_large_value(
    &blobs,
    &tree,
    b"doc/body".to_vec(),
    vec![7; 4096],
    policy,
)?;
```

Sweep unreachable blobs after choosing retained roots:

```rust
let plan = prolly.plan_blob_store_gc(&blobs, std::slice::from_ref(&tree))?;
let sweep = prolly.sweep_blob_store_gc(&blobs, std::slice::from_ref(&tree))?;
```

### Future enhancements

- Use object storage for blobs
- Store checksums and MIME types in metadata values
- Add blob replication before publishing roots
- Add retention classes for attachments

## Filesystem snapshots with named roots

Use this recipe when you want Git-like filesystem snapshots where file contents live in blobs and a named root points at the current tree. It maps to [`filesystem_snapshot.rs`](../examples/filesystem_snapshot.rs).

### Background

A filesystem snapshot is a map from relative path to file state. The expensive bytes are file contents; the state you want to branch, diff, merge, and publish is the ordered path map.

### How prolly helps

`BatchBuilder` can build the initial path map in bulk. `BlobStore` keeps file contents content-addressed. `publish_named_root` moves a durable name such as `refs/heads/main` only after the tree nodes and blob bytes have been written, so a crash before publish leaves the previous snapshot visible.

For later snapshots, load the named root, apply changed path mutations, and use `compare_and_swap_named_root` to move the branch head only if it still points at the tree you edited from.

### Reference design

Root names:

```text
refs/heads/main
refs/heads/<branch>
checkpoint/<timestamp-or-sequence>
```

Tree keys:

```text
path/README.md   -> ValueRef::Blob(file-content-cid)
path/src/lib.rs  -> ValueRef::Blob(file-content-cid)
```

For production, store a typed file-entry envelope instead of a bare `ValueRef`. Include mode, executable bit, size, content CID, optional MIME type, and any platform metadata you need to preserve.

### Implementation sketch

Build the initial snapshot from file blobs:

```rust
let mut builder = BatchBuilder::new(store.clone(), config.clone());

for path in ["README.md", "src/lib.rs"] {
    let bytes = std::fs::read(path)?;
    let blob_ref = blobs.put_blob(&bytes)?;

    builder.add(
        format!("path/{path}").into_bytes(),
        ValueRef::Blob(blob_ref).to_bytes(),
    );
}

let snapshot = builder.build()?;
prolly.publish_named_root(b"refs/heads/main", &snapshot)?;
```

Resolve a snapshot and read file contents:

```rust
let snapshot = prolly
    .load_named_root(b"refs/heads/main")?
    .expect("branch exists");

let readme = prolly.get_large_value(&blobs, &snapshot, b"path/README.md")?;
```

Publish an incremental snapshot with CAS:

```rust
let base = prolly
    .load_named_root(b"refs/heads/main")?
    .unwrap_or_else(|| prolly.create());

let bytes = std::fs::read("README.md")?;
let blob_ref = blobs.put_blob(&bytes)?;
let next = prolly.put(
    &base,
    b"path/README.md".to_vec(),
    ValueRef::Blob(blob_ref).to_bytes(),
)?;

let update = prolly.compare_and_swap_named_root(
    b"refs/heads/main",
    Some(&base),
    Some(&next),
)?;
```

If the update conflicts, another writer moved the branch first. Reload the named root, reapply or merge your path changes, then retry.

### Future enhancements

- Encode `FileEntry` metadata with `VersionedValue`
- Use `batch` for incremental snapshots with many changed, deleted, or renamed paths
- Store directory entries or derive them from path prefixes
- Add ignore rules and path normalization before building keys
- Run blob and node GC from retained branch, tag, and checkpoint roots

## Durable SQLite setup

Use this recipe when a desktop app, local-first app, command-line tool, or agent worker needs a durable embedded store.

### Background

`MemStore` is right for tests and examples. Applications need durability, backups, concurrency settings, and GC discipline.

### How prolly helps

`SqliteStore` persists content-addressed nodes and manifests in one SQLite database. Named roots become durable branch heads. SQLite write transactions give `batch` and manifest updates a strong local backend.

### Reference design

Cargo feature:

```toml
[dependencies]
prolly-map = { version = "0.1", features = ["sqlite"] }
```

Setup:

```rust
use std::sync::Arc;
use prolly::{Config, Prolly, SqliteStore, SqliteStoreConfig};

let store = Arc::new(SqliteStore::open_with_config(
    "app.prolly.sqlite",
    SqliteStoreConfig {
        busy_timeout_ms: 5_000,
        enable_wal: true,
        synchronous_normal: true,
    },
)?);

let config = Config::builder()
    .node_cache_max_nodes(50_000)
    .node_cache_max_bytes(256 * 1024 * 1024)
    .build();

let prolly = Prolly::new(store, config);
```

### Operations

- Keep SQLite `-wal` and `-shm` files with the database during backup
- Use WAL mode for app-style read/write concurrency
- Use non-zero `busy_timeout_ms` for contended local writes
- Publish every durable head as a named root
- Run `plan_store_gc_for_retention` before sweeping
- Use `TokioBlockingStore` in async Tokio apps that call a sync SQLite backend

### Future enhancements

- Add app-level migrations stored under a metadata key
- Add backup and restore examples
- Add store health checks before startup
- Add CLI inspection for SQLite-backed stores

## Object-store backed snapshots

Use this recipe when tree nodes and blobs should live in S3, R2, or another object store.

### Background

Object stores have high latency and cheap durable bytes. They fit immutable content-addressed nodes, but root updates need conditional writes and reads should be concurrent.

### How prolly helps

`AsyncStore` lets object-store reads overlap. Content-addressed node writes are idempotent because the key is the CID. Missing-node planning can upload only content that the destination lacks.

### Reference design

Object layout:

```text
nodes/<cid>       -> serialized node bytes
blobs/<digest>    -> large value bytes
roots/<name>      -> root manifest
hints/<namespace> -> optional performance hints
```

Store behavior:

- `put` is idempotent
- `batch_get_ordered` uses native multi-get or concurrent point reads
- `delete` is reserved for GC
- root manifest updates use conditional writes
- hot internal nodes are cached near the application

### Implementation sketch

Enable async support:

```toml
[dependencies]
prolly-map = { version = "0.1", features = ["async-store"] }
```

Implement `AsyncStore` over your object client. Override `read_parallelism` or `batch_get_ordered` so tree traversal can overlap network reads.

### Future enhancements

- Add an official object-store backend
- Add upload manifests for multi-object publish
- Add background prefetch for hot subtrees
- Add signed root manifests for peer sync

## Browser and WASM storage

Use this recipe when a web app needs local branches, offline reads, and eventual sync.

### Background

Browser storage APIs are async. IndexedDB, Origin Private File System (OPFS), Cache Storage, and remote APIs do not fit a blocking `Store` trait.

### How prolly helps

`AsyncStore` is single-thread friendly and does not require Tokio. A browser store can expose async point reads and writes while `AsyncProlly` handles tree operations.

### Reference design

Storage choices:

- IndexedDB for node bytes and root manifests
- OPFS for larger local blobs
- remote API for peer sync
- in-memory cache for hot nodes

Root names:

```text
browser/<installation-id>/root/main
browser/<installation-id>/root/outbox
browser/<installation-id>/root/synced/<peer-id>
```

### Implementation sketch

Use async APIs in browser-facing code:

```rust
let tree = prolly.create();
let tree = prolly.put(&tree, b"k".to_vec(), b"v".to_vec()).await?;
let value = prolly.get(&tree, b"k").await?;
```

Sync flow:

1. Load local and remote named roots
2. Plan missing local nodes for upload
3. Plan missing remote nodes for download
4. Merge roots if both sides changed
5. CAS-publish the merged root locally and remotely

### Future enhancements

- Add a WASM example store
- Add range-page UI examples
- Add background sync with retry state in named roots
- Add quota-aware blob retention
