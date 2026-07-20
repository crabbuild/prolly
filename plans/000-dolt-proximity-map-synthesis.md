# Dolt Proximity Map: Design Synthesis and Rust Adoption Map

Date: 2026-07-13

Analyzed revisions:

- Rust prolly-map: fa7c219
- Dolt: 6b2372c7d4ded1a54f55c6204304dbb72a33835c

Validation performed:

- cargo test: passed the Rust baseline, including 421 library tests, all
  integration tests, and 97 doctests (2 ignored).
- go test ./store/prolly -run '^TestProximityMap$' -count=1: passed in the
  nested Dolt checkout.

This is a design study, not a claim that the two wire formats are compatible.
Dolt's code is Apache-2.0-licensed. The implementation recommended here should
be written from this behavioral specification and the Rust codebase's
conventions, not copied line-for-line from Dolt.

## Executive conclusion

Dolt's strongest idea is not “use distance as a B-tree comparator.” Its
ProximityMap is a deterministic, content-addressed, hierarchical Voronoi-style
index:

1. A stable hash assigns every key a promotion level.
2. Promoted vectors become representatives at higher levels.
3. Each lower-level vector is assigned to its nearest representative inside
   the current ancestor cluster.
4. Internal entries point to immutable child nodes.
5. Search keeps a bounded set of the nearest representatives while descending.

That combination is unusually well suited to versioned storage. Equal logical
content produces equal roots, unchanged clusters retain their content IDs, and
an ANN index can be diffed, copied, retained, and published like other immutable
state.

Rust should borrow the deterministic hierarchy, content-addressed nodes,
nearest-representative invariant, and bounded beam search. It should not port
Dolt's map interface literally. The recommended Rust design is a compound
immutable ProximityTree:

    ProximityTree descriptor CID
      ├─ authoritative ordered directory Tree
      │    key -> canonical(vector, value)
      └─ derived proximity root
           representative vector/key -> child CID

The ordered directory makes exact get, absent-key detection, delete, logical
diff, merge, and duplicate-vector handling correct. The proximity root is
derived state optimized only for ANN search. This uses the existing Rust
implementation's strongest features instead of forcing ordered-map operations
through a non-ordered tree.

The first implementation should be deterministic bulk build plus ANN search.
Correctness-first mutations may rebuild the derived root from the updated
directory. Localized copy-on-write mutation should be a later optimization,
gated by structural-equivalence tests, recall benchmarks, and write-amplification
measurements.

## 1. What Dolt actually built

### 1.1 Data model

Dolt's low-level tree type is in:

- dolt/go/store/prolly/tree/proximity_map.go:34-42

It contains a NodeStore, a distance function, an ordering used only within a
node, a key-to-vector conversion function, and a root node. The package-level
wrapper in dolt/go/store/prolly/proximity_map.go:34-43 adapts raw byte nodes to
typed Dolt tuples.

Unlike an ordered prolly tree:

- Node keys are sorted only to make node encoding and iteration deterministic.
- Internal keys are representatives, not key-range separators.
- Routing scans every representative in a visited node and chooses by distance.
- Walking the leaves does not provide global key order.

The phrase “ProximityMap” is therefore slightly misleading. The physical
structure is an immutable hierarchical proximity index with leaf payloads.

### 1.2 Deterministic promotion levels

Dolt assigns a key to a native level with:

    level(key) = floor(leading_zeros_32(hash(key)) / log_chunk_size)

Evidence:

- dolt/go/store/prolly/tree/node_splitter.go:271-275
- dolt/go/store/prolly/proximity_map.go:280-299

Let B = 2^log_chunk_size. Ignoring the one hash value whose bits are all zero:

    P(level >= l) = B^-l

With Dolt's default log_chunk_size = 8, B = 256. Roughly one key in 256 is
promoted above the leaf level, one in 65,536 reaches level 2, and so on.

This gives three important properties:

- Expected fanout is controlled without insertion-order-dependent splits.
- The same key always has the same native level.
- A rebuild from the same entries has the same hierarchy.

The level is based on the full encoded key. In Dolt vector indexes that key
includes the vector plus the underlying row identity.

### 1.3 Hierarchical nearest-representative invariant

The builder processes keys from highest native level to lowest. For each key it
walks already-built higher levels. At each level it chooses the closest
candidate representative under the current path prefix. It then records the
key in its native level and repeats a promoted key through every lower level.

Evidence:

- Builder overview: dolt/go/store/prolly/proximity_map.go:232-267
- Path construction: dolt/go/store/prolly/proximity_map.go:395-503
- Candidate range by path prefix: dolt/go/store/prolly/proximity_map.go:531-539
- Closest-candidate scan: dolt/go/store/prolly/proximity_map.go:541-581

The resulting invariant is:

> Every representative in a child node is at least as close to that child's
> parent representative as it is to any uncle representative in the parent
> node, with deterministic input order breaking ties.

Dolt explicitly validates this invariant in
dolt/go/store/prolly/proximity_map_test.go:161-197.

This is best understood as a hierarchy of deterministic Voronoi partitions.
Each internal representative owns one cell; its child node contains the
representatives assigned to that cell at the next level.

### 1.4 Three-stage bulk build

Dolt cannot directly write the final Merkle tree top-down because child hashes
are not known until children are serialized. Its builder therefore uses
ordinary ordered prolly maps as scratch structures:

1. levelMap stores (255 - native_level, encoded_key) -> value, so iteration
   visits high levels first.
2. One pathMap per physical level stores
   (root_rep, ..., parent_rep, key) -> value.
3. A linked chain of vectorIndexChunker objects consumes pathMaps and writes the
   final nodes bottom-up.

Evidence:

- Level map: dolt/go/store/prolly/proximity_map.go:202-229 and 280-325
- Path maps: dolt/go/store/prolly/proximity_map.go:384-528
- Bottom-up assembly: dolt/go/store/prolly/proximity_map.go:583-623
- Stateful chunker chain:
  dolt/go/store/prolly/vector_index_chunker.go:28-125

The transformation is clever because it reuses Dolt's mature ordered prolly
map implementation. The cost is scratch write amplification: the comments at
dolt/go/store/prolly/proximity_map.go:264-267 acknowledge that temporary nodes
are written into the production NodeStore and later become garbage.

### 1.5 Node format

Dolt uses a distinct FlatBuffers message with file identifier IVFF:

- dolt/go/serial/vectorindexnode.fbs:19-64
- dolt/go/store/prolly/message/vector_index.go:31-107

Each node contains:

- sorted encoded keys;
- leaf values or internal child addresses;
- subtree counts and total tree count;
- tree level;
- log chunk size;
- distance type.

The separate file identifier prevents an ordered Map from silently opening a
vector node. The shape-affecting distance and promotion settings are encoded
into node bytes, so they contribute to content IDs. That is a strong storage
design even though Dolt's current open path does not fully exploit it.

### 1.6 Search

Exact-looking Get performs greedy routing:

1. Decode the query vector.
2. Scan every key in the current node.
3. Descend through the closest representative.
4. At the leaf, return the closest entry.

Evidence: dolt/go/store/prolly/tree/proximity_map.go:95-137.

GetClosest performs a level-synchronous beam search:

1. Score all root representatives.
2. Retain at most limit candidates in a min-max heap.
3. For each level, read the retained candidates' child nodes.
4. Score every child representative and retain the nearest limit.
5. Return the final leaf candidates in increasing distance order.

Evidence: dolt/go/store/prolly/tree/proximity_map.go:147-260.

The min-max heap is a good implementation choice: insertion and worst-candidate
eviction stay bounded without fully sorting every expanded frontier.

### 1.7 Incremental mutation

Dolt queues edits in an ordered skip list, computes each edit's promotion
level, and chooses one of three strategies:

- Empty index: build from edits.
- An edit reaches the root level: rebuild the affected root/subtree.
- Representatives in the current node are unchanged: route edits by distance,
  recursively rewrite affected children, and reuse untouched child hashes.

Evidence:

- Strategy selection:
  dolt/go/store/prolly/proximity_mutable_map.go:40-104
- Edit routing and child-CID reuse:
  dolt/go/store/prolly/proximity_mutable_map.go:147-263
- Leaf merge:
  dolt/go/store/prolly/proximity_mutable_map.go:288-350
- Subtree rebuild:
  dolt/go/store/prolly/proximity_mutable_map.go:353-441

The important idea is to distinguish payload-only changes from representative
changes. Updating a leaf payload can preserve geometry. Inserting or deleting a
promoted representative may repartition a whole cluster and requires a wider
rebuild.

The mutation tests compare incremental results with clean rebuilds:

- inserts: dolt/go/store/prolly/proximity_map_test.go:537-601
- updates: dolt/go/store/prolly/proximity_map_test.go:617-710
- deletes: dolt/go/store/prolly/proximity_map_test.go:713-788

## 2. Expected shape and cost

These are model-based estimates, not claims measured in the current checkout.
They assume reasonably balanced spatial clusters.

Let:

- n = number of entries;
- d = vector dimensions;
- B = 2^log_chunk_size;
- H ≈ ceil(log_B n);
- w = search beam width;
- k = requested result count.

Expected promoted representatives at physical level l are n / B^l. Because
promoted representatives repeat downward, expected total physical entries are:

    n × (1 + 1/B + 1/B² + ...) = n × B/(B-1)

At B = 256 this is only about 1.004n. The final tree's representative
duplication is cheap.

Bulk path assignment scans candidate representatives at every depth. Under
balanced fanout, distance work is approximately:

    O(n × B × H × d)

Dolt's path-map keys also carry the complete representative path, so temporary
key bytes can approach O(nH), even though final physical entries remain O(n).

Approximate search distance work is approximately:

    O((root_fanout + w × B × H) × d)

and child-node reads are approximately O(wH), before cache and batch-read
effects.

Localized mutation ranges from one path plus a leaf rewrite to a complete
subtree rebuild. Worst case is O(n), including a new highest-level
representative, deletion of a high-level representative, or a highly skewed
cluster.

## 3. Dolt strengths worth borrowing

| Strength | Why it matters for Rust |
|---|---|
| Hash-derived promotion | History-independent shape is essential for stable roots and branch convergence. |
| Nearest-representative hierarchy | Converts vector search into a bounded tree traversal without a mutable graph. |
| Deterministic total-order tie breaking | Equal distances still produce reproducible roots and results. |
| Separate vector node type | Prevents ordered-map code from interpreting proximity nodes with the wrong routing semantics. |
| Shape metadata in content | Metric and promotion settings cannot silently drift without changing stored identity. |
| Child nodes addressed by content | Unchanged spatial clusters can be reused between versions and transferred independently. |
| Beam search with bounded heap | Gives predictable memory and candidate retention. |
| Incremental/full-rebuild equivalence tests | The clean rebuild is an oracle for complex copy-on-write edits. |
| Subtree counts | Cheap count/stat reporting and stronger node validation. |
| Multiple vector encodings behind conversion | Storage and distance computation are separated, even though current conversions can be expensive. |

## 4. Dolt sharp edges not to copy

### 4.1 Get is not an exact map lookup

At a leaf, Get returns the closest entry without comparing its key to the
query. Therefore a non-empty map can report a value for an absent key. Has is
implemented in terms of Get and inherits the problem.

Evidence:

- dolt/go/store/prolly/tree/proximity_map.go:95-145

An empty root also has no guarded candidate before the leaf callback. The Rust
implementation must test empty and absent lookups explicitly.

### 4.2 Duplicate vectors make greedy exact routing ambiguous

Dolt permits keys with equal vectors and different row identities, but distance
ignores the non-vector identity. Equal-distance representatives are selected by
iteration order. The non-lexicographic-key test at
dolt/go/store/prolly/proximity_map_test.go:791-822 explicitly skips the normal
validation because the helper assumes unique vectors.

Rust should keep exact identity in an ordered directory and use the proximity
tree only to produce candidate identities.

### 4.3 Search breadth is coupled to result count

GetClosest uses limit both as k and as beam width. A k=1 query becomes pure
greedy descent, which is the case most likely to need additional exploration.
Recall cannot be traded against work independently.

Rust should expose k and beam_width separately and require beam_width >= k.

### 4.4 Only L2-squared is truly persisted end to end

The serializer maps only DistanceL2Squared; other values become Null:

- dolt/go/store/prolly/message/vector_index.go:43-49

The loader ignores node metadata and reconstructs the map with hard-coded
L2-squared and the default chunk size:

- dolt/go/store/prolly/shim/shim.go:65-70 and 81-86

Rust must decode and validate shape metadata before search or mutation.

### 4.5 Temporary build trees pollute the production store

Dolt acknowledges that levelMap and pathMaps are written through the normal
NodeStore even though they are temporary:

- dolt/go/store/prolly/proximity_map.go:264-267

Rust should build the hierarchy in memory for the first version and batch-write
only final nodes. A future external-memory builder should use an explicit
scratch store with a cleanup lifecycle.

### 4.6 Repeated vector decoding is in the hot loop

Build and search repeatedly convert encoded tuple keys to float vectors. For
JSON-addressed vectors, conversion may traverse additional content-addressed
JSON state. The search code even notes that parent distances are recomputed:

- dolt/go/store/prolly/tree/proximity_map.go:235

Rust should store canonical float32 vector bytes directly in proximity nodes,
decode once per loaded node, and carry scored candidates across levels.

### 4.7 Mean node size is not a hard node-size bound

Promotion probability controls expected fanout, not maximum encoded bytes or
spatial skew. Dolt has a test with 4,000 entries in one logical node:

- dolt/go/store/prolly/proximity_map_test.go:404-428

Rust stores can have object-size limits. V1 should reject a final node above a
configured max_node_bytes with a clear error. Deterministic overflow pages can
be considered later.

### 4.8 Several generic map methods are meaningless or unfinished

Prefix and key-range methods panic:

- dolt/go/store/prolly/tree/proximity_map.go:52-73

The SQL layer rejects point/range access through a proximity index:

- dolt/go/libraries/doltcore/sqle/index/index_reader.go:389-443

Rust should expose a purpose-built proximity interface rather than make it
implement ordered range APIs.

### 4.9 Incremental support is not universal

Normal write paths have a proximity flusher, but merge/conflict-resolution
paths still rebuild vector indexes from scratch:

- dolt/go/libraries/doltcore/merge/merge_prolly_indexes.go:112-139

This is a useful warning: localized mutation is an optimization, not the
correctness model.

## 5. Rust implementation strengths and gaps

### Existing strengths

The Rust crate already has the foundation a direct proximity implementation
needs:

- Immutable Tree handles and SHA-256 CIDs.
- A generic Store with ordered batch reads and batch writes.
- Deterministic compact node bytes with prefix-compressed keys.
- Bounded node cache and observable I/O metrics.
- Parallel bulk building.
- Structural diff/merge for an authoritative ordered directory.
- Named roots, transactions, snapshot bundles, sync, and GC.
- Strong invariant, malformed-node, history-independence, and backend tests.
- A strict IndexedMap coordinator that treats derived indexes as rebuildable
  state tied to an authoritative source snapshot.

Key evidence:

- Node and current CRAB v1 format: src/prolly/node.rs:12-42 and 96-258
- Ordered Tree handle: src/prolly/tree.rs:8-26
- Store contract: src/prolly/store/mod.rs:93-259
- Manager cache and I/O: src/prolly/mod.rs:281-491 and 680-699
- Cached load and content-addressed save:
  src/prolly/mod.rs:3099-3339
- Maintainer invariants: docs/design.md:7-68

### Current gaps

- Node bytes have no node-kind discriminator beyond leaf/internal semantics.
- CRAB v1 requires internal keys to be ordered separators.
- Sync and snapshot reachability decode every object as ordered Node:
  src/prolly/sync.rs:311-352.
- RootManifest can describe only Tree plus ordered Config:
  src/prolly/manifest.rs:18-151.
- The current vector-sidecar recipe delegates ANN to another system:
  docs/cookbook.md:853-912.
- IndexedMap V1 explicitly excludes vector search:
  docs/superpowers/specs/2026-07-13-indexed-map-secondary-index-design.md:59-72.

These gaps argue for a distinct proximity wire object and handle, not a
reinterpretation of CRAB v1 Node.

## 6. Recommended Rust design

### 6.1 Compound immutable snapshot

Make exact records authoritative in an ordinary ordered Tree:

    directory key   = user identity bytes
    directory value = canonical proximity record envelope
                      { vector dimensions, vector bytes, user value bytes }

Build a separate derived proximity hierarchy whose leaves contain identity and
vector, but not a second copy of the user value.

A canonical descriptor object commits:

- ordered directory root and shape-affecting ordered Config;
- proximity root;
- entry count;
- ProximityConfig;
- proximity wire version.

The descriptor's CID is the ProximityTree identity. Equal records and config
must produce the same descriptor CID regardless of input order or mutation
history.

Benefits:

- get(key) is an exact ordered lookup.
- delete(key) can recover the old vector from the directory.
- duplicate vectors remain distinct by key.
- logical diff and merge operate on the directory.
- the derived ANN root can be rebuilt and verified.
- a single descriptor CID pins both exact and ANN state.

### 6.2 Public interface

Recommended V1 types:

    ProximityMap<S: Store + Clone>
    ProximityTree
    ProximityConfig
    ProximityRecord
    ProximityMutation
    DistanceMetric
    SearchOptions
    Neighbor
    SearchResult
    ProximitySearchStats

Recommended behavior:

- create returns an empty compound snapshot.
- build accepts unsorted ProximityRecord values, rejects duplicate keys, and
  emits a deterministic snapshot.
- get performs exact lookup through the directory.
- search returns at most k neighbors, sorted by (distance, key).
- rebuild_batch applies last-write-wins mutations to the directory and rebuilds
  the derived root. The name must make the O(n) behavior explicit.
- verify checks descriptor, node hashes, dimensions, levels, subtree counts,
  sorted node keys, and the nearest-parent invariant.

Do not expose range, prefix, ordinary ordered cursor, or generic merge methods
on the proximity hierarchy itself.

### 6.3 Shape configuration versus search configuration

ProximityConfig affects content and belongs in the descriptor:

- format_version;
- dimensions;
- DistanceMetric;
- log_chunk_size;
- level_hash_seed;
- max_node_bytes;
- canonical vector encoding version.

SearchOptions affects only runtime work and must not affect CIDs:

- k;
- beam_width;
- optional max_distance;
- optional node-read budget;
- optional distance-evaluation budget.

Validate:

- dimensions > 0;
- 1 <= log_chunk_size <= 16;
- max_node_bytes is large enough for one entry;
- beam_width >= k;
- all input and query values are finite;
- vector length exactly matches dimensions;
- cosine distance rejects zero-norm vectors unless a documented convention is
  chosen.

### 6.4 Canonical vector bytes

Use a language-portable V1 encoding:

- IEEE-754 float32;
- little-endian bytes;
- normalize negative zero to positive zero;
- reject NaN and positive/negative infinity;
- fixed dimension from ProximityConfig.

Use explicit vectors in the API. Do not persist a runtime callback or codec ID.
Application codecs should run before values cross the ProximityMap interface.

### 6.5 Deterministic promotion in Rust

Borrow the probability model, not Dolt's exact hash implementation:

    h = xxHash64(seed = level_hash_seed, bytes = user key)
    native_level = floor(leading_zeros_64(h) / log_chunk_size)

This reuses the crate's existing xxhash-rust dependency and is easier to specify
for future ports. Hash only the stable user key, not vector/value bytes, so a
value update does not randomly change promotion height. A changed vector still
requires geometric reassignment.

Document the exact seed, byte order, all-zero hash behavior, and level cap in
docs/wire-format.md and conformance fixtures before publishing.

### 6.6 Deterministic hierarchy build

Use in-memory indexed entries and parent links for V1:

1. Sort records by key and reject duplicates.
2. Canonicalize vectors and build the ordered directory with BatchBuilder.
3. Compute native levels in parallel; find max_level.
4. Process representatives top-down. Within each ancestor cluster, assign each
   lower-level representative to the candidate minimizing (distance, key).
5. Record parent/child adjacency without serializing complete path tuples.
6. Serialize nodes bottom-up, sorting each node by key.
7. Reject nodes exceeding max_node_bytes.
8. Batch-write final proximity nodes and descriptor only.

Use integer entry IDs and parent links rather than storing the full
representative path in every scratch key. This preserves Dolt's logical result
while avoiding persistent scratch nodes and O(nH) repeated path bytes.

### 6.7 Search

Use a bounded max-heap for each frontier:

1. Score root representatives.
2. Retain beam_width candidates ordered by (distance, key).
3. Batch-load distinct child CIDs for the retained candidates.
4. Carry the parent candidate's score into the repeated self representative;
   do not recompute it.
5. Score other child representatives and retain beam_width.
6. At leaves, keep the best k unique keys.
7. Batch-get exact directory records for the final keys.
8. Return results sorted by (distance, key) with search stats.

Special cases:

- k = 0 returns an empty result without store reads.
- Empty trees return an empty result.
- k > count returns at most count.
- beam_width < k is a validation error.
- Budgets return an explicit incomplete result or a structured error; choose one
  contract and test it.

Expose a brute-force exact search helper behind tests/benchmarks, not as the
default public path. Use it as the recall oracle.

### 6.8 Node and descriptor wire objects

Do not change CRAB v1 for the first slice. Add distinct magic values:

- PRXI version 1: proximity descriptor.
- PRXN version 1: proximity hierarchy node.
- PRVR version 1: directory record envelope.

PRXN contains:

- leaf flag;
- level;
- subtree count;
- entry count;
- sorted entries.

Each entry contains:

- prefix-compressed key;
- canonical fixed-width vector bytes;
- child CID for internal nodes;
- no user value for leaves.

PRXI contains:

- version and shape config;
- optional directory root CID;
- optional proximity root CID;
- directory ordered-config fields that affect content;
- total entry count.

Do not encode cache limits or default beam width because they do not affect
logical content.

### 6.9 Storage and cache reuse

The existing NodeCache is hard-coded to Arc<Node> at
src/prolly/mod.rs:281-360. Generalize it to a private generic
ContentCache<T>, or create a small generic cache module, then instantiate it for
ordered and proximity nodes. Keep separate cache instances so a workload can
budget them independently.

Reuse:

- Store::batch_get_ordered_unique for beam frontier hydration.
- Store::batch_put for final build writes.
- existing Prolly metrics naming and semantics.

Avoid a large refactor of Prolly<S> ownership in the first slice.
ProximityMap<S> may require S: Clone and construct an internal Prolly<S> over a
clone, matching the crate's existing Arc<S> adapter pattern.

## 7. Adoption sequence

### Phase 1 — Strong recommendation: deterministic static core

Implement:

- canonical types/config/errors;
- PRVR, PRXN, and PRXI formats;
- compound ordered directory plus derived proximity root;
- in-memory deterministic bulk build;
- exact get;
- approximate search with independent beam width;
- verification and brute-force recall oracle;
- conformance fixtures and benchmarks.

This is useful on its own for immutable RAG/index snapshots and establishes the
format before mutation complexity.

### Phase 2 — Strong recommendation: lifecycle integration

Add:

- correctness-first rebuild_batch;
- logical diff and merge through the directory followed by deterministic ANN
  rebuild;
- proximity snapshot export/import;
- generic content-reference walking so GC/sync can retain PRXI/PRXN graphs;
- a proximity-specific named-root manifest or a backward-compatible generic
  root-kind design;
- async read/search after the synchronous contract stabilizes.

### Phase 3 — Worth exploring: localized copy-on-write mutation

Port the idea, not the Go implementation:

- Partition edits by nearest representative.
- Reuse untouched child CIDs.
- Rewrite a leaf for payload-only edits.
- Rebuild the smallest cluster whose representative set changes.
- Fall back to whole-index rebuild when a new/deleted high-level representative
  invalidates broad geometry.

Every localized result must equal a clean rebuild's descriptor CID. If it does
not, the optimization is wrong.

### Phase 4 — Speculative: pruning and specialized vector storage

Only after recall/latency data:

- cached normalized vectors for cosine;
- quantized internal representatives while keeping exact leaf vectors;
- metric lower bounds for exact pruning;
- deterministic overflow pages for skewed cells;
- SIMD distance kernels;
- external-memory build with an explicit scratch-store lifecycle.

Quantization must not silently make roots platform-dependent. Any quantized
format needs canonical rounding fixtures across architectures.

## 8. Test and benchmark bar

### Determinism and invariants

- Input order does not change the descriptor CID.
- Clean rebuild and rebuild_batch produce identical descriptor CIDs.
- Same logical data in two stores produces identical bytes and CIDs.
- Every child representative satisfies the nearest-parent/uncle invariant.
- Node entries are key-sorted and subtree counts are exact.
- Equal distances use key bytes as the final tie breaker.

### Correctness edge cases

- Empty get/search.
- Absent exact key.
- Duplicate vectors with distinct keys.
- Duplicate input key rejection.
- k = 0, k > count, beam_width = k, beam_width < k.
- One-dimensional and high-dimensional vectors.
- wrong dimensions, NaN, infinity, negative zero.
- all points identical.
- zero-norm cosine vector.
- a final node exceeding max_node_bytes.
- malformed descriptor/node/record bytes.
- missing or corrupt child CID.

### Search quality

For deterministic random and clustered datasets, compare ANN to brute force:

- recall@1, recall@10, recall@100;
- distance evaluations;
- nodes read and bytes read;
- cache cold and warm latency;
- beam widths k, 2k, 4k, 8k;
- dimensions 8, 128, 768, and 1536;
- n = 1K, 10K, 100K where feasible.

Do not ship a default beam width based only on small correctness fixtures.

### Persistence and versioning

- Old ordered CRAB v1 fixtures remain byte-identical.
- PRXI/PRXN/PRVR V1 fixtures are checked in.
- Snapshot copy verifies CIDs.
- GC retains descriptor, directory, and proximity descendants.
- Logical diff ignores purely structural ANN changes.

## 9. Decisions and non-decisions

Recommended now:

- Direct in-crate implementation: yes.
- Literal Go port: no.
- Authoritative ordered directory: yes.
- Separate proximity wire type: yes.
- Bulk build before localized mutation: yes.
- Independent k and beam width: yes.
- Deterministic key tie breaker: yes.
- L2-squared V1: yes.
- Cosine in V1: only if normalization and zero-vector semantics are fixed in
  the wire spec before code.

Defer:

- HNSW-style mutable graph edges.
- product quantization.
- general query planning.
- automatic IndexedMap integration.
- exact metric-tree guarantees.
- cross-format compatibility with Dolt's IVFF bytes.

## 10. Provenance and licensing note

The Rust Cargo manifest declares MIT OR Apache-2.0, but the repository's
current LICENSE file contains only the MIT text. Dolt source files are
Apache-2.0. Before copying any implementation text, resolve the repository's
dual-license file layout and Apache NOTICE obligations.

The safer recommendation is:

- preserve this document's attribution to Dolt;
- implement from the algorithm and invariants described here;
- do not copy Go functions or comments line-for-line;
- use Rust's existing xxHash64 and storage conventions;
- add compatibility fixtures generated by the Rust implementation.

That produces an independently structured Rust implementation inspired by
Dolt's design rather than a source translation.
