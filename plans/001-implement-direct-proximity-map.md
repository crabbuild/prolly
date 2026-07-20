# Plan 001: Direct Deterministic Proximity Map

Status: Complete (2026-07-13)

Completion evidence: implemented on `codex/proximity-map-core` in the external
worktree `/Users/haipingfu/CrabDB-prolly-worktrees/proximity-map-core`. Final
verification passed `cargo fmt --all -- --check`, Clippy with warnings denied,
Rust 1.81 all-target checking, 438 library tests plus all integration tests, 97
doctests (2 ignored), and optimized benchmark compilation. The recorded smoke
benchmark and operational limitations are in `docs/proximity-map.md`.

Priority: P1

Effort: L

Risk: High

Category: Direction / new persistent data structure

Planned from Rust revision: fa7c219

Reference implementation studied: Dolt revision
6b2372c7d4ded1a54f55c6204304dbb72a33835c

Companion research:
[dolt-proximity-map-synthesis.md](dolt-proximity-map-synthesis.md)

## Objective

Add a Rust-native, content-addressed proximity map that combines:

1. an authoritative ordered directory for exact key semantics; and
2. a deterministic hierarchical proximity index for approximate nearest
   neighbor search.

The first release must support deterministic bulk construction, persistence,
load, exact get, ANN search, structural verification, conformance fixtures,
recall/storage benchmarks, and Dolt-style localized copy-on-write mutation.
Every localized mutation result must have the same canonical descriptor CID as
a clean rebuild of the same final logical records.

This plan is self-contained. The companion research explains the trade study,
but an implementer should not need it to determine the public interfaces,
storage model, algorithms, tests, or acceptance criteria below.

## Why this shape

Dolt's proximity map is a deterministic hierarchical Voronoi-style index, not
an ordered prolly tree with a distance comparator. A stable key hash promotes
some entries to higher levels. At each level, lower entries are assigned to
the nearest representative in their current ancestor cluster. Search follows
one or more nearest representatives downward.

The valuable properties are deterministic topology, immutable child links,
content reuse, and a small search frontier. Its map-shaped interface has three
important weaknesses that this design must not inherit:

- approximate routing cannot prove that an arbitrary key is absent;
- equal vectors do not uniquely identify a logical key; and
- result count is coupled to search beam width.

The Rust implementation therefore makes proximity data a derived index. The
descriptor commits both roots:

~~~text
ProximityTree descriptor CID (PRXI)
  ordered directory root (existing CRAB tree)
    user key -> PRVR canonical vector/value record
  proximity root (PRXN)
    representative key/vector -> child PRXN CID
~~~

Exact key operations use the ordered directory. ANN traversal uses the PRXN
hierarchy, then resolves winning keys through the directory. This preserves
the current crate's exact-map, diff, merge, and content-addressed strengths.

## Current codebase anchors

Read these files before implementation:

- src/prolly/node.rs:12-19 and 96-258 define the existing CRAB v1 node magic,
  version, and codec. The proximity implementation must not change those bytes.
- src/prolly/node.rs:21-42 defines the current ordered Node shape. Its keys are
  separator/data keys, not proximity representatives.
- src/prolly/tree.rs:8-26 defines the small root plus Config tree handle.
- src/prolly/mod.rs:281-491 contains NodeCache, currently specialized to
  Arc<Node>.
- src/prolly/mod.rs:680-699 defines Prolly and its store/cache ownership model.
- src/prolly/mod.rs:3099-3339 contains existing save/load paths and their
  validation conventions.
- src/prolly/store/mod.rs:93-259 exposes single and batched content-addressed
  get/put operations. The new builder and searcher should use batched methods.
- src/prolly/sync.rs:311-352 currently assumes every traversed object decodes
  as an ordered Node. Do not extend sync/GC in this first slice without a
  generic object-reference design.
- src/prolly/manifest.rs:18-151 commits only the existing ordered Tree/Config.
  Do not silently overload RootManifest with proximity state.
- docs/cookbook.md:853-912 currently describes vector search as an external
  sidecar. Update it to distinguish the new native immutable index from
  mutable or specialized external ANN services.
- docs/superpowers/specs/2026-03-11-secondary-index-design.md:59-72 excludes
  vector search from the strict ordered secondary-index design. Keep that
  boundary; do not force this work through IndexedMap.

Relevant Dolt behavioral references:

- dolt/go/store/prolly/tree/node_splitter.go:271-275 assigns deterministic
  promotion levels from leading hash zeros.
- dolt/go/store/prolly/proximity_map.go:232-267 documents the bulk-build
  overview.
- dolt/go/store/prolly/proximity_map.go:395-503 assigns representative paths.
- dolt/go/store/prolly/proximity_map.go:531-623 selects nearest candidates and
  serializes bottom-up.
- dolt/go/store/prolly/tree/proximity_map.go:196-260 implements level-wise beam
  search.
- dolt/go/store/prolly/proximity_mutable_map.go:147-441 shows localized
  mutation, untouched-child reuse, and clean-subtree rebuild behavior. These
  behaviors are required by this plan.
- dolt/go/serial/vectorindexnode.fbs:19-64 records Dolt's independent node
  format and metadata.

Do not copy these Go functions or their comments. Implement from the behavior,
invariants, and Rust interfaces described here.

## Compatibility and provenance constraints

- The adjacent plans/000-dolt-prolly-study-and-rust-design.md and its numbered
  ordered-engine plans are separate work. That study explicitly defers
  proximity maps and assumes a CRAB v1 hard cutover. This plan does not depend
  on those plans and preserves CRAB v1. If both tracks are scheduled, reconcile
  their compatibility decisions before implementation.
- Existing CRAB v1 node bytes and checked-in fixtures must remain byte-for-byte
  unchanged.
- The proximity format is Rust-native and is not wire-compatible with Dolt's
  IVFF FlatBuffers format.
- Use fresh magic values PRVR, PRXN, and PRXI, all at format version 1.
- Use the crate's existing xxHash64 implementation. Do not add a hash crate.
- Dolt is Apache-2.0 licensed. The Rust repository declares MIT OR Apache-2.0
  in Cargo.toml but currently has only an MIT LICENSE file. Avoid translated
  source text; resolve license-file/NOTICE policy separately before copying
  any implementation.
- No existing public type may change its serialized representation or semantic
  behavior as a side effect of this work.

## Scope

### In scope

- New proximity module under src/prolly/proximity/.
- Public immutable ProximityMap and ProximityTree APIs.
- Canonical f32 vector validation/encoding.
- L2-squared distance for format v1.
- Deterministic promotion and nearest-representative hierarchy construction.
- An authoritative existing ordered Tree containing proximity records.
- PRVR, PRXN, and PRXI strict codecs with checked-in fixtures.
- Independent result count and beam width.
- Search budgets, deterministic tie breaking, and observable search stats.
- Batched store reads/writes and a reusable typed content cache.
- Exact get and contains_key through the ordered directory.
- Full rebuild from sorted input as a correctness oracle.
- Dolt-style localized copy-on-write insert, update, delete, and mixed mutation
  batches with deterministic subtree rebuild and untouched-CID reuse.
- Structural verifier.
- Unit, property, integration, persistence, corruption, recall, and benchmark
  coverage.
- Design, wire-format, cookbook, and roadmap documentation.

### Out of scope

- Dolt IVFF byte compatibility.
- Ordered prefix or range scans over the proximity hierarchy.
- RootManifest, VersionedProlly, IndexedMap, proof, merge, sync, GC, named-root,
  or async integration.
- Product/scalar quantization, SIMD-specific kernels, GPU search, or external
  scratch storage.
- Metric plugins or user-defined distance functions.
- HNSW/graph edges.
- Automatic query planning.
- Cosine distance until normalization and zero-vector wire semantics are
  separately approved.

## Proposed public interface

Names may be adjusted to match local style, but semantic boundaries and the
separation between shape configuration and query options are load-bearing.

~~~rust
pub enum DistanceMetric {
    L2Squared,
}

pub struct ProximityConfig {
    pub dimensions: u32,
    pub metric: DistanceMetric,
    pub log_chunk_size: u8,
    pub level_hash_seed: u64,
    pub max_node_bytes: u32,
}

pub struct ProximityTree {
    pub directory: Tree,
    pub proximity_root: ContentId,
    pub descriptor: ContentId,
    pub count: u64,
    pub config: ProximityConfig,
}

pub struct ProximityRecord {
    pub key: Vec<u8>,
    pub vector: Vec<f32>,
    pub value: Vec<u8>,
}

pub struct ProximityMutation {
    pub key: Vec<u8>,
    pub value: Option<(Vec<f32>, Vec<u8>)>,
}

pub struct SearchOptions {
    pub k: usize,
    pub beam_width: usize,
    pub max_nodes: Option<usize>,
    pub max_distance_evaluations: Option<usize>,
}

pub struct Neighbor {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub distance: f32,
}

pub struct ProximitySearchStats {
    pub levels_visited: usize,
    pub nodes_read: usize,
    pub bytes_read: usize,
    pub distance_evaluations: usize,
    pub budget_exhausted: bool,
}

pub struct SearchResult {
    pub neighbors: Vec<Neighbor>,
    pub stats: ProximitySearchStats,
}

pub struct ProximityMutationStats {
    pub nodes_read: usize,
    pub nodes_written: usize,
    pub nodes_reused: usize,
    pub records_rebuilt: usize,
    pub distance_evaluations: usize,
    pub full_proximity_rebuild: bool,
}

pub struct ProximityMap<S: Store + Clone> { /* private */ }

impl<S: Store + Clone> ProximityMap<S> {
    pub fn build(
        store: S,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
    ) -> Result<Self>;

    pub fn load(store: S, descriptor: ContentId) -> Result<Self>;
    pub fn tree(&self) -> &ProximityTree;
    pub fn get(&self, key: &[u8]) -> Result<Option<(Vec<f32>, Vec<u8>)>>;
    pub fn contains_key(&self, key: &[u8]) -> Result<bool>;
    pub fn search(&self, query: &[f32], options: SearchOptions)
        -> Result<SearchResult>;
    pub fn rebuild_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<Self>;
    pub fn mutate_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<(Self, ProximityMutationStats)>;
    pub fn verify(&self) -> Result<ProximityVerification>;
}
~~~

Required validation:

- dimensions must be greater than zero;
- vector length must equal dimensions;
- all components must be finite;
- negative zero is canonicalized to positive zero;
- log_chunk_size must be in 1..=63;
- max_node_bytes must accommodate an empty node and one entry;
- k must be greater than zero;
- beam_width must be at least k;
- budgets, when present, must be greater than zero;
- duplicate user keys in a build are an error, not last-write-wins;
- count conversions and byte-size arithmetic must be checked.

Add specific error variants to src/prolly/error.rs for invalid dimensions,
non-finite vectors, duplicate keys, invalid search options, incompatible
descriptor versions, corrupt proximity records/nodes/descriptors, node size
overflow, and search budget exhaustion if the API chooses a hard-budget mode.

## Persistent model

### PRVR: directory record

The existing ordered directory stores user key -> PRVR bytes. PRVR version 1
contains:

1. magic PRVR;
2. format version u8 = 1;
3. dimensions as unsigned varint;
4. exactly dimensions canonical little-endian IEEE-754 f32 words;
5. value length as unsigned varint;
6. opaque value bytes;
7. trailing checksum only if existing crate conventions already provide one at
   this layer; do not invent a second content hash.

Strict decoding rejects bad magic/version, non-canonical varints, length
overflow, dimension mismatch, non-finite components, and trailing bytes.

### PRXN: proximity node

PRXN version 1 contains:

1. magic PRXN;
2. format version u8 = 1;
3. flags with a leaf bit and all unused bits required to be zero;
4. logical level as u8;
5. subtree_count as unsigned varint;
6. entry_count as unsigned varint;
7. entries in strict ascending user-key byte order.

Each entry contains:

1. key length and key bytes;
2. exactly descriptor.dimensions canonical f32 words;
3. for an internal node, one child ContentId;
4. for a leaf node, no child and no user value.

The hierarchy never stores user values. A leaf search returns candidate keys,
which are resolved through PRVR directory records. This avoids two sources of
truth and makes duplicate vectors safe.

The decoder must reject duplicate/out-of-order keys, an internal entry without
a child, a leaf entry with child bytes, non-finite vectors, inconsistent
subtree counts discovered by verify, and encoded bytes above max_node_bytes.

### PRXI: descriptor

PRXI version 1 contains:

1. magic PRXI;
2. format version u8 = 1;
3. vector encoding ID = canonical-f32-le-v1;
4. dimensions;
5. distance metric ID;
6. log_chunk_size;
7. level_hash_seed;
8. max_node_bytes;
9. logical record count;
10. existing ordered directory Tree root and all Config fields required to
    reconstruct it exactly;
11. proximity root ContentId, or a canonical empty-root ContentId;
12. reserved-field count = zero for version 1.

All shape-affecting values live in PRXI so the same descriptor always routes
and rebuilds identically. Search options do not live in PRXI because they
affect latency/recall, not content identity.

## Determinism contract

### Input order

Build must sort records by user-key bytes before any structural decision.
Insertion order, iterator implementation, hash map iteration order, host
endianness, CPU feature set, and thread scheduling must not affect any CID.

### Vector bytes

- Encode f32 with to_bits and little-endian bytes.
- Convert 0x80000000 to 0x00000000.
- Reject every NaN payload and positive/negative infinity.
- Accumulate L2-squared distance in f64 in a fixed component order, then return
  f32 only at the public boundary if required by the chosen API.
- Compare candidates by total ordering of (distance, key bytes). Do not rely on
  unstable sort or source iteration order for ties.

### Promotion level

For a non-empty user key byte string k:

~~~text
h = xxhash64_with_seed(level_hash_seed, k)
zeros = leading_zero_count(h)       // 0 through 64
level = min(255, zeros / log_chunk_size)
~~~

Hash the key only. Updating a vector or value for an existing key must not
change its promotion level. Define h = 0 as 64 leading zeros. An empty key is
valid unless the ordered map already rejects it; it hashes as an empty byte
sequence.

Let B = 2^log_chunk_size. The expected fraction at or above level l is B^-l,
and expected height is approximately ceil(log_B n). This is a statistical
shape target, not a correctness assumption.

## Canonical bulk-build algorithm

The builder must not persist temporary path maps. Dolt uses persistent scratch
structures and acknowledges that their nodes become garbage. The Rust builder
should keep topology in memory for the first release and batch-write only final
PRVR, PRXN, and PRXI objects.

Represent every sorted input record by a stable integer entry ID. Keep key and
canonical vector once per entry. Keep level assignments and parent/children as
integer IDs or compact arrays; do not clone full path byte strings per level.

### Step A: validate and build the ordered directory

1. Canonicalize and validate every record.
2. Sort by key and reject duplicates.
3. Encode PRVR for each record.
4. Build the existing ordered Tree from key -> PRVR bytes using its canonical
   bulk path.
5. Retain entry ID -> key/vector metadata for proximity construction.

If the directory builder would write before all validation is complete, stage
encoded values in memory first so an invalid vector does not leave a partially
useful descriptor. Unreachable content blobs after a failed store write are
acceptable under the store's existing failure model; never publish PRXI until
all children exist.

### Step B: assign native promotion levels

1. Compute each record's promotion level from the key.
2. Group IDs by native level.
3. Let root_level be the maximum native level, with zero for an empty map.
4. If several entries share root_level, all are representatives in the root
   logical node.

### Step C: assign nearest-representative parents

Process from root_level downward. A record native at level L appears as a
representative at level L and participates as the same logical identity in all
lower levels needed to connect descendants.

Within each ancestor cluster at level L:

1. collect representatives whose native level is at least L - 1;
2. for every child representative/entry that needs placement below the current
   cluster, compute distance to each eligible representative;
3. choose the minimum (distance, representative key);
4. record a parent link;
5. assert that every child has exactly one parent except root representatives.

The load-bearing invariant is:

~~~text
For every internal child representative c assigned to parent p,
distance(c, p) <= distance(c, u) for every sibling parent candidate u,
with equal distances resolved by lexicographically smaller key.
~~~

A straightforward O(n * B * H * d) implementation is acceptable for the first
release under balanced clusters, where H is height, B is mean fanout, and d is
dimensions. Instrument distance evaluations so optimization is evidence-led.

### Step D: serialize bottom-up

1. Begin at leaf logical clusters.
2. Sort each node's entries by key.
3. Compute exact encoded size before writing.
4. If a logical node exceeds max_node_bytes, return NodeTooLarge with level,
   entry count, and encoded size. Do not split arbitrarily: splitting a
   representative set without a specified routing invariant changes search
   semantics.
5. Encode and batch-put nodes at the same level.
6. Attach returned child CIDs to their parents.
7. Continue until the root CID exists.
8. Encode and put PRXI last.

ContentId must be computed and checked through existing Store behavior, not a
new hashing path.

### Empty map

Define canonical empty bytes for a level-zero empty leaf PRXN. Every empty map
with the same shape config must reuse that root CID. Exact get returns None;
search returns an empty result with zero distance evaluations; verify accepts
count zero and the canonical empty leaf only.

## Search algorithm

Search must be level-synchronous beam search, not independent greedy walks.

1. Validate and canonicalize the query without modifying caller memory.
2. Load the root PRXN through the typed cache.
3. Score every root entry and retain the nearest beam_width candidates in a
   bounded max-heap ordered by worst (distance, key).
4. For each lower level, batch-load the distinct child CIDs referenced by the
   current beam.
5. Score child entries. Carry a representative's already-known score when the
   same key/vector descends unchanged; do not recompute it.
6. Retain the nearest beam_width candidates for the next level.
7. At leaf level, retain the nearest k distinct user keys.
8. Batch-get their PRVR records from the ordered directory and validate that
   stored vectors match the leaf candidate bytes.
9. Return neighbors sorted ascending by (distance, key).

SearchOptions.k and SearchOptions.beam_width are independent. The default beam
must be documented and benchmarked; a conservative initial default is
max(k, 32), but it must not be frozen until recall measurements exist.

Budget behavior must be deterministic. Check max_nodes before scheduling a
new batch and max_distance_evaluations before scoring the next candidate in
key order. The initial API should return partial sorted results with
budget_exhausted = true, unless existing crate error conventions strongly
favor a hard error. Whichever contract is chosen must be tested and documented.

## Exact operations and mutation

get and contains_key consult only the ordered directory. They must never route
through PRXN. This guarantees correct absence checks and gives distinct keys
with identical vectors distinct identities.

rebuild_batch is the correctness oracle:

1. sort mutations by key;
2. reject duplicate mutation keys;
3. merge them with the ordered directory's sorted iterator;
4. delete on None and insert/update on Some(vector, value);
5. invoke the canonical bulk builder on the resulting logical records;
6. return the newly loaded ProximityMap.

Its result descriptor CID must equal a clean build of the same final logical
records.

mutate_batch is the production mutation path. It must produce the exact same
directory root, proximity root, and descriptor CID as rebuild_batch while
reading and rewriting only affected proximity clusters whenever the canonical
root representative set is unchanged.

### Localized copy-on-write mutation algorithm

Use the immutable ordered directory as the source of truth and the existing
proximity hierarchy as the partition map. Never mutate a stored node in place.

1. Sort mutations by key and reject duplicate mutation keys before performing
   store writes.
2. Validate/canonicalize every inserted or updated vector and value.
3. Resolve old records for updated/deleted keys through the exact directory.
4. Build the new canonical ordered directory from the final sorted records.
   Characterization during implementation showed that the current ordered
   batch writer can retain a different valid physical shape after long edit
   histories, so its result is not always equal to a clean bulk root. Until the
   canonical resynchronizing writer lands, proximity mutation must bulk-rebuild
   the exact directory while keeping PRXN mutation localized. Preserve the old
   directory and descriptor if any operation fails.
5. Compute each edit's deterministic native promotion level from its key. The
   level is identical for an update because vectors and values are not hashed.
6. Compare the old and new root-level representative sets. If the set changes,
   rebuild the complete proximity hierarchy from the new directory and set
   full_proximity_rebuild = true. Root changes include insertion or deletion of
   a key above the old root level, deletion of the last root representative,
   and an empty/non-empty transition.
7. Otherwise descend the old hierarchy. At each internal node, deterministically
   route non-representative edits to the nearest existing child representative,
   grouping by child CID and processing groups in representative-key order.
8. If an edit adds, removes, or changes the vector of a representative at the
   current logical level, rebuild that entire parent cluster from its new
   logical record set. A representative change can steal or release records
   from sibling children, so rebuilding only the edited child is incorrect.
9. If the representative set is unchanged, recurse only into child groups that
   contain edits. Reuse every untouched child CID byte-for-byte.
10. At a leaf, merge sorted mutations with the existing sorted entries. Encode
    the canonical replacement leaf or remove an empty child as required.
11. Propagate replacement child CIDs upward. If a rebuilt child changes the
    representative set observed by its parent, widen the rebuild to that parent
    cluster. Coalesce edits that meet at a common ancestor so each affected
    cluster is rebuilt once.
12. Encode changed nodes bottom-up with the same builder/codec used by clean
    build. Enforce max_node_bytes; do not use a mutation-only split rule.
13. Write the new PRXI descriptor last, after the new directory and all changed
    PRXN nodes are durable.

The implementation may collect the records of an affected cluster by traversing
that cluster's leaf descendants and applying the relevant mutation subset. It
must not scan unrelated clusters merely to simplify routing. A root-level
representative-set change is the deliberate worst-case full rebuild.

Representative vector updates require special care even though their promotion
level is stable: the changed distances can reassign sibling descendants. Treat
them as representative-set changes for routing purposes and rebuild their
parent cluster. Ordinary value-only updates change PRVR and the directory root
but must reuse the entire PRXN hierarchy and report zero proximity nodes
written.

Clean-rebuild equality is a test oracle, not a runtime double-build. Property
tests must compare mutate_batch with rebuild_batch after every operation in
long deterministic randomized sequences. Production mutate_batch performs only
the localized algorithm plus the documented full-rebuild fallbacks.

Mutation statistics are part of the industrial-quality contract. They make
write amplification and fallback behavior observable and testable. Counts must
be deterministic for a cold cache; warm-cache byte reads may differ but logical
nodes read/written/reused must not.

## Cache extraction

The current NodeCache is specialized to Arc<Node>. Extract the eviction,
capacity, hit/miss, and metrics machinery into a crate-private generic cache,
for example ContentCache<T>, without changing public behavior.

- Existing ordered Prolly continues to use ContentCache<Node> through either a
  type alias or a thin NodeCache wrapper.
- ProximityMap uses a distinct ContentCache<ProximityNode> so node types cannot
  collide under the same ContentId key.
- Descriptor and PRVR caching are optional for the first slice; measure before
  adding them.
- Preserve all current NodeCache tests first, then add type-isolation and PRXN
  cache tests.

Do this as a behavior-preserving preparatory commit. If generic extraction
requires public API breakage, stop and keep a private dedicated PRXN cache
instead.

## Structural verifier

verify performs a full traversal and reports at least record_count,
proximity_node_count, maximum_level, maximum_node_bytes, and distance checks.
It must validate:

- descriptor and node codecs;
- no cycles or repeated child ownership within one hierarchy;
- node level decreases by exactly one along each child edge;
- keys strictly ascend inside every node;
- child subtree_count values sum to the parent subtree_count;
- descriptor count equals directory logical count and root subtree_count;
- every proximity key exists in the directory;
- every stored proximity vector equals its PRVR vector bytes;
- every directory key appears exactly once as a leaf identity;
- promotion levels match key hashes and shape config;
- nearest-representative parent invariant with deterministic tie breaking;
- empty-map canonical representation;
- encoded node sizes do not exceed max_node_bytes.

Verification may be expensive and is not part of load. Load performs only
local descriptor validation and lazy node validation.

## Implementation sequence

Use branch codex/proximity-map-core. Keep commits reviewable and conventional,
matching repository history. Each step must leave existing tests green unless
the step explicitly introduces a compile-fail/red test immediately followed by
its implementation commit.

### 1. Freeze contracts in docs and tests

Files:

- docs/design.md
- docs/design-spec.md
- docs/wire-format.md
- tests/proximity_map.rs (new)
- conformance/proximity-fixtures.v1.json or the repository's established
  fixture location

Actions:

- Add the compound descriptor/directory/index model.
- Specify PRVR/PRXN/PRXI byte layout field-by-field.
- Record canonical f32, promotion, ordering, empty-root, and budget rules.
- Add test helpers that express expected validation failures and deterministic
  root equality. They may be ignored or scoped behind the new module until the
  API compiles; do not commit a permanently red default test suite.

Verification:

~~~sh
cargo test --test proximity_map
~~~

### 2. Generalize the content cache without changing behavior

Files:

- src/prolly/cache.rs (new, if extraction improves clarity)
- src/prolly/mod.rs
- src/prolly/node.rs only if imports move; no codec edits

Actions:

- Move cache mechanics to ContentCache<T>.
- Preserve NodeCache name/type if it is public or referenced by tests.
- Run all existing cache, metrics, and concurrency tests.
- Add a test proving two typed caches can hold objects with the same CID key
  without type confusion.

Verification:

~~~sh
cargo test prolly::tests::cache
cargo test
~~~

### 3. Add value types, validation, and distance primitives

Files:

- src/prolly/proximity/mod.rs
- src/prolly/proximity/vector.rs
- src/prolly/proximity/distance.rs
- src/prolly/error.rs
- src/prolly/mod.rs
- src/lib.rs

Actions:

- Implement config and query validation.
- Implement canonical f32 bytes and strict decoding.
- Implement deterministic f64 L2-squared accumulation.
- Implement total candidate ordering by distance then key.
- Implement promotion level with existing seeded xxHash64.
- Property-test negative zero normalization, rejection of every non-finite
  class, distance symmetry/non-negativity, promotion determinism, and tie
  ordering.

Verification:

~~~sh
cargo test proximity::vector
cargo test proximity::distance
~~~

### 4. Implement strict codecs and golden fixtures

Files:

- src/prolly/proximity/record.rs
- src/prolly/proximity/node.rs
- src/prolly/proximity/descriptor.rs
- tests/proximity_wire.rs
- conformance/proximity-fixtures.v1.json
- docs/wire-format.md

Actions:

- Implement encode, encoded_len, and strict decode for all three objects.
- Make every allocation and length conversion checked.
- Add hand-auditable empty, one-entry, tied-vector, and internal-node fixtures.
- Add one-bit truncation/corruption/trailing-byte tests for each field class.
- Round-trip random valid objects and require encode(decode(bytes)) == bytes.
- Confirm ordered CRAB v1 fixture bytes remain unchanged.

Verification:

~~~sh
cargo test --test proximity_wire
cargo test wire
~~~

### 5. Build the ordered directory and deterministic hierarchy

Files:

- src/prolly/proximity/builder.rs
- src/prolly/proximity/mod.rs
- tests/proximity_build.rs

Actions:

- Stage validated sorted records and reject duplicate keys.
- Use the existing ordered bulk builder for PRVR directory values.
- Implement stable entry IDs, promotion grouping, parent assignment, and
  bottom-up PRXN serialization.
- Batch-put final nodes by level and PRXI last.
- Add instrumentation for distance evaluations and bytes/nodes written.
- Return NodeTooLarge rather than inventing an unsound split.

Tests:

- empty, one record, all same vector, non-lexicographic vector/key order;
- same records across forward, reverse, shuffled, and chunked iterators produce
  identical directory root, proximity root, and descriptor CID;
- update value only keeps promotion levels stable;
- changed vector changes only derived assignments, not key identity;
- nearest-parent invariant for every edge;
- mean level population follows a wide statistical tolerance for seeded large
  data without making distribution a flaky correctness gate;
- injected store failure never yields a published descriptor;
- hard max_node_bytes failure includes diagnostic context.

Verification:

~~~sh
cargo test --test proximity_build
~~~

### 6. Implement load, exact operations, search, and verification

Files:

- src/prolly/proximity/map.rs
- src/prolly/proximity/search.rs
- src/prolly/proximity/verify.rs
- tests/proximity_map.rs
- tests/proximity_search.rs

Actions:

- Load and validate PRXI by CID.
- Reconstruct the ordered directory Tree from committed config.
- Add typed PRXN caching and batched child loads.
- Implement exact get/contains_key through the directory.
- Implement level-synchronous bounded beam search with score reuse.
- Batch-resolve final records and return deterministic results plus stats.
- Enforce budgets in deterministic processing order.
- Implement the full structural verifier.

Tests:

- exact get returns None for absent keys on empty and non-empty maps;
- equal vectors with different keys remain separate and deterministically
  ordered;
- k = 1 with beam 1 matches documented greedy behavior;
- increasing beam never changes result ordering nondeterministically;
- beam < k is rejected;
- cold/warm cache metrics and batched read counts match expectations;
- parent scores are not re-evaluated;
- budgets stop at exact reproducible counters;
- corrupt child CID, wrong level, wrong vector, wrong subtree count, duplicate
  leaf identity, and orphan directory key fail verification;
- brute-force search is exact whenever beam covers all candidates;
- ANN output is a subset of logical records and sorted by distance/key.

Verification:

~~~sh
cargo test --test proximity_map
cargo test --test proximity_search
~~~

### 7. Add canonical rebuild and localized copy-on-write mutation

Files:

- src/prolly/proximity/map.rs
- src/prolly/proximity/mutation.rs
- tests/proximity_mutation.rs

Actions:

- Implement rebuild_batch by merging a sorted unique mutation batch with the
  ordered directory iterator and invoking the canonical builder.
- Implement mutate_batch using the localized algorithm in this plan.
- Reuse untouched child CIDs and rebuild the smallest ancestor cluster whose
  representative routing may change.
- Fall back to a full proximity rebuild only for documented root-level shape
  changes or when correctness requires widening to the root.
- Write the descriptor last and preserve the old map under injected failures.
- Return deterministic ProximityMutationStats.

Tests:

- insert, update vector, update value, delete, delete absent, and mixed batches
  for both representative and non-representative keys;
- mutation input order independence;
- duplicate mutation rejection;
- every localized result CID equals rebuild_batch and a clean build of the final
  logical records;
- value-only updates reuse the complete proximity root;
- ordinary non-representative edits reuse unaffected sibling CIDs;
- representative insertion/deletion/vector update rebuilds its parent cluster
  and reuses nodes outside that cluster;
- root representative changes take the documented full-rebuild path;
- multiple edits sharing an ancestor rebuild that cluster only once;
- long fixed-seed randomized operation sequences compare localized and clean
  roots after every batch;
- mutation stats match actual counting-store reads/writes and reused CIDs;
- old map remains readable after the new map is built;
- injected failure at every write boundary never publishes a descriptor with
  missing children and never alters or invalidates the old descriptor.

Verification:

~~~sh
cargo test --test proximity_mutation
~~~

### 8. Benchmark recall and storage behavior

Files:

- benches/proximity_bench.rs
- Cargo.toml only for the bench target, not new dependencies
- docs/design.md or docs/roadmap.md for recorded results

Benchmark deterministic seeded datasets at dimensions 8, 128, 768, and 1536
and sizes 1K, 10K, and 100K where practical. Compare against an in-process
brute-force scan using the same distance primitive.

Record:

- build wall time and distance evaluations;
- localized mutation latency, distance evaluations, records rebuilt, nodes
  written, and nodes reused for representative/non-representative/value-only
  edits;
- full-rebuild mutation latency and write amplification as the comparison
  baseline;
- encoded bytes per record and node-size distribution;
- tree height and level populations;
- recall@1, recall@10, and recall@100;
- p50/p95 search latency;
- nodes, bytes, and distance evaluations per query;
- beam widths k, 2k, 4k, 8k, 32, and 64 where distinct;
- cold and warm cache behavior.

Do not claim a production default beam until these results exist. If max node
size failures occur on representative workloads, stop and design a documented
overflow/fanout mechanism rather than raising the limit without analysis.

Verification:

~~~sh
cargo bench --bench proximity_bench --no-run
cargo bench --bench proximity_bench
~~~

### 9. Complete public documentation and release checks

Files:

- README.md
- docs/design.md
- docs/design-spec.md
- docs/wire-format.md
- docs/cookbook.md
- docs/roadmap.md
- src/lib.rs and public API rustdoc

Document:

- approximate versus exact semantics;
- why exact get uses an ordered directory;
- deterministic shape/config contract;
- memory and build complexity;
- recall/latency tuning through beam_width;
- budgets and returned stats;
- localized mutation behavior, fallback conditions, statistics, and worst-case
  rebuild cost;
- persistence and format-version stability;
- limitations and deferred integrations;
- provenance: behavior inspired by Dolt's Apache-2.0 proximity map, implemented
  independently in Rust.

Verification:

~~~sh
cargo fmt --all -- --check
cargo check --all-targets
cargo test
cargo test --doc
cargo bench --bench proximity_bench --no-run
~~~

## Required test matrix

### Codec and corruption

- golden byte fixtures for empty/leaf/internal/descriptor/record;
- strict round trip and canonical re-encoding;
- bad magic/version/flags/metric/encoding ID;
- truncated varints, overflow, impossible lengths, trailing bytes;
- NaN, infinities, negative zero input canonicalization;
- out-of-order or duplicate node keys;
- bad ContentId length and inconsistent leaf/internal fields.

### Determinism

- randomized input permutations produce identical three roots;
- repeated builds in fresh stores produce identical CIDs and bytes;
- single-thread and parallel caller contexts produce identical results even if
  the first implementation itself is single-threaded;
- tied distances are key-ordered;
- rebuild_batch equals clean build exactly.
- mutate_batch equals rebuild_batch after every deterministic randomized batch.
- value-only and localized structural edits demonstrate expected PRXN CID reuse.

### Map semantics

- empty/single/many exact get and contains_key;
- absent key in a non-empty map is None;
- duplicate build keys rejected;
- duplicate vectors accepted as distinct keys;
- opaque values including empty and large values;
- original map remains immutable across rebuilds.

### Hierarchy correctness

- promotion level matches reference examples;
- every non-root representative has one parent;
- every directory key appears once at leaf identity;
- nearest-parent invariant including ties;
- level monotonicity and subtree counts;
- max size enforced;
- verify catches every deliberately corrupted invariant.

### Search

- empty result behavior;
- exact brute-force equivalence with exhaustive beam;
- sorted unique results;
- independent k and beam;
- deterministic node/distance budgets;
- correct stats and cache counters;
- score reuse;
- recall tests use fixed seeds and report, rather than hide, weak regions.

### Regression

- all existing 421 library tests and integration tests remain green;
- all existing doctests remain green;
- existing CRAB v1 conformance bytes remain unchanged;
- no behavior change in NodeCache/Prolly load/save paths.

## Complexity and capacity expectations

Let n be records, d dimensions, b = log_chunk_size, B = 2^b, H approximately
ceil(log_B n), and w = beam_width.

- Expected number of representatives across all levels is approximately
  n * B / (B - 1); for b = 8 this is about 1.004n.
- Balanced bulk assignment is approximately O(n * B * H * d) distance work.
- In-memory construction is O(n * d + n * H) metadata, with vectors stored
  once. Avoid O(n * H * d) vector duplication.
- Search is approximately O((root_fanout + w * B * H) * d) distance work and
  O(w * H) node reads before cache/batching effects.
- rebuild_batch is O(n) directory iteration plus a full bulk build.
- mutate_batch currently performs O(n) canonical exact-directory work plus
  affected PRXN cluster traversal/rebuild in the common case. PRXN work becomes
  O(n) when a root representative change or propagated routing change reaches
  the root. A future canonical resynchronizing ordered writer can remove the
  directory-side O(n) fallback without changing proximity bytes or APIs.

These are planning estimates, not acceptance thresholds. Capture observed
values in benchmark output and revisit the design if actual root fanout,
height, or bytes grow pathologically.

## Done criteria

Plan 001 is complete only when all of the following are true:

- ProximityMap builds, loads, gets, searches, verifies, clean-rebuilds, and
  performs localized copy-on-write mutation from the documented public API.
- PRVR/PRXN/PRXI v1 are fully documented and have checked-in golden fixtures.
- Identical logical records plus shape config produce identical descriptor CIDs
  across input permutations and fresh stores.
- Exact operations never depend on approximate routing.
- Duplicate vectors and absent keys have correct tested semantics.
- k and beam_width are independent and budget behavior is deterministic.
- Every hierarchy invariant is checked by verify and exercised by corruption
  tests.
- Existing CRAB v1 fixtures and APIs are unchanged.
- Every mutate_batch result matches rebuild_batch's canonical descriptor CID;
  value-only mutations reuse the entire PRXN root and localized structural
  mutations reuse untouched child CIDs.
- The complete formatting, check, unit, integration, doctest, and bench-build
  commands pass.
- Recall/storage benchmarks and limitations are recorded in the repository.
- No copied Dolt source text or unreviewed new dependency is introduced.

## Stop conditions

Stop implementation and request design review if any of these occurs:

- an existing CRAB v1 fixture changes;
- the implementation requires interpreting PRXN as the existing ordered Node;
- exact get or mutation would have to route through ANN search;
- deterministic roots differ by input order, platform, build profile, or cache
  state;
- a useful dataset exceeds max_node_bytes and cannot be represented without a
  new routing/splitting invariant;
- generic cache extraction requires a public breaking change;
- a new hashing, ANN, serialization, or floating-point dependency appears
  necessary;
- cosine distance is requested without a canonical normalization and zero
  vector specification;
- sync/GC/manifest/proof support becomes a release requirement before a generic
  object-reference traversal design exists;
- baseline tests fail before proximity changes;
- implementation would copy or mechanically translate Dolt source.

## Drift check before implementation

The plan was written against Rust revision fa7c219. Before starting, run:

~~~sh
git diff --stat fa7c219..HEAD -- \
  Cargo.toml \
  src/lib.rs \
  src/prolly/mod.rs \
  src/prolly/node.rs \
  src/prolly/tree.rs \
  src/prolly/error.rs \
  src/prolly/store/mod.rs \
  src/prolly/sync.rs \
  src/prolly/manifest.rs \
  docs/design.md \
  docs/design-spec.md \
  docs/wire-format.md \
  docs/cookbook.md \
  docs/roadmap.md
~~~

If output is non-empty, re-read the changed anchors and update this plan before
coding. Also record the current Dolt SHA if its code is consulted again; this
plan's behavioral analysis is pinned to
6b2372c7d4ded1a54f55c6204304dbb72a33835c.

## Follow-on plans after this slice

Do not silently absorb these into Plan 001:

1. generic content-object reference traversal for proximity-aware sync, GC,
   snapshots, named roots, and manifests;
2. logical diff/merge semantics driven by the authoritative directory;
3. asynchronous store and search APIs;
4. measured overflow-node or deterministic secondary partition design if hard
   node limits require it;
5. quantized/SIMD distance storage and kernels with explicit recall and wire
   versioning analysis.
