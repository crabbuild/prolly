# Versioned DynamoDB Client Release Status

Audit date: 2026-08-09. This is an implementation evidence record, not a
production release approval.

## Product boundary

Plan 019 is the sole active DynamoDB product design: an in-process Rust client
over the Rust DynamoDB store API. No DynamoDB-compatible service or proxy is
required. Plan 018 is archived research only. Rust crates use Cargo naming; any
future npm binding is reserved to the `@crabbuild` scope.

The public version extensions use concise names: `at`, `versions`, `diff`,
`restore`, and `indexes`. There is no `at_version`/`at_versions` surface.

## Proven locally

- Core: 31 unit, 5 frozen-conformance, 48 contract, and 6 property tests pass.
- Client: 21 unit tests, fluent compile fixture, DynamoDB Local differential
  corpus, same-namespace and independently initialized canonical
  fluent/input/core parity, multi-process conflict, stream restart, lease
  fencing, TTL, retention, and maintenance/GC tests pass. Complete client
  lifecycles also pass on caller-owned Tokio current-thread and two-worker
  multi-thread runtimes against DynamoDB Local.
- The native DynamoDB Local differential corpus now exercises every advertised
  condition-function family and boolean/range form with explicit nonempty
  expected key sets: nested paths, `BETWEEN`, `IN`, `NOT`/`AND`/`OR`,
  `attribute_exists`, `attribute_not_exists`, `attribute_type`, `contains`,
  `begins_with`, and `size`. It also compares `SET`, nested
  `if_not_exists`/`list_append`, arithmetic, `REMOVE`, numeric/set `ADD`, set
  `DELETE`, `ALL_NEW`, `UPDATED_NEW`, complete final items, and `ALL_OLD`
  conditional-failure images against the native service behavior.
- Exact-decimal differential cases cover 38-digit values, the documented
  `1e-130` lower bound, signed zero, equivalent trailing-zero spellings,
  boundary arithmetic, and negative/fractional numeric sort order without
  binary floating point. DynamoDB Local preserves result scale in some
  immediate arithmetic return images despite the documented trimmed-zero
  number model; the suite compares those by canonical exact value and requires
  the client to return its documented canonical spelling. Financially
  meaningful display scale must be stored explicitly rather than inferred from
  the number string.
- Logical conflict retries are now an explicit bounded client control: seven
  retries after the first attempt by default, zero through 63 accepted, and
  larger values rejected before provider access. The decoded-node cache keeps
  its 64-MiB default retained serialized-weight ceiling and supports simultaneous node-count and byte
  ceilings, with zero disabling caching. Core tests prove this tuning does not
  change the durable format record; an injected-conflict contract proves zero
  retries publishes nothing while one retry succeeds after exactly one
  conflict. Fluent and DynamoDB Local contracts verify the public builders and
  exact effective values in `capabilities()`.
- The opt-in current-binary soak passes its default four-process, 50-write
  hot-table shape. One writer is killed after submission/before acknowledgement,
  task cancellations and exact-token replays are injected, and the final state
  is exactly 200 items, 201 immutable versions, and 201 unique contiguous
  commits. This is a local baseline, not mixed-version or hosted-AWS evidence.
- Provider: all seven unit and ten DynamoDB Local tests pass, including
  fail-closed schema-race and blob-envelope parsing, atomic transaction
  conformance, immutable-prepublication response loss/conflict, atomic root
  conflict, pre-execution and post-acceptance retry exhaustion, task
  cancellation after acceptance, namespace isolation/root cutover, and
  successful post-acceptance reconciliation. Every transaction retry test
  proves exact reuse of the namespace-bound provider token.
- Admin: command parser/output-safety tests pass.
- Core, client, provider, and admin pass strict Clippy with `-D warnings`.
- The locked reproducible matrix passes with Rust 1.91.1 for native
  `aarch64-apple-darwin` and cross-linked `aarch64-unknown-linux-gnu` and
  `x86_64-unknown-linux-gnu` using GCC 15.2.0. It covers root-library
  minimal/default/Tokio builds; provider/core/client default and
  `--no-default-features` all-target builds; the admin all-target build; and
  exact resolution of `aws-sdk-dynamodb 1.73.0`, `aws-lc-rs 1.17.3`, and
  `aws-lc-sys 0.43.0`. Cross-target results are compile/link evidence, not
  Linux runtime evidence.
- `.github/workflows/versioned-dynamodb-client-required.yml` makes strict
  quality (including benchmark lint/tests), checksum-pinned DynamoDB Local
  contracts and exact runner-v15 full/history diagnostics, Linux AArch64 cross-compilation,
  and clean package/downstream verification independent required-job
  candidates. Repository branch protection must require those jobs; adding the
  workflow file alone is not evidence that GitHub executed it.
- Every release/qualification manifest now has a checked-in `Cargo.lock`,
  including the root and DynamoDB store graphs that were previously ignored.
  This closes a clean-checkout defect where CI commands specified `--locked`
  but had no lockfile to consume. Package creation and extracted archive tests
  also run with `--locked`. Before extracted tests, only the unpublished
  dependency-order packages have their registry source identities replaced by
  exact local archive paths through package-scoped `cargo update --offline`;
  all other locked versions remain fixed. Dependency changes therefore require
  an explicit, reviewable lockfile diff.
- The complete client Rust surface is frozen as a 1,216-line signature and
  trait baseline generated by `cargo-public-api 0.52.0` with
  `nightly-2026-06-19`. Its SHA-256 is
  `04dc620779525275fb5c9c7ab381819a7ff97e2e8210b130dae909ce1b6ac648`.
  Reviewed additive changes export the selected cache default and the
  single-lock cache-occupancy observation so deployments and evidence harnesses
  neither duplicate a magic number nor infer retained weight from RSS.
  The checker is green locally, CI runs it, and package verification requires
  byte-identical inclusion in the client archive.
- DynamoDB Local 3.0.0 was run from AWS's immutable
  `dynamodb_local_2025-07-15.tar.gz` archive. Its 52,583,335-byte payload has
  SHA-256 `1125086301253b89539fa3bdbf1ded0656c596a2d34e0ca3c10af83373e7fd88`;
  the embedded release notes identify version 3.0.0 dated 2025-06-26.
- Extracted `.crate` archives compile in a clean downstream application. The
  extracted core and client archives also compile every packaged unit,
  integration, property, example, and fluent-contract target; their canonical
  fixtures are present and byte-identical. The
  dirty-worktree dependency archive hashes were:
  - `prolly-map 0.7.0`: `9889edd869ffdd57e9aa2c7f28ec20aa99534122ea7e5d5a0569493fb8c9887f`
  - `prolly-store-dynamodb 0.6.0`: `7ffd0427c60675c84eab2287d8407f2e8275ad6c7d706ce5ccc8e958fef7bc97`
  - `prolly-dynamodb-core 0.1.0`: `843f35c2563b6221a59e2b1eeadb65b1c6f549f6a21e285379c41fa6fe0c7fa2`

These hashes are not release artifacts because the worktree was intentionally
dirty. The client archive hash is deliberately not embedded in this packaged
file: doing so would make the archive hash self-referential and stale. Re-run
`scripts/verify_dynamodb_client_packages.sh` from the reviewed clean commit and
store every resulting hash, including the client hash, in the external signed
release attestation.

## Correctness findings closed during audit

- Durable database-format and blob-manifest decoders now use checked field
  extraction rather than panic-dependent fixed-slice conversions. Every blob
  envelope truncation is covered. `AttributePath` deserialization now enforces
  nonempty, name-rooted, bounded paths, and update/condition evaluation
  defensively revalidates paths; crafted serialized expression ASTs can no
  longer turn invalid paths into process panics. Condition and recursive update
  operands have explicit parser and evaluator depth ceilings, including a
  whole-condition preflight that cannot be bypassed through boolean
  short-circuiting. Deserialized update plans and projections reject empty,
  duplicate, overlapping, or structurally invalid paths.
- Physical schema initialization now revalidates the final ACTIVE table after
  every create race. Previously, a concurrent creator could win between the
  initial `DescribeTable` and `CreateTable`; the resulting
  `ResourceInUseException` path waited for ACTIVE but did not validate the
  winner's key schema. Deterministic AWS-protocol replay tests cover both the
  primary and companion roots tables and prove incompatible race winners fail
  closed. The provider crate now also forbids unsafe Rust.
- Cross-process contention exposed a false corruption classification in the
  base/index publication path: the base maps were transaction-pinned while the
  independently named indexed root was read live, so a valid intervening
  commit could make those two observations differ. The mismatch now restarts
  the bounded optimistic attempt from a fresh root set; a persistent mismatch
  still fails closed when the retry budget is exhausted. The original
  four-process soak reproduced the failure, the isolated fixed run passed, and
  three further consecutive four-process/50-write process-death soaks passed.
- Retention now excludes any bounded removal candidate referenced by a live
  ten-minute idempotency record, preserving exact replay return images.
- The full DynamoDB Local lifecycle now also protects an explicitly designated
  historical version through retention application, reads its original item
  afterward, and then completes fenced GC. This proves the provider-backed
  retention/GC path preserves operator-declared evidence, not only the
  in-memory core contract.
- Worker fencing uses a durable independent counter, so release/reacquire cannot
  reuse an ABA fence generation.
- Stream and TTL jobs bind the exact table incarnation; same-name recreation
  cannot read or delete through a stale worker.
- Provider transaction tokens bind physical node table, roots table, and key
  prefix. Identical logical transactions in isolated namespaces cannot collide.
- A successful `TransactWriteItems` response can be lost after deserialization;
  the provider conservatively classifies that outcome as ambiguous, retries
  through a new SDK execution with the exact token, and observes one committed
  node/root transition.
- The publication matrix now cuts validation, blob/node preparation, atomic
  root condition/write, pre-execution failure, accepted-response loss, task
  cancellation, bounded retry exhaustion, reconciliation-read failure, and
  process restart. Each test distinguishes absent, unreachable prepared,
  committed, and outcome-unknown durable states; see `FAULT_INJECTION.md`.
- Provider maintenance scan cursors are now opaque envelopes bound to the exact
  physical table and key prefix. Pagination may advance through unrelated
  physical keys while returning only the configured namespace; another
  namespace cannot reuse the cursor. The full GC contract passes with stale
  unrelated namespaces present in the shared physical table.
- Format bootstrap and indexed-root CAS publication use the same explicit
  durable metadata clock as their owning logical transaction. Deterministic
  traces through fluent, official-input, and direct-core APIs produce
  byte-identical canonical root manifests in three isolated namespaces.
- Format 12 has a checked-in exact record fixture; formats 10 and 11 remain
  historical decode guards only. Exact bytes and semantic decode are frozen;
  malformed envelopes and independent drift of all durable fields fail closed.
  Both older and newer format-version substitutions are covered. This proves
  current format negotiation, not historical-binary rollback, which remains a
  release gate until an independently built prior package exists.
- Every client archive now carries a stable rolling-compatibility probe, and a
  fail-closed coordinator records binary SHA-256 values, compares the complete
  negotiated format record, interleaves old/new writers, verifies exact
  item/version/commit/head state through both binaries, and performs reciprocal
  immutable-version reads. Its 12+12 same-binary DynamoDB Local diagnostic
  passed with 24 items and 25 versions/commits and cleaned the namespace. The
  coordinator rejects identical binaries unless explicitly placed in
  diagnostic mode, so this result validates the harness but is not historical
  mixed-version evidence.
- Core and client conformance fixtures are now package-local instead of
  referring above their crate roots. The packaging gate compiles all targets
  from extracted archives and rejects missing format fixtures or byte drift
  between the core and facade canonical/validation corpora.
- The supported TLS provider is exact-pinned after the next compatible
  `aws-lc-sys` produced an invalid object on the qualified Apple toolchain.
- `benchmarks/dynamodb-client` now supplies an executable client-level
  performance slice: cold/warm point reads, Query/Scan, ten-item batch and
  transaction reads/writes, one/ten/hundred-action atomic writes,
  version-creating Put/Update/Delete requests, exact-version reads, structural
  diff, and history enumeration. Its
  Smithy interceptor records SDK executions, HTTP attempts/retries, complete
  request/response byte counts, per-API fan-out, and physical transaction
  actions per durable raw sample. Returned transition metadata validates every
  advertised version count. The runner captures machine, revision, dependency,
  binary, DynamoDB Local artifact, raw process CPU timing, and normalized peak
  RSS provenance and produces a deterministic percentile report. A
  expanded one-sample/10-record transaction-shape smoke produced
  27/27 valid rows; the immutable read returned the fixture item and diff
  reported 122 changes. The 1, 10, and 100 logical actions each produced one
  table version and 21 physical root-transaction actions in that fixture. These
  are instrumentation regression checks, not published performance claims.
  The expanded rows additionally validated indexed write/GSI read, exact
  128-KiB blob payload write/read, isolated CAS restore, index plan/activation,
  retention plan/application, and fenced GC plan/application.
  The blob row carried 131,106 logical item bytes; restore reused an existing
  immutable version and correctly advertised zero newly created content
  versions.
- The runner-v6 resumable size-matrix diagnostic passed 1 KiB/100 records,
  64 KiB/100 records, and 399,000-byte/30-record cases with exact manifest
  validation and no failed artifacts. It produced 27 rows for the default
  three-transaction-shape case and 26 rows for each two-shape case. Peak
  whole-process RSS was 43,810,816, 51,773,440, and 73,236,480 bytes,
  respectively. Matrix resume rejects older runner versions, partial rows,
  failed runs, revision/configuration drift, missing operations, and duplicate
  substitutions; qualification refuses dirty worktrees by default. These
  single-sample DynamoDB Local runs validate the expanded workload and size
  paths only. A separate five-sample run passed all 135 rows, including five
  retention/GC cycles followed by batch and 1/10/100-action writes. Adding
  this workload exposed and fixed a GC safety defect: durable indexed snapshot
  source/index roots were indirect state-tree references, and raw provider
  deletion also bypassed complete process-cache invalidation. GC now expands
  that nested closure and invalidates node plus branch-lineage caches after
  every successful deletion chunk. The clean 10K/1M qualification profile,
  history-depth matrix, and hosted-AWS measurements remain open gates.
- Runner-v15 matrix-v2 smoke diagnostics now pass end to end and on a second
  validation-only resume. The full matrix covers 100 records at 1 KiB, 64 KiB,
  and 399,000 bytes; the focused history matrix covers depths 10, 100, and
  1,000. Every case carries validated raw, GC where applicable, cache, process,
  binary, and run-manifest evidence. Aggregate validators enforce exact CSV and
  manifest schemas, case order, revision, cache ceiling, sample count, and
  result-directory identity. Disposable DynamoDB Local is removed after every
  case. These dirty one-sample smoke matrices prove harness executability, not
  the clean repeated qualification envelope.
- Format 12 replaces the monolithic indexed coordinator with one bounded active
  snapshot and a current-only per-table catalog of compact locators. Each
  locator binds the exact indexed snapshot ID and a content-addressed
  one-record tree containing the full immutable manifest. Current-only commit
  roots avoid obsolete audit-root history; transaction-pinned and ordered
  batch root reads avoid redundant provider calls. The 1,100-write core
  contract passes beyond the former 1,024 ceiling and verifies 1,110 exact
  roots, including 1,103 immutable version roots.
- GC now expands snapshot-catalog protection directly and uses a per-table
  append-only blob registry for successful writes/imports. Plans canonically
  bind `protected_trees`, `scanned_blob_nodes`, and `scanned_values`; apply
  recomputes them before deletion and fails closed on any mismatch. Full
  depth-1,000 DynamoDB Local GC passes with 984 retained roots, 2,861 protected
  trees, 56 blob-scan nodes, and 3,073 scanned values. The additional protected
  trees are detached historical manifests, and retention directly proves that
  removed locators remove their trees from the protection set. The registry deliberately
  favors safety: blobs referenced only by removed history can remain until an
  explicit exact registry-compaction design is audited.
- Runner v13 requires a revision/sample-bound `gc-reachability.csv` artifact in
  full runs, records its checked history-scaled protected-tree ceiling, and
  rejects over-limit graphs. It also offers fail-closed runner-owned ephemeral
  Docker-volume teardown for long local runs. The latest isolated format-12
  depth-1,000 history append (runner v12) took
  35.552 seconds, 22,797 SDK executions, 322,173,160 request bytes, 505,937,748
  response bytes, and 46,956,544-byte peak RSS. Format 11 took 41.423 seconds,
  24,520 executions, 383,827,260 request bytes, and 752,415,229 response bytes;
  both improve on the superseded
  68.830-103.696 seconds, 58,768-59,812 executions, and
  336,330,752-420,921,344-byte RSS. A five-sample depth-100 accepted-layout run
  had 1.738-second median append, 1,827.8 mean executions, and 10,662,073 mean
  request bytes. Format 12 uses 1,704 mean executions, 7,973,805 mean request
  bytes, and the same 900 transaction actions at depth 100. Its 1.943-second
  local median is treated as emulator variance. A combined audit-root experiment
  lowered latency/calls but increased request bytes and was rejected. These are
  dirty-worktree DynamoDB Local diagnostics, not production envelopes.
- The current write path keeps format 12 unchanged while safely reusing only
  CID-validated nodes from successful prior commits, pinning required roots in
  ordered strongly consistent batches, and suppressing indexed-publication
  readback only for an explicit durable-publication backend capability.
  Rollbacks/conflicts never populate the committed cache. DynamoDB opts in
  because successful writes are durable and the adapter retries every returned
  `UnprocessedItem` until none remain or returns an error; other backends retain
  readback. A five-sample depth-100 rerun uses exactly 803 SDK executions per
  100 appends, 7,729,455 mean request bytes, 291,579 mean response bytes, and
  the unchanged 900 transaction actions. That is 52.9%, 3.1%, and 95.6% below
  the accepted format-12 execution/request/response baselines. Its 1.522-second
  local median is not an AWS latency claim. A one-sample depth-1,000 rerun
  passed with 8,003 executions and 16.709-second append time, versus 22,797 and
  35.552 seconds before the optimization.
- A runner-v14 one-sample depth-10,000 rerun using the selected 64-MiB default
  passes every exact history row in 246.989 seconds with 101,177 SDK executions,
  3.430 GB request bytes, 0.917 GB response bytes, 90,000 transaction actions,
  and 184,401,920-byte peak client RSS. This cuts append time 44.0%, executions
  64.7%, request bytes 21.3%, and response bytes 91.2% from the prior format-12
  result. A 256-MiB diagnostic reached 98,763 executions and 233.487 seconds but
  545,619,968-byte RSS; 64 MiB cuts that RSS 66.2% for 2.4% more calls and 5.8%
  more one-sample local time. Cache weight is not a hard RSS cap, so memory
  remains a production gate. The result does not close clean repeated, 100K,
  hosted-AWS, or memory-envelope qualification.
- Runner v15 adds a manifest-bound `cache-usage.csv` row per sample from the
  client's single-lock occupancy API. A disposable depth-10 history smoke run
  passed all six exact rows and reported 81 primary-client entries, 91,544
  bytes of retained serialized-node weight, no pins, and 24,002,560-byte peak
  RSS under the configured 67,108,864-byte ceiling. Validation rejects missing
  evidence, configuration drift, impossible pinned occupancy, and unpinned
  weight above the ceiling. A separate full-workload smoke passed all 34 exact
  rows plus GC and reported 404 entries, 13,030,178 retained bytes, no pins,
  and 57,884,672-byte peak RSS under the same ceiling. These prove both evidence
  paths locally; they do not replace the outstanding repeated/full-size/hosted
  memory gates.
- The runner-v15 depth-10,000 history rerun passes all six exact rows and
  directly records 2,936 primary-client entries, 67,104,564 retained bytes,
  zero pins, and 183,484,416-byte peak RSS under the manifest-bound
  67,108,864-byte ceiling. The complete workload uses 101,004 SDK executions,
  3,560,311,102 request bytes, and 1,579,639,834 response bytes; append takes
  279.640 seconds. This closes the local 10K cache-bound observation gap, but
  remains one dirty-worktree emulator sample rather than a production envelope.
- A separate runner-v15 cache-disabled history diagnostic passes all six rows
  and records exactly zero entries, zero retained bytes, and zero pins under a
  manifest-bound zero-byte ceiling, with 23,871,488-byte peak RSS. This proves
  the public zero-disables-cache contract and its evidence path agree.
- A one-sample runner-v12 pre-optimization format-12 10K history diagnostic passed all six exact
  rows and cleanup: 10,000 appended versions, 10,001 enumerated versions, exact
  oldest read/diff, and an 80-version retention apply. Append took 441.004
  seconds, 286,898 SDK executions, 90,000 transaction actions, 4.357 GB request
  bytes, 10.392 GB response bytes, and 89,948,160-byte peak client RSS. Against
  format 11, latency fell 31.5%, request bytes 15.0%, response bytes 24.3%, and
  executions 1.8%; RSS increased 4.6% but remains bounded. Safe namespace
  deletion made whole-run time 1,462.62 seconds, down from 2,123 seconds. This
  closes the 10K diagnostic correctness/memory question, but not clean repeated
  qualification or the latency/cost envelope. The current 31-GiB in-memory
  emulator remains unqualified for 100K. The improved run above removes most
  call/response amplification but still leaves material duration/storage
  pressure and a measured 184-MB client RSS envelope.
- A runner-v13 full depth-10K diagnostic passed all 40 rows plus revision-bound
  GC evidence under its recorded 50,000-tree ceiling: 9,984 retained roots,
  29,861 protected trees, 29,969 live nodes (23,346,531 bytes), 170 blob-scan
  nodes, and 30,073 scanned values. GC plan/apply used 321/32 SDK executions and
  1.296/0.320 seconds; whole-process peak RSS was 245,334,016 bytes. The
  runner-owned disposable volume was removed after validation and total wall
  time was 508 seconds. This is dirty local diagnostic evidence, not clean
  repeated or hosted qualification.
- `SECURITY.md` now defines the non-negotiable trusted-server boundary,
  credential-compromise consequences, hostile-tenant table isolation,
  encryption/network/logging controls, evidentiary limits, backup caveats, and
  a fail-closed deployment checklist. Exact-table runtime and provisioner IAM
  templates exclude DeleteTable and wildcard resources. A mechanical test
  compares their action union with every AWS SDK operation invoked by the
  provider, so a new physical API cannot silently leave the policies stale.
  Namespace prefixes remain collision boundaries only, not IAM tenant
  boundaries; version history is explicitly not WORM or legal-retention proof.

## Open production-release gates

The unchecked Plan 019 items are intentional blockers:

1. Publish clean reproducible crates with signed provenance and verify a
   registry-only downstream application.
2. Complete Linux runtime and hosted-AWS qualification. Compilation/linking is
   proven for the declared Apple ARM64, Linux ARM64, and Linux x86_64 target
   set, and rustls with the exact AWS-LC versions is the sole supported TLS
   configuration. Cross-compilation does not prove Linux runtime behavior or
   hosted AWS semantics.
3. Complete mixed-version and hosted-AWS soak qualification with throttling and
   randomized ambiguous/process-death schedules. The current-binary DynamoDB
   Local baseline already proves independent processes, task cancellation, one
   killed writer, exact-token restart, hot-head conflict retry, and exact final
   item/version/commit integrity; see `SOAK.md`.
4. Publish client-level cold/warm benchmarks for every declared operation,
   including physical request/byte/cost and process resource measurements. The
   facade/batch/transaction/history/index/blob/restore/admin harness is
   executable and bounded coordinator/GC scaling is proven at depth 1,000, but
   remaining resource attribution, hosted capacity/cost, and the clean
   10K/100K/full-size matrices are open.
5. Publish measured concurrency, latency, memory/cache, table-size, and
   transaction-shape support envelopes.
6. Test rolling upgrade/downgrade for every supported persisted format. The
   current release safely supports exact format 12 only, freezes its record,
   rejects every record-field drift, and advertises no migration or downgrade.
   Same-source evidence cannot prove rollback to an independently released
   package. The cross-binary probe/coordinator is delivered and diagnostic
   smoke is green, but this gate remains open until that artifact exists and
   passes in both reader/writer directions.
7. Make the single-head versus admission/sharding decision from measured AWS
   contention thresholds. Format 12 currently implements one table head.
8. Stabilize the pre-1.0 Rust API and compatibility policy, then require any
    later language binding to consume the same core and frozen fixtures.

Until these gates close, use the implementation for continued qualification,
not as an unconditional production SLA or cost claim.
