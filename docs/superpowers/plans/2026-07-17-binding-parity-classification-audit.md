# Binding Parity Classification Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Convert the 2,493 strict-release failures into a reviewed, evidence-backed classification inventory that distinguishes manifest debt from genuine application-facing API gaps, then close every genuine gap without sacrificing the existing retained-session, packed-page, and zero-copy performance model.

**Architecture:** Keep `bindings/api/parity.json` as the exhaustive per-Rust-symbol release contract. Enrich rustdoc extraction with stable item metadata and add a deterministic classification-audit report whose buckets never count an unreviewed row as parity. A separately checked-in equivalence catalog documents Rust-only abstractions and their concrete idiomatic host patterns; manifest rows remain the final source of per-language symbols, tests, performance tier, and review rationale. Actual gaps are operations with runtime application behavior but no evidenced portable facade mapping—not merely Rust fields, variants, traits, lifetimes, iterators, or implementation controls.

**Tech Stack:** Python 3 standard library, rustdoc JSON, `unittest`, JSON/Markdown contracts, existing Rust binding-domain facade, per-language parity suites, and existing zero-copy performance harnesses.

---

## Task 1: Produce a Deterministic Classification Audit

**Files:**
- Modify: `scripts/binding_api_inventory.py`
- Modify: `scripts/tests/test_binding_api_inventory.py`
- Create: `bindings/api/classification-audit.json`
- Modify: `bindings/api/README.md`

- [x] **Step 1: Add failing tests for rustdoc item metadata and audit buckets**

Add an `ApiItem` record with `rust`, `kind`, `owner`, and `member_kind`, then test that the existing synthetic rustdoc fixture classifies `prolly::VersionedMap` as a root `struct` and `prolly::VersionedMap::head` as an inherent `function`. Add a second fixture containing a public field, enum variant, trait method, and associated type.

Add an audit test with one implemented mapping and four planned rows. Assert exact mutually-exclusive counts for:

```python
{
    "release_complete": 1,
    "unreviewed_runtime_candidate": 1,
    "unreviewed_data_model": 1,
    "unreviewed_rust_abstraction": 2,
    "reviewed_incomplete": 0,
}
```

The audit must preserve every Rust path in a `rows` array so aggregate counts cannot hide individual symbols.

- [x] **Step 2: Run the focused tests and verify they fail**

Run: `python3 -m unittest scripts.tests.test_binding_api_inventory -v`

Expected: FAIL because `extract_public_api_items` and `build_classification_audit` do not exist.

- [x] **Step 3: Implement metadata extraction and strict audit buckets**

Implement:

```python
@dataclass(frozen=True, order=True)
class ApiItem:
    rust: str
    kind: str
    owner: str | None
    member_kind: str | None

def extract_public_api_items(rustdoc: dict[str, Any]) -> dict[str, ApiItem]: ...

def build_classification_audit(
    items: dict[str, ApiItem], manifest: dict[str, Any]
) -> dict[str, Any]: ...
```

Bucket policy:

- `release_complete`: current strict-release predicate passes.
- `reviewed_incomplete`: `reviewed` is true but evidence is still insufficient.
- `unreviewed_data_model`: root structs/enums/unions plus public fields and variants.
- `unreviewed_rust_abstraction`: traits, trait methods, associated types/constants, type aliases, and primitives.
- `unreviewed_runtime_candidate`: functions and inherent methods, including constructors and async operations.

Do not infer `rust-language-only`, exclusions, or completed parity. The audit is diagnostic; only reviewed manifest data may satisfy the release gate.

Add `audit` to the command parser. It writes deterministic JSON with the rustdoc format/features, summary counts, family counts, owner counts, and sorted rows. Do not include a timestamp.

- [x] **Step 4: Generate and document the checked-in audit**

Run:

```sh
python3 scripts/binding_api_inventory.py audit
python3 -m unittest scripts.tests.test_binding_api_inventory -v
python3 scripts/binding_api_inventory.py check
```

Document that audit bucket counts are triage information, while `check --release` remains the only manifest-completeness gate.

- [x] **Step 5: Commit the audit infrastructure**

```sh
git add scripts/binding_api_inventory.py scripts/tests/test_binding_api_inventory.py bindings/api/classification-audit.json bindings/api/README.md docs/superpowers/plans/2026-07-17-binding-parity-classification-audit.md
git commit -m "feat(bindings): classify strict parity audit debt"
```

---

## Task 2: Make Every Non-Portable Decision Reviewable

**Files:**
- Modify: `scripts/binding_api_inventory.py`
- Modify: `scripts/tests/test_binding_api_inventory.py`
- Create: `bindings/api/idiomatic-equivalents.json`
- Modify: `bindings/api/README.md`
- Modify: `bindings/api/parity.json`

- [x] **Step 1: Add failing validation tests for reviewed classifications**

Require every release-complete `idiomatic`, `rust-language-only`, or `platform-excluded` entry to contain a non-empty `rationale`, a non-empty `docs` list, and `reviewed: true`. Require `rust-language-only` entries to map all eight languages and include a runtime ownership/behavior test. Require platform exclusions to be individual, non-empty reasons and allow only the documented WASM runtime constraints.

Also test that a shared equivalence ID is rejected if absent from `idiomatic-equivalents.json`, and that an equivalence record is rejected unless it has:

```json
{
  "classification": "idiomatic",
  "portable_semantics": "...",
  "performance_contract": "...",
  "language_patterns": {
    "python": "...", "go": "...", "node": "...", "kotlin": "...",
    "java": "...", "ruby": "...", "swift": "...", "wasm": "..."
  },
  "tests": ["..."]
}
```

- [x] **Step 2: Run tests and verify strict policy failures**

Run: `python3 -m unittest scripts.tests.test_binding_api_inventory -v`

Expected: FAIL until equivalence validation and rationale checks exist.

- [x] **Step 3: Implement the equivalence catalog and validator**

Create reviewed equivalence records for these Rust abstraction families:

- `generic-codec`: Rust generic key/value codecs become host codec callbacks or typed wrapper constructors; callbacks are synchronous and copy into Rust-owned buffers before return.
- `iterator-sequence`: Rust iterators become host iterables/sequences for cold paths and bounded packed-page cursors for hot paths; no Rust borrow crosses the call.
- `borrowed-view`: Rust lifetime-bound views become callback-scoped or explicitly leased host views; scoped views never cross `await`.
- `store-trait`: Rust store traits become host callback/protocol interfaces with owned inputs and stable callback errors.
- `builder-typestate`: Rust generic/typestate builders become validated host builders or option records checked at construction.
- `marker-and-associated-type`: compile-time markers and associated types become documented host type constraints plus runtime validation where needed.

Load the catalog in the CLI and pass it to release validation. Equivalence records provide policy and shared evidence only; they must not synthesize per-entry language symbols or change a planned entry to implemented.

- [x] **Step 4: Classify Rust-only abstractions owner by owner**

For every row reported as `unreviewed_rust_abstraction`, inspect its Rust definition and runtime semantics. Update its manifest entry with the correct classification, `equivalence`, `rationale`, `docs`, performance tier, and `reviewed: true`. Use `idiomatic` whenever runtime behavior remains; reserve `rust-language-only` for compile-time-only machinery. Add concrete language symbols and tests only when verified; otherwise preserve `planned` so the row becomes `reviewed_incomplete` and is handled by the application-gap/evidence pass.

After each owner batch run:

```sh
python3 scripts/binding_api_inventory.py audit
python3 scripts/binding_api_inventory.py check
python3 -m unittest scripts.tests.test_binding_api_inventory -v
```

The `unreviewed_rust_abstraction` count must reach zero without adding blanket exclusions.

- [x] **Step 5: Commit the reviewed abstraction classifications**

```sh
git add scripts/binding_api_inventory.py scripts/tests/test_binding_api_inventory.py bindings/api/idiomatic-equivalents.json bindings/api/parity.json bindings/api/classification-audit.json bindings/api/README.md
git commit -m "feat(bindings): document idiomatic Rust equivalents"
```

---

## Task 3: Separate Application Operations from Data-Model Coverage

**Files:**
- Modify: `scripts/binding_api_inventory.py`
- Modify: `scripts/tests/test_binding_api_inventory.py`
- Modify: `bindings/api/parity.json`
- Create: `bindings/api/application-gap-report.json`

- [ ] **Step 1: Add failing gap-report tests**

Test that a planned inherent method is an `unmapped_application_operation`, a reviewed method with all symbols but missing tests is `mapped_missing_evidence`, and fields/variants/associated types never appear as application-operation gaps. Test that their incomplete evidence remains visible in a separate `data_model_or_abstraction_debt` section.

- [ ] **Step 2: Implement `build_application_gap_report`**

Implement:

```python
def build_application_gap_report(
    items: dict[str, ApiItem], manifest: dict[str, Any]
) -> dict[str, Any]: ...
```

Application-operation candidates are public free functions and public inherent/trait methods with runtime behavior. Constructor helpers count as operations. Structs, enums, fields, variants, constants, type aliases, associated types, and marker traits do not count as missing operations, but stay in the strict classification audit until evidenced.

The report contains sorted rows for:

- `unmapped_application_operations`;
- `mapped_missing_evidence`;
- `platform_review_required`;
- `data_model_or_abstraction_debt`.

Each application row includes Rust path, owner, family, current classification/status, missing languages, tests, performance tier, and review rationale.

- [ ] **Step 3: Generate the first defensible application-gap baseline**

Run: `python3 scripts/binding_api_inventory.py gaps`

Manually review constructors, trait methods, and implementation-control methods so false positives are reclassified with explicit rationale rather than filtered by name. Check in `bindings/api/application-gap-report.json`.

- [ ] **Step 4: Commit the application-gap report**

```sh
git add scripts/binding_api_inventory.py scripts/tests/test_binding_api_inventory.py bindings/api/parity.json bindings/api/classification-audit.json bindings/api/application-gap-report.json
git commit -m "feat(bindings): report application-facing parity gaps"
```

---

## Task 4: Close Genuine Runtime Gaps by Domain Family

**Files:**
- Modify as reported: `bindings/uniffi/src/domain/*.rs`, `bindings/uniffi/src/fast_abi.rs`
- Modify as reported: all eight language adapters and public type declarations
- Modify as reported: all eight portable parity suites
- Modify: `bindings/api/parity.json`
- Modify: `bindings/api/application-gap-report.json`

- [ ] **Step 1: Process gaps in dependency order**

Close `core`, `session`, `versioned-map`, `maintenance`, `proof`, `indexed-map`, and `proximity-map` gaps in that order. For each operation, first add one failing Rust facade test and one failing cross-language conformance fixture/test ID, then expose the facade operation and thin idiomatic adapters.

- [ ] **Step 2: Preserve performance semantics for every mapped operation**

Use owned values for cold calls, retained sessions and bounded packed pages for hot multi-result reads, callback-scoped or leased views only where the lifetime is enforceable, and owned clones for async handles. Keep index filtering/source joins and proximity traversal/reranking/proof construction in Rust. Record `owned`, `paged`, or `scoped-view` in each manifest row.

- [ ] **Step 3: Re-run the gap report after every family**

```sh
python3 scripts/binding_api_inventory.py gaps
python3 scripts/binding_api_inventory.py audit
python3 scripts/binding_api_inventory.py check --release
```

The strict gate may remain red while other reviewed rows are incomplete, but the processed family's `unmapped_application_operations` and `mapped_missing_evidence` counts must reach zero before moving on.

---

## Task 5: Final Strict, Cross-Language, and Performance Verification

**Files:**
- Modify: `bindings/VERIFICATION.md`
- Modify: `bindings/api/parity.json`
- Modify: generated audit/gap reports

- [ ] **Step 1: Require zero audit and gap debt**

Run:

```sh
python3 scripts/binding_api_inventory.py check --release
python3 scripts/binding_api_inventory.py audit
python3 scripts/binding_api_inventory.py gaps
```

Expected: strict release PASS; audit reports all 2,503 current operations as `release_complete`; application gap report contains zero unmapped, missing-evidence, or platform-review rows.

- [ ] **Step 2: Run the full language and Rust verification matrix**

Execute every command documented in `bindings/VERIFICATION.md`, including Rust facade/ABI tests, Python, Go, Node/TypeScript, Kotlin, Java, Ruby, Swift production build and XCTest on an XCTest-capable host, and browser WASM tests.

- [ ] **Step 3: Run zero-copy and regression performance gates**

Run the documented point, multi-get, range, diff, conflict, indexed join, and proximity search workloads on the benchmark host. Compare three-run medians and peak RSS to the checked-in baseline. Fail the release for more than 10 percent regression without explicit approval. Verify bounded page sizes, lease cleanup, and no scoped view crossing an async boundary.

- [ ] **Step 4: Commit the completed hard-cutover evidence**

```sh
git add bindings scripts docs/superpowers/plans/2026-07-17-binding-parity-classification-audit.md
git commit -m "feat(bindings): complete portable API parity audit"
```
