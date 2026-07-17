# Binding API parity contract

parity.json is the checked-in contract between the public Rust API and the
Python, Go, Node/TypeScript, Kotlin, Java, Ruby, Swift, and WASM packages.

The inventory is generated from rustdoc JSON, including public root exports,
public fields and variants, trait items, and public inherent associated
functions. This avoids treating a handwritten method list as evidence of
coverage.

## Refreshing the inventory

Run from the repository root:

~~~sh
cargo +nightly rustdoc --lib --features async-store -- \
  -Z unstable-options \
  --output-format json
python3 scripts/binding_api_inventory.py generate
python3 scripts/binding_api_inventory.py check
~~~

Generation preserves reviewed entries and adds new Rust symbols with status
planned. It removes symbols that are no longer in the public Rust inventory.
Review every added or removed operation in the same change that updates the
Rust API.

The manifest records the Rust feature set used for extraction. The checker
also requires feature-gated async sentinel types, so rustdoc generated without
`async-store` cannot silently pass the inventory gate.

The normal check command proves inventory completeness: no current Rust symbol
is missing from the contract, no removed symbol remains, and every entry has a
valid classification and status.

## Release validation

Run:

~~~sh
python3 scripts/binding_api_inventory.py check --release
~~~

Release validation additionally requires:

- status implemented;
- a non-empty binding symbol or an explicit exclusion for all eight languages;
- at least one conformance test ID;
- non-empty reasons for platform exclusions;
- no overlap between implemented language mappings and exclusions.

platform-excluded is reserved for genuine runtime constraints. The approved
WASM exclusions are filesystem stores, SQLite stores, APIs whose only meaning
is blocking an OS thread, and native-thread guarantees. Browser-safe
replacement behavior still needs a mapped symbol and test.

rust-language-only covers compile-time machinery such as lifetimes or marker
types. These entries still require an idiomatic mapping for every language and
a test of the corresponding runtime ownership or behavior.

The generated manifest begins with planned mappings. Inventory check success
must not be described as feature parity; only the strict release check plus the
per-language test matrix is parity evidence.

## Classification audit

Run:

~~~sh
python3 scripts/binding_api_inventory.py audit
~~~

This writes `classification-audit.json`, a deterministic review queue with the
rustdoc item kind, owning type or trait, domain family, manifest state, and one
mutually exclusive audit bucket for every public Rust path. The buckets mean:

- `release_complete`: the existing strict release predicate passes;
- `reviewed_incomplete`: a human reviewed the mapping, but required release
  evidence is still missing;
- `unreviewed_runtime_candidate`: a public free or inherent function that may
  represent application behavior;
- `unreviewed_data_model`: a struct, enum, field, variant, constant, or static;
- `unreviewed_rust_abstraction`: a trait item, type alias, module, primitive,
  or another Rust language surface requiring an idiomatic-equivalence review.

Audit counts are triage information, not parity evidence. In particular, data
model and Rust-abstraction rows are not automatically missing application APIs,
and runtime candidates are not automatically application-facing. No audit
bucket changes a manifest row to implemented or satisfies the strict release
gate.

Public enum variants and public trait items inherit reachability from their
public owner even though rustdoc commonly records their visibility as
`default`. The inventory includes them explicitly.

## Idiomatic equivalents

`idiomatic-equivalents.json` records shared semantic and performance contracts
for Rust abstractions that cannot be copied literally into every host language.
The reviewed families cover generic codecs, iterators and sequences, borrowed
views, store traits, typestate builders, and compile-time marker or associated
types.

The catalog does not synthesize manifest mappings or mark operations complete.
An `idiomatic` or `rust-language-only` manifest row passes release validation
only when it:

- references a valid catalog equivalence with the same classification;
- maps a concrete host symbol or pattern in all eight languages;
- is explicitly reviewed with a non-empty rationale and documentation link;
- cites test evidence shared with the equivalence contract.

A `platform-excluded` row needs the same review metadata and may exclude only
WASM. Each exclusion reason must be non-empty. Native bindings are required to
provide the complete portable application surface; browser-WASM exclusions are
limited to genuine filesystem, SQLite, OS-thread blocking, or native-thread
constraints described above.

Run `python3 scripts/binding_api_inventory.py review-abstractions` after a
rustdoc refresh to apply the exact, checked-in owner/kind review rules. This
command adds classification, equivalence, rationale, and documentation metadata
only. It deliberately preserves `planned`, empty language mappings, and empty
test evidence until the corresponding host API and conformance coverage have
been verified.

## Application gap report

Run:

~~~sh
python3 scripts/binding_api_inventory.py review-audiences
python3 scripts/binding_api_inventory.py apply-reconciliations
python3 scripts/binding_api_inventory.py gaps
~~~

`application-gap-report.json` separates runtime audience review from mapping
evidence. A function is an application gap candidate only after its audience is
explicitly reviewed as `application`. The report sections mean:

- `release_complete_application_operations`: strict mapping and test evidence
  exists;
- `bound_pending_manifest_evidence`: source reconciliation found the behavior
  in all eight wrappers, directly or idiomatically, but concrete manifest
  symbols/tests are not complete yet;
- `confirmed_missing_implementation`: reviewed source evidence confirms the
  application behavior is absent;
- `confirmed_performance_gap`: functional behavior exists, but a Rust
  scoped-view, retained-session, packed-page, or zero-copy contract is not
  consistently exposed;
- `unmapped_application_operations`: reviewed application behavior has no
  manifest mapping yet;
- `mapped_missing_evidence`: at least one mapping exists, but strict language or
  test evidence is incomplete;
- `platform_review_required`: a proposed exclusion still needs release evidence;
- `application_review_required`: runtime behavior has not yet received an
  audience decision and must not be counted as a missing application API;
- `non_application_runtime`: reviewed Rust extension, cursor, borrowed-view, or
  adapter machinery represented by an idiomatic host pattern;
- `data_model_or_abstraction_debt`: incomplete non-function rows that remain in
  the strict release audit.

`unmapped_application_operations` means a confirmed application-facing
*evidence gap*. It is not automatically a confirmed missing implementation;
source-surface reconciliation must next determine whether the operation is
already bound under an idiomatic name, grouped into another host operation, or
actually absent.

`versioned-map-reconciliation.json` is the first exact source reconciliation.
Its four groups partition all 66 incomplete `VersionedMap` methods into direct
bindings, idiomatic bindings, confirmed API gaps, and confirmed performance
gaps. `apply-reconciliations` expands those reviewed groups onto the individual
manifest rows; it does not mark any row implemented.
