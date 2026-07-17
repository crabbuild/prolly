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
