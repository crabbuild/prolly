#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

obsolete='INDEX_(CATALOG|CONTROL)|index_(catalog|control|checkpoint)_root|SecondaryIndexCatalog|IndexCheckpoint|component_head|catalogVersion|indexMapId|supportsTransactions|indexed_map_production|IndexedStoreProfile|ProductionIndexedStoreCapabilities|productionProfile|production_profile'
scope=(
  src/prolly/secondary_index
  src/prolly/error.rs
  src/lib.rs
  bindings/api/application-gap-report.json
  bindings/api/classification-audit.json
  bindings/api/parity.json
  bindings/uniffi/src/domain/indexed.rs
  bindings/node/native/src/portable.rs
  bindings/node/src/indexed.ts
  bindings/node/index.d.ts
  bindings/wasm/src/indexed.rs
  bindings/wasm/src/index.ts
  docs/secondary-index-design.md
  docs/superpowers/plans/2026-07-28-secondary-index-industrial-foundation-hard-cutover.md
  docs/superpowers/specs/2026-07-28-secondary-index-industrial-foundation-hard-cutover-design.md
  examples/secondary_index.rs
)

if rg -n "$obsolete" "${scope[@]}"; then
  echo "obsolete secondary-index architecture symbol found" >&2
  exit 1
fi

echo "secondary-index hard-cutover absence gate passed"
