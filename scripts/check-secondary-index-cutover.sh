#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

obsolete='INDEX_(CATALOG|CONTROL)|index_(catalog|control|checkpoint)_root|SecondaryIndexCatalog|IndexCheckpoint|component_head|catalogVersion|indexMapId|supportsTransactions'
scope=(
  src/prolly/secondary_index
  src/prolly/error.rs
  bindings/node/src/indexed.ts
  bindings/wasm/src/index.ts
  docs/secondary-index-design.md
  examples/secondary_index.rs
)

if rg -n "$obsolete" "${scope[@]}"; then
  echo "obsolete secondary-index architecture symbol found" >&2
  exit 1
fi

echo "secondary-index hard-cutover absence gate passed"
