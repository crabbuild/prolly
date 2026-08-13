#!/usr/bin/env bash
set -euo pipefail

qualification_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../qualification" && pwd)"
if [[ -n "${PROLLY_S3_CARGO_TARGET_DIR:-}" ]]; then
  target_root="$PROLLY_S3_CARGO_TARGET_DIR/clean-downstream"
  mkdir -p "$target_root/core-1.89" "$target_root/client-1.94.1"
else
  target_root="$(mktemp -d "${TMPDIR:-/tmp}/prolly-s3-downstream.XXXXXX")"
  trap 'rm -rf -- "$target_root"' EXIT
fi

export RUSTC_WRAPPER=""

CARGO_TARGET_DIR="$target_root/core-1.89" \
cargo +1.89.0 check \
  --manifest-path "$qualification_root/downstream-core/Cargo.toml" --locked
CARGO_TARGET_DIR="$target_root/client-1.94.1" \
cargo +1.94.1 check \
  --manifest-path "$qualification_root/downstream-client/Cargo.toml" \
  --no-default-features --locked
CARGO_TARGET_DIR="$target_root/client-1.94.1" \
cargo +1.94.1 check \
  --manifest-path "$qualification_root/downstream-client/Cargo.toml" \
  --no-default-features --features foyer-cache --locked

echo "CLEAN_DOWNSTREAM_COMPLETE core_rust=1.89.0 client_rust=1.94.1 feature_sets=minimal,foyer-cache"
