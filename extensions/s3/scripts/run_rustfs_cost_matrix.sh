#!/usr/bin/env bash
set -euo pipefail

matrix_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi

export PROLLY_S3_COST_MATRIX=1
export RUSTC_WRAPPER=""
cargo "$cargo_toolchain" test \
  --manifest-path "$matrix_root/Cargo.toml" \
  -p prolly-s3-client --all-features --test rustfs_repository \
  rustfs_s3_shaped_operation_cost_matrix -- --nocapture --test-threads=1

echo "RUSTFS_COST_MATRIX_COMPLETE object_rows=17 maintenance_rows=24 cross_repository_rows=4 advisory_rows=1 total_rows=46"
