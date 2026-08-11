#!/usr/bin/env bash
set -euo pipefail

matrix_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
limits="${PROLLY_S3_PRODUCTION_LIMITS:-$matrix_root/qualification/production-limits-v1.json}"
scratch="$(mktemp -d)"
test_log="$scratch/cost-matrix.log"

cleanup() {
  rm -f -- "$test_log"
  rmdir "$scratch" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi

export PROLLY_S3_COST_MATRIX=1
export RUSTC_WRAPPER=""
set +e
cargo "$cargo_toolchain" test \
  --manifest-path "$matrix_root/Cargo.toml" \
  -p prolly-s3-client --all-features --test rustfs_repository \
  rustfs_s3_shaped_operation_cost_matrix -- --nocapture --test-threads=1 \
  2>&1 | tee "$test_log"
test_status="${PIPESTATUS[0]}"
set -e
if [[ "$test_status" != "0" ]]; then
  exit "$test_status"
fi

python3 "$matrix_root/scripts/verify_production_limits.py" \
  --limits "$limits" \
  --profile rustfs-development \
  --cost-log "$test_log"

echo "RUSTFS_COST_MATRIX_COMPLETE object_rows=17 maintenance_rows=23 cross_repository_rows=4 advisory_rows=1 total_rows=45"
