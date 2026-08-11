#!/usr/bin/env bash
set -euo pipefail

matrix_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# This single-node RustFS development profile is qualified through 32 hot-branch
# writers. Tiers without a checked-in budget fail closed instead of silently
# becoming qualification evidence.
writer_matrix="${PROLLY_S3_CONTENTION_MATRIX:-1 8 32}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
limits="${PROLLY_S3_PRODUCTION_LIMITS:-$matrix_root/qualification/production-limits-v1.json}"
scratch="$(mktemp -d)"
test_log="$scratch/contention-matrix.log"

cleanup() {
  rm -f -- "$test_log"
  rmdir "$scratch" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi

export PROLLY_S3_CONTENTION=1
export RUSTC_WRAPPER=""
read -r -a writer_tiers <<<"$writer_matrix"
if [[ "${#writer_tiers[@]}" == "0" ]]; then
  echo "PROLLY_S3_CONTENTION_MATRIX must contain at least one writer count" >&2
  exit 2
fi
expected_writers="$(IFS=,; echo "${writer_tiers[*]}")"
for writers in "${writer_tiers[@]}"; do
  if [[ ! "$writers" =~ ^[1-9][0-9]*$ ]] || (( writers > 128 )); then
    echo "invalid writer count in PROLLY_S3_CONTENTION_MATRIX: $writers" >&2
    exit 2
  fi
  set +e
  PROLLY_S3_CONTENTION_WRITERS="$writers" cargo "$cargo_toolchain" test \
    --manifest-path "$matrix_root/Cargo.toml" \
    -p prolly-s3-client --all-features --test rustfs_repository \
    rustfs_contention_latency_probe -- --nocapture \
    2>&1 | tee -a "$test_log"
  test_status="${PIPESTATUS[0]}"
  set -e
  if [[ "$test_status" != "0" ]]; then
    exit "$test_status"
  fi
done

python3 "$matrix_root/scripts/verify_production_limits.py" \
  --limits "$limits" \
  --profile rustfs-development \
  --contention-log "$test_log" \
  --expected-writers "$expected_writers"

echo "RUSTFS_CONTENTION_MATRIX_COMPLETE writers=$writer_matrix"
