#!/usr/bin/env bash
set -euo pipefail

matrix_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# This single-node RustFS development profile is qualified through 32 hot-branch
# writers. Set an explicit matrix containing 128 to probe, not imply, that tier.
writer_matrix="${PROLLY_S3_CONTENTION_MATRIX:-1 8 32}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi

export PROLLY_S3_CONTENTION=1
export RUSTC_WRAPPER=""
for writers in $writer_matrix; do
  if [[ ! "$writers" =~ ^[1-9][0-9]*$ ]] || (( writers > 128 )); then
    echo "invalid writer count in PROLLY_S3_CONTENTION_MATRIX: $writers" >&2
    exit 2
  fi
  PROLLY_S3_CONTENTION_WRITERS="$writers" cargo "$cargo_toolchain" test \
    --manifest-path "$matrix_root/Cargo.toml" \
    -p prolly-s3-client --all-features --test rustfs_repository \
    rustfs_contention_latency_probe -- --nocapture
done

echo "RUSTFS_CONTENTION_MATRIX_COMPLETE writers=$writer_matrix"
