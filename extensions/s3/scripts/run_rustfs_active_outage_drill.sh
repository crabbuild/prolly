#!/usr/bin/env bash
set -euo pipefail

drill_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="${PROLLY_RUSTFS_CONTAINER:-prolly-rustfs}"
data_dir="${PROLLY_RUSTFS_DATA_DIR:-/Volumes/Workspace/prolly-data}"
prefix="${PROLLY_S3_CHAOS_PREFIX:-integration/active-outage/$(date +%s)-$$}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
build_dir="${CARGO_TARGET_DIR:-${PROLLY_S3_CARGO_TARGET_DIR:-/Volumes/Workspace/prolly-build/versioned-s3}}"
evidence_dir="${PROLLY_S3_CHAOS_EVIDENCE_DIR:-}"

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi
if [[ ! -d "$data_dir" ]]; then
  echo "RustFS data directory does not exist: $data_dir" >&2
  exit 2
fi
if ! docker inspect "$container" >/dev/null 2>&1; then
  echo "RustFS container $container does not exist" >&2
  exit 2
fi

mounted_data_dir="$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' "$container")"
if [[ "$mounted_data_dir" != "$data_dir" ]]; then
  echo "RustFS /data mount mismatch: expected=$data_dir actual=$mounted_data_dir" >&2
  exit 2
fi
if [[ -n "$evidence_dir" ]]; then
  if [[ -e "$evidence_dir" ]]; then
    echo "chaos evidence directory already exists: $evidence_dir" >&2
    exit 2
  fi
  mkdir -p "$evidence_dir"
  scratch=""
  test_log="$evidence_dir/active-outage.log"
  verification_log="$evidence_dir/verification.log"
else
  scratch="$(mktemp -d)"
  test_log="$scratch/active-outage.log"
  verification_log=""
fi

wait_for_rustfs() {
  local health
  for _ in $(seq 1 240); do
    health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)"
    if [[ "$health" == "healthy" || "$health" == "running" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "RustFS failed to become healthy: container=$container" >&2
  return 1
}

restore_provider() {
  local running
  running="$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)"
  if [[ "$running" != "true" ]]; then
    docker start "$container" >/dev/null 2>&1 || true
  fi
  wait_for_rustfs >/dev/null 2>&1 || true
}
cleanup() {
  restore_provider
  if [[ -n "$scratch" ]]; then
    rm -f -- "$test_log"
    rmdir "$scratch" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_rustfs
initial_kib="$(du -sk "$data_dir" | awk '{print $1}')"
started_at="$(date +%s)"

export PROLLY_S3_CHAOS=1
export PROLLY_S3_CHAOS_PREFIX="$prefix"
export PROLLY_RUSTFS_CONTAINER="$container"
export CARGO_TARGET_DIR="$build_dir"
export RUSTC_WRAPPER=""

set +e
cargo "$cargo_toolchain" test \
  --manifest-path "$drill_root/Cargo.toml" \
  -p prolly-s3-client --all-features --test rustfs_repository \
  rustfs_active_outage_reconciles_ -- \
  --nocapture --test-threads=1 2>&1 | tee "$test_log"
test_status="${PIPESTATUS[0]}"
set -e
if [[ "$test_status" != "0" ]]; then
  exit "$test_status"
fi
if [[ -n "$verification_log" ]]; then
  python3 "$drill_root/scripts/verify_active_outage_matrix.py" \
    --test-log "$test_log" | tee "$verification_log"
else
  python3 "$drill_root/scripts/verify_active_outage_matrix.py" --test-log "$test_log"
fi

wait_for_rustfs
finished_at="$(date +%s)"
final_kib="$(du -sk "$data_dir" | awk '{print $1}')"
health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"

summary="RUSTFS_ACTIVE_OUTAGE_COMPLETE container=$container prefix=$prefix scenarios=ordinary,merge,multipart,workspace,multi-delete,restore,reset,branch-delete provider_restarts=8 health=$health elapsed_seconds=$((finished_at - started_at)) initial_data_kib=$initial_kib final_data_kib=$final_kib data_growth_kib=$((final_kib - initial_kib))"
echo "$summary"
if [[ -n "$verification_log" ]]; then
  echo "$summary" >>"$verification_log"
fi
