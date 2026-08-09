#!/usr/bin/env bash
set -euo pipefail

drill_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="${PROLLY_RUSTFS_CONTAINER:-prolly-rustfs}"
data_dir="${PROLLY_RUSTFS_DATA_DIR:-/Volumes/Workspace/prolly-data}"
run_id="${PROLLY_S3_BACKUP_RUN_ID:-$(openssl rand -hex 6)}"
prefix="${PROLLY_S3_BACKUP_PREFIX:-integration/backup/$run_id}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
build_dir="${CARGO_TARGET_DIR:-${PROLLY_S3_CARGO_TARGET_DIR:-/Volumes/Workspace/prolly-build/versioned-s3}}"
cleaned=0

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi
if [[ ! "$run_id" =~ ^[a-z0-9]{1,20}$ ]]; then
  echo "backup drill run ID must be 1-20 lowercase alphanumeric bytes: $run_id" >&2
  exit 2
fi
case "$prefix" in
  *[!A-Za-z0-9._/-]* | "" | /* | */)
    echo "backup drill prefix must be non-empty, relative, and path-safe: $prefix" >&2
    exit 2
    ;;
esac
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

run_phase() {
  local phase="$1"
  PROLLY_S3_BACKUP_DRILL=1 \
  PROLLY_S3_BACKUP_PHASE="$phase" \
  PROLLY_S3_BACKUP_RUN_ID="$run_id" \
  PROLLY_S3_BACKUP_PREFIX="$prefix" \
  CARGO_TARGET_DIR="$build_dir" \
  RUSTC_WRAPPER="" \
    cargo "$cargo_toolchain" test \
      --manifest-path "$drill_root/Cargo.toml" \
      -p prolly-s3-client --all-features --test rustfs_backup_restore \
      rustfs_physical_backup_restore_process_helper -- \
      --exact --nocapture --test-threads=1
}

cleanup() {
  if [[ "$cleaned" == "1" ]]; then
    return
  fi
  set +e
  run_phase cleanup >/dev/null 2>&1
  set -e
}
trap cleanup EXIT

initial_kib="$(du -sk "$data_dir" | awk '{print $1}')"
started_at="$(date +%s)"
run_phase run
run_phase cleanup
cleaned=1
finished_at="$(date +%s)"
final_kib="$(du -sk "$data_dir" | awk '{print $1}')"
health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
echo "RUSTFS_BACKUP_RESTORE_COMPLETE container=$container prefix=$prefix source_bucket=removed archive_bucket=removed restore_bucket=removed health=$health elapsed_seconds=$((finished_at - started_at)) initial_data_kib=$initial_kib final_data_kib=$final_kib data_growth_kib=$((final_kib - initial_kib))"
