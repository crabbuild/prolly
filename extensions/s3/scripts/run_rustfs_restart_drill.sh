#!/usr/bin/env bash
set -euo pipefail

drill_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="${PROLLY_RUSTFS_CONTAINER:-prolly-rustfs}"
data_dir="${PROLLY_RUSTFS_DATA_DIR:-/Volumes/Workspace/prolly-data}"
prefix="${PROLLY_S3_RESTART_PREFIX:-integration/restart-drill/$(date +%s)-$$}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
build_dir="${CARGO_TARGET_DIR:-${PROLLY_S3_CARGO_TARGET_DIR:-/Volumes/Workspace/prolly-build/versioned-s3}}"

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi
if ! docker inspect "$container" >/dev/null 2>&1; then
  echo "RustFS container $container does not exist" >&2
  exit 2
fi
if [[ ! -d "$data_dir" ]]; then
  echo "RustFS data directory does not exist: $data_dir" >&2
  exit 2
fi
mounted_data_dir="$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' "$container")"
if [[ "$mounted_data_dir" != "$data_dir" ]]; then
  echo "RustFS /data mount mismatch: expected=$data_dir actual=$mounted_data_dir" >&2
  exit 2
fi

export PROLLY_S3_RESTART_PREFIX="$prefix"
export CARGO_TARGET_DIR="$build_dir"
export RUSTC_WRAPPER=""

initial_kib="$(du -sk "$data_dir" | awk '{print $1}')"
started_at="$(date +%s)"

PROLLY_S3_RESTART_DRILL_PHASE=prepare cargo "$cargo_toolchain" test \
  --manifest-path "$drill_root/Cargo.toml" \
  -p prolly-s3-client --all-features --test rustfs_repository \
  rustfs_restart_recovery_drill -- --nocapture

docker restart "$container" >/dev/null
for _ in $(seq 1 60); do
  health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
  if [[ "$health" == "healthy" || "$health" == "running" ]]; then
    break
  fi
  sleep 1
done
health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
if [[ "$health" != "healthy" && "$health" != "running" ]]; then
  echo "RustFS failed to recover after restart: state=$health" >&2
  exit 1
fi

s3_ready="false"
for _ in $(seq 1 60); do
  if PROLLY_S3_RESTART_DRILL_PHASE=ready cargo "$cargo_toolchain" test \
    --manifest-path "$drill_root/Cargo.toml" \
    -p prolly-s3-client --all-features --test rustfs_repository \
    rustfs_restart_recovery_drill -- --exact >/dev/null 2>&1; then
    s3_ready="true"
    break
  fi
  sleep 1
done
if [[ "$s3_ready" != "true" ]]; then
  echo "RustFS container became healthy but its authenticated S3 API did not become ready" >&2
  exit 1
fi

PROLLY_S3_RESTART_DRILL_PHASE=verify cargo "$cargo_toolchain" test \
  --manifest-path "$drill_root/Cargo.toml" \
  -p prolly-s3-client --all-features --test rustfs_repository \
  rustfs_restart_recovery_drill -- --nocapture

finished_at="$(date +%s)"
final_kib="$(du -sk "$data_dir" | awk '{print $1}')"
health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
echo "RUSTFS_RESTART_DRILL_COMPLETE container=$container prefix=$prefix health=$health s3_ready=$s3_ready elapsed_seconds=$((finished_at - started_at)) initial_data_kib=$initial_kib final_data_kib=$final_kib data_growth_kib=$((final_kib - initial_kib))"
