#!/usr/bin/env bash
set -euo pipefail

drill_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="${PROLLY_RUSTFS_CONTAINER:-prolly-rustfs}"
data_dir="${PROLLY_RUSTFS_DATA_DIR:-/Volumes/Workspace/prolly-data}"
bucket="${PROLLY_RUSTFS_BUCKET:-prolly-versioned-s3-tests}"
prefix="${PROLLY_S3_IAM_PREFIX:-integration/iam/$(date +%s)-$$}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
build_dir="${CARGO_TARGET_DIR:-${PROLLY_S3_CARGO_TARGET_DIR:-/Volumes/Workspace/prolly-build/versioned-s3}}"
mc_image="${PROLLY_RUSTFS_MC_IMAGE:-minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727}"
admin_access_key="${PROLLY_RUSTFS_ACCESS_KEY:-prollyadmin}"
admin_secret_key="${PROLLY_RUSTFS_SECRET_KEY:-prolly-local-secret-change-me}"
run_id="$(openssl rand -hex 5)"
old_access_key="iamold${run_id}"
new_access_key="iamnew${run_id}"
old_secret_key="$(openssl rand -hex 32)"
new_secret_key="$(openssl rand -hex 32)"
policy_name="prolly-versioned-s3-runtime-${run_id}"
scratch="$(mktemp -d)"
policy_file="$scratch/runtime-policy.json"

case "$bucket" in
  *[!a-z0-9.-]* | "")
    echo "IAM drill requires a DNS-compatible lowercase bucket name: $bucket" >&2
    exit 2
    ;;
esac
case "$prefix" in
  *[!A-Za-z0-9._/-]* | "" | /* | */)
    echo "IAM drill prefix must be non-empty, relative, and use only A-Z, a-z, 0-9, '.', '_', '/', or '-': $prefix" >&2
    exit 2
    ;;
esac
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
network="$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{println}}{{end}}' "$container" | head -n 1)"
if [[ -z "$network" ]]; then
  echo "RustFS container is not attached to a Docker network" >&2
  exit 2
fi
if ! docker image inspect "$mc_image" >/dev/null 2>&1; then
  docker pull "$mc_image" >/dev/null
fi

export PROLLY_RUSTFS_ACCESS_KEY="$admin_access_key"
export PROLLY_RUSTFS_SECRET_KEY="$admin_secret_key"

mc_admin() {
  docker run --rm \
    --network "$network" \
    -e PROLLY_RUSTFS_ACCESS_KEY \
    -e PROLLY_RUSTFS_SECRET_KEY \
    -e "RUSTFS_ADMIN_ENDPOINT=http://${container}:9000" \
    -v "$scratch:/work:ro" \
    --entrypoint sh \
    "$mc_image" \
    -c 'mc alias set local "$RUSTFS_ADMIN_ENDPOINT" "$PROLLY_RUSTFS_ACCESS_KEY" "$PROLLY_RUSTFS_SECRET_KEY" >/dev/null && exec mc admin "$@"' \
    sh "$@"
}

cleanup_iam() {
  mc_admin user remove local "$old_access_key" >/dev/null 2>&1 || true
  mc_admin user remove local "$new_access_key" >/dev/null 2>&1 || true
  mc_admin policy remove local "$policy_name" >/dev/null 2>&1 || true
}

cleanup() {
  cleanup_iam
  if [[ -f "$policy_file" ]]; then
    rm -f -- "$policy_file"
  fi
  rmdir "$scratch" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_phase() {
  local phase="$1"
  local access_key="${2:-}"
  local secret_key="${3:-}"
  PROLLY_S3_IAM_DRILL=1 \
  PROLLY_S3_IAM_PHASE="$phase" \
  PROLLY_S3_IAM_PREFIX="$prefix" \
  PROLLY_S3_IAM_ACCESS_KEY="$access_key" \
  PROLLY_S3_IAM_SECRET_KEY="$secret_key" \
  PROLLY_RUSTFS_BUCKET="$bucket" \
  CARGO_TARGET_DIR="$build_dir" \
  RUSTC_WRAPPER="" \
    cargo "$cargo_toolchain" test \
      --manifest-path "$drill_root/Cargo.toml" \
      -p prolly-s3-client --all-features --test rustfs_repository \
      rustfs_iam_rotation_process_helper -- \
      --exact --nocapture --test-threads=1
}

initial_kib="$(du -sk "$data_dir" | awk '{print $1}')"
started_at="$(date +%s)"

run_phase prepare

sed \
  -e "s/__BUCKET__/$bucket/g" \
  -e "s|__PREFIX__|$prefix|g" \
  "$drill_root/policies/runtime-prefix-policy.template.json" >"$policy_file"
mc_admin policy create local "$policy_name" /work/runtime-policy.json >/dev/null
mc_admin user add local "$old_access_key" "$old_secret_key" >/dev/null
mc_admin policy attach local "$policy_name" --user "$old_access_key" >/dev/null

run_phase old-active "$old_access_key" "$old_secret_key"

mc_admin user add local "$new_access_key" "$new_secret_key" >/dev/null
mc_admin policy attach local "$policy_name" --user "$new_access_key" >/dev/null
run_phase new-active "$new_access_key" "$new_secret_key"

mc_admin user disable local "$old_access_key" >/dev/null
run_phase old-revoked "$old_access_key" "$old_secret_key"
run_phase verify

cleanup_iam
if mc_admin user info local "$old_access_key" >/dev/null 2>&1; then
  echo "old IAM drill identity still exists after cleanup" >&2
  exit 1
fi
if mc_admin user info local "$new_access_key" >/dev/null 2>&1; then
  echo "new IAM drill identity still exists after cleanup" >&2
  exit 1
fi
if mc_admin policy info local "$policy_name" >/dev/null 2>&1; then
  echo "IAM drill policy still exists after cleanup" >&2
  exit 1
fi

finished_at="$(date +%s)"
final_kib="$(du -sk "$data_dir" | awk '{print $1}')"
health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
echo "RUSTFS_IAM_DRILL_COMPLETE container=$container bucket=$bucket prefix=$prefix policy=prefix-read-write-no-delete rotation=overlap-then-revoke denied_probes=5 rustfs_provider_deviations=3 identities_removed=2 policies_removed=1 health=$health elapsed_seconds=$((finished_at - started_at)) initial_data_kib=$initial_kib final_data_kib=$final_kib data_growth_kib=$((final_kib - initial_kib))"
