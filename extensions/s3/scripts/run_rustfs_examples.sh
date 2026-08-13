#!/usr/bin/env bash
set -euo pipefail

manifest="extensions/s3/Cargo.toml"
examples=(
  basic_object_workflow
  atomic_batch_and_streaming
  branch_diff_merge
  restore_and_recovery
  history_transfer_and_backup
  integrity_gc_and_observability
)

if ! curl -fsS "${PROLLY_RUSTFS_ENDPOINT:-http://127.0.0.1:9000}/health" >/dev/null; then
  echo "RustFS is not healthy. Start extensions/s3/docker-compose.rustfs.yml first." >&2
  exit 1
fi

for example in "${examples[@]}"; do
  echo "RUNNING_EXAMPLE name=$example"
  cargo run --locked --manifest-path "$manifest" \
    -p prolly-s3-client --example "$example"
done

echo "RUSTFS_EXAMPLES_COMPLETE count=${#examples[@]}"
