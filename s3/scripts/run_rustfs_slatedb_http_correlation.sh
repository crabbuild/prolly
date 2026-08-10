#!/usr/bin/env bash
set -euo pipefail

drill_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="${PROLLY_RUSTFS_CONTAINER:-prolly-rustfs}"
data_dir="${PROLLY_RUSTFS_DATA_DIR:-/Volumes/Workspace/prolly-data}"
endpoint="${PROLLY_RUSTFS_ENDPOINT:-http://127.0.0.1:9000}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
build_dir="${CARGO_TARGET_DIR:-${PROLLY_S3_CARGO_TARGET_DIR:-/Volumes/Workspace/prolly-build/versioned-s3}}"
scratch="$(mktemp -d)"
ready_log="$scratch/proxy-ready.log"
proxy_error_log="$scratch/proxy-error.log"
http_metrics="$scratch/http-metrics.jsonl"
test_log="$scratch/test.log"
proxy_pid=""

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

cleanup() {
  if [[ -n "$proxy_pid" ]] && kill -0 "$proxy_pid" >/dev/null 2>&1; then
    kill -TERM "$proxy_pid" >/dev/null 2>&1 || true
    wait "$proxy_pid" >/dev/null 2>&1 || true
  fi
  rm -f -- "$ready_log" "$proxy_error_log" "$http_metrics" "$test_log"
  rmdir "$scratch" >/dev/null 2>&1 || true
}
trap cleanup EXIT

python3 "$drill_root/scripts/rustfs_http_counting_proxy.py" \
  --target "$endpoint" \
  --metrics "$http_metrics" \
  >"$ready_log" 2>"$proxy_error_log" &
proxy_pid="$!"
proxy_port=""
for _ in $(seq 1 240); do
  if ! kill -0 "$proxy_pid" >/dev/null 2>&1; then
    echo "SlateDB HTTP correlation proxy exited during startup" >&2
    sed -n '1,80p' "$proxy_error_log" >&2
    exit 1
  fi
  proxy_port="$(sed -n 's/^PROXY_READY port=//p' "$ready_log" | head -n 1)"
  if [[ "$proxy_port" =~ ^[0-9]+$ ]]; then
    break
  fi
  sleep 0.25
done
if [[ ! "$proxy_port" =~ ^[0-9]+$ ]]; then
  echo "SlateDB HTTP correlation proxy did not become ready" >&2
  exit 1
fi

initial_kib="$(du -sk "$data_dir" | awk '{print $1}')"
started_at="$(date +%s)"
set +e
PROLLY_S3_COST_MATRIX=1 \
PROLLY_RUSTFS_SLATE_ENDPOINT="http://127.0.0.1:$proxy_port" \
CARGO_TARGET_DIR="$build_dir" \
RUSTC_WRAPPER="" \
  cargo "$cargo_toolchain" test \
    --manifest-path "$drill_root/Cargo.toml" \
    -p prolly-s3-client --all-features --test rustfs_repository \
    rustfs_s3_shaped_operation_cost_matrix_advisory_rebuild -- \
    --exact --nocapture --test-threads=1 2>&1 | tee "$test_log"
test_status="${PIPESTATUS[0]}"
set -e
if [[ "$test_status" != "0" ]]; then
  if [[ -s "$proxy_error_log" ]]; then
    echo "SlateDB HTTP proxy diagnostics:" >&2
    sed -n '1,160p' "$proxy_error_log" >&2
  fi
  exit "$test_status"
fi

kill -TERM "$proxy_pid"
wait "$proxy_pid"
proxy_pid=""
python3 "$drill_root/scripts/verify_slatedb_http_correlation.py" \
  --test-log "$test_log" \
  --http-metrics "$http_metrics"

finished_at="$(date +%s)"
final_kib="$(du -sk "$data_dir" | awk '{print $1}')"
health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
echo "RUSTFS_SLATEDB_HTTP_CORRELATION_RUN_COMPLETE container=$container health=$health elapsed_seconds=$((finished_at - started_at)) initial_data_kib=$initial_kib final_data_kib=$final_kib data_growth_kib=$((final_kib - initial_kib))"
