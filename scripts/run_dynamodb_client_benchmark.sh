#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/dynamodb-client/Cargo.toml"
COMPOSE_FILE="$REPO_ROOT/docker-compose.store-services.yml"
PROJECT="${PROLLY_DYNAMODB_CLIENT_BENCH_PROJECT:-prolly-dynamodb-client-bench}"
PORT="${PROLLY_DYNAMODB_CLIENT_BENCH_PORT:-58001}"
ENDPOINT="${PROLLY_DYNAMODB_CLIENT_BENCH_ENDPOINT:-http://127.0.0.1:${PORT}}"
TABLE="${PROLLY_DYNAMODB_CLIENT_BENCH_TABLE:-prolly_versioned_client_benchmark}"
ROOT_TABLE="${PROLLY_DYNAMODB_CLIENT_BENCH_ROOT_TABLE:-}"
PROFILE="${BENCH_PROFILE:-smoke}"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/dynamodb-client-$(date +%F)}"
PRINT_CONFIG=false
TEARDOWN="${BENCH_TEARDOWN:-namespace}"
NODE_CACHE_MAX_BYTES="${BENCH_NODE_CACHE_MAX_BYTES:-67108864}"

while (($#)); do
  case "$1" in
    --profile) PROFILE="${2:?--profile requires a value}"; shift 2 ;;
    --output) OUTPUT="${2:?--output requires a value}"; shift 2 ;;
    --endpoint) ENDPOINT="${2:?--endpoint requires a value}"; shift 2 ;;
    --table) TABLE="${2:?--table requires a value}"; shift 2 ;;
    --root-table) ROOT_TABLE="${2:?--root-table requires a value}"; shift 2 ;;
    --print-config) PRINT_CONFIG=true; shift ;;
    *) printf 'unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done

if [[ -z "$ROOT_TABLE" ]]; then
  ROOT_TABLE="${TABLE}_roots"
fi

case "$PROFILE" in
  smoke)
    SAMPLES="${BENCH_SAMPLES:-5}"
    RECORDS="${BENCH_RECORDS:-100}"
    ;;
  repeatable)
    SAMPLES="${BENCH_SAMPLES:-100}"
    RECORDS="${BENCH_RECORDS:-10000}"
    ;;
  *)
    printf 'profile must be smoke or repeatable; use run_dynamodb_client_benchmark_matrix.sh for release qualification\n' >&2
    exit 2
    ;;
esac

WORKLOAD="${BENCH_WORKLOAD:-full}"
case "$WORKLOAD" in
  full|history) ;;
  *) printf 'BENCH_WORKLOAD must be full or history\n' >&2; exit 2 ;;
esac

case "$TEARDOWN" in
  namespace)
    CLEANUP_ARGS=()
    ;;
  docker-volume)
    if [[ "${PROLLY_BENCH_SKIP_DOCKER:-0}" == 1 ]]; then
      printf 'BENCH_TEARDOWN=docker-volume requires a runner-owned DynamoDB Local container\n' >&2
      exit 2
    fi
    if [[ "$PROJECT" != prolly-dynamodb-client-bench-ephemeral-* ]]; then
      printf 'BENCH_TEARDOWN=docker-volume requires an explicit ephemeral project name\n' >&2
      exit 2
    fi
    if [[ "$ENDPOINT" != "http://127.0.0.1:${PORT}" ]]; then
      printf 'BENCH_TEARDOWN=docker-volume requires the runner local endpoint\n' >&2
      exit 2
    fi
    CLEANUP_ARGS=(--skip-cleanup)
    ;;
  *)
    printf 'BENCH_TEARDOWN must be namespace or docker-volume\n' >&2
    exit 2
    ;;
esac

if [[ "$PRINT_CONFIG" == true ]]; then
  printf 'profile=%s workload=%s teardown=%s endpoint=%s table=%s root_table=%s node_cache_max_bytes=%s\n' \
    "$PROFILE" "$WORKLOAD" "$TEARDOWN" "$ENDPOINT" "$TABLE" "$ROOT_TABLE" "$NODE_CACHE_MAX_BYTES"
  exit 0
fi

if [[ "$TEARDOWN" == docker-volume ]] &&
  [[ -n "$(docker compose -p "$PROJECT" -f "$COMPOSE_FILE" ps -aq)" ]]; then
  printf 'refusing to reuse a pre-existing ephemeral Compose project: %s\n' "$PROJECT" >&2
  exit 2
fi

VALUE_BYTES="${BENCH_VALUE_BYTES:-1024}"
READ_BATCH_ITEMS="${BENCH_READ_BATCH_ITEMS:-}"
if [[ -z "$READ_BATCH_ITEMS" ]]; then
  READ_BATCH_ITEMS=$((4194304 / (VALUE_BYTES + 37)))
  if ((READ_BATCH_ITEMS > 100)); then READ_BATCH_ITEMS=100; fi
  if ((READ_BATCH_ITEMS < 10)); then READ_BATCH_ITEMS=10; fi
fi
HISTORY_DEPTH="${BENCH_HISTORY_DEPTH:-100}"
TRANSACTION_SHAPES="${BENCH_TRANSACTION_SHAPES:-1,10,100}"
CONCURRENCY_WRITERS="${BENCH_CONCURRENCY_WRITERS:-1,4,8}"
CONCURRENCY_OPERATIONS_PER_WRITER="${BENCH_CONCURRENCY_OPERATIONS_PER_WRITER:-5}"
CONCURRENCY_RETRY_LIMIT="${BENCH_CONCURRENCY_RETRY_LIMIT:-7}"
REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || true)" ]]; then
  DIRTY=true
  DIRTY_ARG=--dirty
else
  DIRTY=false
  DIRTY_ARG=--clean
fi

if [[ -e "$OUTPUT/raw-samples.csv" ]]; then
  printf 'refusing to overwrite existing raw evidence: %s\n' "$OUTPUT/raw-samples.csv" >&2
  exit 2
fi
mkdir -p "$OUTPUT"

record_exit() {
  bench_exit_code=$?
  if [[ "$TEARDOWN" == docker-volume ]]; then
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  elif [[ "${BENCH_CLEANUP:-0}" == 1 && "${PROLLY_BENCH_SKIP_DOCKER:-0}" != 1 ]]; then
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  fi
  if [[ "$bench_exit_code" == 0 ]]; then
    rm -f "$OUTPUT/failure.txt"
  else
    {
      printf 'status=%s\n' "$bench_exit_code"
      printf 'failed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "$OUTPUT/failure.txt"
  fi
}
trap record_exit EXIT

{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  rustc --version
  cargo --version
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
} > "$OUTPUT/machine.txt"

if [[ "${PROLLY_BENCH_SKIP_DOCKER:-0}" != 1 ]]; then
  PROLLY_STORE_DYNAMODB_PORT="$PORT" \
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d dynamodb
  for attempt in $(seq 1 60); do
    if curl --silent --output /dev/null "$ENDPOINT"; then
      break
    fi
    if [[ "$attempt" == 60 ]]; then
      docker compose -p "$PROJECT" -f "$COMPOSE_FILE" logs dynamodb >&2
      exit 1
    fi
    sleep 1
  done
  DYNAMODB_CONTAINER_ID="$(docker compose -p "$PROJECT" -f "$COMPOSE_FILE" ps -q dynamodb)"
  if [[ -z "$DYNAMODB_CONTAINER_ID" ]]; then
    printf 'DynamoDB Local container identity is unavailable\n' >&2
    exit 1
  fi
  docker inspect "$DYNAMODB_CONTAINER_ID" \
    --format 'container_image={{.Config.Image}} container_image_id={{.Image}}' \
    > "$OUTPUT/dynamodb-artifact.txt"
elif [[ -n "${PROLLY_DYNAMODB_LOCAL_ARCHIVE:-}" ]]; then
  shasum -a 256 "$PROLLY_DYNAMODB_LOCAL_ARCHIVE" > "$OUTPUT/dynamodb-artifact.txt"
else
  printf 'external_endpoint=%s artifact_digest=unavailable\n' "$ENDPOINT" \
    > "$OUTPUT/dynamodb-artifact.txt"
fi

{
  printf 'schema=versioned-dynamodb-client-samples-v2\n'
  printf 'runner_version=15\n'
  printf 'summarizer_version=1\n'
  printf 'revision=%s\n' "$REVISION"
  printf 'dirty=%s\n' "$DIRTY"
  printf 'profile=%s\n' "$PROFILE"
  printf 'environment=dynamodb-local\n'
  printf 'endpoint=%s\n' "$ENDPOINT"
  printf 'physical_table=%s\n' "$TABLE"
  printf 'physical_root_table=%s\n' "$ROOT_TABLE"
  printf 'samples=%s\n' "$SAMPLES"
  printf 'records=%s\n' "$RECORDS"
  printf 'value_bytes=%s\n' "$VALUE_BYTES"
  printf 'read_batch_items=%s\n' "$READ_BATCH_ITEMS"
  printf 'history_depth=%s\n' "$HISTORY_DEPTH"
  printf 'workload=%s\n' "$WORKLOAD"
  printf 'teardown=%s\n' "$TEARDOWN"
  printf 'transaction_shapes=%s\n' "$TRANSACTION_SHAPES"
  printf 'concurrency_writers=%s\n' "$CONCURRENCY_WRITERS"
  printf 'concurrency_operations_per_writer=%s\n' "$CONCURRENCY_OPERATIONS_PER_WRITER"
  printf 'concurrency_retry_limit=%s\n' "$CONCURRENCY_RETRY_LIMIT"
  printf 'node_cache_max_bytes=%s\n' "$NODE_CACHE_MAX_BYTES"
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$OUTPUT/run-manifest.txt"

cargo build --release --locked --manifest-path "$MANIFEST" 2>&1 | tee "$OUTPUT/build.log"
cargo tree --locked --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
EXECUTABLE="$REPO_ROOT/benchmarks/dynamodb-client/target/release/prolly-dynamodb-client-bench"
shasum -a 256 "$EXECUTABLE" > "$OUTPUT/binary.sha256"

if [[ "$(uname -s)" == Darwin ]]; then
  TIME_MODE=-l
else
  TIME_MODE=-v
fi
set +e
/usr/bin/time "$TIME_MODE" -o "$OUTPUT/process.time" "$EXECUTABLE" \
  --endpoint "$ENDPOINT" \
  --table "$TABLE" \
  --root-table "$ROOT_TABLE" \
  --output "$OUTPUT" \
  --samples "$SAMPLES" \
  --records "$RECORDS" \
  --value-bytes "$VALUE_BYTES" \
  --read-batch-items "$READ_BATCH_ITEMS" \
  --history-depth "$HISTORY_DEPTH" \
  --workload "$WORKLOAD" \
  --transaction-shapes "$TRANSACTION_SHAPES" \
  --concurrency-writers "$CONCURRENCY_WRITERS" \
  --concurrency-operations-per-writer "$CONCURRENCY_OPERATIONS_PER_WRITER" \
  --concurrency-retry-limit "$CONCURRENCY_RETRY_LIMIT" \
  --node-cache-max-bytes "$NODE_CACHE_MAX_BYTES" \
  --revision "$REVISION" \
  "$DIRTY_ARG" \
  ${CLEANUP_ARGS[@]+"${CLEANUP_ARGS[@]}"} 2>&1 | tee "$OUTPUT/run.log"
benchmark_pipeline_status=("${PIPESTATUS[@]}")
set -e
if ((benchmark_pipeline_status[0] != 0)); then
  printf 'benchmark executable failed with status %s\n' "${benchmark_pipeline_status[0]}" >&2
  exit "${benchmark_pipeline_status[0]}"
fi
if ((benchmark_pipeline_status[1] != 0)); then
  printf 'benchmark log capture failed with status %s\n' "${benchmark_pipeline_status[1]}" >&2
  exit "${benchmark_pipeline_status[1]}"
fi

PEAK_RSS_BYTES="$(python3 "$SCRIPT_DIR/prolly_process_metrics.py" "$OUTPUT/process.time")"
{
  printf 'peak_rss_bytes=%s\n' "$PEAK_RSS_BYTES"
  printf 'raw_time_output=process.time\n'
} > "$OUTPUT/process-resources.txt"
printf 'peak_rss_bytes=%s\n' "$PEAK_RSS_BYTES" >> "$OUTPUT/run-manifest.txt"

python3 "$SCRIPT_DIR/validate_dynamodb_client_benchmark.py" \
  --input "$OUTPUT/raw-samples.csv" \
  --manifest "$OUTPUT/run-manifest.txt" \
  --samples "$SAMPLES" \
  --records "$RECORDS" \
  --value-bytes "$VALUE_BYTES" \
  --read-batch-items "$READ_BATCH_ITEMS" \
  --history-depth "$HISTORY_DEPTH" \
  --workload "$WORKLOAD" \
  --revision "$REVISION" \
  --transaction-shapes "$TRANSACTION_SHAPES" \
  --concurrency-writers "$CONCURRENCY_WRITERS" \
  --concurrency-operations-per-writer "$CONCURRENCY_OPERATIONS_PER_WRITER" \
  --concurrency-retry-limit "$CONCURRENCY_RETRY_LIMIT" \
  --node-cache-max-bytes "$NODE_CACHE_MAX_BYTES"
python3 "$SCRIPT_DIR/summarize_dynamodb_client_benchmark.py" \
  --input "$OUTPUT/raw-samples.csv" --output-dir "$OUTPUT"
printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT/run-manifest.txt"

if [[ "$TEARDOWN" == docker-volume ]]; then
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v
  TEARDOWN=complete
elif [[ "${BENCH_CLEANUP:-0}" == 1 && "${PROLLY_BENCH_SKIP_DOCKER:-0}" != 1 ]]; then
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v
fi

printf 'Versioned DynamoDB client benchmark complete: %s\n' "$OUTPUT"
